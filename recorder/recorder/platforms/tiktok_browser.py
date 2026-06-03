"""
recorder.platforms.tiktok_browser
──────────────────────────────────
Headless-browser fallback for resolving the HLS/FLV pull URL of an
AGE-RESTRICTED TikTok live.

WHY THIS EXISTS:
  The normal path (recorder.platforms.tiktok) asks TikTok's webcast API
  `room/info` for the stream URL. For 18+ streams TikTok refuses that
  request unless it carries a browser-generated X-Bogus/a-bogus signature
  plus a live msToken — TikTokLive's free signer won't sign room/info, and
  yt-dlp hits the same wall. The API returns `{"prompts": ""}` with
  status_code 4003110 → TikTokLive raises AgeRestrictedError.

  A logged-in browser succeeds because TikTok's own `webmssdk` JS runs in
  the page and signs the XHR that fetches the pull URL. So we do exactly
  what the browser does: load `@user/live` with the session cookies in a
  real (headless) Chromium, let TikTok's JS make its signed request, and
  sniff the resulting pull URL off the network. That URL is then handed to
  yt-dlp like any other.

COST / SCOPE:
  This is a FALLBACK ONLY — it is slow (spawns Chromium, ~5–15s) and is
  invoked solely when the API path age-gates. Non-restricted streams never
  touch this module. Requires `playwright` + a Chromium build
  (`python -m playwright install chromium`).

ISOLATION:
  Like the rest of recorder.platforms.tiktok, the async lifetime is exactly
  one call inside a throwaway event loop — no state shared with recorder
  threads. The browser is always closed in a finally.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

log = logging.getLogger(__name__)

# A pull URL must look like a real live stream edge, not an unrelated asset.
# TikTok live edges live on pull-*.tiktokcdn*.com and the path carries a
# stream-<id> segment. We accept both HLS (.m3u8) and FLV (.flv) — yt-dlp's
# ffmpeg downloader ingests either; HLS is preferred when both appear.
_PULL_HINTS = ("pull-", "/stream-", "/game/", "pull_")


def _is_pull_url(url: str) -> bool:
    """True if `url` looks like a live-stream pull edge (HLS or FLV) rather
    than an unrelated page asset. Module-level so it is unit-testable."""
    return (".m3u8" in url or ".flv" in url) and any(h in url for h in _PULL_HINTS)


def resolve_stream_url_via_browser(
    uid: str,
    cookies_file: str | None,
    timeout_s: float = 45.0,
) -> str:
    """Synchronous entry point: return a pull URL for @uid's live, or raise.

    Runs the async sniffer in a throwaway loop so the threaded recorder can
    call it like any other blocking resolve. Raises RuntimeError if nothing
    is captured before timeout (caller treats a raise as 'start failed')."""
    return asyncio.run(_resolve_async(uid.lstrip("@"), cookies_file, timeout_s))


async def _resolve_async(uid: str, cookies_file: str | None, timeout_s: float) -> str:
    try:
        from playwright.async_api import async_playwright
    except ImportError as e:  # pragma: no cover - depends on install extras
        raise RuntimeError(
            "age-restricted live needs the browser fallback, but playwright "
            "is not installed. Run: python -m playwright install chromium"
        ) from e

    cookies = _netscape_to_playwright(cookies_file) if cookies_file else []
    if not cookies:
        raise RuntimeError(
            "browser fallback needs the TikTok session cookies; none loaded "
            f"from {cookies_file!r}"
        )

    # Collected candidate pull URLs (insertion order preserved). An Event lets
    # us return the instant a usable URL appears instead of always waiting the
    # full timeout.
    found: list[str] = []
    got = asyncio.Event()

    def _consider(url: str) -> None:
        if _is_pull_url(url) and url not in found:
            found.append(url)
            # Prefer HLS: only short-circuit immediately on an .m3u8; for an
            # .flv give HLS a brief chance to also show up (handled by the
            # wait loop's grace below).
            got.set()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/129.0.0.0 Safari/537.36"
                )
            )
            await ctx.add_cookies(cookies)
            page = await ctx.new_page()
            page.on("request", lambda r: _consider(r.url))
            page.on("response", lambda r: _consider(r.url))

            await page.goto(
                f"https://www.tiktok.com/@{uid}/live",
                wait_until="domcontentloaded",
                timeout=min(45000, int(timeout_s * 1000)),
            )
            await _dismiss_age_prompt(page)

            # Wait for the first pull URL, then a short grace so a preferred
            # HLS URL can overtake an FLV one that arrived first.
            try:
                await asyncio.wait_for(got.wait(), timeout_s)
            except asyncio.TimeoutError:
                pass
            if found:
                await asyncio.sleep(2.0)  # grace for an HLS to also land
        finally:
            await browser.close()

    if not found:
        raise RuntimeError(
            f"browser fallback could not capture a pull URL for @{uid} "
            f"within {timeout_s:.0f}s (stream ended, or TikTok layout changed)"
        )
    return _pick_best(found)


async def _dismiss_age_prompt(page) -> None:
    """Best-effort: click through TikTok's 'sensitive/age-restricted content'
    interstitial so its JS fires the signed pull-URL request. Never raises —
    if no prompt is present, or the labels shifted, we just proceed and rely
    on the network sniff."""
    for label in ("Continue", "Watch now", "Watch", "I'm over 18",
                  "View", "Skip", "Got it"):
        try:
            el = page.get_by_text(label, exact=False)
            if await el.count():
                await el.first.click(timeout=2000)
                log.debug("tiktok-browser: dismissed prompt via %r", label)
                return
        except Exception:  # noqa: BLE001 - interstitial is optional
            continue


def _pick_best(urls: list[str]) -> str:
    """Prefer HLS (.m3u8) over FLV — HLS survives reconnects better in
    yt-dlp's ffmpeg downloader. Within a kind, first-seen wins."""
    for u in urls:
        if ".m3u8" in u:
            return u
    return urls[0]


def _netscape_to_playwright(path: str) -> list[dict]:
    """Netscape cookies.txt → Playwright add_cookies() dicts. Handles the
    `#HttpOnly_` line prefix (which the bare `#` comment skip would otherwise
    drop, losing sid_tt/sessionid). Best-effort; malformed lines skipped."""
    out: list[dict] = []
    try:
        lines = Path(path).expanduser().read_text().splitlines()
    except OSError:
        return out
    for line in lines:
        s = line.strip()
        http_only = False
        if s.startswith("#HttpOnly_"):
            s = s[len("#HttpOnly_"):]
            http_only = True
        elif not s or s.startswith("#"):
            continue
        parts = s.split("\t")
        if len(parts) < 7:
            continue
        domain, _flag, cpath, secure, _expiry, name, value = parts[:7]
        out.append({
            "name": name,
            "value": value,
            # Playwright wants a leading-dot domain for host-spanning cookies.
            "domain": domain if domain.startswith(".") else "." + domain.lstrip("."),
            "path": cpath or "/",
            "secure": secure.upper() == "TRUE",
            "httpOnly": http_only,
        })
    return out
