"""
recorder.platforms.tiktok
──────────────────────────
TikTok live detection + HLS URL resolution.

IMPLEMENTATION CHOICE (diverges from the guide — see lesson below):
  The guide sketches manual SIGI_STATE scraping + the api-live/detail
  endpoint. As of 2026 that path is brittle: TikTok moved the room blob
  between SIGI_STATE and __UNIVERSAL_DATA_FOR_REHYDRATION__, and the
  detail endpoint shape shifts. The maintained `TikTokLive` library
  (v6.x) absorbs those changes behind a stable `is_live()` / `room_id`
  API. We depend on it rather than re-deriving the scraping each time
  TikTok ships a change. This is the same reasoning as anti-pattern #7
  in the guide ("don't reinvent yt-dlp") applied to live detection.

THREADING vs ASYNC boundary:
  The recorder is threaded by mandate (guide line 44). TikTokLive is
  async. We bridge by running each check in a throwaway event loop via
  asyncio.run() inside the synchronous is_live(). There is no persistent
  loop and no shared state between an async loop and our threads — the
  async lifetime is exactly one function call. This respects anti-pattern
  #2 (don't share state across an asyncio loop and a thread).

ROOM_ID CACHE:
  TikTokLive resolves room_id internally per call. We additionally cache
  the HLS URL resolution path lightly: room_id for a live user is stable
  within a session, so once is_live() has populated client.room_id we can
  reuse it for stream_url() in the same recording attempt.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

# A liveness check runs inside the recorder's poll loop. It MUST fail fast,
# never hang: a stalled TikTok request with no timeout freezes the whole loop
# (the "recorder stops after running through the list" symptom — it's actually
# blocked on one user's network call, not stopped). Bound every network await
# so a hung check just reads as "not live" and polling continues.
_CHECK_TIMEOUT_S = 30.0
_CLOSE_TIMEOUT_S = 10.0


class TikTokLivePlatform:
    """Satisfies the LivePlatform Protocol structurally (no inheritance)."""

    name = "tiktok"

    def __init__(self, cookies_file: str | None, state_dir: str):
        self._cookies_file = cookies_file
        self._room_cache_path = Path(state_dir).expanduser() / "room_id_cache.json"
        self._room_cache: dict[str, str] = self._load_cache()

    # ── cache ─────────────────────────────────────────────────────────────

    def _load_cache(self) -> dict[str, str]:
        try:
            return json.loads(self._room_cache_path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}

    def _save_cache(self) -> None:
        try:
            self._room_cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._room_cache_path.write_text(json.dumps(self._room_cache))
        except OSError as e:
            log.warning("room_id cache write failed: %s", e)

    # ── Protocol surface ──────────────────────────────────────────────────

    def is_live(self, username: str) -> bool:
        """Synchronous liveness check. Never raises — returns False on any
        error so a poll loop can't be killed by a transient network hiccup
        or a TikTok-side change."""
        uid = username.lstrip("@")
        try:
            return asyncio.run(self._is_live_async(uid))
        except Exception as e:
            log.debug("is_live(%s) failed (treated as not-live): %s", uid, e)
            return False

    def stream_url(self, username: str) -> str:
        """Resolve the current HLS pull URL. Called only after is_live()
        returned True. May raise; caller handles a raise as 'start failed'."""
        uid = username.lstrip("@")
        return asyncio.run(self._stream_url_async(uid))

    # ── async internals ───────────────────────────────────────────────────

    async def _make_client(self, uid: str):
        # Imported lazily so importing this module (e.g. for `recorder
        # config`) doesn't require TikTokLive to be installed.
        from TikTokLive import TikTokLiveClient

        web_kwargs: dict = {}
        cookies: dict[str, str] = {}
        if self._cookies_file:
            # TikTokLive forwards web_kwargs to its httpx client. A cookies
            # file improves detection reliability for age-gated / region-
            # restricted lives.
            cookies = _parse_netscape_cookies(self._cookies_file)
            if cookies:
                web_kwargs["httpx_kwargs"] = {"cookies": cookies}
        client = TikTokLiveClient(unique_id=f"@{uid}", web_kwargs=web_kwargs)
        session_id = cookies.get("sessionid")
        if session_id:
            client.web.set_session(
                session_id=session_id,
                tt_target_idc=cookies.get("tt-target-idc"),
            )
        return client

    async def _is_live_async(self, uid: str) -> bool:
        client = await self._make_client(uid)
        try:
            # Bounded: a hung check times out (→ caught by is_live → "not live")
            # instead of stalling the poll loop forever.
            live = await asyncio.wait_for(client.is_live(), _CHECK_TIMEOUT_S)
            if live:
                rid = getattr(client, "room_id", None)
                if rid:
                    self._room_cache[uid] = str(rid)
                    self._save_cache()
            return bool(live)
        finally:
            await self._safe_close(client)

    async def _stream_url_async(self, uid: str) -> str:
        # Import lazily so this module imports without TikTokLive installed.
        from TikTokLive.client.errors import AgeRestrictedError

        client = await self._make_client(uid)
        try:
            # fetch_room_info() resolves the room from EITHER a room_id or a
            # unique_id. Called with no args it falls back to a stored
            # _web.params["room_id"], which is unset on this fresh client (that
            # state is only populated by is_live(), which ran on a different
            # client instance) — hence the KeyError('room_id') in the logs.
            # Query by unique_id instead: it hits the info_by_user/ endpoint,
            # needs no room_id, and returns the same payload shape.
            info = await asyncio.wait_for(
                client.web.fetch_room_info(unique_id=uid), _CHECK_TIMEOUT_S)
            url = _extract_hls_url(info)
            if not url:
                raise RuntimeError(
                    f"could not extract HLS URL for @{uid} from room info; "
                    f"TikTok response shape may have changed"
                )
            return url
        except AgeRestrictedError:
            # 18+ stream: the unsigned webcast API won't return room info
            # (status_code 4003110). Fall back to driving a logged-in headless
            # browser, which lets TikTok's own JS sign the pull-URL request —
            # exactly what playback in a real browser does. See tiktok_browser.
            log.info(
                "tiktok: @%s is age-restricted — webcast API blocked, "
                "trying headless-browser fallback", uid)
            return await self._stream_url_via_browser(uid)
        finally:
            await self._safe_close(client)

    async def _stream_url_via_browser(self, uid: str) -> str:
        from .tiktok_browser import resolve_stream_url_via_browser
        # The resolver runs its own Playwright loop; we're already on an event
        # loop here, so run it in a thread to avoid nesting asyncio.run().
        return await asyncio.to_thread(
            resolve_stream_url_via_browser, uid, self._cookies_file)

    @staticmethod
    async def _safe_close(client) -> None:
        """Close the client's HTTP sessions (httpx + curl_cffi).

        is_live()/stream_url() build a fresh client on EVERY poll. Without
        this close the sessions leak file descriptors and connections; after
        enough polls (accelerated by the per-recording HANDOFF re-scan)
        is_live() starts failing, is caught as 'not live', and the recorder
        appears to stop listening. Bounded + never raises — a slow or failing
        close must not hang or kill the poll loop."""
        try:
            await asyncio.wait_for(client.close(), _CLOSE_TIMEOUT_S)
        except Exception as e:  # noqa: BLE001 — best-effort cleanup
            log.debug("tiktok: client close failed/timed out (ignored): %s", e)


# ── helpers ─────────────────────────────────────────────────────────────────

def _extract_hls_url(room_info: dict) -> str | None:
    """Pull the best HLS URL out of TikTok room info. Tries flv/hls map
    variants in priority order. Returns None if nothing matches."""
    try:
        stream = room_info.get("stream_url", {})
        # Prefer explicit HLS pull URL.
        hls = stream.get("hls_pull_url")
        if hls:
            return hls
        # Multi-quality HLS map.
        hls_map = stream.get("hls_pull_url_map") or {}
        if isinstance(hls_map, dict) and hls_map:
            # Pick the highest-listed quality deterministically.
            return next(iter(hls_map.values()))
        # Fallback: FLV (yt-dlp can still ingest it).
        flv = stream.get("flv_pull_url")
        if isinstance(flv, dict) and flv:
            return next(iter(flv.values()))
        if isinstance(flv, str) and flv:
            return flv
    except AttributeError:
        return None
    return None


def _parse_netscape_cookies(path: str) -> dict[str, str]:
    """Minimal Netscape cookies.txt → {name: value}. Best-effort; bad
    lines are skipped. We only need the values httpx will send as a cookie
    header, not full attributes."""
    out: dict[str, str] = {}
    try:
        for line in Path(path).expanduser().read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 7:
                out[parts[5]] = parts[6]
    except OSError:
        pass
    return out
