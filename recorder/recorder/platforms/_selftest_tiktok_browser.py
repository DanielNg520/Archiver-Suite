"""
Validation harness (not shipped) for recorder.platforms.tiktok_browser.

Covers the pure helpers (cookie parsing, pull-URL detection, HLS-over-FLV
preference) AND the full async resolve flow against a FAKE Playwright — so
the capture/prompt/timeout/early-return behavior is exercised without a real
browser or network. Run:

    python recorder/recorder/platforms/_selftest_tiktok_browser.py
"""
from __future__ import annotations

import asyncio
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from recorder.platforms import tiktok_browser as tb   # noqa: E402

OK = "✓"
_passed = 0


def check(cond: bool, msg: str) -> None:
    global _passed
    if not cond:
        raise AssertionError(msg)
    _passed += 1
    print(f"  {OK} {msg}")


def expect_raises(exc, fn, msg: str) -> None:
    try:
        fn()
    except exc:
        global _passed
        _passed += 1
        print(f"  {OK} {msg}")
        return
    raise AssertionError(f"expected {exc.__name__}: {msg}")


# ── pure helper: _is_pull_url ─────────────────────────────────────────────

def test_is_pull_url():
    print("_is_pull_url:")
    pos = [
        "https://pull-o5-sg01.tiktokcdn-us.com/stage/stream-184.flv?_session_id=x",
        "https://pull-hls-f16-sg01.tiktokcdn.com/stage/stream-156_hd/index.m3u8?sign=a",
        "https://x.tiktokcdn.com/game/stream-9.m3u8",
    ]
    neg = [
        "https://www.tiktok.com/@user/live",            # no media ext
        "https://sf16.tiktokcdn.com/avatar/thumb.jpeg", # image, no hint
        "https://pull-x.tiktokcdn.com/banner.png",      # hint but not media
        "https://example.com/video.m3u8",               # media but no pull hint
        "",                                             # empty
    ]
    for u in pos:
        check(tb._is_pull_url(u), f"accepts pull URL: …{u[-32:]}")
    for u in neg:
        check(not tb._is_pull_url(u), f"rejects non-pull: {u[:40]!r}")


# ── pure helper: _pick_best ───────────────────────────────────────────────

def test_pick_best():
    print("_pick_best:")
    flv = "https://pull-a.tiktokcdn.com/stream-1.flv?s=1"
    flv2 = "https://pull-b.tiktokcdn.com/stream-2.flv?s=2"
    hls = "https://pull-c.tiktokcdn.com/stream-1.m3u8?s=3"
    check(tb._pick_best([flv, hls]) == hls, "prefers HLS even when FLV seen first")
    check(tb._pick_best([hls, flv]) == hls, "keeps HLS when HLS seen first")
    check(tb._pick_best([flv, flv2]) == flv, "FLV-only: first-seen wins")
    check(tb._pick_best([hls]) == hls, "single HLS returned")


# ── pure helper: _netscape_to_playwright ──────────────────────────────────

def test_cookie_parse():
    print("_netscape_to_playwright:")
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "tiktok.txt"
        p.write_text(
            "# Netscape HTTP Cookie File\n"
            "\n"
            "# a normal comment\n"
            ".tiktok.com\tTRUE\t/\tTRUE\t1999999999\tsessionid\tABC123\n"
            "#HttpOnly_.tiktok.com\tTRUE\t/\tTRUE\t1999999999\tsid_tt\tDEF456\n"
            "tiktok.com\tFALSE\t/path\tFALSE\t0\tplain\tval\n"
            "broken line without enough fields\n"
            "\t\t\t\t\t\t\n"
        )
        cookies = tb._netscape_to_playwright(str(p))
        by = {c["name"]: c for c in cookies}

        check("sessionid" in by, "parses normal sessionid cookie")
        check(by["sessionid"]["value"] == "ABC123", "sessionid value correct")
        check(by["sessionid"]["secure"] is True, "secure flag TRUE parsed")
        check("sid_tt" in by, "parses #HttpOnly_ cookie (not dropped as comment)")
        check(by["sid_tt"]["httpOnly"] is True, "HttpOnly flag set on sid_tt")
        check(by["sid_tt"]["value"] == "DEF456", "sid_tt value correct")
        check(by["plain"]["domain"] == ".tiktok.com",
              "bare domain normalized to leading-dot")
        check(by["plain"]["secure"] is False, "secure FALSE parsed")
        check(by["plain"]["path"] == "/path", "custom path preserved")
        # malformed/empty rows produced nothing extra
        check(set(by) == {"sessionid", "sid_tt", "plain"},
              "malformed/blank/comment lines skipped")

    # missing file → empty list, no raise
    check(tb._netscape_to_playwright("/no/such/file.txt") == [],
          "missing cookie file → [] (no raise)")


# ── fake Playwright harness ───────────────────────────────────────────────

class _FakeLocator:
    def __init__(self, count): self._count = count
    async def count(self): return self._count
    @property
    def first(self): return self
    async def click(self, timeout=None): self.clicked = True


class _FakePage:
    """Records handlers, lets the test fire fake network events, and tracks
    goto/prompt interactions."""
    def __init__(self, emit_urls, prompt_label=None, goto_exc=None):
        self._emit = emit_urls
        self._prompt_label = prompt_label
        self._goto_exc = goto_exc
        self._handlers = {"request": [], "response": []}
        self.goto_url = None
        self.dismissed = None

    def on(self, event, fn): self._handlers[event].append(fn)

    async def goto(self, url, wait_until=None, timeout=None):
        self.goto_url = url
        if self._goto_exc:
            raise self._goto_exc
        # Fire the queued network URLs as 'request' events.
        for u in self._emit:
            for fn in self._handlers["request"]:
                fn(type("R", (), {"url": u}))

    def get_by_text(self, label, exact=False):
        if label == self._prompt_label:
            self.dismissed = label
            return _FakeLocator(1)
        return _FakeLocator(0)

    async def wait_for_timeout(self, ms): pass


class _FakeContext:
    def __init__(self, page): self._page = page; self.added = None
    async def add_cookies(self, cookies): self.added = cookies
    async def new_page(self): return self._page


class _FakeBrowser:
    def __init__(self, page):
        self._page = page
        self.closed = False
        self.last_context = None
    async def new_context(self, **kw):
        self.last_context = _FakeContext(self._page)
        return self.last_context
    async def close(self): self.closed = True


class _FakeChromium:
    def __init__(self, browser): self._browser = browser; self.launched = False
    async def launch(self, headless=True):
        self.launched = True
        return self._browser


class _FakePW:
    def __init__(self, chromium): self.chromium = chromium
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False


def _install_fake_pw(monkey_page):
    """Patch tiktok_browser's lazy `from playwright.async_api import
    async_playwright` by injecting a fake module into sys.modules."""
    import types
    browser = _FakeBrowser(monkey_page)
    chromium = _FakeChromium(browser)

    def async_playwright():
        return _FakePW(chromium)

    mod = types.ModuleType("playwright.async_api")
    mod.async_playwright = async_playwright
    pkg = types.ModuleType("playwright")
    pkg.async_api = mod
    sys.modules["playwright"] = pkg
    sys.modules["playwright.async_api"] = mod
    return browser


def _cookie_file(tmp: Path) -> str:
    p = tmp / "c.txt"
    p.write_text(".tiktok.com\tTRUE\t/\tTRUE\t1999999999\tsessionid\tABC\n")
    return str(p)


def test_resolve_flow():
    print("_resolve_async (fake browser):")
    with tempfile.TemporaryDirectory() as d:
        cf = _cookie_file(Path(d))

        # 1) FLV-only stream, with an age prompt to dismiss
        flv = "https://pull-o5.tiktokcdn-us.com/stage/stream-1.flv?_session_id=x"
        page = _FakePage([flv], prompt_label="Continue")
        browser = _install_fake_pw(page)
        url = asyncio.run(tb._resolve_async("ingyongcuong", cf, timeout_s=3))
        check(url == flv, "captures FLV pull URL from network events")
        check(page.goto_url.endswith("/@ingyongcuong/live"), "navigated to live page")
        check(page.dismissed == "Continue", "dismissed the age prompt")
        injected = {c["name"] for c in browser.last_context.added}
        check("sessionid" in injected, "session cookies injected into context")
        check(browser.closed, "browser closed after resolve")

        # 2) Both FLV and HLS present → HLS wins (preference + grace)
        hls = "https://pull-c.tiktokcdn.com/stage/stream-1.m3u8?sign=a"
        page2 = _FakePage([flv, hls])
        _install_fake_pw(page2)
        url2 = asyncio.run(tb._resolve_async("u", cf, timeout_s=3))
        check(url2 == hls, "prefers HLS when both FLV and HLS captured")

        # 3) No pull URLs emitted → RuntimeError after timeout
        page3 = _FakePage(["https://www.tiktok.com/@u/live"])
        _install_fake_pw(page3)
        expect_raises(
            RuntimeError,
            lambda: asyncio.run(tb._resolve_async("u", cf, timeout_s=1)),
            "no pull URL captured → RuntimeError",
        )

        # 4) prompt absent → still resolves (dismissal is best-effort)
        page4 = _FakePage([flv], prompt_label=None)
        _install_fake_pw(page4)
        url4 = asyncio.run(tb._resolve_async("u", cf, timeout_s=3))
        check(url4 == flv and page4.dismissed is None,
              "resolves even when no age prompt is present")


def test_resolve_no_cookies():
    print("_resolve_async guards:")
    with tempfile.TemporaryDirectory() as d:
        empty = Path(d) / "empty.txt"
        empty.write_text("# only a comment\n")
        page = _FakePage([])
        _install_fake_pw(page)
        expect_raises(
            RuntimeError,
            lambda: asyncio.run(tb._resolve_async("u", str(empty), timeout_s=1)),
            "empty cookie file → RuntimeError (no session)",
        )
        expect_raises(
            RuntimeError,
            lambda: asyncio.run(tb._resolve_async("u", None, timeout_s=1)),
            "None cookie file → RuntimeError",
        )


def test_resolve_pw_missing():
    print("_resolve_async without playwright installed:")
    with tempfile.TemporaryDirectory() as d:
        cf = _cookie_file(Path(d))
        # Remove playwright from sys.modules AND block re-import.
        import builtins
        saved = {k: sys.modules.pop(k) for k in list(sys.modules)
                 if k == "playwright" or k.startswith("playwright.")}
        real_import = builtins.__import__

        def blocked(name, *a, **k):
            if name == "playwright" or name.startswith("playwright."):
                raise ImportError("blocked for test")
            return real_import(name, *a, **k)

        builtins.__import__ = blocked
        try:
            expect_raises(
                RuntimeError,
                lambda: asyncio.run(tb._resolve_async("u", cf, timeout_s=1)),
                "missing playwright → RuntimeError with install hint",
            )
        finally:
            builtins.__import__ = real_import
            sys.modules.update(saved)


def main():
    print("tiktok_browser selftest\n" + "─" * 40)
    test_is_pull_url()
    test_pick_best()
    test_cookie_parse()
    test_resolve_flow()
    test_resolve_no_cookies()
    test_resolve_pw_missing()
    print("─" * 40)
    print(f"ALL PASSED — {_passed} assertions")


if __name__ == "__main__":
    main()
