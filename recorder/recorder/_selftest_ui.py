"""
Self-test for recorder.ui — the terminal presentation layer.

Pins the rendering contract that both faces depend on: prefix stripping, the
event→glyph map, colour gating (TTY / NO_COLOR), and the size/duration helpers.

Run: PYTHONPATH=core:recorder python3 -m recorder._selftest_ui
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from recorder import ui                                       # noqa: E402

_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    _checks += 1
    if not cond:
        print(f"✗ FAIL: {label}")
        raise SystemExit(1)
    print(f"✓ {label}")


def _render(color: bool, msg: str, *, level: int = logging.INFO,
            ev: str | None = None) -> str:
    rec = logging.LogRecord("r", level, __file__, 1, msg, (), None)
    if ev is not None:
        rec.ev = ev
    return ui.ConsoleFormatter(color=color).format(rec)


def test_helpers() -> None:
    print("\n── duration + size helpers ──")
    check(ui.human_duration(8) == "8s", "seconds only")
    check(ui.human_duration(252) == "4m12s", "minutes + seconds, zero-padded")
    check(ui.human_duration(6248) == "1h44m", "hours + minutes")
    check(ui.human_duration(-5) == "0s", "negative clamps to 0s")
    check(ui.human_size(1_234_567_890) == "1.2 GB", "GB carries one decimal")
    check(ui.human_size(840_000_000) == "840 MB", "MB is whole")
    check(ui.human_size(12_000) == "12 KB", "KB is whole")
    check(ui.human_size(500) == "500 B", "sub-KB is bytes")


def test_prefix_strip() -> None:
    print("\n── legacy scope prefixes are stripped ──")
    out = _render(False, "recorder: stopped")
    check(out.endswith("stopped") and "recorder:" not in out,
          "'recorder: ' prefix removed")
    out = _render(False, "capture: yt-dlp exited rc=0")
    check(out.endswith("yt-dlp exited rc=0"), "'capture: ' prefix removed")
    out = _render(False, "@alice queued (new)")
    check(out.endswith("@alice queued (new)"), "a clean message is untouched")


def test_event_and_level_glyphs() -> None:
    print("\n── event + level glyphs ──")
    check("●" in _render(False, "live", ev="live"), "live event → ● glyph")
    check("■" in _render(False, "end", ev="rec_end"), "rec_end event → ■ glyph")
    check("⚑" in _render(False, "ho", ev="handoff"), "handoff event → ⚑ glyph")
    # Unknown event falls back to the level glyph, never crashes.
    check("⚠" in _render(False, "w", level=logging.WARNING, ev="bogus"),
          "unknown event falls back to the WARNING glyph")
    check("✖" in _render(False, "e", level=logging.ERROR),
          "ERROR with no event → ✖ glyph")


def test_color_gating() -> None:
    print("\n── colour gating ──")
    plain = _render(False, "@alice is LIVE", ev="live")
    check("\033[" not in plain, "color=False emits NO ANSI escape codes")
    check("INFO" in plain and "live" not in plain.split()[0],
          "file face carries the level name")
    tinted = _render(True, "@alice is LIVE", ev="live")
    check("\033[" in tinted, "color=True emits ANSI escape codes")
    check("INFO" not in tinted, "TTY face drops the level name (compact)")

    # NO_COLOR forces colour off even on a (simulated) TTY.
    saved = os.environ.get("NO_COLOR")
    os.environ["NO_COLOR"] = "1"
    try:
        check(ui.color_enabled() is False, "NO_COLOR disables colour")
    finally:
        if saved is None:
            del os.environ["NO_COLOR"]
        else:
            os.environ["NO_COLOR"] = saved


def test_warning_tints_body_not_events() -> None:
    print("\n── only warnings/errors tint the whole line ──")
    warn = _render(True, "dead stream", level=logging.WARNING)
    # body is wrapped in a colour run (escape appears twice: glyph + body).
    check(warn.count("\033[33m") >= 2, "WARNING tints both glyph and body")
    live = _render(True, "@alice is LIVE", ev="live")
    # green glyph, but the body text itself carries no colour run.
    check(live.count("\033[32m") == 1, "a lifecycle event tints only its glyph")


def main() -> int:
    print("recorder.ui self-test")
    test_helpers()
    test_prefix_strip()
    test_event_and_level_glyphs()
    test_color_gating()
    test_warning_tints_body_not_events()
    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
