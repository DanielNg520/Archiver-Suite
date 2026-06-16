"""
Self-test for core.termui — the shared worker presentation engine.

Covers the engine surface every worker depends on: the two formatter faces,
the event/level glyph map, colour gating, setup_logging's console+file tee and
library-quieting, and the banner/field/helpers.

Run: PYTHONPATH=core python3 -m core._selftest_termui
"""

from __future__ import annotations

import io
import logging
import os
import sys
import tempfile
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import termui                                        # noqa: E402

_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    _checks += 1
    if not cond:
        print(f"✗ FAIL: {label}")
        raise SystemExit(1)
    print(f"✓ {label}")


def _render(color: bool, msg: str, *, level=logging.INFO, ev=None) -> str:
    rec = logging.LogRecord("w", level, __file__, 1, msg, (), None)
    if ev is not None:
        rec.ev = ev
    return termui.ConsoleFormatter(color=color).format(rec)


def test_faces_and_glyphs() -> None:
    print("\n── formatter faces + glyph vocabulary ──")
    tty = _render(True, "@u uploading clip.mp4", ev="upload")
    check("\033[" in tty and "▲" in tty, "TTY face: colour + event glyph")
    check("INFO" not in tty, "TTY face drops the level name")

    fileface = _render(False, "drain: @u sent", ev="sent")
    check("\033[" not in fileface, "file face: no ANSI")
    check("INFO" in fileface and "✓" in fileface and "drain:" not in fileface,
          "file face: level name, glyph, scope prefix stripped")

    check("◆" in _render(False, "x", ev="discover"), "archiver event glyph")
    check("●" in _render(False, "x", ev="live"), "recorder event glyph")
    check("⚠" in _render(False, "x", level=logging.WARNING, ev="nope"),
          "unknown event → WARNING level glyph")


def test_color_gating() -> None:
    print("\n── colour gating respects NO_COLOR ──")
    saved = os.environ.get("NO_COLOR")
    os.environ["NO_COLOR"] = "1"
    try:
        check(termui.color_enabled() is False, "NO_COLOR forces colour off")
        check(termui.paint("x", "red") == "x", "paint() is a no-op when off")
    finally:
        os.environ.pop("NO_COLOR", None) if saved is None else None


def test_setup_logging_tee_and_quiet(tmp: Path) -> None:
    print("\n── setup_logging: console+file tee, libs quieted ──")
    log_file = tmp / "logs" / "w.log"
    buf = io.StringIO()
    termui.setup_logging(False, stream=buf, log_file=str(log_file),
                         quiet=("noisylib",))
    logging.getLogger("worker").info("hello", extra={"ev": "ok"})
    logging.getLogger("noisylib").info("should be hidden")
    logging.getLogger("noisylib").warning("should appear")
    for h in logging.getLogger().handlers:
        h.flush()

    console = buf.getvalue()
    check("hello" in console, "console handler received the record")
    text = log_file.read_text()
    check("hello" in text and "INFO" in text, "file tee wrote the record (plain)")
    check("\033[" not in text, "file tee is never colourised")
    check("should be hidden" not in text and "should appear" in text,
          "quieted lib: INFO suppressed, WARNING kept")
    # Reset root handlers so we don't leak into other tests.
    logging.getLogger().handlers.clear()


def test_helpers_and_panels() -> None:
    print("\n── helpers + banner/field ──")
    check(termui.human_duration(6248) == "1h44m", "human_duration")
    check(termui.human_size(1_234_567_890) == "1.2 GB", "human_size")
    check(termui.age(None) == "never", "age(None)")
    check(termui.short_time("2026-06-16T09:07:00Z") == "09:07:00", "short_time")

    out = io.StringIO()
    with redirect_stdout(out):
        termui.banner("worker", [("a", "1"), ("longlabel", "2")],
                      subtitle="sub")
        termui.field("status", "running", accent="green")
    s = out.getvalue()
    check("worker" in s and "sub" in s, "banner shows title + subtitle")
    check("a" in s and "longlabel" in s and "running" in s,
          "banner fields + status field rendered")
    check(termui.RULE in s, "banner draws the rule")


def main() -> int:
    print("core.termui self-test")
    test_faces_and_glyphs()
    test_color_gating()
    with tempfile.TemporaryDirectory() as d:
        test_setup_logging_tee_and_quiet(Path(d))
    test_helpers_and_panels()
    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
