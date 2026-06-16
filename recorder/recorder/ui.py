"""
recorder.ui
───────────
Presentation layer for the recorder's terminal output.

The recorder is a long-running foreground process whose log IS its interface,
so the messages double as both a live narrative (on a TTY) and an archival log
(under launchd, where stdout is redirected to a file). This module renders one
clean stream that serves both:

  • On a TTY  → a compact, colorized event feed: `HH:MM:SS  ●  @user is LIVE`.
                A small glyph vocabulary gives each lifecycle moment its own
                visual rhythm so the eye can scan a day of activity at a glance.
  • In a file → a plain, fully-timestamped, level-tagged line (no escape codes),
                so `grep`/log-rotation keep working unchanged.

Both come from ONE logging.Formatter, so every existing `log.info(...)` call is
styled for free; lifecycle calls opt into a richer glyph by passing
`extra={"ev": "<name>"}`. Colour is suppressed automatically when stdout is not
a TTY or when NO_COLOR is set.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import RecorderConfig

# ── colour ──────────────────────────────────────────────────────────────────

_RESET = "\033[0m"
_COLORS = {
    "dim":    "\033[2m",
    "bold":   "\033[1m",
    "red":    "\033[31m",
    "green":  "\033[32m",
    "yellow": "\033[33m",
    "blue":   "\033[34m",
    "cyan":   "\033[36m",
}


def color_enabled() -> bool:
    """True when it's safe to emit ANSI colour: a real TTY and NO_COLOR unset."""
    if os.environ.get("NO_COLOR") is not None:
        return False
    try:
        return sys.stdout.isatty()
    except (AttributeError, ValueError):
        return False


def _paint(text: str, *styles: str, on: bool) -> str:
    if not on or not styles:
        return text
    codes = "".join(_COLORS[s] for s in styles if s in _COLORS)
    return f"{codes}{text}{_RESET}" if codes else text


# ── event vocabulary ────────────────────────────────────────────────────────
#
# Each lifecycle event maps to (glyph, colour). A log call selects one with
# `extra={"ev": "live"}`; everything else falls back to a level-based glyph so
# stray warnings/errors still render consistently.
_EVENTS: dict[str, tuple[str, str]] = {
    "start":   ("▸", "bold"),    # session came up / now listening
    "listen":  ("·", "dim"),     # idle — nobody live
    "live":    ("●", "green"),   # a stream went live, recording begins
    "rec_end": ("■", "cyan"),    # a recording finished
    "remux":   ("⤳", "dim"),     # container fixed for Telegram
    "queued":  ("↑", "blue"),    # handed to the upload queue
    "handoff": ("⚑", "yellow"),  # rescanning the priority list
    "sweep":   ("✦", "dim"),     # startup reconciliation
    "stop":    ("○", "dim"),     # shutting down
}
_LEVEL_GLYPH: dict[int, tuple[str, str]] = {
    logging.DEBUG:    ("·", "dim"),
    logging.INFO:     ("·", "dim"),
    logging.WARNING:  ("⚠", "yellow"),
    logging.ERROR:    ("✖", "red"),
    logging.CRITICAL: ("✖", "red"),
}

# Redundant scope prefixes baked into legacy messages; stripped so the feed
# reads as prose instead of "recorder: capture: …".
_STRIP_PREFIXES = (
    "recorder: ", "capture: ", "remux: ", "record-once: ",
    "startup-sweep: ", "enqueue: ", "tiktok lock ", "cli: ",
)


def _clean(msg: str) -> str:
    for p in _STRIP_PREFIXES:
        if msg.startswith(p):
            return msg[len(p):]
    return msg


class ConsoleFormatter(logging.Formatter):
    """One formatter, two faces — colorized glyph feed on a TTY, plain
    timestamped log line in a file."""

    def __init__(self, *, color: bool):
        super().__init__()
        self.color = color

    def format(self, record: logging.LogRecord) -> str:
        msg = _clean(record.getMessage())
        ev = getattr(record, "ev", None)
        glyph, hue = _EVENTS.get(ev) or _LEVEL_GLYPH.get(
            record.levelno, ("·", "dim"))

        if record.exc_info:                       # keep tracebacks attached
            msg = f"{msg}\n{self.formatException(record.exc_info)}"

        if self.color:
            ts = _paint(self.formatTime(record, "%H:%M:%S"), "dim", on=True)
            mark = _paint(glyph, hue, on=True)
            # Only actual warnings/errors tint the whole line; lifecycle events
            # keep a coloured glyph but plain text so the feed stays calm.
            body = (_paint(msg, hue, on=True)
                    if record.levelno >= logging.WARNING else msg)
            return f"{ts}  {mark}  {body}"
        # File face: full date + level, no colour, glyph kept as a cheap marker.
        ts = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        return f"{ts} {record.levelname:<7} {glyph} {msg}"


def setup_logging(verbose: bool) -> None:
    """Install the console formatter on the root logger. Idempotent enough for
    the CLI's single call; replaces any handlers basicConfig would add."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ConsoleFormatter(color=color_enabled()))
    root.addHandler(handler)


# ── formatting helpers ──────────────────────────────────────────────────────

def human_duration(seconds: float) -> str:
    """'8s' · '4m12s' · '1h44m' — compact, leading unit only when nonzero."""
    s = int(max(0, seconds))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{sec:02d}s"
    return f"{sec}s"


def _short_time(iso_ts: str) -> str:
    """'HH:MM:SS' from an ISO timestamp; the raw string if it can't be parsed."""
    from datetime import datetime
    try:
        return datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).strftime("%H:%M:%S")
    except (ValueError, TypeError, AttributeError):
        return str(iso_ts)


def _age(iso_ts: str | None) -> str:
    """'just now' / '6m ago' / '3h ago' / '2d ago' from an ISO timestamp."""
    from datetime import datetime, timezone
    if not iso_ts:
        return "never"
    try:
        ts = iso_ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        secs = (datetime.now(timezone.utc) - dt).total_seconds()
    except (ValueError, TypeError):
        return str(iso_ts)
    if secs < 60:
        return "just now"
    if secs < 3600:
        return f"{int(secs // 60)}m ago"
    if secs < 86400:
        return f"{int(secs // 3600)}h ago"
    return f"{int(secs // 86400)}d ago"


def human_size(num_bytes: int) -> str:
    """Bytes → '1.2 GB' / '840 MB' / '12 KB' (SI, one decimal above MB)."""
    n = float(max(0, num_bytes))
    for unit, step in (("GB", 1_000_000_000), ("MB", 1_000_000), ("KB", 1_000)):
        if n >= step:
            val = n / step
            return f"{val:.1f} {unit}" if unit == "GB" else f"{val:.0f} {unit}"
    return f"{int(n)} B"


# ── banner / panels ─────────────────────────────────────────────────────────

_RULE = "━" * 58


def banner(config: "RecorderConfig") -> None:
    """Session header printed once at `recorder start`. Width-robust (a fixed
    rule, no box math) so it survives any terminal size and the log file."""
    on = color_enabled()
    users = config.tiktok_users
    roster = "  ".join(f"@{u}" for u in users) or "(none configured)"

    title = _paint("recorder", "bold", on=on) + _paint(" · tiktok live", "dim", on=on)
    print()
    print(_paint(_RULE, "dim", on=on))
    print(f"  {title}")
    print(f"  {_paint('users ', 'dim', on=on)}  {len(users)} · {roster}")
    print(f"  {_paint('poll  ', 'dim', on=on)}  every {config.poll_interval_s:.0f}s")
    print(f"  {_paint('output', 'dim', on=on)}  {config.output_dir}")
    print(f"  {_paint('queue ', 'dim', on=on)}  {config.db_path}")
    print(_paint(_RULE, "dim", on=on))


def field(label: str, value: str, *, accent: str | None = None) -> None:
    """One aligned 'label  value' line for the `status` panel."""
    on = color_enabled()
    lbl = _paint(f"{label:<9}", "dim", on=on)
    val = _paint(value, accent, on=on) if accent else value
    print(f"  {lbl} {val}")
