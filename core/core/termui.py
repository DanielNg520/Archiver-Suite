"""
core.termui
───────────
Shared terminal presentation layer for every worker's foreground output.

A worker's log IS its interface, so each message must serve two audiences at
once. ONE logging.Formatter renders both:

  • On a TTY  → a compact, colorized event feed: `HH:MM:SS  ●  @user is LIVE`.
                A small glyph vocabulary gives each lifecycle moment its own
                visual rhythm so a day of activity scans at a glance.
  • In a file → a plain, fully-timestamped, level-tagged line (no escape codes),
                so launchd logs, `grep`, and rotation keep working unchanged.

Every existing `log.info(...)` is styled for free; a call opts into a richer
glyph with `extra={"ev": "<name>"}`. Colour is auto-suppressed off a TTY or
under NO_COLOR. Noisy third-party loggers (telethon, …) are pinned to WARNING so
their chatter never reaches the feed.

Workers share ONE event vocabulary (the union below) — a glyph is just
decoration, so an unused tag costs nothing, and one consistent visual language
across recorder/archiver/dispatcher is the point.
"""

from __future__ import annotations

import logging
import os
import sys

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
    """True when ANSI colour is safe: a real TTY with NO_COLOR unset."""
    if os.environ.get("NO_COLOR") is not None:
        return False
    try:
        return sys.stdout.isatty()
    except (AttributeError, ValueError):
        return False


def paint(text: str, *styles: str, on: bool | None = None) -> str:
    """Wrap text in ANSI styles. `on=None` auto-detects; pass a bool to force."""
    if on is None:
        on = color_enabled()
    if not on or not styles:
        return text
    codes = "".join(_COLORS[s] for s in styles if s in _COLORS)
    return f"{codes}{text}{_RESET}" if codes else text


# ── event vocabulary (shared union across all workers) ──────────────────────

EVENTS: dict[str, tuple[str, str]] = {
    # lifecycle (any worker)
    "start":   ("▸", "bold"),
    "listen":  ("·", "dim"),
    "idle":    ("·", "dim"),
    "stop":    ("○", "dim"),
    "sweep":   ("✦", "dim"),
    "ok":      ("✓", "green"),
    # recorder
    "live":    ("●", "green"),
    "reconnect": ("⟲", "yellow"),
    "rec_end": ("■", "cyan"),
    "remux":   ("⤳", "dim"),
    "queued":  ("↑", "blue"),
    "handoff": ("⚑", "yellow"),
    # dispatcher
    "upload":  ("▲", "blue"),
    "sent":    ("✓", "green"),
    "album":   ("▤", "cyan"),
    "dedup":   ("⊝", "dim"),
    "retry":   ("↻", "yellow"),
    "flood":   ("◷", "yellow"),
    # archiver
    "discover": ("◆", "blue"),
    "download": ("↓", "cyan"),
    "ingest":   ("＋", "green"),
    "sort":     ("⇄", "dim"),
    "reconcile": ("↺", "dim"),
    "backfill": ("▒", "dim"),
    "banned":   ("✗", "red"),
}
_LEVEL_GLYPH: dict[int, tuple[str, str]] = {
    logging.DEBUG:    ("·", "dim"),
    logging.INFO:     ("·", "dim"),
    logging.WARNING:  ("⚠", "yellow"),
    logging.ERROR:    ("✖", "red"),
    logging.CRITICAL: ("✖", "red"),
}

# Pure scope prefixes that duplicate the worker name — stripped so the feed
# reads as prose, not "drain: telethon: …". Semantic prefixes (auto-backfill:,
# reconcile:) are deliberately NOT here; reword those at the source instead.
_STRIP_PREFIXES = (
    "recorder: ", "capture: ", "remux: ", "record-once: ", "startup-sweep: ",
    "enqueue: ", "tiktok lock ", "cli: ", "drain: ", "telethon: ",
    "fast_upload: ", "dispatcher: ", "archiver: ",
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
        glyph, hue = EVENTS.get(ev) or _LEVEL_GLYPH.get(
            record.levelno, ("·", "dim"))

        if record.exc_info:                       # keep tracebacks attached
            msg = f"{msg}\n{self.formatException(record.exc_info)}"

        if self.color:
            ts = paint(self.formatTime(record, "%H:%M:%S"), "dim", on=True)
            mark = paint(glyph, hue, on=True)
            # Only real warnings/errors tint the whole line; lifecycle events
            # keep a coloured glyph but plain text so the feed stays calm.
            body = (paint(msg, hue, on=True)
                    if record.levelno >= logging.WARNING else msg)
            return f"{ts}  {mark}  {body}"
        # File face: full date + level, no colour, glyph kept as a cheap marker.
        ts = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        return f"{ts} {record.levelname:<7} {glyph} {msg}"


# Third-party loggers whose INFO chatter is noise in a worker feed.
_DEFAULT_QUIET = ("telethon", "asyncio", "httpx", "httpcore", "urllib3")


def setup_logging(verbose: bool, *, quiet: tuple[str, ...] = _DEFAULT_QUIET,
                  stream=None, log_file: str | None = None) -> None:
    """Install the console formatter on the root logger (replacing whatever
    basicConfig would add) and pin noisy libraries to WARNING.

    With `log_file`, also tee to a file using the PLAIN (no-colour) face, so the
    archival log stays grep-friendly while the console gets the colour feed.
    Idempotent enough for a CLI's single call."""
    import os.path

    root = logging.getLogger()
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    for h in list(root.handlers):
        root.removeHandler(h)

    console = logging.StreamHandler(stream or sys.stdout)
    console.setFormatter(ConsoleFormatter(color=color_enabled()))
    root.addHandler(console)

    if log_file:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(ConsoleFormatter(color=False))   # never ANSI in a file
        root.addHandler(fh)

    for name in quiet:
        logging.getLogger(name).setLevel(
            logging.INFO if verbose else logging.WARNING)


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


def human_size(num_bytes: int) -> str:
    """Bytes → '1.2 GB' / '840 MB' / '12 KB' (SI, one decimal above MB)."""
    n = float(max(0, num_bytes))
    for unit, step in (("GB", 1_000_000_000), ("MB", 1_000_000), ("KB", 1_000)):
        if n >= step:
            val = n / step
            return f"{val:.1f} {unit}" if unit == "GB" else f"{val:.0f} {unit}"
    return f"{int(n)} B"


def short_time(iso_ts: str) -> str:
    """'HH:MM:SS' from an ISO timestamp; the raw string if unparseable."""
    from datetime import datetime
    try:
        return datetime.fromisoformat(
            iso_ts.replace("Z", "+00:00")).strftime("%H:%M:%S")
    except (ValueError, TypeError, AttributeError):
        return str(iso_ts)


def age(iso_ts: str | None) -> str:
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


# ── banner / panels ─────────────────────────────────────────────────────────

RULE = "━" * 58


def banner(title: str, fields: list[tuple[str, str]], *,
           subtitle: str | None = None) -> None:
    """Session header: a fixed rule (no box math, width-robust), a bold title
    with an optional dim subtitle, then aligned label/value rows."""
    on = color_enabled()
    head = paint(title, "bold", on=on)
    if subtitle:
        head += paint(f" · {subtitle}", "dim", on=on)
    print()
    print(paint(RULE, "dim", on=on))
    print(f"  {head}")
    for label, value in fields:
        print(f"  {paint(format(label, '<8'), 'dim', on=on)}  {value}")
    print(paint(RULE, "dim", on=on))


def field(label: str, value: str, *, accent: str | None = None) -> None:
    """One aligned 'label  value' line for a status panel."""
    on = color_enabled()
    lbl = paint(f"{label:<10}", "dim", on=on)
    val = paint(value, accent, on=on) if accent else value
    print(f"  {lbl} {val}")
