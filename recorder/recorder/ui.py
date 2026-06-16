"""
recorder.ui
───────────
Recorder-facing facade over the shared core.termui presentation layer.

The engine (formatter, colour, glyph vocabulary, helpers) lives in core.termui
so every worker renders one consistent interface. This module keeps only what
is recorder-specific — chiefly the session banner — and re-exports the handful
of names recorder.state / recorder.watch / recorder.cli reference, so those call
sites stay stable.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from core import termui as _t
# Re-exports (stable names used across the recorder package).
from core.termui import (                                       # noqa: F401
    ConsoleFormatter, color_enabled, field, human_duration, human_size,
)

if TYPE_CHECKING:
    from .config import RecorderConfig

# Private aliases recorder.watch/cli already import by these names.
_paint = _t.paint
_short_time = _t.short_time
_age = _t.age


def setup_logging(verbose: bool) -> None:
    _t.setup_logging(verbose)


def banner(config: "RecorderConfig") -> None:
    """Session header for `recorder start`."""
    roster = "  ".join(f"@{u}" for u in config.tiktok_users) or "(none configured)"
    _t.banner("recorder", [
        ("users", f"{len(config.tiktok_users)} · {roster}"),
        ("poll", f"every {config.poll_interval_s:.0f}s"),
        ("output", str(config.output_dir)),
        ("queue", str(config.db_path)),
    ], subtitle="tiktok live")
