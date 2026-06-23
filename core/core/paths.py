"""
core.paths
──────────
The single source of truth for the suite's shared ON-DISK layout — the files
that are a CROSS-PROCESS CONTRACT: one worker writes them, another reads them.
These locations used to be hard-coded independently in each worker (and again in
ops), so the writer and the reader could silently drift apart. Defining them
once here makes that drift impossible — the same reasoning that already keeps the
DB path in core.schema.

What lives here (writer → reader):
  • tiktok_lock        recorder.lock  →  archiver.lock_reader, ops.health
  • dispatcher_progress dispatcher.progress (writer+reader) → ops.health
  • archiver_loop       archiver.loop_state (writer+reader) → ops.health
  • recorder_pid        recorder.cli  →  ops.health   (DEFAULT; the recorder may
                        relocate its state dir, which only it and ops-via-default
                        can know — so this is the agreed default, not a guarantee)

Functions (not constants) so the value reflects $HOME at call time and stays
consistent with core.schema.db_path()'s style. The DB path itself stays in
core.schema (db_path); import it from there.
"""

from __future__ import annotations

from pathlib import Path


def locks_dir() -> Path:
    """Directory holding the suite's cross-process lock files."""
    return Path("~/.config/archiver-suite/locks").expanduser()


def tiktok_lock() -> Path:
    """Recorder's TikTok soft-lock: present ⇒ a live capture is in flight, so the
    archiver skips its TikTok download step. Written by recorder.lock; read by
    archiver.lock_reader and ops.health."""
    return locks_dir() / "tiktok.lock"


def dispatcher_progress() -> Path:
    """Dispatcher's upload-progress heartbeat (see core.heartbeat)."""
    return Path("~/.config/dispatcher/progress.json").expanduser()


def archiver_loop() -> Path:
    """Archiver loop's phase heartbeat (running/sleeping; see core.heartbeat)."""
    return Path("~/.config/archiver/loop.json").expanduser()


def recorder_pid() -> Path:
    """Default recorder pid file. The recorder writes it under its configured
    state dir (default ~/.recorder); ops reads this default location."""
    return Path("~/.recorder/pid").expanduser()
