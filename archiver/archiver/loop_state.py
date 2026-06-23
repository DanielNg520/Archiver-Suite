"""
archiver.loop_state
───────────────────
Cross-process phase heartbeat for `archiver loop`. The loop writes a tiny JSON
file at each phase transition — "running" while a scan cycle executes, and
"sleeping" between cycles (carrying the wake time) — so `ops health` /
`ops watch` can show whether the archiver is actively working or resting
between loops, instead of just "process alive".

A FILE, not the DB — mirrors dispatcher.progress: the phase is ephemeral
status, ops deliberately reads only on-disk artifacts (it imports no worker
package), and hammering the shared SQLite for a status line would be backwards.
Validity is gated on the writer pid being alive, so a crashed loop can never
leave a lying "running"/"sleeping" status behind. The path is fixed (not
derived from config) so the standalone ops reader can find it without importing
the archiver.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from core import heartbeat, paths

DEFAULT_PATH = paths.archiver_loop()

_PHASES = ("running", "sleeping")


def write_running(run_n: int, *, platform: str | None = None,
                  user: str | None = None, path: Path = DEFAULT_PATH) -> None:
    """Mark a scan cycle as in progress. Called once generically at cycle start
    (pre/post phases: reconcile, ingest, …) and again per user as the run walks
    them, so the heartbeat can name exactly what's being scanned right now."""
    now = time.time()
    state = {"pid": os.getpid(), "phase": "running", "run_n": run_n,
             "since": now, "updated_at": now}
    if platform:
        state["platform"] = platform
    if user:
        state["user"] = user
    _write(state, path)


def write_sleeping(run_n: int, wake_at: float, *,
                   path: Path = DEFAULT_PATH) -> None:
    """Mark the loop as resting until `wake_at` (epoch secs)."""
    now = time.time()
    _write({"pid": os.getpid(), "phase": "sleeping", "run_n": run_n,
            "since": now, "wake_at": wake_at, "updated_at": now}, path)


def clear(path: Path = DEFAULT_PATH) -> None:
    """Remove the heartbeat — call when the loop exits, so a stopped loop
    doesn't read back as forever 'sleeping' (belt-and-suspenders with the
    pid-liveness check on the reader)."""
    heartbeat.clear(path)


def _write(state: dict, path: Path) -> None:
    heartbeat.write_atomic(path, state)


def read(path: Path = DEFAULT_PATH) -> dict | None:
    """Current loop phase, or None if absent / malformed / writer gone."""
    return heartbeat.read_live(path, validate=lambda d: d.get("phase") in _PHASES)
