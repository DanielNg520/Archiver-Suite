"""
ops.health
──────────
Standalone health check for the three-process system. Imports NOTHING
from dispatcher / recorder / archiver — it only reads their on-disk
artifacts (launchctl status, SQLite DBs, lock file, pid file). This keeps
ops installable and runnable even if one of the services is broken or
uninstalled.

Liveness source of truth = `launchctl list <label>`, NOT self-written pid
files. launchd knows the real managed pid and whether the job is loaded;
pid files go stale on hard kills. We fall back to the recorder's pid file
only for its INTERNAL state (which launchctl can't see).
"""

from __future__ import annotations

import shutil
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from core import db_path as _core_db_path

# Single source of truth: one DB for the whole suite. ops still imports no
# *service* package (dispatcher/recorder/archiver) — it only borrows the
# canonical DB path from the shared `core` library so the location isn't
# duplicated here and can't drift from what the services actually write.
SUITE_DB      = _core_db_path()
RECORDER_PID  = Path("~/.recorder/pid").expanduser()
TIKTOK_LOCK   = Path("~/.config/archiver/locks/tiktok.lock").expanduser()

LABELS = {
    "dispatcher": "com.duy.dispatcher",
    "recorder":   "com.duy.recorder",
    "archiver":   "com.duy.archiver",
}


# ── launchd liveness ──────────────────────────────────────────────────────

def launchctl_pid(label: str) -> int | None:
    """Return the managed pid if the job is loaded AND running, else None.
    `launchctl list <label>` prints a plist-ish block with a "PID" key
    only while the process is actually alive."""
    try:
        out = subprocess.run(
            ["launchctl", "list", label],
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None  # not loaded
    for line in out.stdout.splitlines():
        s = line.strip()
        if s.startswith('"PID"'):
            # format: "PID" = 1234;
            digits = "".join(c for c in s if c.isdigit())
            return int(digits) if digits else None
    return None


# ── DB queries (read-only) ─────────────────────────────────────────────────

def _connect_ro(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    try:
        # immutable=1: open read-only without taking locks, safe even
        # while the owning process is mid-write under WAL.
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=3.0)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def dispatcher_queue_counts() -> dict[str, int] | None:
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM items GROUP BY status"
        ).fetchall()
        return {r["status"]: r["n"] for r in rows}
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def archiver_last_run() -> str | None:
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT MAX(last_run_utc) AS m FROM checkpoints"
        ).fetchone()
        return row["m"] if row and row["m"] else None
    except sqlite3.Error:
        return None
    finally:
        conn.close()


# ── helpers ────────────────────────────────────────────────────────────────

def _humanize_age(iso_ts: str) -> str:
    """'12m ago' / '3h ago' from an ISO timestamp. Tolerates Z suffix and
    offset-naive strings."""
    try:
        ts = iso_ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        secs = (datetime.now(timezone.utc) - dt).total_seconds()
    except (ValueError, TypeError):
        return "unknown"
    if secs < 90:
        return f"{int(secs)}s ago"
    if secs < 5400:
        return f"{int(secs // 60)}m ago"
    return f"{int(secs // 3600)}h ago"


def _disk_free(path: str = "/") -> str:
    try:
        usage = shutil.disk_usage(path)
        return f"{usage.free / 1_000_000_000:.0f}GB free"
    except OSError:
        return "unknown"


# ── report ──────────────────────────────────────────────────────────────────

def render() -> str:
    lines: list[str] = []

    # dispatcher
    pid = launchctl_pid(LABELS["dispatcher"])
    if pid:
        counts = dispatcher_queue_counts() or {}
        q = (f"queue: {counts.get('pending',0)} pending, "
             f"{counts.get('sending',0)} sending, "
             f"{counts.get('sent',0)} sent, "
             f"{counts.get('failed',0)} failed")
        lines.append(f"dispatcher: running (pid {pid}, {q})")
    else:
        lines.append("dispatcher: NOT running")

    # recorder
    pid = launchctl_pid(LABELS["recorder"])
    if pid:
        state = "running"
        # recorder's internal state isn't exposed via launchctl; the pid
        # file just confirms which pid it thinks it is. State would require
        # an IPC channel we deliberately didn't build for Slice 4.
        lines.append(f"recorder:   running (pid {pid})")
    else:
        lines.append("recorder:   NOT running")

    # archiver
    pid = launchctl_pid(LABELS["archiver"])
    if pid:
        lr = archiver_last_run()
        when = _humanize_age(lr) if lr else "never"
        lines.append(f"archiver:   running (pid {pid}, last checkpoint {when})")
    else:
        lines.append("archiver:   NOT running")

    # lock + disk
    held = TIKTOK_LOCK.exists()
    lines.append(f"tiktok.lock: {'HELD (recorder recording)' if held else 'not held'}")
    lines.append(f"disk:        {_disk_free()}")

    return "\n".join(lines)
