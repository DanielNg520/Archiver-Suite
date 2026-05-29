"""
core.schema
───────────
The one schema every process shares. Owns the DDL and the connection
factory. No process defines its own tables anymore; they all call
core.schema.connect() against the same file.

Tables
──────
  items        ONE row per media file, cradle to grave. Merges the old
               archive.db.media (catalog) and dispatcher.db.upload_queue
               (lifecycle). See core.models for the status state machine.
               Identity keys:
                 - file_path UNIQUE  (one row per physical file)
                 - (platform, identifier) UNIQUE  (one row per platform post)

  checkpoints  per (platform, username): last_run_utc + date_floor.
               Archiver-private, but lives here so there's one DB file.

  circuit      per platform: circuit-breaker state for self-healing.

  metadata     generic key/value (cookie refresh timestamps, etc.)

WAL + busy_timeout: multiple processes (archiver, recorder, dispatcher,
ops) open this file concurrently. WAL lets readers run during a writer;
busy_timeout makes brief lock contention block-and-retry instead of
raising SQLITE_BUSY.
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

# Default location. Override with $ARCHIVER_DB for tests / alternate setups.
DEFAULT_DB_PATH = "~/.config/archiver-suite/suite.db"


def db_path() -> Path:
    return Path(os.environ.get("ARCHIVER_DB", DEFAULT_DB_PATH)).expanduser()


ITEMS_DDL = """
CREATE TABLE IF NOT EXISTS items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT    NOT NULL,                       -- 'archiver' | 'recorder'
    platform        TEXT    NOT NULL,
    username        TEXT    NOT NULL,
    identifier      TEXT    NOT NULL,
    file_path       TEXT    NOT NULL UNIQUE,
    upload_date     TEXT,                                   -- YYYYMMDD post date
    file_size_bytes INTEGER,
    title           TEXT,
    discovered_at   TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'pending',     -- see core.models.Status
    priority        INTEGER NOT NULL DEFAULT 100,           -- lower drains first
    caption         TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    claimed_at      TEXT,
    sent_at         TEXT,
    last_error      TEXT,
    tg_message_id   INTEGER,
    UNIQUE (platform, identifier)
);

-- Claim path: cheapest possible scan for the next pending row.
CREATE INDEX IF NOT EXISTS idx_items_pending
    ON items (priority, discovered_at)
    WHERE status='pending';

-- Watchdog path: find stale in-flight rows.
CREATE INDEX IF NOT EXISTS idx_items_sending
    ON items (claimed_at)
    WHERE status='sending';

-- Per-user lifecycle queries (pending list, stats, reset, purge).
CREATE INDEX IF NOT EXISTS idx_items_user_status
    ON items (platform, username, status);

-- Download-floor query: MAX(upload_date WHERE status='sent').
CREATE INDEX IF NOT EXISTS idx_items_user_uploaddate
    ON items (platform, username, upload_date);

CREATE TABLE IF NOT EXISTS checkpoints (
    platform     TEXT NOT NULL,
    username     TEXT NOT NULL,
    last_run_utc TEXT,
    date_floor   TEXT,
    PRIMARY KEY (platform, username)
);

CREATE TABLE IF NOT EXISTS circuit (
    platform           TEXT PRIMARY KEY,
    consecutive_fails  INTEGER NOT NULL DEFAULT 0,
    tripped_until_utc  TEXT,
    last_error         TEXT
);

CREATE TABLE IF NOT EXISTS metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _retry_locked(fn, *, attempts: int = 5):
    for i in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() or i == attempts - 1:
                raise
            time.sleep(0.2 * (2 ** i))


def connect(path: str | os.PathLike | None = None,
            *, init: bool = True) -> sqlite3.Connection:
    """
    Open (and by default initialize) the suite DB.

    check_same_thread=False because the recorder touches its store from an
    asyncio callback thread; all writes are short and serialized by the
    busy_timeout, so this is safe for our access pattern.
    """
    p = Path(path).expanduser() if path is not None else db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(p), check_same_thread=False, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    _retry_locked(lambda: conn.execute("PRAGMA journal_mode=WAL"))
    conn.execute("PRAGMA foreign_keys=ON")
    if init:
        _retry_locked(lambda: conn.executescript(ITEMS_DDL))
        conn.commit()
    return conn
