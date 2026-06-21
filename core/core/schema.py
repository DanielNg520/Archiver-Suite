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

-- Send-order clustering: claim_batch anchors each (platform, username) cluster
-- on its first-appearance MIN(discovered_at) so a user's media drains
-- contiguously (core.store._CLUSTER_COLS). discovered_at as the trailing column
-- makes that correlated MIN an index-leftmost lookup, not a per-user row scan.
CREATE INDEX IF NOT EXISTS idx_items_user_disc
    ON items (platform, username, discovered_at);

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


# ── Versioned migrations ──────────────────────────────────────────────────────
#
# ITEMS_DDL above is the IMMUTABLE base schema (PRAGMA user_version 0). It is
# never edited again. Every later change is an ordered, forward-only migration
# keyed by target version, applied once and recorded in PRAGMA user_version.
#
# Why not just edit ITEMS_DDL? CREATE TABLE IF NOT EXISTS silently no-ops on an
# existing DB, so a new column added to the DDL would reach fresh installs but
# never existing ones — exactly the drift this runner removes. A fresh DB starts
# at user_version 0 (base DDL), then runs the same migrations an existing DB
# does, so both arrive at SCHEMA_VERSION by the identical path.
#
# Each migration is (target_version, [statements]). Statements run one at a time
# (not executescript) so the whole upgrade is ONE transaction that rolls back
# cleanly on failure — including the user_version bump, which lives in the DB
# header and participates in the transaction.
SCHEMA_VERSION = 4


class SchemaVersionError(RuntimeError):
    """The DB was written by a NEWER build than this binary understands
    (its user_version exceeds SCHEMA_VERSION). Fail loud at connect rather
    than operate blindly on a schema with columns/semantics we don't know —
    the one real hazard of a multi-binary install where the pieces can be
    upgraded out of lockstep."""

    def __init__(self, found: int, known: int) -> None:
        self.found, self.known = found, known
        super().__init__(
            f"suite.db schema is v{found} but this build only knows v{known}. "
            f"Upgrade this component (pipx upgrade) — do not downgrade the DB."
        )

_MIGRATIONS: list[tuple[int, list[str]]] = [
    (1, [
        # Redesign columns: per-file content hash (global dedup), explicit
        # Telegram destination (chat_id-named folders), and explicit album
        # batch identity (decouples grouping from per-file caption text).
        "ALTER TABLE items ADD COLUMN content_hash TEXT",
        "ALTER TABLE items ADD COLUMN chat_id      TEXT",
        "ALTER TABLE items ADD COLUMN group_key    TEXT",
        # O(log n) "have these exact bytes already shipped?" lookup for the
        # dispatcher's dedup guarantee. Partial index → only sent rows, the
        # only ones that can suppress a new send.
        "CREATE INDEX IF NOT EXISTS idx_items_hash_sent "
        "    ON items (content_hash) WHERE status='sent'",
    ]),
    (2, [
        # Producer-side ingest/reconcile paths ask "have these bytes ever
        # appeared?" across all statuses, not just sent rows. Keep that lookup
        # indexed for large archives while preserving NULL-friendly storage.
        "CREATE INDEX IF NOT EXISTS idx_items_hash "
        "    ON items (content_hash) WHERE content_hash IS NOT NULL",
    ]),
    (3, [
        # Per-user "has the full back-catalogue been walked yet?" flag. A newly
        # added user must be fetched with NO date-min so gallery-dl/yt-dlp walk
        # the entire timeline; once that one full pass completes we set this and
        # subsequent runs go back to incremental (date-min) fetching. Existing
        # users are assumed already backfilled, so flag them done — only genuinely
        # new users (no checkpoint row yet) default to "needs full history".
        "ALTER TABLE checkpoints ADD COLUMN full_history_done INTEGER NOT NULL DEFAULT 0",
        "UPDATE checkpoints SET full_history_done = 1",
    ]),
    (4, [
        # Forum-topic destination. The twin of chat_id (schema v1): explicit on
        # orphaned rows whose folder name carries a `.t<topic_id>` suffix, NULL
        # for platform/recorder rows (which resolve their destination — chat AND
        # topic — from the env chain at send time). A forum's message_thread_id;
        # NULL → the chat's General topic (no reply_to). INTEGER, not TEXT,
        # because a thread id is always a positive int and Telethon's reply_to
        # wants one — keeps the send-time call total.
        "ALTER TABLE items ADD COLUMN topic_id INTEGER",
    ]),
]


def _apply_migrations(conn: sqlite3.Connection) -> None:
    """Bring `conn` up to SCHEMA_VERSION. Safe under concurrent openers:
    BEGIN IMMEDIATE serializes would-be migrators, and the re-read of
    user_version inside the lock means a process that lost the race sees the
    bumped version and applies nothing (no duplicate-column crash)."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        current = conn.execute("PRAGMA user_version").fetchone()[0]
        # Forward-only: a DB newer than this binary is a hard stop. Migrations
        # never go backward, so silently proceeding would run new-schema rows
        # through old-schema code. Raise INSIDE the lock, before any write.
        if current > SCHEMA_VERSION:
            conn.rollback()
            raise SchemaVersionError(current, SCHEMA_VERSION)
        for version, statements in _MIGRATIONS:
            if current >= version:
                continue
            for stmt in statements:
                conn.execute(stmt)
            # PRAGMA can't be parameterized; version is our own trusted int.
            conn.execute(f"PRAGMA user_version = {version}")
            current = version
        conn.commit()
    except Exception:
        conn.rollback()
        raise


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
    # Throughput tuning. synchronous=NORMAL is the canonical WAL setting: a
    # commit no longer fsyncs on every transaction (the default FULL does),
    # so the many small commits the producers make — one per add_item, per
    # checkpoint — stop paying a disk-sync each. The only durability cost vs
    # FULL is that an OS-level crash/power-loss can lose the LAST committed
    # transaction; it can NEVER corrupt the DB under WAL. Every write here is
    # reconstructable on the next run (re-download / re-reconcile), so that
    # trade is right for this workload. temp_store=MEMORY keeps sort/temp
    # B-trees (large reconcile scans) off disk; cache_size=-65536 gives the
    # page cache 64 MB; mmap_size maps up to 256 MB for read-heavy scans.
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-65536")
    conn.execute("PRAGMA mmap_size=268435456")
    if init:
        _retry_locked(lambda: conn.executescript(ITEMS_DDL))
        conn.commit()
        _retry_locked(lambda: _apply_migrations(conn))
    return conn
