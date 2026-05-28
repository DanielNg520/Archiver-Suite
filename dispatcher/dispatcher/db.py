"""
dispatcher.db
─────────────
The upload queue. SQLite WAL, raw SQL (no ORM), one connection per
QueueDB instance.

KEY DESIGN POINTS:

  1. WAL mode + busy_timeout
     Multiple writers (dispatcher + archiver + recorder) all hit this DB
     concurrently. WAL allows readers to coexist with one writer without
     blocking. busy_timeout=5000 makes brief contention WAIT rather than
     immediately fail with "database is locked".

  2. Atomic claim
     SQLite has no row-level lock. To claim a pending row without races:
         BEGIN IMMEDIATE                        -- acquires DB-wide write lock
         SELECT id FROM upload_queue ...        -- find candidate
         UPDATE ... WHERE id=? AND status='pending'  -- claim it
         COMMIT
     If two processes race to claim the same row, only ONE'S UPDATE will
     match (because the other already changed status to 'claimed').
     We check cursor.rowcount==1 and retry on miss.

  3. INSERT OR IGNORE on (source, file_path) UNIQUE
     Idempotent enqueue: if archiver re-runs and re-submits a file that's
     already pending, we silently no-op. No duplicate uploads.

  4. Watchdog: reset_stuck_claimed()
     If dispatcher crashes mid-send, a row stays in 'claimed' forever.
     On startup we revert any 'claimed' row older than N minutes to
     'pending'. This may cause a duplicate upload (we can't tell if the
     crash happened before or after Telegram received the file) — see
     drain.py lesson on this tradeoff.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

log = logging.getLogger(__name__)


def now_iso() -> str:
    """UTC ISO8601 with 'Z' suffix. SQLite sorts these lexically."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Schema ────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS upload_queue (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT    NOT NULL,
    platform      TEXT    NOT NULL,
    username      TEXT    NOT NULL,
    file_path     TEXT    NOT NULL,
    caption       TEXT,
    priority      INTEGER NOT NULL DEFAULT 100,
    submitted_at  TEXT    NOT NULL,
    status        TEXT    NOT NULL DEFAULT 'pending',
    attempts      INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    claimed_at    TEXT,
    sent_at       TEXT,
    seen_by_archiver INTEGER NOT NULL DEFAULT 0,
    UNIQUE (source, file_path)
);

CREATE INDEX IF NOT EXISTS idx_pending
    ON upload_queue (status, priority, submitted_at)
    WHERE status='pending';

CREATE INDEX IF NOT EXISTS idx_claimed
    ON upload_queue (status, claimed_at)
    WHERE status='claimed';
"""


# ── Row dataclass ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class QueueRow:
    """
    Immutable snapshot of a queue row. We return these instead of raw
    sqlite3.Row so callers can't accidentally mutate the cursor's row dict.
    """
    id:           int
    source:       str
    platform:     str
    username:     str
    file_path:    str
    caption:      str | None
    priority:     int
    submitted_at: str
    status:       str
    attempts:     int
    last_error:   str | None
    claimed_at:   str | None
    sent_at:      str | None
    seen_by_archiver: int = 0

    @classmethod
    def from_row(cls, r: sqlite3.Row) -> "QueueRow":
        return cls(**{k: r[k] for k in r.keys()})


# ── The QueueDB ───────────────────────────────────────────────────────────

class QueueDB:
    """
    SQLite-backed upload queue. One instance per process is fine — the
    sqlite3 connection itself is single-threaded by default. If you need
    multi-thread access from one process, pass check_same_thread=False
    and serialize at a higher level.
    """

    def __init__(self, db_path: str):
        self._path = Path(db_path).expanduser()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self._path),
            timeout=10.0,
            isolation_level=None,   # autocommit; we manage txns explicitly
        )
        self._conn.row_factory = sqlite3.Row
        # PRAGMAs first — WAL must be set BEFORE any other write activity.
        self._conn.executescript(
            "PRAGMA journal_mode=WAL;"
            "PRAGMA synchronous=NORMAL;"     # NORMAL is durable enough w/ WAL
            "PRAGMA busy_timeout=5000;"
            "PRAGMA foreign_keys=ON;"
        )
        self._conn.executescript(_SCHEMA)
        self._migrate_seen_by_archiver()

    def _migrate_seen_by_archiver(self) -> None:
        """Add seen_by_archiver to upload_queue if a pre-slice-3 DB lacks it.
        Idempotent; CREATE TABLE IF NOT EXISTS won't alter an existing table,
        so the column add must be explicit."""
        cols = {r["name"] for r in
                self._conn.execute("PRAGMA table_info(upload_queue)")}
        if "seen_by_archiver" not in cols:
            log.info("db: migrating upload_queue (adding seen_by_archiver)")
            self._conn.execute(
                "ALTER TABLE upload_queue "
                "ADD COLUMN seen_by_archiver INTEGER NOT NULL DEFAULT 0"
            )

    def close(self) -> None:
        self._conn.close()

    # ── Transaction helper ────────────────────────────────────────────────

    @contextmanager
    def _immediate_txn(self) -> Iterator[sqlite3.Cursor]:
        """
        BEGIN IMMEDIATE — grabs the DB-wide write lock right away rather
        than upgrading from a read lock on first write (the default).
        Avoids deadlock when two writers each have a read lock and both
        try to upgrade simultaneously.
        """
        cur = self._conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            yield cur
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise

    # ── Enqueue ───────────────────────────────────────────────────────────

    def enqueue(
        self,
        *,
        source:    str,
        platform:  str,
        username:  str,
        file_path: str,
        caption:   str | None = None,
        priority:  int        = 100,
    ) -> int | None:
        """
        Insert a new queue row. Returns the rowid on insert, or None if
        the (source, file_path) pair already exists (idempotent).

        We don't surface "already exists" as an error — re-enqueue is a
        legitimate operation (caller may have retried after a crash).
        """
        with self._immediate_txn() as cur:
            cur.execute(
                """
                INSERT OR IGNORE INTO upload_queue
                  (source, platform, username, file_path, caption,
                   priority, submitted_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (source, platform, username, file_path, caption,
                 priority, now_iso()),
            )
            if cur.rowcount == 0:
                return None
            return cur.lastrowid

    # ── Claim ─────────────────────────────────────────────────────────────

    def claim_next(self) -> QueueRow | None:
        """
        Atomically claim the highest-priority pending row.

        Order: priority ASC, submitted_at ASC (oldest first within priority).
        Returns None if nothing pending.

        Race safety: if two dispatchers race, BEGIN IMMEDIATE serializes
        them. The losing one will see status='claimed' on its UPDATE and
        rowcount==0 — we retry up to a small bound to find the next row.
        """
        for _ in range(5):
            with self._immediate_txn() as cur:
                cur.execute(
                    """
                    SELECT * FROM upload_queue
                    WHERE status='pending'
                    ORDER BY priority ASC, submitted_at ASC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row is None:
                    return None

                cur.execute(
                    """
                    UPDATE upload_queue
                       SET status='claimed',
                           claimed_at=?,
                           attempts=attempts+1
                     WHERE id=? AND status='pending'
                    """,
                    (now_iso(), row["id"]),
                )
                if cur.rowcount == 1:
                    # Re-read so the QueueRow reflects post-claim state
                    # (status, attempts, claimed_at all updated).
                    cur.execute(
                        "SELECT * FROM upload_queue WHERE id=?", (row["id"],)
                    )
                    fresh = cur.fetchone()
                    return QueueRow.from_row(fresh)
                # else: someone else claimed it; loop and try the next one
        log.warning("claim_next: 5 retries exhausted under contention")
        return None

    # ── Mark done / failed ────────────────────────────────────────────────

    def mark_done(self, row_id: int) -> None:
        with self._immediate_txn() as cur:
            cur.execute(
                """
                UPDATE upload_queue
                   SET status='done', sent_at=?, last_error=NULL
                 WHERE id=?
                """,
                (now_iso(), row_id),
            )

    def mark_failed(self, row_id: int, error: str, *, max_retries: int) -> str:
        """
        Mark the current attempt failed. If attempts >= max_retries,
        status -> 'failed' (terminal). Otherwise revert to 'pending' so
        the drain loop will pick it up again.

        Returns the final status ('failed' or 'pending') so the caller
        can log the right thing.
        """
        with self._immediate_txn() as cur:
            cur.execute(
                "SELECT attempts FROM upload_queue WHERE id=?", (row_id,)
            )
            r = cur.fetchone()
            if r is None:
                log.warning("mark_failed: id=%d not found", row_id)
                return "missing"

            new_status = "failed" if r["attempts"] >= max_retries else "pending"
            cur.execute(
                """
                UPDATE upload_queue
                   SET status=?, last_error=?, claimed_at=NULL
                 WHERE id=?
                """,
                (new_status, error[:1000], row_id),  # cap error text
            )
            return new_status

    def requeue(self, row_id: int, *, reason: str | None = None) -> None:
        """
        Send a claimed row back to pending WITHOUT counting it as a
        failed attempt. Used after FloodWait: we waited the server-
        requested duration; the request itself wasn't a failure.
        """
        with self._immediate_txn() as cur:
            cur.execute(
                """
                UPDATE upload_queue
                   SET status='pending',
                       claimed_at=NULL,
                       attempts=MAX(0, attempts-1),
                       last_error=?
                 WHERE id=?
                """,
                (reason, row_id),
            )

    # ── Watchdog ──────────────────────────────────────────────────────────

    def reset_stuck_claimed(self, older_than_minutes: int = 10) -> int:
        """
        Revert claimed rows older than N minutes back to pending. Run
        once at startup — that's when the previous dispatcher's crashed
        claims are sitting in limbo. Returns the number of rows reset.

        We decrement attempts because the claim incremented it; the row
        was never actually attempted-to-completion, so it deserves its
        retry budget back.

        Cutoff is computed in Python using the SAME format now_iso()
        writes ("...THH:MM:SSZ"). claimed_at is compared as a string, so
        both operands MUST share one format — lexical order only equals
        chronological order when the encoding is identical. SQLite's
        datetime('now', ...) yields "YYYY-MM-DD HH:MM:SS" (space, no Z),
        which sorts WRONG against our stored "T...Z" values.
        """
        cutoff = (
            datetime.now(timezone.utc) - timedelta(minutes=older_than_minutes)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._immediate_txn() as cur:
            cur.execute(
                """
                UPDATE upload_queue
                   SET status='pending',
                       claimed_at=NULL,
                       attempts=MAX(0, attempts-1),
                       last_error='startup watchdog: reset stuck claim'
                 WHERE status='claimed'
                   AND claimed_at < ?
                """,
                (cutoff,),
            )
            n = cur.rowcount
            if n > 0:
                log.warning(
                    "watchdog: reset %d stuck-claimed row(s) older than %dm",
                    n, older_than_minutes,
                )
            return n

    # ── Inspection (for CLI / status) ─────────────────────────────────────

    def counts_by_status(self) -> dict[str, int]:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT status, COUNT(*) AS n FROM upload_queue GROUP BY status"
        )
        return {r["status"]: r["n"] for r in cur.fetchall()}

    def list_rows(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[QueueRow]:
        cur = self._conn.cursor()
        if status:
            cur.execute(
                """
                SELECT * FROM upload_queue
                WHERE status=?
                ORDER BY priority ASC, submitted_at ASC
                LIMIT ? OFFSET ?
                """,
                (status, limit, offset),
            )
        else:
            cur.execute(
                """
                SELECT * FROM upload_queue
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )
        return [QueueRow.from_row(r) for r in cur.fetchall()]

    def get(self, row_id: int) -> QueueRow | None:
        cur = self._conn.cursor()
        cur.execute("SELECT * FROM upload_queue WHERE id=?", (row_id,))
        r = cur.fetchone()
        return QueueRow.from_row(r) if r else None

    # ── Manual operations (CLI) ───────────────────────────────────────────

    def retry(self, row_id: int) -> bool:
        """Reset a failed/done row back to pending with attempts=0."""
        with self._immediate_txn() as cur:
            cur.execute(
                """
                UPDATE upload_queue
                   SET status='pending', attempts=0, claimed_at=NULL,
                       last_error=NULL, sent_at=NULL
                 WHERE id=?
                """,
                (row_id,),
            )
            return cur.rowcount == 1

    def cancel(self, row_id: int) -> bool:
        """Force a row to 'failed' status manually."""
        with self._immediate_txn() as cur:
            cur.execute(
                """
                UPDATE upload_queue
                   SET status='failed',
                       last_error='manually cancelled',
                       claimed_at=NULL
                 WHERE id=? AND status IN ('pending', 'claimed')
                """,
                (row_id,),
            )
            return cur.rowcount == 1
