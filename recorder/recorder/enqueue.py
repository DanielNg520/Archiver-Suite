"""
recorder.enqueue
────────────────
Writes finished recordings into dispatcher.db. Same decoupling rule as
archiver.dispatch_client: connect to the SQLite file directly, never
import the dispatcher package. The schema is the contract.

source='recorder', priority=20 (archiver is 10 and drains first — a
recording is less time-sensitive than a VOD backlog since it's already
captured to disk).
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

RECORDER_PRIORITY = 20


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class EnqueueClient:
    def __init__(self, dispatcher_db_path: str):
        self._path = Path(dispatcher_db_path).expanduser()

    def enqueue(
        self,
        *,
        platform:  str,
        username:  str,
        file_path: str,
        caption:   str | None,
        priority:  int = RECORDER_PRIORITY,
    ) -> bool:
        """Insert one job. Returns True if inserted, False if it already
        existed (idempotent on source+file_path). Opens and closes a
        connection per call — recorder enqueues are infrequent (once per
        finished stream), so connection churn is irrelevant and a short-
        lived connection avoids holding a handle across long recordings."""
        if not self._path.exists():
            raise RuntimeError(
                f"dispatcher.db not found at {self._path}. Is the dispatcher "
                f"installed and started at least once?"
            )
        conn = sqlite3.connect(str(self._path), timeout=10.0)
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            cur = conn.execute(
                """INSERT OR IGNORE INTO upload_queue
                     (source, platform, username, file_path, caption,
                      priority, submitted_at, status)
                   VALUES ('recorder', ?, ?, ?, ?, ?, ?, 'pending')""",
                (platform, username, file_path, caption, priority, _now_iso()),
            )
            conn.commit()
            inserted = cur.rowcount > 0
            log.info("enqueue: %s @%s %s → %s",
                     platform, username, Path(file_path).name,
                     "queued" if inserted else "already queued")
            return inserted
        finally:
            conn.close()
