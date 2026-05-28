"""
archiver.dispatch_client
────────────────────────
Thin client that enqueues upload jobs into dispatcher.db.

Deliberately does NOT import dispatcher.db.QueueDB. The contract between
archiver and dispatcher is the SQLite schema, not a Python class. This
keeps archiver installable and runnable without the dispatcher package
present (feature flag off → this module is never constructed).

The INSERT mirrors QueueDB.enqueue's idempotency: INSERT OR IGNORE on
(source, file_path). Re-running `archiver run` before the dispatcher has
drained won't create duplicate jobs.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

ARCHIVER_PRIORITY = 10   # archiver drains before recorder (priority 20)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class DispatchClient:
    """One connection to dispatcher.db for the duration of a run.

    Use as a context manager so the connection is always closed:
        with DispatchClient(path) as dc:
            dc.enqueue(...)
    """

    def __init__(self, dispatcher_db_path: str):
        self._path = Path(dispatcher_db_path).expanduser()
        if not self._path.exists():
            raise RuntimeError(
                f"dispatcher.db not found at {self._path}. Start the "
                f"dispatcher once to create it, or set ARCHIVER_USE_DISPATCHER"
                f"=false to use the legacy direct-upload path."
            )
        self.conn = sqlite3.connect(str(self._path), timeout=10.0)
        self.conn.execute("PRAGMA busy_timeout=5000")

    def __enter__(self) -> "DispatchClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.conn.close()

    def enqueue(
        self,
        *,
        platform:  str,
        username:  str,
        file_path: str,
        caption:   str | None,
        priority:  int = ARCHIVER_PRIORITY,
    ) -> bool:
        """
        Insert one job. Returns True if a row was inserted, False if it
        already existed (idempotent). source is hard-coded 'archiver' —
        it's both the dispatcher's routing/diagnostic tag and the join key
        the reconcile bridge filters on.
        """
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO upload_queue
                 (source, platform, username, file_path, caption,
                  priority, submitted_at, status)
               VALUES ('archiver', ?, ?, ?, ?, ?, ?, 'pending')""",
            (platform, username, file_path, caption, priority, _now_iso()),
        )
        self.conn.commit()
        return cur.rowcount > 0
