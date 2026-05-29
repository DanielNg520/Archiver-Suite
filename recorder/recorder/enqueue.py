"""
recorder.enqueue
────────────────
Registers finished recordings in the shared suite DB via core.ItemStore.

Single source of truth: there is no separate dispatcher.db and no raw SQL
here anymore. Writing the item row IS the enqueue — the dispatcher claims
it from the same `items` table on its next poll. The recorder no longer
needs to know the dispatcher's schema; `core` owns it.

source='recorder', priority=20 (the archiver enqueues at 10 and therefore
drains first — a finished recording is less time-sensitive than a VOD
backlog, since it's already safely captured to disk).
"""

from __future__ import annotations

import logging
from pathlib import Path

from core import ItemStore

log = logging.getLogger(__name__)

RECORDER_PRIORITY = 20


def _recorder_identifier(file_path: str) -> str:
    """Synthesize the (platform, identifier) key for a recording.

    Recordings have no upstream post id, so we derive a stable identifier
    from the filename stem. This MUST match core.migrate's scheme
    (`recorder_<stem>`) so a recording migrated from the legacy queue and
    the same file re-enqueued live collide on UNIQUE(platform, identifier)
    instead of duplicating. The real per-file dedup guarantee is the
    separate UNIQUE(file_path) constraint; this key just has to be present
    and stable.
    """
    return f"recorder_{Path(file_path).stem or 'item'}"


class EnqueueClient:
    """Opens a short-lived ItemStore per enqueue call.

    A recording runs for minutes-to-hours; we deliberately do NOT hold a
    DB handle open across that window. Enqueues happen once per finished
    stream, so per-call connect/close churn is irrelevant, and a short-
    lived connection avoids keeping a WAL handle (and any lock) alive
    while nothing is being written.
    """

    def __init__(self, db_path: str | None = None):
        # None → core resolves the default suite DB ($ARCHIVER_DB or the
        # packaged default). No "db not found" guard: core.connect() runs
        # CREATE TABLE IF NOT EXISTS idempotently, so whichever process
        # connects first creates the schema. This is what removes the old
        # install-order requirement.
        self._db_path = db_path

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
        existed (idempotent on file_path / synthesized identifier)."""
        store = ItemStore.open(self._db_path)
        try:
            inserted = store.add_item(
                source     = "recorder",
                platform   = platform,
                username   = username,
                identifier = _recorder_identifier(file_path),
                file_path  = file_path,
                caption    = caption,
                priority   = priority,
            )
            log.info("enqueue: %s @%s %s → %s",
                     platform, username, Path(file_path).name,
                     "queued" if inserted else "already queued")
            return inserted
        finally:
            store.close()
