"""
core.backfill
─────────────
One-time (resumable) backfill of items.content_hash for rows created before
content-hashing existed. After this runs, the dispatcher's global-dedup
guarantee and the reconcile re-introduction guard cover EVERY tracked file,
not just newly-ingested ones — so moving an old, already-uploaded file back
into a folder is recognized by content (not just by name) and cleaned up.

PROPERTIES
  - Resumable / idempotent: only rows WHERE content_hash IS NULL are touched,
    and progress is committed in batches, so an interrupted run resumes where
    it left off and a finished run is a no-op.
  - Safe: it only fills the column; it never deletes a file or row. (On-disk
    duplicate *files* are still `archiver dedup`'s job; pending rows that turn
    out to duplicate a sent one are suppressed by the dispatcher on next drain,
    now that they have a hash.)
  - Honest about cost: it reads every file with a NULL hash in full. That's the
    point — there is no cheaper way to learn a file's content identity.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .hashing import full_hash
from .store import ItemStore

log = logging.getLogger(__name__)


@dataclass
class BackfillReport:
    scanned: int = 0   # NULL-hash rows considered
    hashed:  int = 0   # successfully hashed + updated
    missing: int = 0   # file gone on disk (left NULL — nothing to hash)
    failed:  int = 0   # unreadable (left NULL)

    def __str__(self) -> str:
        return (
            f"backfill: scanned={self.scanned}, hashed={self.hashed}, "
            f"missing={self.missing}, failed={self.failed}"
        )


def backfill_content_hashes(
    store:        ItemStore,
    *,
    batch_commit: int = 500,
    progress:     Callable[[int, int], None] | None = None,
) -> BackfillReport:
    """Fill content_hash for every row that lacks one and whose file exists.

    batch_commit: rows per commit — bounds work lost to a crash and keeps the
    write lock short. progress(done, total): optional callback for a UI/log.
    """
    rows = store.conn.execute(
        "SELECT id, file_path FROM items WHERE content_hash IS NULL"
    ).fetchall()
    total = len(rows)
    rep = BackfillReport()
    if not total:
        return rep          # clean DB — stay silent (auto-runs every cycle)
    log.info("backfill: %d row(s) without a content_hash", total)

    pending_writes = 0
    for i, row in enumerate(rows, 1):
        rep.scanned += 1
        p = Path(row["file_path"])
        if not p.exists():
            rep.missing += 1
        else:
            digest = full_hash(p)
            if digest is None:
                rep.failed += 1
            else:
                store.conn.execute(
                    "UPDATE items SET content_hash=? WHERE id=?",
                    (digest, row["id"]),
                )
                rep.hashed += 1
                pending_writes += 1
                if pending_writes >= batch_commit:
                    store.conn.commit()
                    pending_writes = 0
        if progress is not None and (i % batch_commit == 0 or i == total):
            progress(i, total)

    store.conn.commit()
    log.info("%s", rep)
    return rep
