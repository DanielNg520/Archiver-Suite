"""
core.ingest
───────────
The ONE primitive every producer funnels a finished file through to become a
claimable item row. Replaces the scattered "stat → resolve → add_item" snippets
the archiver/recorder open-coded, and is the only path loose/orphaned files have.

TEMPLATE METHOD — register_file runs a fixed skeleton:

    stabilize → hash → dedup-collapse → resolve identity → insert

Each step is a private function so a future producer can override one without
re-deriving the others. The order is the contract:

  1. stabilize FIRST. A half-written file must never get a row — a row makes it
     claimable, and the dispatcher would upload (and then delete) garbage. This
     is the load-bearing guard for loose-file drops, where mid-copy is common.

  2. hash before insert so EVERY row carries content_hash. The dispatcher's
     global-dedup guarantee is only as good as this stamp being universal.

  3. dedup-collapse BEFORE inserting a second row. Global dedup means "these
     exact bytes are removed as if never there": if a row already holds this
     content_hash we keep exactly one physical copy (the better-named one, via
     core.dedup winner rules) and never create a duplicate row.

  4/5. identity + insert only happen for genuinely new content.

ATOMICITY: ingestion runs in a producer's single-process pass, so the
read-then-write on content_hash isn't globally atomic — but the items table's
UNIQUE(file_path) and UNIQUE(platform, identifier) constraints are the backstop
that rejects a racing duplicate, so the worst case is a redundant hash, never a
double row.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from . import identity, stability
from .dedup import _pick_winner
from .files import cleanup_sidecars
from .hashing import full_hash
from .store import ItemStore

log = logging.getLogger(__name__)


class IngestOutcome(str, Enum):
    """What register_file did. str-Enum so it logs/serializes as plain text."""
    INSERTED       = "inserted"        # new content → new pending row
    DEDUP_DROPPED  = "dedup_dropped"   # bytes already known; incoming file deleted
    DEDUP_ADOPTED  = "dedup_adopted"   # incoming file won on name; row re-pointed, old deleted
    ALREADY_KNOWN  = "already_known"   # this exact file_path already has a row
    UNSTABLE       = "unstable"        # still being written; skipped this pass
    HASH_FAILED    = "hash_failed"     # unreadable; skipped


@dataclass(frozen=True)
class IngestResult:
    outcome:      IngestOutcome
    item_id:      int | None = None    # set for INSERTED / DEDUP_ADOPTED
    content_hash: str | None = None

    @property
    def inserted(self) -> bool:
        return self.outcome is IngestOutcome.INSERTED


def register_file(
    store:    ItemStore,
    path:     Path,
    *,
    source:    str,
    platform:  str,
    username:  str,
    chat_id:   str | None = None,
    group_key: str | None = None,
    caption:   str | None = None,
    priority:  int = 100,
) -> IngestResult:
    """Register one finished media file as a pending upload. Never raises for
    an expected condition (unstable / unreadable / duplicate) — it reports the
    outcome so a bulk scan can keep going."""
    path = Path(path)

    # 1. stabilize — refuse to register a file that's still being written.
    if not stability.is_stable(path):
        return IngestResult(IngestOutcome.UNSTABLE)

    # Cheap short-circuit: this exact path is already tracked.
    if store.has_file_path(str(path)):
        return IngestResult(IngestOutcome.ALREADY_KNOWN)

    # 2. hash — the global-dedup key, stamped on every row.
    digest = full_hash(path)
    if digest is None:
        return IngestResult(IngestOutcome.HASH_FAILED)

    # 3. dedup-collapse — if these exact bytes already have a row, keep one copy.
    twin = store.find_by_content_hash(digest)
    if twin is not None:
        return _collapse(store, path, twin, digest)

    # 4. resolve identity (sidecar > filename > path-hash fallback).
    ident = identity.resolve(path)

    # 5. insert — writing the row IS the enqueue.
    inserted = store.add_item(
        source          = source,
        platform        = platform,
        username        = username,
        identifier      = ident.identifier,
        file_path       = str(path),
        upload_date     = ident.upload_date,
        title           = ident.title,
        caption         = caption,
        priority        = priority,
        content_hash    = digest,
        chat_id         = chat_id,
        group_key       = group_key,
    )
    if not inserted:
        # Lost a race on UNIQUE(platform, identifier) — another row claimed
        # this identity between our checks and the insert. Treat as known.
        return IngestResult(IngestOutcome.ALREADY_KNOWN, content_hash=digest)

    row = store.conn.execute(
        "SELECT id FROM items WHERE file_path=?", (str(path),),
    ).fetchone()
    return IngestResult(IngestOutcome.INSERTED,
                        item_id=row["id"] if row else None,
                        content_hash=digest)


def _collapse(
    store:  ItemStore,
    incoming: Path,
    twin:   "object",   # core.models.Item; avoid import cycle in annotation
    digest: str,
) -> IngestResult:
    """Resolve a byte-identical collision between an incoming file and an
    existing row's file. Keep exactly ONE physical copy — the winner by
    core.dedup rules (canonical name > sidecar > has-row > earliest > path) —
    and never create a second row."""
    existing = Path(twin.file_path)

    # If the twin's file vanished, the incoming copy simply takes its place:
    # re-point the row, no deletion needed.
    if not existing.exists():
        store.relink_file(twin.id, str(incoming))
        log.info("ingest: dedup adopt (twin file gone) %s → row id=%d",
                 incoming.name, twin.id)
        return IngestResult(IngestOutcome.DEDUP_ADOPTED, item_id=twin.id,
                            content_hash=digest)

    # db_meta: the twin has a row (use its discovered_at); the incoming file
    # has none yet. _pick_winner reads None as "no row".
    winner, _losers = _pick_winner(
        [incoming, existing],
        {incoming: None, existing: twin.discovered_at},
    )

    if winner == existing:
        # Existing copy wins → incoming is the redundant one. Delete it "as if
        # never there" (file + sidecars). No new row.
        cleanup_sidecars(str(incoming))
        log.info("ingest: dedup drop %s (dup of row id=%d)",
                 incoming.name, twin.id)
        return IngestResult(IngestOutcome.DEDUP_DROPPED, content_hash=digest)

    # Incoming wins (better/canonical name) → ADOPT: re-point the row at the
    # incoming file, then retire the old copy.
    store.relink_file(twin.id, str(incoming))
    cleanup_sidecars(str(existing))
    log.info("ingest: dedup adopt %s → row id=%d (retired %s)",
             incoming.name, twin.id, existing.name)
    return IngestResult(IngestOutcome.DEDUP_ADOPTED, item_id=twin.id,
                        content_hash=digest)
