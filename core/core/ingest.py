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
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable

from . import identity, media_prep, stability
from .dedup import _pick_winner
from .files import cleanup_sidecars
from .grouping import split_group_key
from .hashing import full_hash
from .models import Status
from .stores import ProducerStore

log = logging.getLogger(__name__)


class IngestOutcome(str, Enum):
    """What register_file did. str-Enum so it logs/serializes as plain text."""
    INSERTED       = "inserted"        # new content → new pending row
    REARMED        = "rearmed"         # bytes' only twin had FAILED; row reset to pending
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
        """True when this call made content newly claimable — a brand-new row
        (INSERTED) or a re-armed previously-failed twin (REARMED)."""
        return self.outcome in (IngestOutcome.INSERTED, IngestOutcome.REARMED)


def register_file(
    store:    ProducerStore,
    path:     Path,
    *,
    source:    str,
    platform:  str,
    username:  str,
    chat_id:    str | None = None,
    group_key:  str | None = None,
    caption:    str | None = None,
    priority:   int = 100,
    identifier: str | None = None,
    topic_id:   int | None = None,
) -> IngestResult:
    """Register one finished media file as a pending upload. Never raises for
    an expected condition (unstable / unreadable / duplicate) — it reports the
    outcome so a bulk scan can keep going.

    `identifier` overrides the resolver's identifier (date/title still come
    from the resolver chain). Producers with their own stable identity scheme
    — the recorder's `recorder_<stem>` — pass it so a file registered live and
    the same file re-registered by a sweep collide on UNIQUE(platform,
    identifier) instead of duplicating."""
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

    # 4. resolve identity (sidecar > filename > path-hash fallback). An
    #    explicit producer identifier wins; date/title still resolve normally.
    ident = identity.resolve(path)

    # 5. insert — writing the row IS the enqueue.
    inserted = store.add_item(
        source          = source,
        platform        = platform,
        username        = username,
        identifier      = identifier or ident.identifier,
        file_path       = str(path),
        upload_date     = ident.upload_date,
        title           = ident.title,
        caption         = caption,
        priority        = priority,
        content_hash    = digest,
        chat_id         = chat_id,
        group_key       = group_key,
        topic_id        = topic_id,
    )
    if not inserted:
        # Lost a race on UNIQUE(platform, identifier) — another row claimed
        # this identity between our checks and the insert. Treat as known.
        return IngestResult(IngestOutcome.ALREADY_KNOWN, content_hash=digest)

    return IngestResult(IngestOutcome.INSERTED,
                        item_id=store.id_of(str(path)),
                        content_hash=digest)


# Outcomes that mean "this output is handled" — a row exists, was re-armed, or
# its bytes were deduped away. UNSTABLE / HASH_FAILED mean "couldn't register,
# retry next pass" and must NOT let us retire the original (its bytes would be
# lost). Mirrors archiver.reconcile._register's accounted/not-accounted split.
_ACCOUNTED = frozenset({
    IngestOutcome.INSERTED, IngestOutcome.ALREADY_KNOWN,
    IngestOutcome.DEDUP_DROPPED, IngestOutcome.DEDUP_ADOPTED,
    IngestOutcome.REARMED,
})


@dataclass
class PreparedResult:
    """Aggregate outcome of register_media: Telegram-prep one source file, then
    register every output it produced (one for a passthrough/convert, several
    for an oversize split). `outcomes` is per-output, in output order."""
    outcomes:    list[IngestOutcome] = field(default_factory=list)
    prep_ok:     bool                = True
    transformed: bool                = False   # prep replaced the original
    error:       str | None          = None
    busy:        bool                = False   # another worker is preparing this
                                               # same source now; skip, retry next

    @property
    def inserted(self) -> int:
        return sum(1 for o in self.outcomes if o == IngestOutcome.INSERTED)

    @property
    def any_inserted(self) -> bool:
        """True when this call made content newly claimable (a fresh row or a
        re-armed failed twin) — the register_file.inserted contract, lifted to
        the multi-output case so callers keep a single 'did anything queue?'."""
        return any(o in (IngestOutcome.INSERTED, IngestOutcome.REARMED)
                   for o in self.outcomes)

    @property
    def all_accounted(self) -> bool:
        return bool(self.outcomes) and all(o in _ACCOUNTED for o in self.outcomes)


def register_media(
    store:    ProducerStore,
    path:     Path,
    *,
    source:    str,
    platform:  str,
    username:  str,
    chat_id:    str | None = None,
    caption:    str | None = None,
    priority:   int = 100,
    topic_id:   int | None = None,
    group_key:  str | None = None,
    split_threshold_bytes: int | None = None,
    identifier_for: Callable[[Path], str] | None = None,
    caption_for:    Callable[[Path], str | None] | None = None,
    album_split_parts: bool = True,
    retire_original:   Callable[[Path], None] | None = None,
) -> PreparedResult:
    """Telegram-prep a file, then register every output as a pending upload.

    This is the media-prep layer ABOVE register_file — the same prepare →
    register-each-output → retire-original skeleton core.orphaned and
    archiver.reconcile run, lifted into core so the recorder's live enqueue and
    startup sweep share one Telegram-readiness guarantee. A producer that goes
    through here can never enqueue a file Telegram rejects at upload
    (FilePartsInvalid): a non-streamable video is converted, one over the upload
    ceiling (or `split_threshold_bytes`, recorder split mode) is split into
    parts, and EACH output is registered. Images and already-streamable
    in-ceiling files pass straight through (one cheap ffprobe), so the common
    case behaves exactly as a bare register_file.

    `split_threshold_bytes` lowers the split trigger below the ~3.9 GiB ceiling
    (recorder split mode); None keeps the ceiling. `identifier_for(out)` /
    `caption_for(out)` override identity/caption per output (the recorder's
    recorder_<stem> scheme); a None from caption_for falls back to `caption`.
    Split parts share one album group_key minted from the ORIGINAL stem when
    `album_split_parts`, so the dispatcher ships them as one ordered batch. An
    explicit `group_key` overrides that mint and is applied to EVERY output — a
    producer stamps it to album a set of files spanning separate register_media
    calls (the recorder's reconnect-stitched broadcast segments).
    `retire_original(path)` is invoked once prep replaced the original AND every
    output is accounted for — the caller owns the delete policy / safebrake; it
    is skipped if any output failed to register, so source bytes are never lost.

    Never raises for an expected condition — a prep or register failure is
    reported, the original is left on disk, and the caller keeps going."""
    path = Path(path)
    # Stabilize BEFORE prep — the same "a half-written file must never get a row"
    # guard register_file runs, lifted ABOVE prepare(). prepare() probes the file
    # (ffprobe) to decide convert/split/passthrough; probing a file that is still
    # being written reads an incomplete stream, and a passthrough of that raw file
    # would enqueue bytes that only look like a valid video once the write
    # finishes — exactly the wedge that had a raw HEVC re-encoded on every send.
    # is_stable's stat-sleep-stat catches an in-flight copy; register_file re-checks
    # each output, so this is the outer of two guards, not a replacement.
    if not stability.is_stable(path):
        return PreparedResult(
            prep_ok=False,
            error=f"not yet stable (still being written): {path.name}")
    try:
        prep = media_prep.prepare(path, split_threshold_bytes=split_threshold_bytes)
    except Exception as e:                       # pragma: no cover — defensive
        log.exception("register_media: prepare raised on %s", path)
        return PreparedResult(prep_ok=False, error=str(e))
    if prep.busy:
        # Another worker's sweep is already converting/splitting this exact file.
        # Not a failure and not raw-registerable — leave it for the next sweep,
        # by which time the holder will have registered it and retired the source.
        return PreparedResult(prep_ok=True, busy=True)
    if not prep.ok:
        return PreparedResult(prep_ok=False, error=prep.error)

    # Split parts of one original share a synthetic album key, minted from the
    # ORIGINAL stem so every part — whatever its per-part name — lands in the
    # same ordered album rather than going out as separate messages.
    #
    # An explicit caller `group_key` overrides the per-file split mint: it lets a
    # producer album a SET of files that arrive through separate register_media
    # calls (the recorder stamps every segment of one reconnect-stitched
    # broadcast with one key). It also wins when a segment is itself oversize and
    # split — those sub-parts then join the broadcast album rather than forming
    # their own, so the whole broadcast still ships as one ordered batch.
    album_gk = group_key if group_key is not None else (
        split_group_key(platform, username, path.stem)
        if prep.individual and album_split_parts else None)

    outcomes: list[IngestOutcome] = []
    for out in prep.outputs:
        ident = identifier_for(out) if identifier_for else None
        cap = caption_for(out) if caption_for else None
        res = register_file(
            store, out,
            source     = source,
            platform   = platform,
            username   = username,
            chat_id    = chat_id,
            group_key  = album_gk,
            caption    = cap if cap is not None else caption,
            priority   = priority,
            identifier = ident,
            topic_id   = topic_id,
        )
        outcomes.append(res.outcome)

    result = PreparedResult(outcomes=outcomes, prep_ok=True,
                            transformed=prep.transformed)
    if (prep.transformed and retire_original is not None
            and result.all_accounted):
        retire_original(path)
    return result


def _collapse(
    store:  ProducerStore,
    incoming: Path,
    twin:   "object",   # core.models.Item; avoid import cycle in annotation
    digest: str,
) -> IngestResult:
    """Resolve a byte-identical collision between an incoming file and an
    existing row's file. Keep exactly ONE physical copy — the winner by
    core.dedup rules (canonical name > sidecar > has-row > earliest > path).

    A second row is never created. But if the twin had permanently FAILED its
    bytes were never delivered, so collapsing onto it silently would lose the
    re-introduced content. In that case we keep the winning copy AND re-arm the
    row (failed → pending) so it sends — reported as REARMED. find_by_content_
    hash prefers a deliverable twin, so a 'failed' twin means it's the only one."""
    failed = (twin.status == Status.FAILED.value)
    existing = Path(twin.file_path)

    def _finish(adopted: bool) -> IngestResult:
        """Apply the re-arm (if the twin had failed) and pick the outcome."""
        if failed and store.rearm_failed(twin.id):
            log.info("ingest: re-arm failed twin id=%d from %s "
                     "(bytes never delivered)", twin.id, incoming.name)
            return IngestResult(IngestOutcome.REARMED, item_id=twin.id,
                                content_hash=digest)
        outcome = (IngestOutcome.DEDUP_ADOPTED if adopted
                   else IngestOutcome.DEDUP_DROPPED)
        return IngestResult(outcome,
                            item_id=(twin.id if adopted else None),
                            content_hash=digest)

    # If the twin's file vanished, the incoming copy simply takes its place:
    # re-point the row, no deletion needed.
    if not existing.exists():
        store.relink_file(twin.id, str(incoming))
        log.info("ingest: dedup adopt (twin file gone) %s → row id=%d",
                 incoming.name, twin.id)
        return _finish(adopted=True)

    # db_meta: the twin has a row (use its discovered_at); the incoming file
    # has none yet. _pick_winner reads None as "no row".
    winner, _losers = _pick_winner(
        [incoming, existing],
        {incoming: None, existing: twin.discovered_at},
    )

    if winner == existing:
        # Existing copy wins → incoming is the redundant one. Delete it "as if
        # never there" (file + sidecars); the row keeps pointing at `existing`.
        cleanup_sidecars(str(incoming))
        log.info("ingest: dedup drop %s (dup of row id=%d)",
                 incoming.name, twin.id)
        return _finish(adopted=False)

    # Incoming wins (better/canonical name) → ADOPT: re-point the row at the
    # incoming file, then retire the old copy.
    store.relink_file(twin.id, str(incoming))
    cleanup_sidecars(str(existing))
    log.info("ingest: dedup adopt %s → row id=%d (retired %s)",
             incoming.name, twin.id, existing.name)
    return _finish(adopted=True)
