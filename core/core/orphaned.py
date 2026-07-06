"""
core.orphaned
─────────────
Ingest "loose" files that belong to no platform. The user drops them into
top-level folders under output_dir whose NAME is the Telegram chat_id they
should be sent to:

    output_dir/<chat_id>/<subpath…>/<file>

The folder name IS the routing authority — no env var, no config lookup. Each
file becomes a normal pending item (source='orphaned') and flows through the
same dedup, batching, send, and delete paths as everything else.

GROUPING / CAPTION
  - file in a SUBFOLDER  → album per subfolder. group_key='<chat_id>/<sub>',
    no per-file caption; the dispatcher builds 'sub\\nfile1\\nfile2' at send.
  - file DIRECTLY in the chat_id folder → sent INDIVIDUALLY, one message per
    file with its own filename as caption. Forced by a unique batch key
    (group_key=NULL, caption=the filename) so claim_batch never groups two of
    them; the displayed caption is still the stem.

DISCRIMINATOR (the safety-critical bit)
  A top-level folder is a route dir iff its name is NOT a known platform AND
  is a syntactically valid chat_id (core.routing.is_chat_id). Anything else is
  skipped with a warning — we never guess, because a wrong guess uploads
  private content to the wrong place.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from . import media_prep, stability
from .dedup import MEDIA_EXTENSIONS
from .deletion import DeletionGuard
from .files import cleanup_sidecars, ORPHANED_SOURCE_NAME
from .ingest import register_file, IngestOutcome
from .routing import parse_route
from .grouping import split_group_key
from .store import ItemStore

log = logging.getLogger(__name__)

# Quarantine for files that fail to hash (unreadable / corrupt). Stored as one
# JSON blob {path: mtime_ns} in the metadata table so a repeated auto-ingest
# pass doesn't re-read + re-warn on the same bad file every cycle. A file is
# re-attempted only once its mtime changes (it was replaced/fixed). One read +
# at most one write per folder — never a per-file query.
_HASHFAIL_META_KEY = "orphaned_hashfail"

# Same mtime-keyed memo, but for media-prep: records a file we converted/split
# whose ORIGINAL we then could NOT delete (delete-after-split off, or the scope
# is safebraked) so we don't re-convert/re-split it on every sweep. Also records
# a prep FAILURE (bad/oversize file ffmpeg or AutoSplitter couldn't handle) so a
# wedged file isn't reprocessed each cycle. Cleared when the file's mtime moves.
_PREPPED_META_KEY = "orphaned_prepped"

# Files this ingester accepts. The canonical MEDIA_EXTENSIONS plus the extra
# containers media_prep can rescue by converting them to a streamable .mp4.
_INGESTABLE_EXTS = MEDIA_EXTENSIONS | media_prep.PREP_VIDEO_EXTS

# THE definition lives in core.files (ORPHANED_SOURCE_NAME): files.py is the
# low-level leaf this module already imports, and album_bucket needs the value
# there without importing back up (which WOULD be circular). Re-exported here
# under its canonical name so every existing `from core.orphaned import
# ORPHANED_SOURCE` (and core.__init__) keeps working unchanged.
ORPHANED_SOURCE = ORPHANED_SOURCE_NAME
# Synthetic platform value for orphaned rows. Keeps them out of every
# platform/recorder album (source + platform are both in the claim key) and
# out of the archiver's per-platform reconcile loop.
ORPHANED_PLATFORM = "orphaned"

# Lower numbers drain first. Live recordings default to 5 and normal archive
# items to 10, so chat_id-folder uploads sit directly between them.
CHAT_ID_PRIORITY = 6

# Any NON-STREAMABLE video original (its container/codec won't play inline) is
# uploaded alongside its streamable conversion rather than being deleted: the
# user wants the full-quality source archived in Telegram AND a previewable
# .mp4 in the folder's album. The kept original is sent as an individual
# document (never albumed with its own preview); the dispatcher skips its
# streamable net for source='orphaned', so the bytes go up as-is. The signal is
# prep.converted (a format conversion happened), not the extension — see
# ingest_folder; media_prep.is_nonstreamable_video makes the matching send-side
# call.
#
# EXCEPT these containers: they are transient/low-value wrappers (e.g. raw .flv
# stream dumps) where only the converted .mp4 is worth keeping. The original is
# converted and then deleted as usual — no document upload.
KEEP_ORIGINAL_SKIP_EXTS = {".flv"}


@dataclass
class OrphanedReport:
    """Per chat_id-folder result, str()-able into a log line."""
    chat_id:   str
    scanned:   int  = 0
    inserted:  int  = 0
    deduped:   int  = 0   # DEDUP_DROPPED + DEDUP_ADOPTED
    known:     int  = 0   # ALREADY_KNOWN
    unstable:  int  = 0
    failed:    int  = 0   # HASH_FAILED
    skipped_dir: bool = False   # set when the top-level name wasn't a chat_id
    errors:    list[str] = field(default_factory=list)

    def __str__(self) -> str:
        if self.skipped_dir:
            return f"[orphaned] {self.chat_id}: SKIPPED (not a platform or chat_id)"
        return (
            f"[orphaned] {self.chat_id}: scanned={self.scanned}, "
            f"+{self.inserted}, deduped={self.deduped}, known={self.known}, "
            f"unstable={self.unstable}, failed={self.failed}"
        )


def ingest_chat_id_dirs(
    store:           ItemStore,
    output_dir:      str | Path,
    *,
    known_platforms: list[str] | set[str],
    priority:        int = CHAT_ID_PRIORITY,
    guard:           DeletionGuard | None = None,
) -> list[OrphanedReport]:
    """Scan output_dir's top-level folders; ingest every chat_id-named one.
    Returns one report per top-level folder considered.

    `guard` (optional) gates deletion of an original that media_prep replaced
    with a converted/split copy: a safebraked scope keeps its original."""
    base = Path(output_dir)
    reports: list[OrphanedReport] = []
    if not base.exists():
        return reports

    known = {p.lower() for p in known_platforms}
    for entry in sorted(base.iterdir()):
        try:
            if not entry.is_dir():
                continue
        except OSError:
            continue
        name = entry.name
        # Skip dotfolders (e.g. media_prep working dirs, macOS cruft) silently —
        # they are never a destination chat_id.
        if name.startswith("."):
            continue
        if name.lower() in known:
            continue   # a platform dir — the archiver's reconcile pass owns it
        route = parse_route(name)
        if route is None:
            log.warning(
                "orphaned: top-level dir %r is neither a known platform nor a "
                "valid chat_id — skipping (rename it to the destination chat_id, "
                "optionally with a `.t<topic_id>` suffix, to route it)", name,
            )
            reports.append(OrphanedReport(chat_id=name, skipped_dir=True))
            continue
        reports.append(ingest_folder(
            store, entry, chat_id=route.chat_id, topic_id=route.topic_id,
            priority=priority, guard=guard))
    return reports


_OUTCOME_TALLY = {
    IngestOutcome.INSERTED:      "inserted",
    IngestOutcome.REARMED:       "inserted",   # re-armed a failed twin → enqueued
    IngestOutcome.DEDUP_DROPPED: "deduped",
    IngestOutcome.DEDUP_ADOPTED: "deduped",
    IngestOutcome.ALREADY_KNOWN: "known",
    IngestOutcome.UNSTABLE:      "unstable",
    IngestOutcome.HASH_FAILED:   "failed",
}


def ingest_folder(
    store: ItemStore, folder: Path, *, chat_id: str,
    topic_id: int | None = None,
    priority: int = CHAT_ID_PRIORITY,
    guard: DeletionGuard | None = None,
) -> OrphanedReport:
    """Ingest every media file under `folder`, routed to `chat_id`. Two shapes:

      - file in a SUBFOLDER  → album per subfolder. group_key='<chat_id>/<sub>',
        no per-file caption (the dispatcher builds 'sub\\nfile1\\nfile2' at send).
      - file DIRECTLY in folder → sent INDIVIDUALLY, one message per file with
        its own filename as caption. We force that by giving each such file a
        unique batch key (group_key=NULL, caption=the filename), so claim_batch
        never groups two of them; the displayed caption is still the stem.

    MEDIA-PREP: before a file is registered it is run through media_prep, which
    makes a video Telegram-streamable (converting an incompatible format) and
    splits anything over the upload ceiling into <=1 GiB parts. One source file
    may therefore become several queue rows (the split parts, each sent as its
    own message). When prep replaces an original, that original is deleted
    (gated by `guard`'s safebrake); a protected scope keeps it and is memoized
    so it isn't reprocessed every sweep.

    Reusable for both the chat_id-folder sweep and `archiver ingest --path`,
    where `folder` is arbitrary and `chat_id` is supplied explicitly."""
    rep = OrphanedReport(chat_id=chat_id)
    quarantine, q_dirty = _load_quarantine(store), False
    prepped, p_dirty = _load_prepped(store), False
    # NOT wrapped in store.batch() — deliberately. register_file's dedup-collapse
    # can delete a file from disk (the "adopt" branch deletes the losing copy)
    # right alongside its relink_file DB write. A filesystem delete is not part
    # of the SQLite transaction, so folding these inserts into a roll-back-able
    # batch would create a window where the batch rolls back (a failed flush
    # under lock contention) AFTER the file was already deleted — leaving a row
    # pointing at a missing file. Per-file autocommit keeps each relink durable
    # before its paired delete. Loose drops are low-volume, so the lost insert
    # amortization here is immaterial; the high-volume reconcile path (pure
    # inserts, no in-loop destructive FS op) is the one that stays batched.
    for f in sorted(folder.rglob("*")):
        try:
            if not f.is_file():
                continue
        except OSError:
            continue
        # Skip dotfiles — chiefly macOS AppleDouble sidecars (._name) on
        # exFAT/FAT volumes: they pass the extension filter but are metadata
        # stubs, not media. Matches dedup.py's convention.
        if f.name.startswith("."):
            continue
        if f.suffix.lower() not in _INGESTABLE_EXTS:
            continue
        rep.scanned += 1

        key = str(f)
        try:
            mtime = str(f.stat().st_mtime_ns)
        except OSError:
            mtime = None

        # Skip a known-bad file until it changes (hash failure → still counts as
        # failed for visibility; a prep failure / deliberately-kept prepped
        # original → known, it's handled and quiet — see the two META keys).
        if mtime is not None and quarantine.get(key) == mtime:
            rep.failed += 1
            continue
        if mtime is not None and prepped.get(key) == mtime:
            rep.known += 1
            continue

        # Cheap short-circuit: this exact path already has a row. Skip prep
        # entirely (a known file is already converted/split as needed) — this is
        # what keeps repeated sweeps from re-probing every file.
        if store.has_file_path(key):
            rep.known += 1
            continue

        # Stabilize BEFORE prep — the same guard reconcile and register_media run.
        # A loose drop is where mid-copy is MOST common, and prepare() probes/
        # converts the file: probing a half-written file reads an incomplete
        # stream (a passthrough would enqueue not-yet-final bytes; a convert would
        # transcode a partial source). register_file re-checks each OUTPUT, but by
        # then prep has already acted — so the check must also gate prep's input.
        if not stability.is_stable(f):
            rep.unstable += 1
            continue

        # ── Media-prep: convert non-streamable formats, split oversize files ──
        try:
            prep = media_prep.prepare(f)
        except Exception as e:               # pragma: no cover — defensive
            rep.errors.append(f"{f.name}: prep {e}")
            log.exception("orphaned: media_prep raised on %s", f)
            continue
        if prep.busy:
            # Another worker's sweep (recorder/archiver) is already preparing this
            # exact file — skip it this pass and let that holder register it.
            # NOT memoized: we WANT to reconsider it next sweep, once it's free.
            rep.unstable += 1
            continue
        if not prep.ok:
            # Couldn't prepare safely (bad/oversize file ffmpeg or AutoSplitter
            # refused). Leave the original in place and memoize so we don't keep
            # retrying it every sweep until the user replaces it.
            if mtime is not None:
                prepped[key] = mtime
                p_dirty = True
            rep.failed += 1
            rep.errors.append(f"{f.name}: {prep.error}")
            continue

        # KEEP-ORIGINAL: a NON-STREAMABLE source (its container/codec won't play
        # inline) is converted for the album AND uploaded as its own full-quality
        # downloadable document. Register the document FIRST and individually
        # (group_key=NULL): a 'single' send waits for no min-batch, so it goes out
        # BEFORE its converted copy is batched with the folder's already-
        # streamable videos. The dispatcher skips the streamable net for
        # source='orphaned' and ships the non-streamable bytes as a document
        # (send: is_nonstreamable_video). Pure oversize splits (streamable but too
        # big) are NOT kept here — prep.converted is False — and fall through to
        # the delete-after-split policy below. Low-value containers (.flv) are
        # excluded: only their converted .mp4 is kept, the original is deleted.
        keep_original_as_doc = (
            prep.converted and f.suffix.lower() not in KEEP_ORIGINAL_SKIP_EXTS)
        if keep_original_as_doc:
            try:
                res = register_file(
                    store, f,
                    source    = ORPHANED_SOURCE,
                    platform  = ORPHANED_PLATFORM,
                    username  = chat_id,
                    chat_id   = chat_id,
                    topic_id  = topic_id,
                    group_key = None,
                    caption   = f.name,
                    priority  = priority,
                )
                setattr(rep, _OUTCOME_TALLY[res.outcome],
                        getattr(rep, _OUTCOME_TALLY[res.outcome]) + 1)
            except Exception as e:           # pragma: no cover — defensive
                rep.errors.append(f"{f.name}: keep-original {e}")
                log.exception("orphaned: register_file (keep-original) on %s", f)

        # Register every output. Split parts of one original share a synthetic
        # group_key so the dispatcher albums them (ordered) as a single batch;
        # for a passthrough or single conversion the file's location decides
        # grouping. The key is minted from the ORIGINAL stem (f), so all parts —
        # whatever their per-part names — land in the same album.
        split_gk = (split_group_key(ORPHANED_PLATFORM, chat_id, f.stem)
                    if prep.individual else None)
        outcomes: list[IngestOutcome] = []
        for out in prep.outputs:
            if split_gk is not None:
                group_key, caption = split_gk, out.name
            else:
                group_key, caption = _route_for(folder, chat_id, out)
            try:
                res = register_file(
                    store, out,
                    source    = ORPHANED_SOURCE,
                    platform  = ORPHANED_PLATFORM,
                    username  = chat_id,
                    chat_id   = chat_id,
                    topic_id  = topic_id,
                    group_key = group_key,
                    caption   = caption,
                    priority  = priority,
                )
            except Exception as e:           # pragma: no cover — defensive
                rep.errors.append(f"{out.name}: {e}")
                log.exception("orphaned: register_file raised on %s", out)
                continue
            outcomes.append(res.outcome)
            setattr(rep, _OUTCOME_TALLY[res.outcome],
                    getattr(rep, _OUTCOME_TALLY[res.outcome]) + 1)

        # Passthrough (untouched original): keep the legacy hash-failure
        # quarantine so a corrupt loose file isn't re-hashed every sweep.
        if not prep.transformed:
            if (IngestOutcome.HASH_FAILED in outcomes) and mtime is not None:
                quarantine[key] = mtime
                q_dirty = True
            elif key in quarantine:
                del quarantine[key]
                q_dirty = True
            continue

        # A kept non-streamable original (registered as a document above) is the
        # archive copy — never delete it at ingest; memoize (mtime-keyed) so the
        # next sweep skips it even if its row was dedup-collapsed onto a twin.
        if keep_original_as_doc:
            if mtime is not None:
                prepped[key] = mtime
                p_dirty = True
            continue

        # Transformed: the original has been replaced by its output(s). Only
        # retire it once EVERY output is accounted for (registered or deduped) —
        # if any registration raised we keep the original so its bytes aren't
        # lost, and memoize so the failure isn't retried forever.
        all_accounted = len(outcomes) == len(prep.outputs)
        removed = all_accounted and _delete_replaced_original(guard, chat_id, f)
        if not removed and mtime is not None:
            # Safebrake-protected, delete-after-split off, or a partial-register
            # failure: remember the kept original so the next sweep skips it.
            prepped[key] = mtime
            p_dirty = True

    if q_dirty:
        store.meta_set(_HASHFAIL_META_KEY, json.dumps(quarantine))
    if p_dirty:
        store.meta_set(_PREPPED_META_KEY, json.dumps(prepped))
    return rep


def _route_for(
    folder: Path, chat_id: str, out: Path,
) -> tuple[str | None, str | None]:
    """(group_key, caption) for a non-split output file. A file in a subfolder
    albums by subfolder; a file directly in a ROOT sends alone. (Split parts are
    keyed by the caller via split_group_key — they never reach here.)

    A `#hashtag` folder is a VIRTUAL ROOT: a file sitting directly inside one
    (`chat_id/#tag/file`) uploads INDIVIDUALLY just like a file in the chat_id
    folder itself, while a deeper subfolder under it (`chat_id/#tag/sub/file`)
    is still an album keyed by its full subpath. Detected by the file's
    IMMEDIATE parent name starting with '#'."""
    try:
        parent = out.relative_to(folder).parent
    except ValueError:
        return None, out.name
    subpath = "" if parent == Path(".") else parent.as_posix()
    # Album unless the file sits directly in a root: the chat_id folder (empty
    # subpath) or a `#hashtag` folder (immediate parent starts with '#').
    if subpath and not parent.name.startswith("#"):
        return f"{chat_id}/{subpath}", None
    # Directly in a root → its own message (see drain.orphaned_caption).
    return None, out.name


def _delete_replaced_original(
    guard: DeletionGuard | None, chat_id: str, original: Path,
) -> bool:
    """Delete an original that media_prep replaced. Returns True if removed.

    delete-after-split (default ON) gates whether we delete at all; the
    DeletionGuard safebrake can still veto a protected scope. With no guard we
    fall back to the suite's legacy unconditional cleanup."""
    if not media_prep.delete_after_split():
        return False
    if guard is not None:
        return guard.delete(
            ORPHANED_PLATFORM, chat_id, str(original),
            reason="media-prep replaced original with streamable/split copy",
        )
    cleanup_sidecars(str(original))
    return True


def _load_quarantine(store: ItemStore) -> dict[str, str]:
    """The {path: mtime_ns} hash-failure quarantine, or {} if unset/corrupt."""
    try:
        return json.loads(store.meta_get(_HASHFAIL_META_KEY) or "{}")
    except (json.JSONDecodeError, TypeError):
        return {}


def _load_prepped(store: ItemStore) -> dict[str, str]:
    """The {path: mtime_ns} media-prep memo (kept originals + prep failures),
    or {} if unset/corrupt."""
    try:
        return json.loads(store.meta_get(_PREPPED_META_KEY) or "{}")
    except (json.JSONDecodeError, TypeError):
        return {}


def subfolder_of(chat_id: str, group_key: str | None) -> str:
    """Display subfolder for an orphaned row: group_key with the leading
    '<chat_id>/' stripped. Empty when the file sat directly under the chat_id
    folder. Used by the dispatcher to build the album caption header."""
    if not group_key:
        return ""
    prefix = f"{chat_id}/"
    return group_key[len(prefix):] if group_key.startswith(prefix) else ""
