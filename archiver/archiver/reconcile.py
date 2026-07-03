"""
archiver.reconcile
──────────────────
"Reconcile v2": walk the on-disk archive, register every stable media
file in the DB, and seed the per-extractor archives so the next
download pass doesn't try to re-fetch already-present content.

This replaces the simple `db.reconcile()` of v1 in three ways:

  1. Uses `core.stability.is_stable()` to skip half-written files
     instead of registering them as broken pending uploads.

  2. Uses `core.identity.resolve()` for every file, so:
       - sidecar JSONs (when present) drive the identifier/date/title
       - manual files (no sidecar, no filename pattern) still get a
         stable hash-based identifier and an mtime-based date
       - the same logic runs whether the file was just downloaded or
         dropped in by the user 6 months ago

  3. AFTER inserting DB rows, seeds the per-platform extractor archives
     (gallery-dl sqlite for X/Instagram, yt-dlp txt for TikTok). This
     is the bootstrap step that prevents a 5,000-post account from
     re-walking its entire timeline on first run with a pre-existing
     archive.

Used by:
  - `Archiver._archive_user()` — every normal run (catches new manual files,
    crashed-mid-download orphans, etc.)
  - `cli.cmd_bootstrap` — explicit "I just dropped my whole archive here,
    teach the system about it" operation.

Both call the same function. Bootstrap is just reconcile + log + advance
checkpoint based on the discovered MAX(upload_date).
"""

from __future__ import annotations

import logging
import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from core import (
    identity, stability, cleanup_sidecars, DeletionGuard, media_prep,
    split_group_key,
)
from core.hashing import full_hash

# Upload priority for re-registered recordings. MUST match the recorder's live
# enqueue (recorder.enqueue.RECORDER_PRIORITY) so a reconciled recording and a
# freshly-recorded one drain in the same order. Default 5 = ahead of the
# archiver's VOD backlog (10); override with $RECORDER_UPLOAD_PRIORITY.
_RECORDER_PRIORITY = int(os.environ.get("RECORDER_UPLOAD_PRIORITY", "5"))

if TYPE_CHECKING:
    from core import ProducerStore
    from .platforms import Platform

log = logging.getLogger(__name__)

# The canonical "files we consider media" set — one definition in core.files.
# Sidecars (.json) are excluded by construction.
from core.files import MEDIA_EXTENSIONS  # noqa: E402

ROOT_CLUSTER_MIN_PREFIX = 5
RECORDER_CONFIG_TOML = Path.home() / ".config" / "recorder" / "config.toml"
RECORDER_DEFAULT_OUTPUT_DIR = Path.home() / "recorder-output"


@dataclass
class ReconcileReport:
    """Per-(platform, user) result. Aggregated into bootstrap output."""
    platform:        str
    username:        str
    scanned:         int = 0
    skipped_unstable: int = 0
    inserted:        int = 0
    already_known:   int = 0
    manual_files:    int = 0   # subset of `inserted` with is_manual=True
    seeded_archive:  int = 0
    deleted_dupes:   int = 0   # re-introduced files whose bytes were already sent
    prep_failed:     int = 0   # convertible containers media_prep couldn't rescue
    max_upload_date: str | None = None
    archive_entries: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        manual_note = f" ({self.manual_files} manual)" if self.manual_files else ""
        seed_note = f", seeded {self.seeded_archive}" if self.seeded_archive else ""
        dup_note = f", deleted {self.deleted_dupes} dup" if self.deleted_dupes else ""
        prep_note = f", prep-failed {self.prep_failed}" if self.prep_failed else ""
        return (
            f"[{self.platform}] @{self.username}: scanned {self.scanned}, "
            f"+{self.inserted}{manual_note}, known {self.already_known}, "
            f"unstable {self.skipped_unstable}{seed_note}{dup_note}{prep_note}, "
            f"floor={self.max_upload_date or '-'}"
        )


def reconcile_user(
    platform: "Platform",
    username: str,
    db: "ProducerStore",
    output_dir: str,
    seed_extractor_archive: bool = True,
    guard: "DeletionGuard | None" = None,
) -> ReconcileReport:
    """
    Walk {output_dir}/{platform.name}/{username}/ (RECURSIVE — picks up
    subfolders the user manually adds).

    For each stable media file: resolve identity, INSERT-OR-IGNORE into DB.
    After the walk, optionally seed the platform's extractor archive
    with all known non-manual identifiers.

    `seed_extractor_archive=False` is useful in tight test loops; in
    production, leave it True — it's cheap (it only writes entries that
    aren't already there) and crucial for correctness.
    """
    report = ReconcileReport(platform=platform.name, username=username)
    user_dir = Path(output_dir) / platform.name / username
    return _reconcile_dir(
        platform=platform,
        username=username,
        db=db,
        scan_dir=user_dir,
        recursive=True,
        seed_extractor_archive=seed_extractor_archive,
        report=report,
        guard=guard,
    )


def reconcile_platform_root(
    platform: "Platform",
    db: "ProducerStore",
    output_dir: str,
    guard: "DeletionGuard | None" = None,
) -> ReconcileReport:
    """
    Reconcile media files directly inside {output_dir}/{platform.name}/.

    This intentionally scans only direct child files. Per-user subfolders
    are handled by reconcile_user(), and recursively walking the platform
    root would double-scan every configured user directory.
    """
    username = "_root"
    report = ReconcileReport(platform=platform.name, username=username)
    platform_dir = Path(output_dir) / platform.name
    captions = _loose_root_captions(platform_dir)
    return _reconcile_dir(
        platform=platform,
        username=username,
        db=db,
        scan_dir=platform_dir,
        recursive=False,
        seed_extractor_archive=False,
        report=report,
        source="archiver",
        caption_for_path=lambda path: captions.get(path),
        guard=guard,
    )


def reconcile_recordings(
    db: "ProducerStore",
    records_dir: str | Path | None = None,
    guard: "DeletionGuard | None" = None,
) -> list[ReconcileReport]:
    """
    Reconcile TikTok recorder output into the shared upload queue.

    Recorder writes {output_dir}/{username}/... files, so each direct
    subfolder is treated as a recorded TikTok user. Loose files directly in
    the recorder root are queued as @ _root.
    """
    root = Path(records_dir).expanduser() if records_dir else _recorder_output_dir()
    reports: list[ReconcileReport] = []
    if not root.exists():
        return reports

    # Recorder "split mode" (config.toml): when on, every recording over the
    # configured size (default 2 GiB) is cut into <=that-size parts, instead of
    # only splitting above the ~3.9 GiB upload ceiling.
    split_threshold = _recorder_split_threshold_bytes()

    root_files = [p for p in root.iterdir() if p.is_file()]
    if root_files:
        report = ReconcileReport(platform="tiktok", username="_root")
        reports.append(_reconcile_dir(
            platform=None,
            username="_root",
            db=db,
            scan_dir=root,
            recursive=False,
            seed_extractor_archive=False,
            report=report,
            source="recorder",
            caption_for_path=lambda path: _recording_caption("_root", path),
            identifier_for_path=_recorder_identifier,
            priority=_RECORDER_PRIORITY,
            guard=guard,
            split_threshold_bytes=split_threshold,
        ))

    for user_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        report = ReconcileReport(platform="tiktok", username=user_dir.name)
        reports.append(_reconcile_dir(
            platform=None,
            username=user_dir.name,
            db=db,
            scan_dir=user_dir,
            recursive=True,
            seed_extractor_archive=False,
            report=report,
            source="recorder",
            caption_for_path=lambda path, user=user_dir.name: (
                _recording_caption(user, path)
            ),
            identifier_for_path=_recorder_identifier,
            priority=_RECORDER_PRIORITY,
            guard=guard,
            split_threshold_bytes=split_threshold,
        ))
    return reports


def _reconcile_dir(
    *,
    platform: "Platform | None",
    username: str,
    db: "ProducerStore",
    scan_dir: Path,
    recursive: bool,
    seed_extractor_archive: bool,
    report: ReconcileReport,
    source: str = "archiver",
    caption_for_path: Callable[[Path], str | None] | None = None,
    identifier_for_path: Callable[[Path], str] | None = None,
    priority: int = 10,
    guard: "DeletionGuard | None" = None,
    split_threshold_bytes: int | None = None,
) -> ReconcileReport:
    if not scan_dir.exists():
        return report

    # Track new (identity, file_path) pairs we inserted, so we can also
    # seed the extractor archive with them.
    new_archive_entries: list[str] = []

    def _register(out: Path, *, group_key: str | None = None) -> bool:
        """Register one ready-to-send file — a canonical original, or a
        media_prep output for a converted container. Returns True once the file
        is accounted for (a row was inserted, an existing row already covers it,
        or a re-introduced already-sent copy was removed/kept), so a replaced
        original may be retired. False only when the file vanished mid-walk and
        must be retried next pass.

        `group_key` (set only for split parts) makes all parts of one original
        share an album identity, so the dispatcher ships them as one batch."""
        out_str = str(out)

        ident = identity.resolve(out)
        identifier = (identifier_for_path(out) if identifier_for_path
                      else ident.identifier)
        try:
            size = out.stat().st_size
        except OSError:
            log.warning("  reconcile: vanished mid-walk: %s", out)
            return False

        # Content-hash the new file — the move/rename-proof identity.
        digest = full_hash(out)

        # Re-introduction guard: if these exact bytes already belong to an
        # ALREADY-UPLOADED row (under a different path), this is a copy the user
        # moved back in. Don't re-enqueue it — delete it from disk (unless the
        # safebrake shields this scope). Twins with NULL hashes (pre-content_hash)
        # won't match; identity-collision still blocks re-upload below.
        if digest is not None:
            twin = db.find_by_content_hash(digest)
            if (twin is not None
                    and twin.status == "sent"
                    and twin.file_path != out_str):
                if guard is None:
                    cleanup_sidecars(out_str)
                    removed = True
                else:
                    removed = guard.delete(report.platform, username, out_str,
                                           reason="reconcile-reintroduced-dup")
                if removed:
                    report.deleted_dupes += 1
                    log.info("  reconcile: deleted re-introduced already-"
                             "uploaded file %s (bytes already sent as id=%d)",
                             out.name, twin.id)
                else:
                    log.info("  reconcile: kept re-introduced dup %s (safebrake; "
                             "already sent as id=%d, not re-enqueued)",
                             out.name, twin.id)
                return True

        inserted = db.add_item(
            source          = source,
            platform        = report.platform,
            username        = username,
            identifier      = identifier,
            file_path       = out_str,
            upload_date     = ident.upload_date,
            file_size_bytes = size,
            title           = ident.title,
            caption         = caption_for_path(out) if caption_for_path else None,
            priority        = priority,
            content_hash    = digest,
            group_key       = group_key,
        )
        if inserted:
            report.inserted += 1
            if ident.is_manual:
                report.manual_files += 1
                log.info("  reconcile: + (manual) %s [%s]",
                         out.name, ident.upload_date)
            else:
                log.info("  reconcile: + %s [%s]", out.name, ident.upload_date)
            entry = (
                identity.archive_entry_for(platform.name, ident)
                if platform is not None else None
            )
            if entry:
                new_archive_entries.append(entry)
        else:
            # INSERT OR IGNORE hit a UNIQUE constraint (same (platform,
            # identifier) or file_path already present). Rare; log + move on.
            report.already_known += 1
            log.debug("  reconcile: collision on %s id=%s", out.name, identifier)
        return True

    files = scan_dir.rglob("*") if recursive else scan_dir.iterdir()
    # NOT wrapped in db.batch() — deliberately. Each new file is content-hashed
    # (full_hash, a whole-file read) right before its add_item. Under WAL the
    # writer lock is taken at the first write of a transaction and held until
    # commit, so batching these inserts would keep the cross-process write lock
    # held across every subsequent file's hash — on a large first-run scan that
    # is many seconds, past the dispatcher's busy_timeout, surfacing as its
    # 'database is locked'. Per-file autocommit releases the lock before the next
    # hash. It's cheap here: under synchronous=NORMAL a WAL commit doesn't fsync
    # (fsync only at checkpoint), so we are NOT paying a sync per file.
    for f in sorted(files):
        if not f.is_file():
            continue
        # Skip dotfiles — chiefly macOS AppleDouble sidecars (._name) created
        # on exFAT/FAT volumes. They pass the extension filter (._clip.mp4) but
        # are 4KB metadata stubs, not media. Matches dedup.py's convention.
        if f.name.startswith("."):
            continue
        ext = f.suffix.lower()
        convertible = (ext in media_prep.CONVERTIBLE_VIDEO_EXTS
                       and ext not in MEDIA_EXTENSIONS)
        if ext not in MEDIA_EXTENSIONS and not convertible:
            continue

        report.scanned += 1

        path_str = str(f)
        # Fast path: file already known to DB → skip stability + identity.
        if db.has_file_path(path_str):
            report.already_known += 1
            continue

        # Stability probe BEFORE we commit to inserting. Costs at most
        # ~1.5s per unstable file; near-zero for the common quiescent case.
        if not stability.is_stable(f):
            report.skipped_unstable += 1
            continue

        # Telegram-readiness, decided by PROBE rather than extension. EVERY new
        # video runs through media_prep.prepare(): a canonical-extension file
        # whose codecs are already streamable passes through untouched (one
        # cheap ffprobe, only ever paid for files not yet in the DB), while a
        # .webm/.mkv container, an HEVC-in-.mp4, or a convertible container
        # (.ts/.flv/.m4v… — e.g. a crashed recording that never reached the
        # recorder's remux) is converted to a streamable .mp4 (split if
        # oversize). Previously only the oddball extensions were converted, so
        # platform downloads with bad codecs uploaded non-streamable.
        # Images pass through prepare() untouched by construction.
        # split_threshold_bytes (recorder split mode) lowers the split trigger
        # below the default ~3.9 GiB ceiling for this scan dir only.
        prep = media_prep.prepare(f, split_threshold_bytes=split_threshold_bytes)
        if prep.busy:
            # Another worker's sweep (recorder/orphaned) is already converting or
            # splitting this same file — skip this pass, retry next, so we don't
            # launch a second clobbering encode onto the same output.
            report.skipped_unstable += 1
            continue
        if not prep.ok:
            # Couldn't make it streamable safely. Leave the original on disk
            # (never lose bytes); it is retried next pass.
            report.prep_failed += 1
            log.warning("  reconcile: media_prep failed for %s: %s",
                        f.name, prep.error)
            continue
        if not prep.transformed and convertible:
            # A convertible container that prep left untouched (prep disabled,
            # or not a usable video) — skip rather than enqueue an unplayable
            # container (prior behaviour). Canonical media passes through.
            report.prep_failed += 1
            log.warning("  reconcile: %s not converted to a streamable "
                        "format — skipped", f.name)
            continue
        targets, transformed = prep.outputs, prep.transformed

        # Split parts (prep.individual) all share one album key, minted from the
        # ORIGINAL stem so the dispatcher batches them into a single ordered
        # album rather than sending each part as its own message.
        split_gk = (split_group_key(report.platform, username, f.stem)
                    if prep.individual else None)
        accounted = sum(1 for out in targets if _register(out, group_key=split_gk))

        # Retire the replaced original only once every output is accounted for,
        # so a partial-registration failure keeps the source bytes on disk.
        if transformed and accounted == len(targets):
            _retire_replaced_original(guard, report.platform, username, f)

    # Seed extractor archive — only for entries we actually added this
    # pass (we don't need to keep re-seeding already-known ones).
    if seed_extractor_archive and new_archive_entries:
        try:
            n = platform.seed_archive(username, new_archive_entries)
            report.seeded_archive = n
        except Exception as e:
            # Seeding failure is non-fatal — worst case is the extractor
            # re-walks and dedups via DB. Log loudly so a recurring issue
            # gets noticed.
            log.warning("  reconcile: seed_archive failed for %s/%s: %s",
                        report.platform, username, e)

    # Compute the upload_date floor for checkpoint use.
    report.max_upload_date = db.max_upload_date(report.platform, username)

    return report


def _retire_replaced_original(
    guard: "DeletionGuard | None", platform: str, username: str, original: Path,
) -> None:
    """Delete a convertible original that media_prep replaced with streamable
    output(s). delete-after-split (default ON) gates whether we delete at all;
    the DeletionGuard safebrake can still veto a protected scope. With no guard
    we fall back to the suite's legacy unconditional cleanup. A kept original is
    re-converted on the next pass (idempotent: its output's file_path already
    has a row), so nothing is lost — it just isn't cleaned up."""
    if not media_prep.delete_after_split():
        return
    if guard is not None:
        guard.delete(
            platform, username, str(original),
            reason="reconcile media-prep replaced original with streamable copy",
        )
    else:
        cleanup_sidecars(str(original))


def _recorder_config() -> dict:
    """The [recorder] table from the recorder's config.toml, or {} when the
    file is missing/unreadable (warn-and-default, like the rest of reconcile)."""
    if not RECORDER_CONFIG_TOML.exists():
        return {}
    try:
        with RECORDER_CONFIG_TOML.open("rb") as f:
            data = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        log.warning("recordings reconcile: could not read %s: %s",
                    RECORDER_CONFIG_TOML, e)
        return {}
    rec = data.get("recorder", {})
    return rec if isinstance(rec, dict) else {}


def _recorder_output_dir() -> Path:
    raw = _recorder_config().get("output_dir")
    return Path(raw).expanduser() if raw else RECORDER_DEFAULT_OUTPUT_DIR


# Default part size / split trigger for the recorder "split mode" (GiB). Each
# part is <= this and any recording above it is split.
_RECORDER_SPLIT_DEFAULT_GIB = 2.0


def _recorder_split_threshold_bytes() -> int | None:
    """Recorder "split mode", from the recorder's config.toml [recorder] table:

        [recorder]
        split_at_chunk_size = true    # enable (default: false)
        split_chunk_gib     = 2.0     # part size / split trigger (default: 2)

    Returns the byte threshold when enabled, else None (the normal ~3.9 GiB
    upload ceiling applies). A bad split_chunk_gib warns and falls back to the
    2 GiB default rather than disabling — a wedged tunable mustn't lose the
    feature silently."""
    rec = _recorder_config()
    if not rec.get("split_at_chunk_size", False):
        return None
    raw = rec.get("split_chunk_gib", _RECORDER_SPLIT_DEFAULT_GIB)
    try:
        gib = float(raw)
    except (TypeError, ValueError):
        log.warning("recordings reconcile: split_chunk_gib=%r is not a number "
                    "— using %s", raw, _RECORDER_SPLIT_DEFAULT_GIB)
        gib = _RECORDER_SPLIT_DEFAULT_GIB
    if gib <= 0:
        log.warning("recordings reconcile: split_chunk_gib=%s must be > 0 — "
                    "using %s", gib, _RECORDER_SPLIT_DEFAULT_GIB)
        gib = _RECORDER_SPLIT_DEFAULT_GIB
    return int(gib * 1024 ** 3)


def _recording_caption(username: str, path: Path) -> str:
    return f"@{username} · tiktok · live · {path.stem} #live"


def _recorder_identifier(path: Path) -> str:
    return f"recorder_{path.stem or 'item'}"


def _loose_root_captions(platform_dir: Path) -> dict[Path, str]:
    if not platform_dir.exists():
        return {}
    files = sorted(
        p for p in platform_dir.iterdir()
        if p.is_file() and p.suffix.lower() in MEDIA_EXTENSIONS
    )
    captions: dict[Path, str] = {}
    for group in _cluster_by_normalized_prefix(files, ROOT_CLUSTER_MIN_PREFIX):
        caption = _display_prefix_for_group(group)
        for path in group:
            captions[path] = caption
    return captions


def _cluster_by_normalized_prefix(
    files: list[Path],
    min_prefix_chars: int,
) -> list[list[Path]]:
    groups: list[list[Path]] = []
    current: list[Path] = []
    current_norm = ""

    for path in sorted(files, key=lambda p: _normalized_stem(p).lower()):
        norm = _normalized_stem(path)
        if not current:
            current = [path]
            current_norm = norm
            continue

        lcp = _common_prefix(current_norm, norm)
        if len(lcp) >= min_prefix_chars:
            current.append(path)
            current_norm = lcp
        else:
            groups.append(current)
            current = [path]
            current_norm = norm

    if current:
        groups.append(current)
    return groups


def _display_prefix_for_group(group: list[Path]) -> str:
    if len(group) == 1:
        return group[0].stem

    normalized_prefix = _normalized_stem(group[0])
    for path in group[1:]:
        normalized_prefix = _common_prefix(
            normalized_prefix,
            _normalized_stem(path),
        )

    chars_needed = len(normalized_prefix)
    alnum_seen = 0
    display_chars: list[str] = []
    for char in group[0].stem:
        if char.isalnum():
            alnum_seen += 1
        display_chars.append(char)
        if alnum_seen >= chars_needed:
            break

    display = "".join(display_chars).rstrip(" _-.")
    return display or normalized_prefix or group[0].stem


def _normalized_stem(path: Path) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", path.stem).lower()


def _common_prefix(left: str, right: str) -> str:
    end = 0
    for a, b in zip(left, right):
        if a != b:
            break
        end += 1
    return left[:end]
