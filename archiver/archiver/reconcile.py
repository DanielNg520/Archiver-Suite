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
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from core import identity, stability, cleanup_sidecars
from core.hashing import full_hash

if TYPE_CHECKING:
    from core import ItemStore
    from .platforms import Platform

log = logging.getLogger(__name__)

# Same canonical set used elsewhere — kept in sync intentionally; this
# is the "files we consider media." Sidecars (.json) are excluded.
MEDIA_EXTENSIONS = {
    ".mp4", ".mov", ".webm", ".mkv",   # video
    ".jpg", ".jpeg", ".png", ".webp",  # image
    ".gif",                            # animated
}

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
    max_upload_date: str | None = None
    archive_entries: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        manual_note = f" ({self.manual_files} manual)" if self.manual_files else ""
        seed_note = f", seeded {self.seeded_archive}" if self.seeded_archive else ""
        dup_note = f", deleted {self.deleted_dupes} dup" if self.deleted_dupes else ""
        return (
            f"[{self.platform}] @{self.username}: scanned {self.scanned}, "
            f"+{self.inserted}{manual_note}, known {self.already_known}, "
            f"unstable {self.skipped_unstable}{seed_note}{dup_note}, "
            f"floor={self.max_upload_date or '-'}"
        )


def reconcile_user(
    platform: "Platform",
    username: str,
    db: "ItemStore",
    output_dir: str,
    seed_extractor_archive: bool = True,
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
    )


def reconcile_platform_root(
    platform: "Platform",
    db: "ItemStore",
    output_dir: str,
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
    )


def reconcile_recordings(
    db: "ItemStore",
    records_dir: str | Path | None = None,
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
            priority=20,
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
            priority=20,
        ))
    return reports


def _reconcile_dir(
    *,
    platform: "Platform | None",
    username: str,
    db: "ItemStore",
    scan_dir: Path,
    recursive: bool,
    seed_extractor_archive: bool,
    report: ReconcileReport,
    source: str = "archiver",
    caption_for_path: Callable[[Path], str | None] | None = None,
    identifier_for_path: Callable[[Path], str] | None = None,
    priority: int = 10,
) -> ReconcileReport:
    if not scan_dir.exists():
        return report

    # Track new (identity, file_path) pairs we inserted, so we can also
    # seed the extractor archive with them.
    new_archive_entries: list[str] = []

    files = scan_dir.rglob("*") if recursive else scan_dir.iterdir()
    for f in sorted(files):
        if not f.is_file():
            continue
        if f.suffix.lower() not in MEDIA_EXTENSIONS:
            continue

        report.scanned += 1

        path_str = str(f)
        # Fast path: file already known to DB → skip stability probe + identity.
        if db.has_file_path(path_str):
            report.already_known += 1
            continue

        # Stability probe BEFORE we commit to inserting. Costs at most
        # ~1.5s per unstable file; near-zero for the common quiescent case.
        if not stability.is_stable(f):
            report.skipped_unstable += 1
            continue

        ident = identity.resolve(f)
        identifier = identifier_for_path(f) if identifier_for_path else ident.identifier

        try:
            size = f.stat().st_size
        except OSError:
            log.warning("  reconcile: vanished mid-walk: %s", f)
            continue

        # Content-hash the new file — the move/rename-proof identity. Only NEW
        # paths reach here (has_file_path fast-path above skipped known ones),
        # so on a quiescent archive this hashes nothing.
        digest = full_hash(f)

        # Re-introduction guard: if these exact bytes already belong to an
        # ALREADY-UPLOADED row (under a different path), this is a copy the user
        # moved back in. Don't re-enqueue it — delete it from disk. (Same-path
        # files never get here; the canonical sent file at its own path is
        # untouched. Twins created before content_hash existed have NULL hashes
        # and won't match — identity-collision still blocks re-upload, just
        # without the disk cleanup.)
        if digest is not None:
            twin = db.find_by_content_hash(digest)
            if (twin is not None
                    and twin.status == "sent"
                    and twin.file_path != path_str):
                cleanup_sidecars(path_str)
                report.deleted_dupes += 1
                log.info("  reconcile: deleted re-introduced already-uploaded "
                         "file %s (bytes already sent as id=%d)", f.name, twin.id)
                continue

        inserted = db.add_item(
            source          = source,
            platform        = report.platform,
            username        = username,
            identifier      = identifier,
            file_path       = path_str,
            upload_date     = ident.upload_date,
            file_size_bytes = size,
            title           = ident.title,
            caption         = caption_for_path(f) if caption_for_path else None,
            priority        = priority,
            content_hash    = digest,
        )
        if inserted:
            report.inserted += 1
            if ident.is_manual:
                report.manual_files += 1
                log.info("  reconcile: + (manual) %s [%s]", f.name, ident.upload_date)
            else:
                log.info("  reconcile: + %s [%s]", f.name, ident.upload_date)
            # Collect archive seed entries (manual files get None back)
            entry = (
                identity.archive_entry_for(platform.name, ident)
                if platform is not None else None
            )
            if entry:
                new_archive_entries.append(entry)
        else:
            # add_item returned False — INSERT OR IGNORE hit a UNIQUE
            # constraint (same (platform, identifier) or file_path already
            # present, possibly under a different path). Rare; log + move on.
            report.already_known += 1
            log.debug("  reconcile: collision on %s id=%s",
                      f.name, identifier)

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


def _recorder_output_dir() -> Path:
    if not RECORDER_CONFIG_TOML.exists():
        return RECORDER_DEFAULT_OUTPUT_DIR
    try:
        with RECORDER_CONFIG_TOML.open("rb") as f:
            data = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError) as e:
        log.warning(
            "recordings reconcile: could not read %s: %s; using %s",
            RECORDER_CONFIG_TOML,
            e,
            RECORDER_DEFAULT_OUTPUT_DIR,
        )
        return RECORDER_DEFAULT_OUTPUT_DIR
    raw = data.get("recorder", {}).get("output_dir")
    return Path(raw).expanduser() if raw else RECORDER_DEFAULT_OUTPUT_DIR


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
