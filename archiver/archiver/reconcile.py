"""
archiver.reconcile
──────────────────
"Reconcile v2": walk the on-disk archive, register every stable media
file in the DB, and seed the per-extractor archives so the next
download pass doesn't try to re-fetch already-present content.

This replaces the simple `db.reconcile()` of v1 in three ways:

  1. Uses `archiver.stability.is_stable()` to skip half-written files
     instead of registering them as broken pending uploads.

  2. Uses `archiver.identity.resolve()` for every file, so:
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
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from . import identity, stability

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
    max_upload_date: str | None = None
    archive_entries: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        manual_note = f" ({self.manual_files} manual)" if self.manual_files else ""
        seed_note = f", seeded {self.seeded_archive}" if self.seeded_archive else ""
        return (
            f"[{self.platform}] @{self.username}: scanned {self.scanned}, "
            f"+{self.inserted}{manual_note}, known {self.already_known}, "
            f"unstable {self.skipped_unstable}{seed_note}, "
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
    if not user_dir.exists():
        return report

    # Track new (identity, file_path) pairs we inserted, so we can also
    # seed the extractor archive with them.
    new_archive_entries: list[str] = []

    for f in sorted(user_dir.rglob("*")):
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

        try:
            size = f.stat().st_size
        except OSError:
            log.warning("  reconcile: vanished mid-walk: %s", f)
            continue

        inserted = db.add_item(
            source          = "archiver",
            platform        = platform.name,
            username        = username,
            identifier      = ident.identifier,
            file_path       = path_str,
            upload_date     = ident.upload_date,
            file_size_bytes = size,
            title           = ident.title,
            priority        = 10,
        )
        if inserted:
            report.inserted += 1
            if ident.is_manual:
                report.manual_files += 1
                log.info("  reconcile: + (manual) %s [%s]", f.name, ident.upload_date)
            else:
                log.info("  reconcile: + %s [%s]", f.name, ident.upload_date)
            # Collect archive seed entries (manual files get None back)
            entry = identity.archive_entry_for(platform.name, ident)
            if entry:
                new_archive_entries.append(entry)
        else:
            # add_item returned False — INSERT OR IGNORE hit a UNIQUE
            # constraint (same (platform, identifier) or file_path already
            # present, possibly under a different path). Rare; log + move on.
            report.already_known += 1
            log.debug("  reconcile: collision on %s id=%s",
                      f.name, ident.identifier)

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
                        platform.name, username, e)

    # Compute the upload_date floor for checkpoint use.
    report.max_upload_date = db.max_upload_date(platform.name, username)

    return report
