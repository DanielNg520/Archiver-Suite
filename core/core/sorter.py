"""
core.sorter
───────────
Route loose files out of an `unsorted/` drop folder into the canonical
`<platform>/<username>/` tree, by parsing the username out of a
`username_timestamp_…` filename.

    output_dir/unsorted/1stagram_0406_1780186897_3915641126.mp4
        → output_dir/instagram/1stagram_0406/1stagram_0406_1780186897_3915641126.mp4

This is FILESYSTEM NORMALIZATION ONLY — it never touches the DB. It runs as a
pre-step before fetch / reconcile / ingest, so once a file lands in its
platform/user folder the existing reconcile-and-upload path picks it up with no
extra wiring (dedup, delete-policy, batching all apply automatically). Keeping
it out of the DB preserves the suite's "one table, one truth" contract: the
sorter moves bytes, reconcile/ingest do the enqueue.

USERNAME EXTRACTION (the parse contract)
  The reliable boundary in `username_timestamp_id` names is the Unix timestamp,
  NOT "the first digit" — usernames legitimately contain digits and underscores
  (`1stagram_0406`). So we split on '_' and take everything BEFORE the first
  segment that is a 10-digit Unix timestamp in the plausible 2023–2027 range
  (leading 17/18). No timestamp segment → we can't identify the user, so the
  file is LEFT IN PLACE and logged, never guessed at.

ROBUSTNESS
  - per-file isolation: one unparseable/locked file never aborts the sweep.
  - atomic moves: os.replace on one filesystem; copy→fsync→replace→unlink across
    filesystems, so a crash mid-move never leaves a half-written file at dst.
  - never overwrite: a name collision at the destination is skipped + logged.
  - sidecars (yt-dlp / gallery-dl .json) travel with their media file.
"""

from __future__ import annotations

import errno
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from .dedup import MEDIA_EXTENSIONS

log = logging.getLogger(__name__)

# A 10-digit Unix timestamp with a leading 17/18 — i.e. 1_700_000_000 (Nov 2023)
# .. 1_899_999_999 (Mar 2030). Wide enough to never reject a real download date,
# tight enough that a 10-digit *media id* of the same width won't usually match
# (ids aren't time-ordered into this band). We take the FIRST matching segment,
# so the username is everything to its left.
_TIMESTAMP_RE = re.compile(r"^1[78]\d{8}$")

DEFAULT_UNSORTED_DIRNAME = "unsorted"


@dataclass
class SortReport:
    """Result of one sort sweep, str()-able into a log line."""
    platform:            str
    dry_run:             bool = False
    scanned:             int  = 0
    moved:               int  = 0
    skipped_no_username: int  = 0   # no parseable timestamp → user unknown
    skipped_collision:   int  = 0   # destination file already exists
    created_dirs:        int  = 0
    errors:              list[str] = field(default_factory=list)

    def __str__(self) -> str:
        tag = "[sort dry-run]" if self.dry_run else "[sort]"
        return (
            f"{tag} → {self.platform}/: scanned={self.scanned}, "
            f"moved={self.moved}, new_dirs={self.created_dirs}, "
            f"no_user={self.skipped_no_username}, "
            f"collision={self.skipped_collision}, errors={len(self.errors)}"
        )


def extract_username(stem: str) -> str | None:
    """Username = the '_'-joined segments before the first Unix-timestamp
    segment. None when there is no such segment, or it's the very first one
    (nothing precedes it to name the user). `stem` is the filename WITHOUT its
    extension.

        '1stagram_0406_1780186897_3915641126' → '1stagram_0406'
        'bob_1780186897'                       → 'bob'
        '1780186897_only'                      → None  (timestamp leads)
        'no_timestamp_here'                    → None
    """
    parts = stem.split("_")
    for i, seg in enumerate(parts):
        if _TIMESTAMP_RE.match(seg):
            return "_".join(parts[:i]) if i > 0 else None
    return None


def _sidecars(media: Path) -> list[Path]:
    """Existing metadata sidecars for a media file (yt-dlp / gallery-dl shapes):
    <stem>.json, <stem>.info.json, <name>.json. Mirrors core.files.cleanup_
    sidecars so the two paths agree on what counts as a sidecar."""
    cands = (
        media.with_suffix(".json"),
        media.with_suffix(".info.json"),
        media.parent / (media.name + ".json"),
    )
    seen: set[Path] = set()
    out: list[Path] = []
    for c in cands:
        if c == media or c in seen:
            continue
        seen.add(c)
        try:
            if c.is_file():
                out.append(c)
        except OSError:
            pass
    return out


def _safe_move(src: Path, dst: Path) -> None:
    """Move src → dst with no torn-write window. os.replace is atomic on one
    filesystem; across filesystems we copy to a temp beside dst, fsync it,
    atomically rename it into place, then unlink the source. dst is assumed
    free (caller checks for collisions)."""
    try:
        os.replace(src, dst)
        return
    except OSError as e:
        if e.errno != errno.EXDEV:
            raise
    # Cross-device: stage in the destination directory so the final rename is
    # same-filesystem (hence atomic). .sorttmp is unlinked on any failure.
    tmp = dst.with_name(dst.name + ".sorttmp")
    try:
        shutil.copy2(src, tmp)
        with open(tmp, "rb") as fh:
            os.fsync(fh.fileno())
        os.replace(tmp, dst)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    src.unlink()


def sort_unsorted(
    output_dir: str | Path,
    *,
    platform: str,
    dry_run: bool = False,
    unsorted_dirname: str = DEFAULT_UNSORTED_DIRNAME,
) -> SortReport:
    """Sweep `output_dir/<unsorted_dirname>/` and move each media file (plus its
    sidecars) into `output_dir/<platform>/<username>/`, creating the user folder
    if absent. Top-level files only — subfolders are left untouched. Returns a
    SortReport; never raises for a single bad file."""
    rep = SortReport(platform=platform, dry_run=dry_run)
    base = Path(output_dir)
    src_dir = base / unsorted_dirname
    try:
        if not src_dir.is_dir():
            return rep
    except OSError:
        return rep

    # Dry-run dir-creation accounting (no filesystem to consult).
    planned_dirs: set[Path] = set()

    for f in sorted(src_dir.iterdir()):
        try:
            if not f.is_file():
                continue
        except OSError:
            continue
        # Dotfiles (notably macOS AppleDouble ._* stubs) and non-media files are
        # left alone — sidecars travel with their media, not on their own.
        if f.name.startswith("."):
            continue
        if f.suffix.lower() not in MEDIA_EXTENSIONS:
            continue
        rep.scanned += 1

        username = extract_username(f.stem)
        if not username:
            rep.skipped_no_username += 1
            log.warning(
                "sort: %r has no recognizable timestamp segment — leaving in "
                "%s/ (rename to <username>_<unixts>_… to route it)",
                f.name, unsorted_dirname,
            )
            continue

        dest_dir = base / platform / username
        dst = dest_dir / f.name
        try:
            if dst.exists():
                rep.skipped_collision += 1
                log.warning("sort: %s already exists under %s/%s/ — skipping",
                            f.name, platform, username)
                continue
        except OSError as e:                       # pragma: no cover — defensive
            rep.errors.append(f"{f.name}: {e}")
            continue

        if dry_run:
            rep.moved += 1
            if not dest_dir.exists() and dest_dir not in planned_dirs:
                planned_dirs.add(dest_dir)
                rep.created_dirs += 1
            log.info("sort[dry-run]: %s → %s/%s/", f.name, platform, username)
            continue

        # Collect sidecars BEFORE moving the media (paths are computed off the
        # source location).
        sidecars = _sidecars(f)
        try:
            if not dest_dir.exists():
                dest_dir.mkdir(parents=True, exist_ok=True)
                rep.created_dirs += 1
            _safe_move(f, dst)
            for sc in sidecars:
                sc_dst = dest_dir / sc.name
                if sc_dst.exists():
                    continue       # never clobber an existing sidecar
                try:
                    _safe_move(sc, sc_dst)
                except OSError as e:               # sidecar move is best-effort
                    log.warning("sort: sidecar %s move failed: %s", sc.name, e)
            rep.moved += 1
            log.info("sort: %s → %s/%s/", f.name, platform, username)
        except OSError as e:
            rep.errors.append(f"{f.name}: {e}")
            log.exception("sort: move failed for %s", f)

    return rep
