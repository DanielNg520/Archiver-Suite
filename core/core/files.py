"""
core.files
──────────
Filesystem helpers shared by the dispatcher (delete-after-upload) and the
archiver (disk-full purge). One definition of "what counts as this media
file's sidecars," so the two delete paths can't drift on which extras they
remove.
"""

from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)


# ── Media-type buckets (album grouping) ───────────────────────────────────────
#
# Telegram albums must be homogeneous in practice: photos group with photos,
# videos with videos. GIFs and anything unrecognized are sent individually
# (the old archiver.telegram did the same — gifs/other never went in an album).
# These sets are the ONE definition; the dispatcher's batch claim and any
# future caller share them so "what counts as a photo" can't drift.

PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTS = {".mp4", ".mov", ".webm", ".mkv"}

ALBUM_MAX = 10  # Telegram's hard limit on items per album


def media_bucket(file_path: str) -> str:
    """Classify a file for album grouping: 'photo', 'video', or 'single'.

    'single' (gifs + anything unrecognized) is the catch-all that the drain
    loop sends one-at-a-time rather than batching — matching the old
    uploader, which only ever albumed photos and videos.
    """
    ext = Path(file_path).suffix.lower()
    if ext in PHOTO_EXTS:
        return "photo"
    if ext in VIDEO_EXTS:
        return "video"
    return "single"


def cleanup_sidecars(file_path: str) -> None:
    """Delete a media file plus its known metadata sidecars. UNGATED —
    callers are responsible for checking delivery status / policy first.

    Sidecar shapes covered:
      yt-dlp:     <stem>.info.json   and  <stem>.json
      gallery-dl: <full_name>.json   (e.g. clip.mp4.json)
    """
    p = Path(file_path)
    try:
        p.unlink(missing_ok=True)
    except OSError as e:
        log.warning("cleanup: unlink %s failed: %s", p.name, e)
        return
    for suffix in (".json", ".info.json"):
        try:
            p.with_suffix(suffix).unlink(missing_ok=True)
        except OSError:
            pass
    try:
        (p.parent / (p.name + ".json")).unlink(missing_ok=True)
    except OSError:
        pass
