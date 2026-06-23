"""
dispatcher.image_fix
─────────────────────
Normalize images that Telegram's photo pipeline refuses.

WHY THIS EXISTS
  Sending a file as a Telegram *photo* makes the server decode and re-encode
  it. If the image violates Telegram's photo constraints — width+height over
  10000, an aspect ratio beyond ~20:1, a file larger than 10 MB, or an
  encoding the server can't read (CMYK JPEG, some WebP/PNG, a truncated file)
  — it raises IMAGE_PROCESS_FAILED. In an album send that's atomic: one bad
  image fails all ten, and since the failure is deterministic, retrying never
  helps. Instagram dumps routinely contain such images.

WHAT WE DO
  Probe each photo with ffprobe (already part of the toolchain — see
  media_meta). The ones that would be rejected are *isolated* and re-encoded
  with ffmpeg into a downscaled, baseline JPEG that Telegram accepts; the good
  ones are left untouched. The caller then sends the whole batch together as
  one album. ffmpeg/ffprobe failures degrade gracefully: we return None and
  the caller falls back to sending the original (status quo for that file).

  No new dependency: the recorder already shells out to ffmpeg/ffprobe, so we
  reuse them rather than pull in Pillow.
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

from core import ffmpeg, ffprobe

log = logging.getLogger(__name__)

# ffprobe answers in well under a second; cap so a wedged probe can't stall
# the drain loop. A timeout just means "treat as needs-fix" (conservative).
_PROBE_TIMEOUT_S   = 20.0
_CONVERT_TIMEOUT_S = 120.0

# Telegram photo limits (MTProto). A photo send must satisfy ALL of these or
# the server raises IMAGE_PROCESS_FAILED. We stay safely inside them.
_MAX_DIM_SUM = 10000              # width + height
_MAX_AR      = 20.0               # long side / short side
_MAX_BYTES   = 10 * 1024 * 1024   # 10 MB
# Downscale targets tried in order (longest side, JPEG q:v). Smaller/lower
# steps are fallbacks if the first re-encode still exceeds the size cap.
_TARGETS = ((4096, "2"), (2048, "5"), (1280, "8"))


def image_dimensions(path: str) -> tuple[int, int] | None:
    """(width, height) via ffprobe, or None if it can't be read (corrupt or
    an encoding ffprobe rejects — which Telegram would reject too)."""
    data = ffprobe.probe_json(
        path, select_streams="v:0", show_entries="stream=width,height",
        timeout=_PROBE_TIMEOUT_S,
    )
    if data is None:
        return None
    try:
        streams = data.get("streams") or []
        w = int(streams[0]["width"])
        h = int(streams[0]["height"])
    except (ValueError, KeyError, IndexError, TypeError):
        return None
    if w <= 0 or h <= 0:
        return None
    return (w, h)


def photo_needs_fix(path: str) -> bool | None:
    """Verdict for sending `path` as a Telegram photo:
      False → safe to send as-is.
      True  → would be rejected; re-encode with make_safe_photo first.
      None  → aspect ratio too extreme to fix by downscaling; the caller
              should send it as a document instead (can't be a clean photo).
    """
    dims = image_dimensions(path)
    if dims is None:
        return True  # unreadable / odd encoding → re-encode to fix
    w, h = dims
    if max(w, h) / min(w, h) > _MAX_AR:
        return None
    if w + h > _MAX_DIM_SUM:
        return True
    try:
        if os.path.getsize(path) > _MAX_BYTES:
            return True
    except OSError:
        return True
    return False


def make_safe_photo(path: str) -> str | None:
    """Re-encode `path` into a downscaled baseline JPEG that Telegram will
    accept, in a temp file. Returns the temp path (caller must delete it) or
    None if conversion failed (caller falls back to the original)."""
    src = Path(path)
    fd, tmp = tempfile.mkstemp(prefix="tgfix_", suffix=".jpg")
    os.close(fd)
    for target, q in _TARGETS:
        # force_original_aspect_ratio=decrease only ever shrinks (never
        # upscales a small-but-odd-encoded image); crop to even dims keeps the
        # yuvj420p encoder happy.
        vf = (f"scale='min({target},iw)':'min({target},ih)'"
              ":force_original_aspect_ratio=decrease,"
              "crop=trunc(iw/2)*2:trunc(ih/2)*2")
        ok = ffmpeg.run_ffmpeg(
            ["ffmpeg", "-y", "-v", "error", "-i", str(src),
             "-vf", vf, "-pix_fmt", "yuvj420p", "-q:v", q, tmp],
            what=f"photo-normalize {src.name}", timeout=_CONVERT_TIMEOUT_S,
        )
        if not ok:
            break
        try:
            size = os.path.getsize(tmp)
        except OSError:
            break
        if size <= _MAX_BYTES:
            log.info("image_fix: normalized %s → %s (≤%d side, q=%s, %d bytes)",
                     src.name, Path(tmp).name, target, q, size)
            return tmp
        # too big still — fall through to a smaller/lower-quality target
    try:
        os.unlink(tmp)
    except OSError:
        pass
    return None
