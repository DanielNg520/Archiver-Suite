"""
dispatcher.media_meta
─────────────────────
Probe a video's *display* geometry and duration with ffprobe so the sender
can attach an explicit DocumentAttributeVideo on upload.

WHY THIS EXISTS
  Telethon only auto-detects video width/height/duration when the optional
  `hachoir` package is installed. It is NOT a dependency of this suite, so
  without help every video uploads as DocumentAttributeVideo(0, 1, 1) — a
  1×1, zero-duration clip — and Telegram renders it at a bogus/squished
  resolution (this is the "weird resolution" bug). ffprobe is already part of
  the toolchain (the recorder shells out to ffmpeg/ffprobe), so we reuse it
  rather than add a Python media parser.

DISPLAY vs CODED dimensions
  Telegram shows a video at the dimensions we declare, so we must report what
  the viewer should SEE, not the raw coded frame:
    • sample_aspect_ratio (SAR) ≠ 1:1 → scale coded width by SAR (anamorphic
      TikTok HLS lands here and is the usual culprit behind stretching).
    • a 90°/270° rotation → swap width and height.
  Everything here degrades gracefully: any probe failure returns None and the
  caller simply uploads without explicit attributes (status quo), so a missing
  or misbehaving ffprobe can never block a send.
"""

from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from core import ffmpeg, ffprobe
from core.files import media_bucket

log = logging.getLogger(__name__)

# ffprobe should answer in well under a second; cap it so a wedged probe can
# never stall the drain loop. A timeout just means "no attributes this time".
_PROBE_TIMEOUT_S = 20.0

# Generating a single thumbnail frame is cheap, but the `thumbnail` filter has
# to decode a window of frames to score them, so allow a little more headroom
# than the probe. A timeout just means "upload without an explicit thumbnail".
_THUMB_TIMEOUT_S = 60.0

# Telegram thumbnails are small JPEGs; 320px on the long edge is the documented
# ceiling. The `thumbnail` filter scores a window of frames and picks the most
# representative one, which is what skips the solid black/white fade-in frames
# Telegram's server would otherwise grab as frame 0 (the "black/white poster"
# bug). The window covers the first ~300 frames (~10s at 30fps) — long enough
# to clear a typical fade-in.
_THUMB_WINDOW_FRAMES = 300
_THUMB_MAX_EDGE = 320


@dataclass(frozen=True)
class VideoMeta:
    """Display geometry + duration, ready to become a DocumentAttributeVideo."""
    width:    int
    height:   int
    duration: int  # seconds, rounded; 0 if unknown (Telegram tolerates 0)


def _ratio(value: str | None) -> float | None:
    """Parse an ffprobe 'num:den' (SAR) or 'num/den' ratio into a float.

    Returns None for the many ways ffprobe says "unknown" ('0:1', 'N/A', '',
    None) so callers can treat those as "no scaling"."""
    if not value or value in ("N/A", "0:1", "0/1"):
        return None
    sep = ":" if ":" in value else "/" if "/" in value else None
    try:
        if sep:
            num, den = value.split(sep, 1)
            num_f, den_f = float(num), float(den)
            return num_f / den_f if den_f and num_f else None
        return float(value) or None
    except (ValueError, ZeroDivisionError):
        return None


def _rotation(stream: dict) -> int:
    """Net rotation in degrees, normalized to {0, 90, 180, 270}.

    Sources, newest ffmpeg first: a Display Matrix side-data 'rotation'
    (signed, e.g. -90), then the legacy tags.rotate. Either can be present."""
    deg = 0.0
    for sd in stream.get("side_data_list", []) or []:
        if "rotation" in sd:
            try:
                deg = float(sd["rotation"])
            except (TypeError, ValueError):
                deg = 0.0
            break
    else:
        tag = (stream.get("tags") or {}).get("rotate")
        if tag is not None:
            try:
                deg = float(tag)
            except (TypeError, ValueError):
                deg = 0.0
    return int(round(deg)) % 360


def _duration(stream: dict, fmt: dict) -> int:
    """Container duration wins (covers VFR/edit lists); fall back to the
    stream's own duration. 0 when neither is a real number."""
    for raw in (fmt.get("duration"), stream.get("duration")):
        try:
            d = float(raw)
            if d > 0:
                return int(round(d))
        except (TypeError, ValueError):
            continue
    return 0


def probe_video(file_path: str) -> VideoMeta | None:
    """Return display geometry + duration, or None if `file_path` isn't a
    recognized video, has no video stream, or ffprobe is unavailable/fails.

    Never raises — robustness here means a probe problem degrades to
    "upload without explicit attributes", not a failed send."""
    if media_bucket(file_path) != "video":
        return None

    data = ffprobe.probe_json(
        file_path,
        select_streams="v:0",
        show_entries=(
            "stream=width,height,sample_aspect_ratio,duration:"
            "stream_tags=rotate:stream_side_data=rotation:"
            "format=duration"
        ),
        timeout=_PROBE_TIMEOUT_S,
    )
    if data is None:
        log.warning("probe: %s: ffprobe failed — uploading without explicit "
                    "video attributes", Path(file_path).name)
        return None

    streams = data.get("streams") or []
    if not streams:
        return None
    stream = streams[0]
    try:
        width = int(stream["width"])
        height = int(stream["height"])
    except (KeyError, TypeError, ValueError):
        return None
    if width <= 0 or height <= 0:
        return None

    # Anamorphic pixels → widen the displayed frame to its true aspect.
    sar = _ratio(stream.get("sample_aspect_ratio"))
    if sar and sar > 0 and abs(sar - 1.0) > 1e-3:
        width = max(1, int(round(width * sar)))

    # Portrait-rotated frames declare swapped dimensions to the viewer.
    if _rotation(stream) in (90, 270):
        width, height = height, width

    return VideoMeta(
        width=width, height=height, duration=_duration(stream, data.get("format", {})),
    )


def make_thumbnail(file_path: str) -> str | None:
    """Render a representative JPEG thumbnail for a video, or None.

    WHY THIS EXISTS
      With no explicit thumbnail, Telegram's server auto-generates the inline
      poster from frame 0 of the upload. Clips that open on a fade-in (solid
      black) or a blank/white leader therefore show an all-black/all-white
      preview even though the video itself plays fine — the "black/white poster"
      bug. We hand Telegram a frame chosen by ffmpeg's `thumbnail` filter, which
      scores a window of frames and rejects uniform outliers, so the poster
      lands on real picture content.

    Returns the path to a temp .jpg the CALLER must delete, or None if this
    isn't a video / ffmpeg is unavailable or fails. Never raises — a thumbnail
    problem degrades to "upload without an explicit thumbnail", never a failed
    send."""
    if media_bucket(file_path) != "video":
        return None

    fd, out = tempfile.mkstemp(prefix="tgthumb_", suffix=".jpg")
    os.close(fd)

    # thumbnail=N picks the most representative frame from each N-frame window;
    # scale fits it inside the Telegram size ceiling while preserving aspect.
    vf = (
        f"thumbnail={_THUMB_WINDOW_FRAMES},"
        f"scale={_THUMB_MAX_EDGE}:{_THUMB_MAX_EDGE}:"
        f"force_original_aspect_ratio=decrease"
    )
    cmd = [
        "ffmpeg", "-y", "-v", "error",
        "-i", file_path,
        "-vf", vf,
        "-frames:v", "1",
        "-f", "mjpeg",
        out,
    ]
    ok = ffmpeg.run_ffmpeg(
        cmd, what=f"thumbnail {Path(file_path).name}", timeout=_THUMB_TIMEOUT_S)
    if not ok or not Path(out).exists() or Path(out).stat().st_size == 0:
        log.warning("thumbnail: %s: no frame produced — uploading without an "
                    "explicit thumbnail", Path(file_path).name)
        _discard(out)
        return None
    return out


def _discard(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass
