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

import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

from core.files import media_bucket

log = logging.getLogger(__name__)

# ffprobe should answer in well under a second; cap it so a wedged probe can
# never stall the drain loop. A timeout just means "no attributes this time".
_PROBE_TIMEOUT_S = 20.0


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

    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries",
        "stream=width,height,sample_aspect_ratio,duration:"
        "stream_tags=rotate:stream_side_data=rotation:"
        "format=duration",
        "-of", "json",
        file_path,
    ]
    try:
        out = subprocess.run(
            cmd, capture_output=True, timeout=_PROBE_TIMEOUT_S,
        )
        if out.returncode != 0:
            raise RuntimeError(
                out.stderr.decode(errors="replace").strip() or "non-zero exit")
        data = json.loads(out.stdout or b"{}")
    except (OSError, subprocess.TimeoutExpired, RuntimeError,
            json.JSONDecodeError) as e:
        log.warning("probe: %s: ffprobe failed (%s) — uploading without "
                    "explicit video attributes", Path(file_path).name, e)
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
