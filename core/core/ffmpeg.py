"""
core.ffmpeg
───────────
One place that runs ffmpeg. The convert (media_prep), photo-normalize
(image_fix), and thumbnail (media_meta) paths each carried their own
subprocess + timeout + rc/stderr handling; the invocation boilerplate is now
shared here. Each caller still owns what it does with the result (check output
size, probe the produced file, try the next quality target, …) — this only
removes the duplicated "run a command, survive a failure" wrapper.

CONTRACT: never raises. A missing ffmpeg, a timeout, or a non-zero exit are all
reported as False (with a logged reason); the caller degrades to its own
fallback. The companion to core.ffprobe.
"""

from __future__ import annotations

import logging
import subprocess

log = logging.getLogger(__name__)


def run_ffmpeg(cmd: list[str], *, what: str, timeout: float) -> bool:
    """Run an ffmpeg `cmd`, returning True on success (exit 0). On any failure
    — ffmpeg missing, timeout, or non-zero exit — log a concise reason and
    return False; never raise. `what` is a short label for the log line."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        log.warning("ffmpeg: %s failed: %s", what, e)
        return False
    if r.returncode != 0:
        log.warning("ffmpeg: %s rc=%d: %s",
                    what, r.returncode, (r.stderr or "").strip()[:300])
        return False
    return True
