"""
core.stability
──────────────
"Is this file safe to process, or still being written?"

A reconcile pass that runs WHILE an extractor is downloading (think:
`archiver run` interrupted, but the worker thread isn't yet killed; or
your manual file copy hasn't finished) can pick up a half-written file,
register it in the DB, and then try to upload garbage.

The classic fix is the rsync/Syncthing pattern: stat the file twice
with a small gap; if size or mtime changed, it's not done. Combined
with extension/name blocklists for known "still-writing" markers.

Cost: one extra stat + a short sleep per file, the first time we see
it. Cached results on a per-process basis so a normal reconcile of a
quiescent archive folder doesn't pay the sleep cost at all.

Why 1.5 seconds (and not 5 like the bulk uploader): extractor downloads
complete one file at a time in 1-3s typical. After the extractor calls
its sidecar postprocessor and closes the file handle, the size is final.
1.5 seconds is enough headroom for that close-fsync gap while still
keeping the reconcile pass fast.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

log = logging.getLogger(__name__)

# Suffixes that indicate a file is still being downloaded by some tool.
# yt-dlp uses .part; browsers use .crdownload (Chrome) and .partial.
_INCOMPLETE_SUFFIXES = frozenset({".part", ".tmp", ".crdownload", ".partial",
                                   ".ytdl"})

# Hidden files (leading dot). We never want to process these — they're
# typically metadata files (.DS_Store), lockfiles, or in-flight downloads
# from tools that prefix with a dot.
def _is_hidden(path: Path) -> bool:
    return path.name.startswith(".")


# Minimum reasonable media file size in bytes. Anything smaller is almost
# certainly a placeholder or a 1-pixel error PNG.
MIN_FILE_BYTES = 100

# Probe gap: time between the two stat() calls.
PROBE_INTERVAL_S = 1.5

# Files older than this many seconds are assumed stable without a probe
# (avoids the 1.5s wait for the common case of "reconciling a folder
# that's been quiet for hours").
QUIESCENT_AGE_S = 5.0


def is_stable(path: Path) -> bool:
    """
    Return True iff `path` is a file safe to register in the DB / upload.

    The checks, cheapest first:
      1. Path is a file with a non-hidden, non-incomplete-suffix name.
      2. Size meets the minimum.
      3. EITHER mtime is older than QUIESCENT_AGE_S (instant pass) OR
         a stat-sleep-stat probe shows size/mtime unchanged.
    """
    if _is_hidden(path):
        return False
    # Check all suffixes: catches both "foo.part" and "foo.mp4.part" styles.
    if any(s.lower() in _INCOMPLETE_SUFFIXES for s in path.suffixes):
        return False

    try:
        s1 = path.stat()
    except OSError as e:
        log.debug("stability: stat failed for %s: %s", path, e)
        return False

    if s1.st_size < MIN_FILE_BYTES:
        log.debug("stability: %s too small (%d bytes)", path.name, s1.st_size)
        return False

    # Fast path: file hasn't been touched recently → trust it.
    age = time.time() - s1.st_mtime
    if age > QUIESCENT_AGE_S:
        return True

    # Slow path: file changed within the quiescent window. Probe.
    time.sleep(PROBE_INTERVAL_S)
    try:
        s2 = path.stat()
    except OSError as e:
        log.debug("stability: second stat failed for %s: %s", path, e)
        return False

    if s2.st_size != s1.st_size or s2.st_mtime != s1.st_mtime:
        log.info("stability: %s still being written; skipping this pass",
                 path.name)
        return False
    return True
