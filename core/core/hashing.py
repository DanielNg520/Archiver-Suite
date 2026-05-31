"""
core.hashing
────────────
One definition of "the content hash of a media file," shared by ingest
(which stamps `items.content_hash` on every row) and dedup (which collapses
byte-identical copies). Two callers, one algorithm — so "are these the same
bytes?" can never be answered two different ways.

The funnel (size → 64 KB prefix → full SHA-256) is a separate concern that
lives in core.dedup; this module owns only the hash primitives it builds on.
"""

from __future__ import annotations

import hashlib
import logging
from pathlib import Path

log = logging.getLogger(__name__)

# Partial-hash window. 64 KB is the sweet spot for media files: big enough
# that MP4 ftyp/moov, JPEG SOI+APP, and PNG IHDR have all diverged; small
# enough that a million-file prefilter reads <60 GB instead of terabytes.
PARTIAL_HASH_BYTES = 64 * 1024

# Streaming chunk for the full hash. 1 MB balances syscall overhead vs.
# memory pressure — the standard streaming-hash choice.
FULL_HASH_CHUNK = 1024 * 1024


def partial_hash(path: Path, n_bytes: int = PARTIAL_HASH_BYTES) -> str | None:
    """SHA-256 of the first `n_bytes`. None on read failure (caller drops it
    from the candidate set rather than crashing a whole scan)."""
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            h.update(f.read(n_bytes))
    except OSError as e:
        log.warning("hashing: partial-read failed on %s: %s", path, e)
        return None
    return h.hexdigest()


def full_hash(path: Path) -> str | None:
    """Streaming full SHA-256. None on read failure."""
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            while chunk := f.read(FULL_HASH_CHUNK):
                h.update(chunk)
    except OSError as e:
        log.warning("hashing: full-read failed on %s: %s", path, e)
        return None
    return h.hexdigest()
