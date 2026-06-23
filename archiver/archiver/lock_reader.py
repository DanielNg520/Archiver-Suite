"""
archiver.lock_reader
────────────────────
Reads the recorder's TikTok soft-lock. The recorder (Slice 4) writes a
JSON lockfile while it holds a TikTok live session; the archiver checks
for it and skips the TikTok *download* step (uploads of existing backlog
still proceed).

One-way coupling: archiver only reads. The recorder owns writing/removing.
Filename and location are part of the cross-process contract defined in
the implementation guide's shared on-disk layout.
"""

from __future__ import annotations

from core import heartbeat, paths

# Cross-process contract: one definition in core.paths (recorder writes it).
LOCK_PATH = paths.tiktok_lock()


def tiktok_lock_held() -> bool:
    """True only when a LIVE recorder holds the TikTok download lock.

    The lockfile is a pid-stamped JSON heartbeat, so we gate on the writer pid
    being alive (core.heartbeat). This SELF-HEALS the stale-lock failure mode: a
    recorder SIGKILLed mid-recording leaves the file behind, and a bare
    existence check would block TikTok downloads forever; gating on liveness lets
    the archiver resume the moment the recorder is gone. A live recorder's lock
    still reads as held, so an in-progress recording is never trampled."""
    return heartbeat.read_live(LOCK_PATH) is not None
