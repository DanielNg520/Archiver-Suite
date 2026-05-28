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

from pathlib import Path

LOCK_PATH = Path("~/.config/archiver/locks/tiktok.lock").expanduser()


def tiktok_lock_held() -> bool:
    """True if the recorder currently holds the TikTok download lock.

    Presence of the file is the signal; contents (pid/started_at) are for
    human debugging only. A stale lock (recorder crashed without cleanup)
    is a known failure mode handled operationally in Slice 5's runbook,
    not here — this function intentionally does no liveness check, to keep
    the read side dead-simple and dependency-free.
    """
    return LOCK_PATH.exists()
