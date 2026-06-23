"""
recorder.lock
─────────────
TikTokLock: the soft lock that tells the archiver "I'm recording TikTok,
skip your TikTok download." Context manager — writes a JSON lockfile on
enter, removes it on exit.

Contract with archiver.lock_reader (Slice 3):
  - File location: ~/.config/archiver-suite/locks/tiktok.lock
  - Presence = lock held. The archiver only checks existence.
  - JSON contents (pid, started_at, block) are for human/ops debugging
    and future extension (e.g. block="full"), not required by the reader.

Cleanup guarantees, honestly stated:
  __exit__ removes the file on any normal exit (including exceptions). On
  SIGKILL or power loss, __exit__ does NOT run and the file is left
  behind — a stale lock. __del__ is NOT a reliable backstop for hard
  kills, so we don't pretend it is. Stale-lock recovery is an operational
  concern handled in the Slice 5 runbook (and the pid field is what makes
  a liveness check possible there).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from core import heartbeat, paths

log = logging.getLogger(__name__)

# Cross-process contract: the canonical location lives in core.paths so the
# recorder (writer) and archiver/ops (readers) can never drift apart.
DEFAULT_LOCK_PATH = str(paths.tiktok_lock())


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TikTokLock:
    def __init__(self, lock_path: str = DEFAULT_LOCK_PATH,
                 recorder_pid: int | None = None):
        self.path = Path(lock_path).expanduser()
        self.pid = recorder_pid if recorder_pid is not None else os.getpid()
        # Who is being recorded right now. Set by the recorder just before it
        # acquires the lock for a capture; surfaced in the lockfile so ops (and
        # any human) can see the in-progress user — the recording isn't in the
        # items table until it finishes, so the lockfile is the only live source.
        self.username: str | None = None

    def __enter__(self) -> "TikTokLock":
        # The lockfile is a pid-stamped heartbeat: written atomically (so the
        # archiver never reads a half-written file) and read back through the
        # same liveness gate, so a crashed recorder's lock self-heals to
        # not-held instead of starving TikTok archiving.
        heartbeat.write_atomic(self.path, {
            "pid":        self.pid,
            "started_at": _now_iso(),
            "block":      "download",
            "username":   self.username,
        })
        log.debug("tiktok lock acquired (pid=%d) at %s", self.pid, self.path)
        return self

    def __exit__(self, *exc) -> None:
        heartbeat.clear(self.path)
        log.debug("tiktok lock released")
