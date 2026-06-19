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

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_LOCK_PATH = "~/.config/archiver-suite/locks/tiktok.lock"


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
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid":        self.pid,
            "started_at": _now_iso(),
            "block":      "download",
            "username":   self.username,
        }
        # Write atomically so the archiver never reads a half-written file.
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload))
        os.replace(tmp, self.path)
        log.debug("tiktok lock acquired (pid=%d) at %s", self.pid, self.path)
        return self

    def __exit__(self, *exc) -> None:
        try:
            self.path.unlink(missing_ok=True)
            log.debug("tiktok lock released")
        except OSError as e:
            log.warning("failed to remove lockfile %s — manual cleanup "
                        "may be needed: %s", self.path, e)
