"""Process-level singleton lock for the Telegram session owner."""

from __future__ import annotations

import fcntl
import os
from pathlib import Path


class DispatcherAlreadyRunning(RuntimeError):
    pass


class DispatcherInstanceLock:
    """Hold an advisory lock for as long as one dispatcher owns a session.

    The lock file may remain after a crash, but the kernel lock is released
    automatically when the process exits. This avoids stale-PID heuristics.

    PLACEMENT: the lock must resolve to the SAME path no matter where the
    process was started. A bare session name (the common case) is anchored to
    a fixed config dir — a CWD-relative path would let a launchd-started
    dispatcher (CWD /) and a manual one (CWD ~) each take "the" lock on two
    different files and both run. A session name that is itself a path keeps
    the lock beside the session file, which is equally absolute.
    """

    _LOCK_DIR = Path("~/.config/dispatcher").expanduser()

    def __init__(self, session_name: str):
        base = Path(session_name).expanduser()
        if base.parent == Path("."):           # bare name → fixed dir
            base = self._LOCK_DIR / base.name
        self.path = base.with_name(f"{base.name}.dispatcher.lock")
        self._file = None

    def holder_pid(self) -> int | None:
        """PID of the process currently holding the lock, or None.

        Probe, not acquisition: try a non-blocking shared lock on a separate
        handle. Success means nobody holds the exclusive lock (released
        immediately); failure means a live holder, whose PID is the file body.
        Diagnosis only — the holder can exit between probe and use."""
        try:
            with self.path.open("r", encoding="utf-8") as probe:
                try:
                    fcntl.flock(probe.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                except BlockingIOError:
                    text = probe.read().strip()
                    return int(text) if text.isdigit() else None
                fcntl.flock(probe.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        return None

    def __enter__(self) -> "DispatcherInstanceLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.seek(0)
            text = handle.read().strip()
            handle.close()
            holder = f"pid {text}" if text.isdigit() else "unknown pid"
            session = self.path.name.removesuffix(".dispatcher.lock")
            raise DispatcherAlreadyRunning(
                f"another dispatcher ({holder}) already owns Telegram session "
                f"{session!r} — if launchd manages it, that instance restarted "
                f"on boot; stop it with "
                f"`launchctl bootout gui/$UID/com.duy.dispatcher` "
                f"or check it with `dispatcher status`"
            ) from None

        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()}\n")
        handle.flush()
        self._file = handle
        return self

    def __exit__(self, *_exc) -> None:
        if self._file is None:
            return
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None
