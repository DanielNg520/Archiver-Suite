"""
core.instance_lock
──────────────────
Generic single-instance (singleton) advisory lock, generalized from the
dispatcher's proven pattern so EVERY long-running worker enforces one live
process per name: `archiver loop`/`run`/`start` share the "archiver" lock,
`recorder start` holds "recorder". (The dispatcher keeps its own session-keyed
DispatcherInstanceLock — its singleton-ness is per Telegram session, not per
worker name — but the mechanism is identical.)

Why flock, not a PID file: the kernel releases an flock automatically when the
holder exits OR crashes (even SIGKILL / power loss), so there is no stale-lock
problem and no unreliable PID-liveness heuristic. The PID written into the file
is diagnostics only — it tells a human/ops WHICH process holds it.

PLACEMENT: the path resolves to a fixed config dir, never CWD-relative, so a
launchd-started worker (CWD /) and a manually started one (CWD ~) contend for
the SAME file instead of each taking "the" lock on two different paths and both
running — the exact failure mode this guard exists to prevent.
"""

from __future__ import annotations

import fcntl
import os
from pathlib import Path

_LOCK_DIR = Path("~/.config/archiver-suite/locks").expanduser()


class InstanceAlreadyRunning(RuntimeError):
    """Raised when another process already holds this named instance lock."""


class InstanceLock:
    """Context manager holding an exclusive advisory lock for `name`.

    Usage:
        with InstanceLock("archiver"):
            ...                      # only one such process runs at a time
    """

    def __init__(self, name: str, *, lock_dir: "str | Path | None" = None,
                 path: "str | Path | None" = None):
        # `path` (a full lock-file path) wins when a caller needs a non-default
        # location/suffix — e.g. the dispatcher keys its lock to a Telegram
        # session file rather than a worker name. Otherwise the path is derived
        # from `name` under the shared locks dir (or `lock_dir`).
        self.name = name
        if path is not None:
            self.path = Path(path).expanduser()
        else:
            d = Path(lock_dir).expanduser() if lock_dir else _LOCK_DIR
            self.path = d / f"{name}.instance.lock"
        self._file = None

    def _already_running_error(self, holder: str) -> Exception:
        """The exception raised when another instance holds the lock. Hook so a
        subclass can surface a domain-specific message/type (the dispatcher
        points the operator at launchctl) without re-implementing the flock."""
        return InstanceAlreadyRunning(
            f"another '{self.name}' instance ({holder}) is already running — "
            f"only one may run at a time. Stop it first, or check it "
            f"(`ops status`)."
        )

    def holder_pid(self) -> int | None:
        """PID of the live holder, or None. Probe (non-blocking shared lock on a
        separate handle), not acquisition: success means nobody holds it (we
        release immediately); failure means a live holder whose PID is the file
        body. Diagnosis only — the holder may exit between probe and use."""
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

    def __enter__(self) -> "InstanceLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.seek(0)
            text = handle.read().strip()
            handle.close()
            holder = f"pid {text}" if text.isdigit() else "an unknown pid"
            raise self._already_running_error(holder) from None
        # We hold the lock; record our pid for diagnostics.
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
