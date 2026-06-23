"""Process-level singleton lock for the Telegram session owner.

A thin specialization of core.InstanceLock: the flock mechanism, holder-pid
probe, and stale-free crash safety all live in core (one implementation for
every worker). This subclass only contributes what's dispatcher-specific —
the lock path is keyed to the Telegram SESSION (its singleton-ness is per
session, not per worker name) and the "already running" message points the
operator at launchd.
"""

from __future__ import annotations

from pathlib import Path

from core import InstanceLock, InstanceAlreadyRunning


class DispatcherAlreadyRunning(InstanceAlreadyRunning):
    """Another dispatcher already owns this Telegram session."""


class DispatcherInstanceLock(InstanceLock):
    """Hold an advisory lock for as long as one dispatcher owns a session.

    The kernel releases the lock automatically when the holder exits or crashes,
    so there is no stale-PID problem. PLACEMENT: the lock resolves to the SAME
    path regardless of CWD — a bare session name is anchored to a fixed config
    dir, and a session name that is itself a path keeps the lock beside the
    session file (equally absolute). A CWD-relative path would let a
    launchd-started dispatcher (CWD /) and a manual one (CWD ~) each lock a
    different file and both run — the exact failure this guard prevents.
    """

    _LOCK_DIR = Path("~/.config/dispatcher").expanduser()

    def __init__(self, session_name: str):
        base = Path(session_name).expanduser()
        if base.parent == Path("."):           # bare name → fixed dir
            base = self._LOCK_DIR / base.name
        super().__init__(
            base.name, path=base.with_name(f"{base.name}.dispatcher.lock"))

    def _already_running_error(self, holder: str) -> Exception:
        session = self.path.name.removesuffix(".dispatcher.lock")
        return DispatcherAlreadyRunning(
            f"another dispatcher ({holder}) already owns Telegram session "
            f"{session!r} — if launchd manages it, that instance restarted "
            f"on boot; stop it with "
            f"`launchctl bootout gui/$UID/com.duy.dispatcher` "
            f"or check it with `dispatcher status`"
        )
