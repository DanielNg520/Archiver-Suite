"""
core.deletion
─────────────
ONE chokepoint every deletion path in the suite funnels through before it
unlinks a media file. The suite deletes files in several independent places —
delete-after-upload (dispatcher), dedup-suppression of an already-delivered
copy (dispatcher), reconcile's re-introduction cleanup (archiver), the
disk-full emergency purge (archiver), and the `purge-sent` command. Each used
to call `cleanup_sidecars` directly, so the "safebrake" (ProtectionPolicy)
would have had to be re-checked, identically, in five places.

DeletionGuard collapses that into a single object: callers express INTENT
("delete this file, it belongs to <platform>/<user>, because <reason>") and the
guard decides. It consults ProtectionPolicy first and refuses — returning
False, never raising — when the owning scope is shielded. This is the
Strategy/Facade pattern: the policy decision lives in one place, the call sites
stay declarative, and adding a new deletion path means routing it through the
guard rather than re-implementing the check.

Construct one per process from the shared PolicyStore and dependency-inject it
into whatever might delete (drain loop, orchestrator, reconcile, CLI handlers).
A None guard is treated by callers as "no safebrake configured" so legacy code
paths and unit tests keep their original unconditional behavior.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .files import cleanup_sidecars
from .policies import ProtectionPolicy
from .policy_store import PolicyStore

log = logging.getLogger(__name__)


class DeletionGuard:
    """Policy-gated wrapper around cleanup_sidecars.

    The only state is a ProtectionPolicy; all decisions resolve through the
    shared PolicyStore so the guard always reflects current config.toml.
    """

    def __init__(self, store: PolicyStore):
        self._protection = ProtectionPolicy(store)

    def is_protected(self, platform: str, username: str) -> bool:
        """True iff the (platform, user) scope is safebraked from deletion."""
        return self._protection.is_protected(platform, username)

    def delete(
        self,
        platform: str,
        username: str,
        file_path: str,
        *,
        reason: str = "",
    ) -> bool:
        """Delete file_path + sidecars unless the owning scope is protected.

        Returns True if the file was removed, False if the safebrake blocked
        it. Never raises — cleanup_sidecars already swallows unlink errors.
        """
        if self._protection.is_protected(platform, username):
            log.info(
                "safebrake: %s/@%s is protected — KEPT %s (would-delete reason=%s)",
                platform, username, Path(file_path).name, reason or "unspecified",
            )
            return False
        cleanup_sidecars(file_path)
        return True
