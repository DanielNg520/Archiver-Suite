"""
dispatcher.policies
───────────────────
Ported from archiver.policies. DeletePolicy only — dedup is archiver-only
and has no role in upload dispatch.

Same Specification-on-Repository pattern: each policy owns its TOML key
and default; PolicyStore handles storage and hierarchical resolution.
"""

from __future__ import annotations

import logging

from .policy_store import PolicyStore

log = logging.getLogger(__name__)


class BooleanPolicy:
    """Base for any bool-valued policy. Subclasses set KEY and DEFAULT."""

    KEY:     str  = ""     # MUST override
    DEFAULT: bool = False  # MUST override (intentionally explicit)

    def __init__(self, store: PolicyStore):
        if not self.KEY:
            raise TypeError(
                f"{type(self).__name__} must set KEY (non-empty TOML key)."
            )
        self._store = store

    def is_enabled(self, platform: str, username: str) -> bool:
        value = self._store.get(
            self.KEY,
            platform = platform,
            username = username,
            default  = self.DEFAULT,
        )
        if not isinstance(value, bool):
            log.warning(
                "policy %s: non-bool value %r for %s/%s — falling back to %s. "
                "Fix the value in config.toml.",
                self.KEY, value, platform, username, self.DEFAULT,
            )
            return self.DEFAULT
        return value

    def explain(self, platform: str, username: str) -> str:
        value, source = self._store.explain(
            self.KEY,
            platform = platform,
            username = username,
            default  = self.DEFAULT,
        )
        return f"{value} (from {source})"


class DeletePolicy(BooleanPolicy):
    """Delete local file after a successful Telegram upload."""
    KEY     = "delete_after_upload"
    DEFAULT = False

    def should_delete(self, platform: str, username: str) -> bool:
        return self.is_enabled(platform, username)
