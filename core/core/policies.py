"""
core.policies
───────────────────
Shared Specification-on-Repository policies: each policy owns its TOML key
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


class DedupPolicy(BooleanPolicy):
    """Whether to run content-hash dedup after a successful download."""
    KEY     = "dedup_after_download"
    DEFAULT = False

    def should_dedup(self, platform: str, username: str) -> bool:
        return self.is_enabled(platform, username)


# ── Validation ────────────────────────────────────────────────────────────────

def validate_overrides(
    store:       PolicyStore,
    known_users: dict[str, tuple[str, ...]],
) -> list[str]:
    """
    Find per-user override sections whose (platform, user) doesn't match
    any configured user. Almost always a typo or stale config from when
    the user was removed without unsetting the override.

    Returns warning strings; does not raise (typos shouldn't crash a run).
    Called at startup so issues surface immediately.
    """
    valid: set[tuple[str, str]] = set()
    for platform, users in known_users.items():
        for u in users:
            valid.add((platform, u))

    warnings: list[str] = []
    for plat, user, _overrides in store.iter_user_overrides():
        if (plat, user) not in valid:
            warnings.append(
                f"policies: per-user override [platform.{plat}.user.\"{user}\"] "
                f"in config.toml doesn't match any configured user. "
                f"Will be ignored. Remove it or add the user."
            )
    return warnings
