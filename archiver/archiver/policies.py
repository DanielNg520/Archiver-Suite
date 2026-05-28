"""
archiver.policies
─────────────────
Typed, validated wrappers around PolicyStore. Each policy is a small
Specification class: it owns its TOML key, its default, and exposes a
named accessor. Storage and persistence come from PolicyStore.

ADDING A NEW POLICY:
  1. Subclass BooleanPolicy (or add a new base for non-bool types)
  2. Set KEY and DEFAULT
  3. (Optional) add a named accessor for readability at call sites
  4. Inject into the orchestrator where the decision is made

That's it. No new env vars, no new resolver, no new CLI mutator
boilerplate — the generic `archiver policy show/set/unset` already
handles arbitrary keys.

WHY A BASE CLASS RATHER THAN PLAIN FUNCTIONS:
  Each policy has identity (its key, its default). Plain functions
  force every call site to repeat the key string, which is exactly
  the drift bug that motivated this rewrite of DeletePolicy.

TYPE SAFETY:
  `is_enabled` defends against a malformed config.toml in which the
  value is the wrong type (e.g. string "false" instead of bool false).
  It logs and falls back to DEFAULT rather than crashing. This is the
  one place a string-typed bool could sneak in — TOML itself enforces
  types, but a hand-edited file could put `"yes"` instead of `true`.
"""

from __future__ import annotations

import logging

from .policy_store import PolicyStore

log = logging.getLogger(__name__)


class BooleanPolicy:
    """
    Base for any bool-valued policy. Subclasses set KEY and DEFAULT.

    Instances are cheap (one dict-traversal per lookup) and stateless;
    construct one per Archiver and pass it down.
    """

    KEY:     str  = ""    # MUST override
    DEFAULT: bool = False # MUST override (intentionally explicit)

    def __init__(self, store: PolicyStore):
        if not self.KEY:
            raise TypeError(
                f"{type(self).__name__} must set KEY (a non-empty TOML key)."
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
        """Human-readable resolution: '<value> (from <source>)'."""
        value, source = self._store.explain(
            self.KEY,
            platform = platform,
            username = username,
            default  = self.DEFAULT,
        )
        return f"{value} (from {source})"


# ── Concrete policies ─────────────────────────────────────────────────────────

class DeletePolicy(BooleanPolicy):
    """Whether to delete a local file after a successful Telegram upload."""
    KEY     = "delete_after_upload"
    DEFAULT = False

    # Keeps the call-site idiom: `if policy.should_delete(plat, user)`.
    # Drops cleanly into the existing uploader/orchestrator without
    # touching downstream code.
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
