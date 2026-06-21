"""
core.policies
───────────────────
Shared Specification-on-Repository policies: each policy owns its TOML key
and default; PolicyStore handles storage and hierarchical resolution.
"""

from __future__ import annotations

import logging

from .files import ALBUM_MAX
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


class RecorderDeletePolicy(BooleanPolicy):
    """Delete recorded files after a successful Telegram upload.

    Default ON: recordings are large and, once delivered to Telegram, the
    local copy is redundant. The dispatcher's maybe_delete gate still re-reads
    status=='sent' before unlinking, so a file is only ever removed after a
    confirmed upload. Set delete_after_upload_records=false in config.toml to
    keep local copies. (The general delete_after_upload for VOD downloads stays
    OFF by default — see DeletePolicy.)"""
    KEY     = "delete_after_upload_records"
    DEFAULT = True

    def should_delete_recording(self) -> bool:
        return self.is_enabled("tiktok", "_recorder")


class ProtectionPolicy(BooleanPolicy):
    """Safebrake: when enabled for a (platform, user) scope, NOTHING may delete
    that scope's files. It overrides every deletion path in the suite —
    delete-after-upload (DeletePolicy / RecorderDeletePolicy), dispatcher
    dedup-suppression of an already-delivered copy, reconcile's re-introduction
    cleanup, the disk-full emergency purge, and the `purge-sent` command all
    consult it (via core.DeletionGuard) before unlinking.

    Resolved hierarchically (user → platform → global) like every other policy,
    so you can shield a single user or an entire platform. Default OFF — the
    safebrake is opt-in; absent it, the normal delete policies decide. Set with
    `archiver safebrake set --platform X [--user Y] --on true`."""
    KEY     = "protect_from_deletion"
    DEFAULT = False

    def is_protected(self, platform: str, username: str) -> bool:
        return self.is_enabled(platform, username)


class DedupPolicy(BooleanPolicy):
    """Whether to run content-hash dedup after a successful download."""
    KEY     = "dedup_after_download"
    DEFAULT = False

    def should_dedup(self, platform: str, username: str) -> bool:
        return self.is_enabled(platform, username)


class BatchPolicy:
    """Minimum-album-size gate for PLATFORM (archiver) uploads.

    The dispatcher holds a platform user's pending items and only sends an
    album once `min_batch_size` of them have accumulated in the same group +
    media bucket — so you get full albums instead of a trickle of singletons.
    Default 10 (== ALBUM_MAX, a full Telegram album).

    To stop a tail of <size items lingering forever (a user with 7 photos and
    no new downloads), `max_wait_hours` flushes a partial batch once its oldest
    item has waited that long. Default 7 days (168h).

    Resolution is hierarchical (user → platform → global) via PolicyStore, so
    you can tune per-user. Set min_batch_size=1 to disable the gate entirely
    (restores the old send-whatever-is-pending behavior). This policy is an
    int/float pair, not a BooleanPolicy, so it doesn't subclass it.
    """
    SIZE_KEY       = "min_batch_size"
    WAIT_KEY       = "min_batch_max_wait_h"
    DEFAULT_SIZE   = 10
    DEFAULT_WAIT_H = 168.0   # 7 days

    def __init__(self, store: PolicyStore):
        self._store = store

    def min_batch_size(self, platform: str, username: str) -> int:
        """Required items before a platform album is sent. Clamped to
        [1, ALBUM_MAX] — a threshold above the album cap is unreachable."""
        v = self._store.get(self.SIZE_KEY, platform=platform, username=username,
                            default=self.DEFAULT_SIZE)
        try:
            n = int(v)
        except (TypeError, ValueError):
            log.warning("policy %s: non-int %r for %s/%s — using %d",
                        self.SIZE_KEY, v, platform, username, self.DEFAULT_SIZE)
            n = self.DEFAULT_SIZE
        return max(1, min(n, ALBUM_MAX))

    def max_wait_hours(self, platform: str, username: str) -> float:
        """Hours an under-size batch may wait before it's flushed anyway.
        0 disables flushing (strict: only ever send full batches)."""
        v = self._store.get(self.WAIT_KEY, platform=platform, username=username,
                            default=self.DEFAULT_WAIT_H)
        try:
            h = float(v)
        except (TypeError, ValueError):
            log.warning("policy %s: non-float %r for %s/%s — using %.1f",
                        self.WAIT_KEY, v, platform, username, self.DEFAULT_WAIT_H)
            h = self.DEFAULT_WAIT_H
        return max(0.0, h)


class DownloadPolicy(BooleanPolicy):
    """Whether the archiver FETCHES new media for a platform (default on).

    Turn it OFF to treat a built-in platform as reconcile-and-upload-only: you
    manage its files by hand (e.g. a manual Instagram backup), and each run
    still walks the folder and uploads everything — configured users,
    disk-discovered users, and loose root files — but downloads nothing and
    needs no auth/cookies. Resolved at platform scope (user dimension unused).
    """
    KEY     = "download_enabled"
    DEFAULT = True

    def enabled_for(self, platform: str) -> bool:
        value = self._store.get(self.KEY, platform=platform, default=self.DEFAULT)
        if not isinstance(value, bool):
            log.warning("policy %s: non-bool %r for %s — using %s",
                        self.KEY, value, platform, self.DEFAULT)
            return self.DEFAULT
        return value


class AutoIngestPolicy(BooleanPolicy):
    """Whether `archiver start` also ingests chat_id (orphaned) folders each
    cycle. Global toggle — drop loose files into output_dir/<chat_id>/… and
    they're enqueued automatically instead of needing a manual `archiver
    ingest`. Default ON: dropping a file in a chat_id folder should Just Work.
    Set auto_ingest_orphaned=false in config.toml to require the manual step."""
    KEY     = "auto_ingest_orphaned"
    DEFAULT = True

    def enabled(self) -> bool:
        """Global-scope read (no platform/user dimension for this toggle)."""
        value = self._store.get(self.KEY, default=self.DEFAULT)
        if not isinstance(value, bool):
            log.warning("policy %s: non-bool %r — using %s",
                        self.KEY, value, self.DEFAULT)
            return self.DEFAULT
        return value


class SortPolicy(BooleanPolicy):
    """Whether `archiver start` first sorts output_dir/unsorted/ each cycle —
    moving username_timestamp-named files into <platform>/<username>/ before the
    fetch/reconcile/ingest phases see them. Global toggle, default OFF (the sort
    only runs when you opt in; `archiver sort` works on demand regardless).

    The destination platform for the sweep is a companion string key (default
    'instagram'), since unsorted filenames carry the username but not which
    platform folder they belong under. Set with `archiver auto-sort set`."""
    KEY              = "sort_unsorted"
    DEFAULT          = False
    PLATFORM_KEY     = "sort_unsorted_platform"
    DEFAULT_PLATFORM = "instagram"

    def enabled(self) -> bool:
        """Global-scope read (no platform/user dimension for this toggle)."""
        value = self._store.get(self.KEY, default=self.DEFAULT)
        if not isinstance(value, bool):
            log.warning("policy %s: non-bool %r — using %s",
                        self.KEY, value, self.DEFAULT)
            return self.DEFAULT
        return value

    def target_platform(self) -> str:
        """Destination platform folder for the auto-sort sweep."""
        value = self._store.get(self.PLATFORM_KEY, default=self.DEFAULT_PLATFORM)
        if not isinstance(value, str) or not value.strip():
            log.warning("policy %s: non-string %r — using %s",
                        self.PLATFORM_KEY, value, self.DEFAULT_PLATFORM)
            return self.DEFAULT_PLATFORM
        return value.strip()


class FailedRetryPolicy(BooleanPolicy):
    """Whether the DISPATCHER's periodic housekeeping auto-re-queues
    terminally-'failed' uploads, so a failure caused by a transient condition
    (a chat that was briefly unreachable, a network blip that outlived the
    per-send retry budget) heals on its own instead of waiting for a manual
    `archiver reset failed`. Lives in the queue owner (dispatcher.drain), so it
    runs on the ~15-min housekeeping cadence regardless of the archiver loop.

    Global toggle, default OFF — a file that fails for a PERMANENT reason
    (oversized, corrupt, a media Telegram rejects) would otherwise get re-armed
    every housekeeping pass and burn its full send-retry budget again, turning a
    single poison row into a perpetual re-upload storm that starves the rest of
    the queue. Opt in with auto_retry_failed=true in config.toml (or
    `archiver auto-retry set --enabled true`) once failures are known-transient;
    otherwise heal them deliberately with `archiver reset failed`.

    Independent of the missing-file sweep, which is unconditional cleanup: a
    'failed' row whose file is gone from disk can never succeed, so the
    dispatcher deletes it every housekeeping pass regardless of this policy (see
    ItemStore.delete_failed_missing — run BEFORE this re-queue, so a missing
    file is never re-armed)."""
    KEY     = "auto_retry_failed"
    DEFAULT = False

    def enabled(self) -> bool:
        """Global-scope read (no platform/user dimension for this toggle)."""
        value = self._store.get(self.KEY, default=self.DEFAULT)
        if not isinstance(value, bool):
            log.warning("policy %s: non-bool %r — using %s",
                        self.KEY, value, self.DEFAULT)
            return self.DEFAULT
        return value


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
