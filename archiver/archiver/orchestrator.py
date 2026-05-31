"""
archiver.orchestrator
─────────────────────
Template Method that drives an archive cycle. Delegates the variable
steps to Platform strategies.

Skeleton (same for every platform):
  1. Circuit-breaker check (per-platform tripped flag from prior run)
  2. Health check
  3. If unhealthy → attempt_recovery()
  4. For each user:
       a. Reconcile disk → DB (uses identity-resolver + stability check;
          picks up manually-added subfolder content automatically)
       b. Download new media (date-min/dateafter from db.max_sent_upload_date)
       c. (removed) Uploads are handled by the dispatcher — the archiver
          only inserts pending rows during download
       d. Advance last_run_utc and date_floor checkpoints

The CHECKPOINT change vs v1:
  v1 stored only `last_run_utc` and used that as the date filter.
  v2 stores `date_floor = MAX(upload_date WHERE status='sent')` and
  uses THAT as the date filter. This keeps incremental work correct
  under `delete_after_upload=true` and after long gaps between runs.
  last_run_utc is kept as the fallback when there's no completed upload
  yet (first run).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Config
from .lock_reader import tiktok_lock_held
from .platforms import Platform, AuthError, HealthStatus
from .reconcile import (
    reconcile_platform_root, reconcile_recordings, reconcile_user,
)

from core import (
    ItemStore, DeletePolicy, DedupPolicy, DownloadPolicy, dedup_user,
    cleanup_sidecars, validate_overrides as _validate_policies,
)

log = logging.getLogger(__name__)


def build_platforms(config: Config) -> list[Platform]:
    """Instantiate every Platform whose config block is present."""
    platforms: list[Platform] = []
    if config.x:
        from .platforms import XPlatform
        platforms.append(XPlatform(config))
    if config.tiktok:
        from .platforms import TikTokPlatform
        platforms.append(TikTokPlatform(config))
    if config.instagram:
        from .platforms import InstagramPlatform
        platforms.append(InstagramPlatform(config))
    # User-managed folders treated as platforms (no download).
    if config.local_platforms:
        from .platforms import LocalPlatform
        for name in config.local_platforms:
            platforms.append(LocalPlatform(config, name))
    return platforms


# ── Orchestrator ──────────────────────────────────────────────────────────────

class Archiver:
    """
    Drives a full archive cycle across all configured platforms.
    Stateful in-memory only — persistent state lives in the DB.
    """

    def __init__(self, config: Config, db: ItemStore):
        self.config = config
        self.db     = db
        # Policies share the single PolicyStore on config. Adding another
        # policy in the future is one class + one line here.
        self.delete_policy   = DeletePolicy(config.policy_store)
        self.dedup_policy    = DedupPolicy(config.policy_store)
        self.download_policy = DownloadPolicy(config.policy_store)
        # Per-platform tripped flag for THIS run. Resets each new Archiver.
        self._tripped: set[str] = set()

    async def run(self,
                  platform_filter: str | None = None,
                  user_filter:     str | None = None) -> dict[str, dict]:
        """Run a full cycle. Returns per-(platform, user) results."""
        if not self._verify_output_dir():
            return {
                "preflight": {
                    "status": "error",
                    "reason": f"OUTPUT_DIR not writable: {self.config.output_dir}",
                }
            }

        platforms = build_platforms(self.config)
        # Full platform-name set (before any --platform filter) so the orphaned
        # ingest pass knows which top-level dirs are platforms vs chat_id folders.
        known_platform_names = {p.name for p in platforms}
        if platform_filter:
            platforms = [p for p in platforms if p.name == platform_filter]
            if not platforms:
                log.error("No matching platform: %s", platform_filter)
                return {}

        # Validate per-(platform, user) env overrides BEFORE we start
        # work — typos in DELETE_AFTER_UPLOAD_* or TELEGRAM_CHAT_ID_*
        # otherwise silently fall through and the user wonders why.
        self._log_overrides(platforms)

        run_time = datetime.now(timezone.utc)
        results: dict[str, dict] = {}

        # Downloading writes pending rows straight into the shared items
        # table; the dispatcher drains them asynchronously. There is no
        # enqueue handoff and no reconcile bridge — one table, one truth.
        await self._run_platforms(platforms, user_filter, run_time, results)
        if self.config.reconcile_after_run:
            # Post-run sweep reconciles+uploads EVERY enabled platform
            # (download-disabled ones included), so they're covered here.
            await self._reconcile_after_run(platforms, user_filter)
        else:
            # No global sweep — but download-disabled platforms must STILL be
            # reconciled+uploaded (that's their whole point), so do just those.
            for platform in platforms:
                if not self.download_policy.enabled_for(platform.name):
                    await self._reconcile_one_platform(platform, user_filter)
        self._maybe_ingest_orphaned(known_platform_names)
        return results

    def _maybe_ingest_orphaned(self, known_platform_names: set[str]) -> None:
        """When the auto_ingest_orphaned policy is on, scan output_dir's
        chat_id-named folders and enqueue loose files — the automated form of
        `archiver ingest`. Off by default; toggle via `archiver auto-ingest`."""
        from core import AutoIngestPolicy, ingest_chat_id_dirs

        if not AutoIngestPolicy(self.config.policy_store).enabled():
            return
        reports = ingest_chat_id_dirs(
            self.db, self.config.output_dir,
            known_platforms=known_platform_names,
        )
        total = sum(r.inserted for r in reports)
        if total:
            log.info("auto-ingest: enqueued %d loose file(s) from chat_id folders",
                     total)
        for r in reports:
            if not r.skipped_dir and (r.inserted or r.deduped):
                log.info("  %s", r)

    async def _run_platforms(
        self,
        platforms: list[Platform],
        user_filter: str | None,
        run_time: datetime,
        results: dict[str, dict],
    ) -> None:
        """The per-platform / per-user loop."""
        for platform in platforms:
            # Download disabled → skip fetch AND the auth/cookies health-check
            # entirely. The folder is still reconciled + uploaded (always, in
            # run() above), just never downloaded — for hand-managed platforms
            # like a manual Instagram backup.
            if not self.download_policy.enabled_for(platform.name):
                log.info("[%s] download disabled — reconcile/upload only",
                         platform.name)
                continue
            if not await self._ensure_platform_healthy(platform):
                self._tripped.add(platform.name)
                log.error("Skipping platform %s — health check failed", platform.name)
                continue

            users = platform.users
            if user_filter:
                users = tuple(u for u in users if u == user_filter)
                if not users:
                    log.warning("User %s not configured for %s",
                                user_filter, platform.name)
                    continue

            for i, username in enumerate(users):
                if i > 0:
                    await asyncio.sleep(self.config.sleep_max * 2)
                if platform.name in self._tripped:
                    log.warning("[%s/%s] skipped — circuit tripped this run",
                                platform.name, username)
                    results[f"{platform.name}/{username}"] = {
                        "status": "skipped", "reason": "circuit-tripped",
                    }
                    continue

                key = f"{platform.name}/{username}"
                try:
                    results[key] = await self._archive_user(
                        platform, username, run_time,
                    )
                except Exception as e:
                    log.error("[%s] uncaught error: %s",
                              key, e, exc_info=True)
                    results[key] = {"status": "error", "reason": str(e)}

    def _log_overrides(self, platforms: list[Platform]) -> None:
        """Validate + log delete-policy and dedup-policy at startup."""
        known_users = {p.name: p.users for p in platforms}

        # Typo validation first — these go to WARN unconditionally.
        for w in _validate_policies(self.config.policy_store, known_users):
            log.warning(w)

        # Resolution summary — only INFO when something non-default.
        any_delete = False
        any_dedup  = False
        for p in platforms:
            for u in p.users:
                if self.delete_policy.should_delete(p.name, u):
                    any_delete = True
                    log.info("delete-after-upload: [%s] @%s → %s",
                             p.name, u, self.delete_policy.explain(p.name, u))
                if self.dedup_policy.should_dedup(p.name, u):
                    any_dedup = True
                    log.info("dedup-after-download: [%s] @%s → %s",
                             p.name, u, self.dedup_policy.explain(p.name, u))
        if not any_delete:
            log.info("delete-after-upload: OFF for all users this run")
        if not any_dedup:
            log.info("dedup-after-download: OFF for all users this run")

    async def _reconcile_after_run(
        self,
        platforms: list[Platform],
        user_filter: str | None,
    ) -> None:
        """Optional final disk sweep: dedup user folders, then queue any
        stable media files missing from the shared DB."""
        log.info("post-run reconcile: starting")
        total_inserted = 0
        total_deleted = 0
        total_bytes_freed = 0

        for platform in platforms:
            ins, dele, freed = await self._reconcile_one_platform(
                platform, user_filter)
            total_inserted += ins
            total_deleted += dele
            total_bytes_freed += freed

        if not user_filter:
            recording_reports = await asyncio.to_thread(
                reconcile_recordings, self.db,
            )
            for report in recording_reports:
                if report.scanned or report.inserted:
                    log.info("post-run reconcile recordings: %s", report)
                total_inserted += report.inserted

        log.info(
            "post-run reconcile: queued %d file(s), dedup deleted %d file(s) "
            "(%.1f MB)",
            total_inserted,
            total_deleted,
            total_bytes_freed / (1024 * 1024),
        )

    async def _reconcile_one_platform(
        self,
        platform: Platform,
        user_filter: str | None,
    ) -> tuple[int, int, int]:
        """Walk one platform's folder and queue everything missing from the DB:
        loose root files, then each user (configured ∪ disk-discovered) with a
        content-dedup pass. Returns (inserted, deleted, bytes_freed). This is
        the 'always upload everything' half — used both by the post-run sweep
        and, for download-disabled platforms, unconditionally each run."""
        inserted = deleted = bytes_freed = 0

        if not user_filter:
            root_report = await asyncio.to_thread(
                reconcile_platform_root,
                platform, self.db, self.config.output_dir,
            )
            if root_report.scanned or root_report.inserted:
                log.info("reconcile: %s", root_report)
            inserted += root_report.inserted

        for username in self._reconcile_users_for_platform(platform, user_filter):
            user_dir = Path(self.config.output_dir) / platform.name / username
            dedup_report = await asyncio.to_thread(
                dedup_user, platform.name, username, user_dir, self.db,
                dry_run=False,
            )
            if dedup_report.confirmed_groups:
                log.info("dedup: %s", dedup_report)
            deleted += dedup_report.deleted
            bytes_freed += dedup_report.bytes_freed

            report = await asyncio.to_thread(
                reconcile_user, platform, username, self.db,
                self.config.output_dir, True,
            )
            if report.inserted or report.seeded_archive:
                log.info("reconcile: %s", report)
            inserted += report.inserted

        return inserted, deleted, bytes_freed

    def _reconcile_users_for_platform(
        self,
        platform: Platform,
        user_filter: str | None,
    ) -> tuple[str, ...]:
        if user_filter:
            return (user_filter.lstrip("@"),)

        platform_dir = Path(self.config.output_dir) / platform.name
        disk_users = {
            p.name
            for p in platform_dir.iterdir()
            if p.is_dir()
        } if platform_dir.exists() else set()

        users = set(platform.users) | disk_users
        return tuple(sorted(users))

    # ── Per-user cycle ───────────────────────────────────────────────────────

    async def _download_with_recovery(
        self, platform: Platform, username: str,
    ) -> dict:
        """Run platform.download with the original auth-failure and
        disk-full recovery behavior. Returns {'count': int} on success or
        {'_error': <result dict>} when the caller should early-return.
        Extracted verbatim from the old inline 4b block so the lockfile
        branch above can bypass it cleanly."""
        try:
            count = await asyncio.to_thread(platform.download, username, self.db)
            return {"count": count}
        except AuthError as e:
            handled = await self._handle_auth_failure(platform, str(e))
            if handled:
                try:
                    count = await asyncio.to_thread(
                        platform.download, username, self.db,
                    )
                    return {"count": count}
                except AuthError as e2:
                    log.error("  Auth still failing after recovery: %s", e2)
                    await self._handle_auth_failure(platform, str(e2),
                                                    attempt_recovery=False)
                    self._tripped.add(platform.name)
                    return {"_error": {"status": "auth-failed", "reason": str(e2)}}
            return {"_error": {"status": "auth-failed", "reason": str(e)}}
        except OSError as e:
            if getattr(e, "errno", None) == 28:  # ENOSPC
                log.warning("  Disk full — purging already-sent files")
                self._purge_sent_files(platform.name, username)
                try:
                    count = await asyncio.to_thread(
                        platform.download, username, self.db,
                    )
                    return {"count": count}
                except Exception as e2:
                    log.error("  Retry after disk-full failed: %s", e2)
                    return {"_error": {"status": "error",
                                       "reason": "disk-full-unresolved"}}
            raise

    async def _archive_user(
        self,
        platform: Platform,
        username: str,
        run_time: datetime,
    ) -> dict:
        log.info("━━━ [%s] @%s ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                 platform.name, username)

        # 4a. Reconcile disk → DB (Reconcile v2: stability + identity)
        report = await asyncio.to_thread(
            reconcile_user, platform, username, self.db,
            self.config.output_dir, True,
        )
        if report.inserted:
            log.info("  Reconciled: %s", report)

        # 4b. Download
        if platform.name == "tiktok" and tiktok_lock_held():
            log.info("  [tiktok] lockfile present (recorder active) — "
                     "skipping download; pending uploads still processed")
            new_count = 0
        else:
            dl = await self._download_with_recovery(platform, username)
            if dl.get("_error"):
                return dl["_error"]
            new_count = dl["count"]

        # 4c. (removed) No enqueue handoff: the download above already
        # inserted pending rows into the shared items table, which the
        # dispatcher drains on its own schedule.

        # 4d. Advance checkpoints. The download reached here without an
        # early-return error, so it completed. date_floor reads
        # MAX(upload_date WHERE status='sent') straight from the shared
        # table — it only moves past posts the dispatcher has actually
        # confirmed delivered, so a crash or a slow queue never loses
        # ground, even though sending is asynchronous.
        self.db.set_last_run(platform.name, username, run_time)
        new_floor = self.db.max_sent_upload_date(platform.name, username)
        self.db.set_date_floor(platform.name, username, new_floor)
        self.db.reset_circuit(platform.name)
        log.info("  ✓ Checkpoint → last_run=%s floor=%s",
                 run_time.strftime("%Y-%m-%d %H:%M UTC"), new_floor or "-")

        s = self.db.stats(platform.name, username)
        log.info("  Stats: total=%d sent=%d pending=%d failed=%d (%.1f MB)",
                 s["total"], s["sent"], s["pending"], s["failed"], s["total_mb"])

        # 4e. Optional dedup pass — policy opt-in. dedup_user only removes
        # files already status='sent' (confirmed delivered), so in-flight
        # or pending files are never deleted out from under the dispatcher.
        if self.dedup_policy.should_dedup(platform.name, username):
            user_dir = Path(self.config.output_dir) / platform.name / username
            dedup_report = await asyncio.to_thread(
                dedup_user,
                platform.name, username, user_dir, self.db,
                dry_run=False,
            )
            if dedup_report.confirmed_groups:
                log.info("  Dedup: %s", dedup_report)

        return {
            "status":     "ok",
            "downloaded": new_count,
            "pending":    s["pending"],
            "sent":       s["sent"],
            "failed":     s["failed"],
        }

    async def _ensure_platform_healthy(self, platform: Platform) -> bool:
        status: HealthStatus = platform.health_check()
        if status.healthy:
            return True

        log.warning("[%s] unhealthy: %s", platform.name, status.reason)
        log.info("[%s] attempting recovery…", platform.name)

        if not await asyncio.to_thread(platform.attempt_recovery):
            log.error("[%s] recovery failed — manual intervention required",
                      platform.name)
            return False

        status = platform.health_check()
        if not status.healthy:
            log.error("[%s] still unhealthy after recovery: %s",
                      platform.name, status.reason)
            return False

        log.info("[%s] recovered ✓", platform.name)
        return True

    async def _handle_auth_failure(self, platform: Platform, error_msg: str,
                                   attempt_recovery: bool = True) -> bool:
        fails = self.db.bump_circuit_fail(platform.name, error_msg)
        log.warning("[%s] auth failure #%d", platform.name, fails)

        if fails >= self.config.auth_failure_threshold:
            until = datetime.now(timezone.utc) + timedelta(hours=6)
            self.db.trip_circuit(platform.name, until)
            self._tripped.add(platform.name)
            log.error(
                "[%s] CIRCUIT TRIPPED after %d consecutive auth failures. "
                "Skipping for this run.", platform.name, fails,
            )
            return False

        if not attempt_recovery:
            return False

        return await asyncio.to_thread(platform.attempt_recovery)

    # ── Pre-flight + disk pressure ────────────────────────────────────────────

    def _verify_output_dir(self) -> bool:
        out = Path(self.config.output_dir)
        try:
            out.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            log.error("OUTPUT_DIR cannot be created: %s (%s)", out, e)
            if "Volumes" in str(out):
                log.error("  → Is the external drive mounted? "
                          "Check: ls /Volumes/")
            return False

        probe = out / ".archiver_writetest"
        try:
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
        except OSError as e:
            log.error("OUTPUT_DIR exists but is not writable: %s (%s)",
                      out, e)
            return False

        return True

    def _purge_sent_files(self, platform: str, username: str) -> int:
        """Force-delete files with status='sent' (disk-full path).
        Bypasses DeletePolicy intentionally — the rows are already sent."""
        freed = 0
        for fp in self.db.sent_file_paths(platform, username):
            path = Path(fp)
            try:
                if path.exists():
                    freed += path.stat().st_size
                cleanup_sidecars(fp)
            except OSError:
                pass
        log.info("  Purged %.1f MB of already-sent files", freed / 1_048_576)
        return freed


# ── Bootstrap entry point ─────────────────────────────────────────────────────

async def bootstrap(config: Config, db: ItemStore,
                    platform_filter: str | None = None,
                    user_filter:     str | None = None) -> dict:
    """
    One-shot operation: absorb an existing on-disk archive into the
    system without performing any network requests.

    Steps per (platform, user):
      1. reconcile_user(...) — walks disk, registers everything in DB,
         seeds the extractor's archive file with known identifiers.
      2. set_date_floor() — so the next `archiver run` is incremental.

    Does NOT touch Telegram. Does NOT trigger any extractor. Safe to run
    repeatedly — reconcile + seed are both idempotent.
    """
    platforms = build_platforms(config)
    if platform_filter:
        platforms = [p for p in platforms if p.name == platform_filter]

    summary: dict = {}
    for platform in platforms:
        users = platform.users
        if user_filter:
            users = tuple(u for u in users if u == user_filter)

        for username in users:
            report = await asyncio.to_thread(
                reconcile_user, platform, username, db, config.output_dir, True,
            )
            # Bootstrap also writes the date_floor checkpoint. The reconcile
            # function already computed report.max_upload_date.
            if report.max_upload_date:
                db.set_date_floor(platform.name, username, report.max_upload_date)
            summary[f"{platform.name}/{username}"] = report
            log.info("bootstrap: %s", report)

    return summary
