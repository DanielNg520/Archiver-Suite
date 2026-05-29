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
       b. Download new media (date-min/dateafter from db.max_upload_date)
       c. Upload pending to Telegram (per-platform peer via TelegramRouter)
       d. On clean uploads: advance both last_run_utc AND date_floor

The CHECKPOINT change vs v1:
  v1 stored only `last_run_utc` and used that as the date filter.
  v2 stores `date_floor = MAX(upload_date WHERE telegram_sent=1)` and
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
from .db import ArchiveDB
from .dedup import dedup_user
from .dispatch_client import DispatchClient
from .lock_reader import tiktok_lock_held
from .platforms import Platform, AuthError, HealthStatus
from .policies import DeletePolicy, DedupPolicy, validate_overrides as _validate_policies
from .reconcile import reconcile_user
from .tg_router import TelegramRouter, validate_overrides as _validate_router

log = logging.getLogger(__name__)


# ── Local sidecar cleanup (disk-full purge only) ──────────────────────────────
#
# The legacy archiver.telegram._cleanup was the old uploader's delete helper.
# That module is gone — the dispatcher now owns all sending AND all
# delete-after-upload. The ONE remaining archiver-side delete is the disk-full
# emergency purge of already-confirmed-sent files (telegram_sent=1), which must
# work even when the dispatcher isn't running. We inline the unlink here rather
# than import dispatcher.delete: the archiver↔dispatcher contract is the SQLite
# schema (see dispatch_client.py), not a Python import. ~8 lines is cheaper than
# coupling the two installable packages.

def _purge_one(file_path: str) -> None:
    """Unlink a media file plus its yt-dlp / gallery-dl sidecars. Ungated;
    callers must already know the row is sent. Mirrors the old
    archiver.telegram._cleanup byte-for-byte so purge behavior is unchanged."""
    p = Path(file_path)
    try:
        p.unlink(missing_ok=True)
    except OSError as e:
        log.warning("    cleanup failed: %s", e)
        return
    for suffix in (".json", ".info.json"):       # yt-dlp: <stem>.info.json
        try:
            p.with_suffix(suffix).unlink(missing_ok=True)
        except OSError:
            pass
    try:                                          # gallery-dl: <full_name>.json
        (p.parent / (p.name + ".json")).unlink(missing_ok=True)
    except OSError:
        pass


# ── Platform registry ─────────────────────────────────────────────────────────

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
    return platforms


# ── Orchestrator ──────────────────────────────────────────────────────────────

class Archiver:
    """
    Drives a full archive cycle across all configured platforms.
    Stateful in-memory only — persistent state lives in the DB.
    """

    def __init__(self, config: Config, db: ArchiveDB):
        self.config = config
        self.db     = db
        # Policies share the single PolicyStore on config. Adding another
        # policy in the future is one class + one line here.
        self.delete_policy = DeletePolicy(config.policy_store)
        self.dedup_policy  = DedupPolicy(config.policy_store)
        self.router = TelegramRouter(
            default_chat_id = config.telegram.chat_id,
        )
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

        # Dispatcher is the only upload path. The archiver never opens a
        # Telegram session; it enqueues into dispatcher.db and the dispatcher
        # process owns the session, the sends, the album/sort decisions (it
        # makes none — one row, one send), and delete-after-upload.
        log.info("upload mode: dispatcher (enqueue → %s)",
                 self.config.dispatcher_db_path)
        await self._run_platforms(platforms, user_filter, run_time, results)
        # Pull any completions the dispatcher recorded since the last run
        # (flips archive.db rows telegram_sent 2 → 1, advancing date_floor).
        self.db.reconcile_dispatch_outcomes(self.config.dispatcher_db_path)
        return results

    async def _run_platforms(
        self,
        platforms: list[Platform],
        user_filter: str | None,
        run_time: datetime,
        results: dict[str, dict],
    ) -> None:
        """The per-platform / per-user loop."""
        for platform in platforms:
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
        """Validate + log delete-policy, dedup-policy, and router resolution at startup."""
        known_users = {p.name: p.users for p in platforms}

        # Typo validation first — these go to WARN unconditionally.
        for w in _validate_policies(self.config.policy_store, known_users):
            log.warning(w)
        for w in _validate_router(known_users):
            log.warning(w)

        # Resolution summary — only INFO when something non-default.
        any_delete = False
        any_dedup  = False
        any_route  = False
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
                chat_explanation = self.router.explain(p.name, u)
                if "global TELEGRAM_CHAT_ID" not in chat_explanation:
                    any_route = True
                    log.info("telegram-route: [%s] @%s → %s",
                             p.name, u, chat_explanation)
        if not any_delete:
            log.info("delete-after-upload: OFF for all users this run")
        if not any_dedup:
            log.info("dedup-after-download: OFF for all users this run")
        if not any_route:
            log.info("telegram-route: all users → global TELEGRAM_CHAT_ID")

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

        # 4c. Upload: hand off to the dispatcher. "sent" here means
        # "handed off"; the real send happens later in the dispatcher.
        enqueued, failed = self._enqueue_pending(platform.name, username)
        sent = enqueued

        # 4d. Advance checkpoints (both last_run_utc AND date_floor).
        #
        # all_ok semantics differ by mode:
        #   direct mode  — failed==0 means every file reached Telegram.
        #   dispatcher mode — failed==0 means every file was ENQUEUED
        #     cleanly. The actual send happens later in the dispatcher.
        #
        # date_floor reads MAX(upload_date WHERE telegram_sent=1). In
        # dispatcher mode enqueued files are telegram_sent=2 (queued), so
        # they DON'T move the floor until reconcile_dispatch_outcomes()
        # flips them to 1 on a later run. That's the desired conservative
        # behavior: the download cutoff only advances past files Telegram
        # has actually confirmed, even though the handoff is async.
        all_ok = failed == 0
        if all_ok:
            self.db.set_last_run(platform.name, username, run_time)
            new_floor = self.db.max_upload_date(platform.name, username,
                                                 sent_only=True)
            self.db.set_date_floor(platform.name, username, new_floor)
            self.db.reset_circuit(platform.name)
            log.info("  ✓ Checkpoint → last_run=%s floor=%s (enqueued=%d)",
                     run_time.strftime("%Y-%m-%d %H:%M UTC"),
                     new_floor or "-", sent)
        else:
            log.warning(
                "  ✗ %d enqueue failure(s) — checkpoint NOT advanced. "
                "Run: archiver reset failed --platform %s --user %s",
                failed, platform.name, username,
            )

        s = self.db.stats(platform.name, username)
        log.info("  Stats: total=%d sent=%d pending=%d failed=%d (%.1f MB)",
                 s["total"], s["sent"], s["pending"], s["failed"], s["total_mb"])

        # 4e. Optional dedup pass — gated on clean run + policy opt-in.
        # Stays gated on telegram_sent=1 (confirmed sent). In dispatcher
        # mode, freshly-enqueued files are state 2 and are NOT eligible for
        # dedup deletion until the dispatcher confirms them — correct,
        # since deleting an in-flight file would force a re-download.
        if all_ok and self.dedup_policy.should_dedup(platform.name, username):
            user_dir = Path(self.config.output_dir) / platform.name / username
            dedup_report = await asyncio.to_thread(
                dedup_user,
                platform.name, username, user_dir, self.db,
                dry_run=False,
            )
            if dedup_report.confirmed_groups:
                log.info("  Dedup: %s", dedup_report)

        return {
            "status":     "ok" if all_ok else "partial",
            "downloaded": new_count,
            "uploaded":   sent,
            "failed":     failed,
        }

    def _enqueue_pending(self, platform: str, username: str) -> tuple[int, int]:
        """Dispatcher-mode replacement for uploader.upload_pending.

        Reads pending rows from archive.db, inserts each into dispatcher.db
        with priority=10, and flips the archive.db row to telegram_sent=2
        (queued). Returns (enqueued_count, failed_count).

        A 'failure' here is an enqueue error (e.g. dispatcher.db locked or
        missing) — NOT a send failure. The file stays pending (we don't
        mark_queued on failure), so the next run retries the enqueue.
        """
        rows = self.db.pending_uploads(platform, username)
        if not rows:
            return 0, 0

        # Same unmounted-storage guard the legacy uploader uses: if the
        # first file's parent dir is gone, the drive is likely unmounted.
        # Leave everything pending rather than enqueue paths that the
        # dispatcher will only fail to read.
        first_parent = Path(rows[0]["file_path"]).parent
        if not first_parent.exists():
            log.error(
                "  [%s] @%s: parent dir unreachable (%s) — storage may be "
                "unmounted. Skipping enqueue; rows stay pending.",
                platform, username, first_parent,
            )
            return 0, 0

        enqueued = failed = 0
        try:
            with DispatchClient(self.config.dispatcher_db_path) as dc:
                chat_explain = self.router.explain(platform, username)
                log.info("  Enqueuing: %d pending for @%s [%s] → dispatcher "
                         "(dest resolved by dispatcher: %s)",
                         len(rows), username, platform, chat_explain)
                for row in rows:
                    caption = self._make_caption(platform, username, row)
                    try:
                        dc.enqueue(
                            platform=platform,
                            username=username,
                            file_path=row["file_path"],
                            caption=caption,
                            priority=10,
                        )
                        self.db.mark_queued(row["file_path"])
                        enqueued += 1
                    except Exception as e:
                        log.error("    enqueue failed for %s: %s",
                                  Path(row["file_path"]).name, e)
                        failed += 1
        except RuntimeError as e:
            # DispatchClient ctor failed (dispatcher.db missing). Whole
            # batch stays pending. This is a config/ops problem, surfaced
            # loud; rows are safe.
            log.error("  Cannot reach dispatcher.db: %s", e)
            return 0, len(rows)

        log.info("  @%s [%s]: queued %d, enqueue-failed %d",
                 username, platform, enqueued, failed)
        return enqueued, failed

    def _make_caption(self, platform: str, username: str, row) -> str:
        """Caption for a single enqueued file. Matches the legacy single-
        file caption from TelegramUploader._upload_single so dispatched
        files look identical to directly-sent ones. The dispatcher sends
        each queue row as a single file, so there's no album-caption
        variant to mirror here."""
        identifier = (row["identifier"] or "") if "identifier" in row.keys() else ""
        return (
            f"@{username} · {platform} · {identifier}"
            if identifier else
            f"@{username} · {platform}"
        )

    # ── Health + recovery ─────────────────────────────────────────────────────

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
        """Force-delete files marked telegram_sent=1 (disk-full path).
        Bypasses DeletePolicy intentionally — the rows are already sent."""
        freed = 0
        for fp in self.db.sent_file_paths(platform, username):
            path = Path(fp)
            try:
                if path.exists():
                    freed += path.stat().st_size
                _purge_one(fp)
            except OSError:
                pass
        log.info("  Purged %.1f MB of already-sent files", freed / 1_048_576)
        return freed


# ── Bootstrap entry point ─────────────────────────────────────────────────────

async def bootstrap(config: Config, db: ArchiveDB,
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
