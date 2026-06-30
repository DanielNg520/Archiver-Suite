"""
archiver.cli
────────────
Command-line entry. Subcommands instead of toggle flags — `archiver --help`
shows them all; each subcommand has its own --help.

Subcommands:
  run                    Normal archive cycle (downloads + uploads)
  loop                   Run `run` forever with random intervals (Ctrl-C to stop)
  bootstrap              One-shot: absorb existing on-disk archive into DB +
                         seed extractor archives + set date_floor checkpoints.
  reset failed           Re-queue failed uploads
  auto-retry             Show/toggle automatic per-cycle re-queue of failed
                         uploads (default ON; missing-file rows always cleaned)
  reset uploads          Re-queue ALL uploads (no re-download)
  reset user             Full wipe for one user (re-download + re-upload)
  reset all              Nuke DB rows + checkpoints for EVERY user
  reconcile              Scan disk for files missing from the DB
  dedup                  Content-hash duplicate removal (dry-run by default)
  stats                  Print per-platform / per-user counts
  health                 Run platform health checks (no downloads)
  cookies refresh        Manually refresh TikTok/Instagram cookies from Firefox
  cookies list           List available Firefox profiles
  config list/add/remove User-list management (edits config.toml)
  policy                 Show or edit delete-after-upload policy
  dedup-policy           Show or edit dedup-after-download policy
  migrate                One-shot: import legacy .env user lists + delete
                         policies into config.toml

CONFIG SURFACES:
  - .env       → secrets (API tokens, chat IDs, session paths)
  - config.toml → user lists + behavior policies (delete, dedup, ...)

  User lists live in TOML to avoid env-var encoding hazards with
  unicode / special-character usernames. Telegram chat IDs stay in .env
  because they're always numeric or @-handles.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .config import Config
from .orchestrator import Archiver, bootstrap, build_platforms
from .reconcile import (
    reconcile_platform_root, reconcile_recordings, reconcile_user,
)

from core import ItemStore, DeletePolicy, RecorderDeletePolicy, DedupPolicy
from core import (
    AutoIngestPolicy, CHAT_ID_PRIORITY, DeletionGuard, DownloadPolicy,
    ProtectionPolicy,
)
from core import SortPolicy
from core import cli as core_cli
from core import termui


PLATFORM_CHOICES = ["x", "tiktok", "instagram"]


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging(log_file: str, verbose: bool = False) -> None:
    termui.setup_logging(verbose, log_file=log_file)


log = logging.getLogger("archiver")


# ── Parser ────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog        = "archiver",
        description = "Multi-platform media archiver (X + TikTok + Instagram → Telegram).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("-v", "--verbose", action="store_true", help="DEBUG-level logging")

    sub = p.add_subparsers(dest="cmd", required=True, metavar="COMMAND")

    # ── start (harmonized verb: run the role; --once for a single cycle) ───
    s_start = sub.add_parser(
        "start", help="Run the archiver (continuous; --once for a single cycle)")
    s_start.add_argument("--once", action="store_true",
                         help="Run a single archive cycle and exit")
    s_start.add_argument("--platform", choices=PLATFORM_CHOICES,
                         help="Limit to one platform")
    s_start.add_argument("--user", metavar="USERNAME",
                         help="Limit to one username (no @)")
    s_start.add_argument("--min", dest="min_sleep", type=float, default=7200)
    s_start.add_argument("--max", dest="max_sleep", type=float, default=14400)
    s_start.add_argument("--max-fails", type=int, default=5)

    # ── run (alias: `start --once`) ───
    s_run = sub.add_parser("run", help="Single archive cycle (alias: start --once)")
    s_run.add_argument("--platform", choices=PLATFORM_CHOICES,
                       help="Limit to one platform")
    s_run.add_argument("--user", metavar="USERNAME",
                       help="Limit to one username (no @)")
    s_run.add_argument("--full-history", action="store_true",
                       help="Re-walk the entire timeline for the targeted "
                            "user(s), fetching old posts skipped by the "
                            "incremental date-min cutoff. Already-downloaded "
                            "posts are skipped (no re-download/re-send). "
                            "Requires --user or --platform.")

    # ── queue (shared noun, identical across all three binaries) ───
    core_cli.add_queue_parser(sub)

    # ── ingest (chat_id / orphaned folders) ───
    s_ingest = sub.add_parser(
        "ingest",
        help="Ingest loose files from chat_id-named folders under output_dir "
             "(folder name = Telegram destination). Dedups + enqueues them.",
    )
    s_ingest.add_argument(
        "--priority", type=int, default=CHAT_ID_PRIORITY,
        help=f"Queue priority for ingested files (default {CHAT_ID_PRIORITY}).")
    s_ingest.add_argument(
        "--path", metavar="DIR",
        help="Ingest an arbitrary folder (not just chat_id dirs under "
             "output_dir). Requires --chat.")
    s_ingest.add_argument(
        "--chat", metavar="CHAT_ID",
        help="Destination chat_id for --path ingestion.")

    # ── auto-ingest (toggle) ───
    s_ai = sub.add_parser(
        "auto-ingest",
        help="Show/toggle automatic ingest of chat_id folders each "
             "`archiver start` cycle. Default off.")
    ai_sub = s_ai.add_subparsers(dest="ai_action", required=False, metavar="ACTION",
                                 help="omit to print state; 'set'/'unset' to change")
    ai_set = ai_sub.add_parser("set", help="Enable or disable auto-ingest")
    ai_set.add_argument("--enabled", choices=["true", "false"], required=True)
    ai_sub.add_parser("unset", help="Remove the setting (back to default: off)")

    # ── sort (route output_dir/unsorted/ into <platform>/<username>/) ───
    s_sort = sub.add_parser(
        "sort",
        help="Move files from output_dir/unsorted/ into <platform>/<username>/ "
             "by parsing username_timestamp filenames. Then they upload like "
             "any platform file.")
    s_sort.add_argument(
        "--platform", default="instagram",
        help="Destination platform folder (default instagram).")
    s_sort.add_argument(
        "--dry-run", action="store_true",
        help="Preview the moves without changing anything on disk.")
    s_sort.add_argument(
        "--unsorted-name", default="unsorted", metavar="NAME",
        help="Source folder name under output_dir (default 'unsorted').")

    # ── auto-sort (toggle) ───
    s_au = sub.add_parser(
        "auto-sort",
        help="Show/toggle automatic sort of output_dir/unsorted/ each "
             "`archiver start` cycle. Default off.")
    au_sub = s_au.add_subparsers(dest="as_action", required=False, metavar="ACTION",
                                 help="omit to print state; 'set'/'unset' to change")
    au_set = au_sub.add_parser("set", help="Enable or disable auto-sort")
    au_set.add_argument("--enabled", choices=["true", "false"], required=True)
    au_set.add_argument(
        "--platform", default=None,
        help="Destination platform for auto-sort (default instagram).")
    au_sub.add_parser("unset", help="Remove the setting (back to default: off)")

    # ── auto-retry (toggle) ───
    s_ar = sub.add_parser(
        "auto-retry",
        help="Show/toggle automatic re-queue of failed uploads. Default OFF "
             "(opt-in: a permanently-failing row would otherwise re-arm itself "
             "every pass and starve the queue). Applied by the dispatcher's "
             "periodic housekeeping (this command just sets the shared policy). "
             "Failed rows whose file is missing are always cleaned up there, "
             "regardless of this setting.")
    ar_sub = s_ar.add_subparsers(dest="ar_action", required=False, metavar="ACTION",
                                 help="omit to print state; 'set'/'unset' to change")
    ar_set = ar_sub.add_parser("set", help="Enable or disable auto-retry")
    ar_set.add_argument("--enabled", choices=["true", "false"], required=True)
    ar_sub.add_parser("unset", help="Remove the setting (back to default: off)")

    # ── local (user-managed folders treated as platforms, no download) ───
    s_local = sub.add_parser(
        "local",
        help="Manage user-managed folders treated as platforms (reconciled + "
             "uploaded with platform semantics, but no download).")
    local_sub = s_local.add_subparsers(dest="local_cmd", required=True)
    la = local_sub.add_parser("add", help="Register a folder name as a local platform")
    la.add_argument("name")
    lr = local_sub.add_parser("remove", help="Unregister a local platform (files kept)")
    lr.add_argument("name")
    local_sub.add_parser("list", help="List local platforms")

    # ── download (per-platform fetch on/off) ───
    s_dl = sub.add_parser(
        "download",
        help="Show or toggle whether a platform DOWNLOADS (default on). Off = "
             "reconcile + upload the folder only, no fetch, no cookies needed.")
    s_dl.add_argument("--platform", choices=PLATFORM_CHOICES)
    dl_sub = s_dl.add_subparsers(dest="download_action", required=False, metavar="ACTION",
                                 help="omit to print resolution; 'set'/'unset' to change")
    dl_set = dl_sub.add_parser("set", help="Enable/disable download for a platform")
    dl_set.add_argument("--platform", choices=PLATFORM_CHOICES, required=True,
                        help="Required — avoids accidentally toggling ALL platforms.")
    dl_set.add_argument("--enabled", choices=["true", "false"], required=True)
    dl_unset = dl_sub.add_parser("unset", help="Remove the override (back to default: on)")
    dl_unset.add_argument("--platform", choices=PLATFORM_CHOICES, required=True,
                          help="Required — avoids accidentally toggling ALL platforms.")

    # ── backfill ───
    s_backfill = sub.add_parser(
        "backfill",
        help="One-time: compute content_hash for existing rows that lack one "
             "(makes move/rename dedup retroactive). Resumable; reads each file.")
    s_backfill.add_argument("--batch-commit", type=int, default=500,
                            help="Rows per DB commit (default 500).")

    # ── bootstrap ───
    s_boot = sub.add_parser(
        "bootstrap",
        help="Absorb existing on-disk archive: reconcile + seed extractor "
             "archives + set checkpoints. No network. Use once when "
             "migrating an existing media library.",
    )
    s_boot.add_argument("--platform", choices=PLATFORM_CHOICES,
                        help="Limit to one platform")
    s_boot.add_argument("--user", metavar="USERNAME",
                        help="Limit to one user")

    # ── reset ───
    s_reset = sub.add_parser("reset", help="Reset operations")
    reset_sub = s_reset.add_subparsers(dest="reset_cmd", required=True)

    # --platform is free-form (NOT restricted to built-in platforms) so these
    # also scope to source='orphaned' (platform='orphaned', user=<chat_id>) and
    # local platforms. e.g. `reset uploads --platform orphaned --user -100123`.
    rf = reset_sub.add_parser("failed", help="Re-queue failed uploads")
    rf.add_argument("--platform", metavar="PLATFORM",
                    help="x|tiktok|instagram, 'orphaned' (chat_id folders), or a "
                         "local-platform name")
    rf.add_argument("--user", metavar="USERNAME",
                    help="username, or the chat_id for orphaned rows")

    ru = reset_sub.add_parser("uploads", help="Re-queue ALL uploads (no re-download)")
    ru.add_argument("--platform", metavar="PLATFORM",
                    help="x|tiktok|instagram, 'orphaned' (chat_id folders), or a "
                         "local-platform name")
    ru.add_argument("--user", metavar="USERNAME",
                    help="username, or the chat_id for orphaned rows")

    ruser = reset_sub.add_parser("user", help="Full wipe: re-download + re-upload")
    ruser.add_argument("--platform", choices=PLATFORM_CHOICES, required=True)
    ruser.add_argument("--user", metavar="USERNAME", required=True)

    rall = reset_sub.add_parser(
        "all",
        help="Nuke DB rows + checkpoints for EVERY user (files on disk preserved)",
    )
    rall.add_argument("--yes", action="store_true",
                      help="Skip the y/N confirmation prompt")

    # ── reconcile ───
    s_rec = sub.add_parser(
        "reconcile",
        help="Dedup platform folders, then scan disk for files missing from DB",
    )
    s_rec.add_argument("--platform", choices=PLATFORM_CHOICES)
    s_rec.add_argument("--user", metavar="USERNAME")
    s_rec.add_argument("--no-dedup", action="store_true",
                       help="Skip the pre-reconcile content dedup pass")
    s_rec.add_argument("--dry-run-dedup", action="store_true",
                       help="Report duplicate files but do not delete them")

    # ── dedup ───
    s_dedup = sub.add_parser(
        "dedup",
        help="Content-hash duplicate removal. Dry-run by default; pass --yes "
             "to actually delete.",
    )
    s_dedup.add_argument("--platform", choices=PLATFORM_CHOICES)
    s_dedup.add_argument("--user", metavar="USERNAME")
    s_dedup.add_argument("--yes", action="store_true",
                          help="Perform actual deletion. Without this, only "
                               "reports what would be deleted.")

    # ── stats ───
    s_stats = sub.add_parser("stats", help="Show DB counts")
    s_stats.add_argument("--platform", choices=PLATFORM_CHOICES)
    s_stats.add_argument("--user", metavar="USERNAME")

    # ── health ───
    sub.add_parser("health", help="Run platform health checks (no downloads)")

    # ── loop ───
    s_loop = sub.add_parser(
        "loop",
        help="Run `archiver run` forever with random intervals (Ctrl-C to stop)",
    )
    s_loop.add_argument("--min", dest="min_sleep", type=float, default=7200)
    s_loop.add_argument("--max", dest="max_sleep", type=float, default=14400)
    s_loop.add_argument("--max-fails", type=int, default=5)
    s_loop.add_argument("--platform", choices=PLATFORM_CHOICES)
    s_loop.add_argument("--user", metavar="USERNAME")
    s_loop.add_argument(
        "--ingest-interval", dest="ingest_interval", type=float, default=180.0,
        metavar="SEC",
        help="How often (seconds) the background ingest sweeper walks the "
             "record/orphaned/local drop folders, independent of the download "
             "cycle (default 180; min 30). 0 disables the sweeper.")

    # ── cookies ───
    s_ck = sub.add_parser("cookies", help="Cookie management")
    ck_sub = s_ck.add_subparsers(dest="ck_cmd", required=True)
    ck_sub.add_parser("list", help="List Firefox profiles")
    ck_ref = ck_sub.add_parser("refresh", help="Refresh TikTok/Instagram cookies")
    ck_ref.add_argument("--platform", choices=["tiktok", "instagram"],
                        default="tiktok",
                        help="Which platform's cookies (default: tiktok)")
    ck_ref.add_argument("--profile", metavar="NAME",
                        help="Override FIREFOX_PROFILE for this run")

    # ── config ───
    s_cfg = sub.add_parser("config", help="View and edit user lists (config.toml)")
    cfg_sub = s_cfg.add_subparsers(dest="config_cmd", required=True)

    cfg_list = cfg_sub.add_parser("list", help="List configured users")
    cfg_list.add_argument("--platform", choices=PLATFORM_CHOICES)

    cfg_add = cfg_sub.add_parser("add", help="Add a user to a platform")
    cfg_add.add_argument("--platform", choices=PLATFORM_CHOICES, required=True)
    cfg_add.add_argument("--user", metavar="USERNAME", required=True)

    cfg_rem = cfg_sub.add_parser("remove", help="Remove a user from a platform")
    cfg_rem.add_argument("--platform", choices=PLATFORM_CHOICES, required=True)
    cfg_rem.add_argument("--user", metavar="USERNAME", required=True)

    # ── banned (accounts auto-retired as gone) ───
    s_banned = sub.add_parser(
        "banned",
        help="List or manage accounts auto-detected as banned/suspended/"
             "deleted (removed from the active user list during runs).")
    banned_sub = s_banned.add_subparsers(dest="banned_cmd", required=False,
                                         metavar="ACTION",
                                         help="omit to list; 'unban' to restore")
    bl = banned_sub.add_parser("list", help="List banned accounts")
    bl.add_argument("--platform", choices=PLATFORM_CHOICES)
    bu = banned_sub.add_parser(
        "unban", help="Remove an account from the banned list")
    bu.add_argument("--platform", choices=PLATFORM_CHOICES, required=True)
    bu.add_argument("--user", metavar="USERNAME", required=True)
    bu.add_argument("--re-add", action="store_true",
                    help="Also re-add the account to the active user list")

    # ── platform (run enablement) ───
    s_plat = sub.add_parser(
        "platform",
        help="Show or edit platforms included in archiver run",
    )
    plat_sub = s_plat.add_subparsers(dest="platform_cmd", required=True)
    plat_sub.add_parser("list", help="List enabled platforms")

    plat_add = plat_sub.add_parser("add", help="Enable a platform for runs")
    plat_add.add_argument("platform", choices=PLATFORM_CHOICES)

    plat_rem = plat_sub.add_parser("remove", help="Disable a platform for runs")
    plat_rem.add_argument("platform", choices=PLATFORM_CHOICES)

    # ── run-settings ───
    s_run_settings = sub.add_parser(
        "run-settings",
        help="Show or edit run-level behavior flags",
    )
    run_settings_sub = s_run_settings.add_subparsers(
        dest="run_settings_cmd",
        required=True,
    )
    run_settings_sub.add_parser("show", help="Show run-level behavior flags")

    rs_reconcile = run_settings_sub.add_parser(
        "reconcile-after-run",
        help="Turn the post-run reconcile sweep on or off",
    )
    rs_reconcile.add_argument("value", choices=["on", "off"])

    rs_delete_records = run_settings_sub.add_parser(
        "delete-records-after-upload",
        help="Turn delete-after-upload for recorder files on or off",
    )
    rs_delete_records.add_argument("value", choices=["on", "off"])

    # ── policy (delete-after-upload) ───
    s_pol = sub.add_parser(
        "policy",
        help="Show or edit delete-after-upload policy per (platform, user)",
    )
    s_pol.add_argument("--platform", choices=PLATFORM_CHOICES)
    s_pol.add_argument("--user", metavar="USERNAME")
    pol_sub = s_pol.add_subparsers(dest="policy_action", required=False,
                                    metavar="ACTION",
                                    help="omit to print resolution; "
                                         "'set'/'unset' to mutate config.toml")

    pol_set = pol_sub.add_parser("set",
        help="Set delete-after-upload at global, per-platform, or per-user scope")
    pol_set.add_argument("--platform", choices=PLATFORM_CHOICES)
    pol_set.add_argument("--user", metavar="USERNAME",
                          help="Per-user override. Requires --platform.")
    pol_set.add_argument("--delete", choices=["true", "false"], required=True)

    pol_unset = pol_sub.add_parser("unset",
        help="Remove an override at global, per-platform, or per-user scope")
    pol_unset.add_argument("--platform", choices=PLATFORM_CHOICES)
    pol_unset.add_argument("--user", metavar="USERNAME",
                            help="Per-user override. Requires --platform.")

    # ── dedup-policy ───
    s_dp = sub.add_parser(
        "dedup-policy",
        help="Show or edit dedup-after-download policy per (platform, user)",
    )
    s_dp.add_argument("--platform", choices=PLATFORM_CHOICES)
    s_dp.add_argument("--user", metavar="USERNAME")
    dp_sub = s_dp.add_subparsers(dest="dp_action", required=False,
                                  metavar="ACTION",
                                  help="omit to print resolution; "
                                       "'set'/'unset' to mutate config.toml")

    dp_set = dp_sub.add_parser("set",
        help="Set dedup-after-download at global, per-platform, or per-user scope")
    dp_set.add_argument("--platform", choices=PLATFORM_CHOICES)
    dp_set.add_argument("--user", metavar="USERNAME",
                          help="Per-user override. Requires --platform.")
    dp_set.add_argument("--enabled", choices=["true", "false"], required=True)

    dp_unset = dp_sub.add_parser("unset",
        help="Remove an override at global, per-platform, or per-user scope")
    dp_unset.add_argument("--platform", choices=PLATFORM_CHOICES)
    dp_unset.add_argument("--user", metavar="USERNAME",
                            help="Per-user override. Requires --platform.")

    # ── safebrake (protect-from-deletion policy) ───
    s_sb = sub.add_parser(
        "safebrake",
        help="Shield a platform/user from ALL deletion (delete-after-upload, "
             "dedup cleanup, disk-full purge, purge-sent). Show or edit per "
             "(platform, user).",
    )
    s_sb.add_argument("--platform", choices=PLATFORM_CHOICES)
    s_sb.add_argument("--user", metavar="USERNAME")
    sb_sub = s_sb.add_subparsers(dest="safebrake_action", required=False,
                                 metavar="ACTION",
                                 help="omit to print resolution; "
                                      "'set'/'unset' to mutate config.toml")
    sb_set = sb_sub.add_parser("set",
        help="Turn the safebrake on/off at global, per-platform, or per-user scope")
    sb_set.add_argument("--platform", choices=PLATFORM_CHOICES)
    sb_set.add_argument("--user", metavar="USERNAME",
                        help="Per-user override. Requires --platform.")
    sb_set.add_argument("--on", choices=["true", "false"], required=True,
                        help="true = protect (never delete); false = unprotect")
    sb_unset = sb_sub.add_parser("unset",
        help="Remove the safebrake override at the given scope")
    sb_unset.add_argument("--platform", choices=PLATFORM_CHOICES)
    sb_unset.add_argument("--user", metavar="USERNAME",
                          help="Per-user override. Requires --platform.")

    # ── purge-sent (reclaim disk: delete on-disk copies of uploaded files) ───
    s_purge = sub.add_parser(
        "purge-sent",
        help="Delete on-disk files whose rows are already status='sent'. "
             "Honors the safebrake. Use --dry-run to preview first.",
    )
    s_purge.add_argument("--platform", help="Limit to one platform (free-form: "
                         "matches recorder 'tiktok', orphaned chat-id, etc.)")
    s_purge.add_argument("--user", dest="username", metavar="USERNAME",
                         help="Limit to one user (requires --platform).")
    s_purge.add_argument("--source", choices=["archiver", "recorder", "orphaned"],
                         help="Limit to one producer source.")
    s_purge.add_argument("--dry-run", action="store_true",
                         help="Show what would be deleted; touch nothing.")
    s_purge.add_argument("--yes", action="store_true",
                         help="Skip the confirmation prompt (non-interactive).")

    # ── migrate ───
    sub.add_parser(
        "migrate",
        help="One-shot: import legacy .env user lists + DELETE_AFTER_UPLOAD_* "
             "overrides into config.toml. Idempotent; safe to re-run.",
    )

    return p


# ── Command handlers ──────────────────────────────────────────────────────────

def cmd_start(args, config: Config, db: ItemStore) -> int:
    """Harmonized run verb: continuous by default, single cycle with --once.
    Delegates to the existing run/loop implementations."""
    once = getattr(args, "once", False)
    plats = [f"{name} ({len(pc.users)})"
             for name in ("x", "tiktok", "instagram")
             if (pc := getattr(config, name)) is not None]
    termui.banner("archiver", [
        ("platforms", "  ".join(plats) or "(none configured)"),
        ("output", config.output_dir),
        ("mode", "single cycle" if once else "continuous loop"),
    ], subtitle="multi-platform → telegram")
    if once:
        return cmd_run(args, config, db)
    return cmd_loop(args, config, db)


def cmd_queue(args, config: Config, db: ItemStore) -> int:
    """Shared queue noun — same implementation as recorder/dispatcher."""
    return core_cli.handle_queue(db, args)


def cmd_ingest(args, config: Config, db: ItemStore) -> int:
    """Ingest loose files and enqueue them as source='orphaned' rows (deduped).
    Default: scan output_dir's chat_id folders. --path DIR --chat CHAT_ID:
    ingest an arbitrary folder to an explicit chat (same dedup guarantee)."""
    from core import ingest_chat_id_dirs, ingest_folder, parse_route

    guard = DeletionGuard(config.policy_store)

    if args.path:
        if not args.chat:
            log.error("ingest --path requires --chat <chat_id>")
            return 2
        # parse_route (not bare is_chat_id) so a dash-free numeric id is re-signed
        # to its canonical -100… form before it lands on the row — otherwise the
        # dispatcher would resolve it as a PeerUser. Also accepts a .t<topic>
        # suffix, routing the ingest to a specific forum topic.
        route = parse_route(args.chat)
        if route is None:
            log.error("ingest --chat %r is not a valid chat_id", args.chat)
            return 2
        folder = Path(args.path).expanduser()
        if not folder.is_dir():
            log.error("ingest --path %s is not a directory", folder)
            return 2
        rep = ingest_folder(
            db, folder, chat_id=route.chat_id, topic_id=route.topic_id,
            priority=args.priority, guard=guard)
        print(rep)
        return 0

    reports = ingest_chat_id_dirs(
        db, config.output_dir,
        known_platforms=list(PLATFORM_CHOICES) + list(config.local_platforms),
        priority=args.priority,
        guard=guard,
    )
    if not reports:
        print("No chat_id folders found under", config.output_dir)
        return 0
    total_inserted = 0
    for rep in reports:
        print(rep)
        total_inserted += rep.inserted
    log.info("ingest: %d file(s) newly enqueued across %d folder(s)",
             total_inserted, len(reports))
    return 0


def cmd_auto_ingest(args, config: Config, db: ItemStore) -> int:
    """Show/toggle the global auto_ingest_orphaned policy."""
    store  = config.policy_store
    policy = AutoIngestPolicy(store)
    action = getattr(args, "ai_action", None)
    if action == "set":
        value = args.enabled == "true"
        store.set(policy.KEY, value)
        log.info("auto-ingest set: global → %s (key=%s)", value, policy.KEY)
        log.info("Takes effect on the next `archiver start` cycle.")
        return 0
    if action == "unset":
        removed = store.unset(policy.KEY)
        log.info("auto-ingest unset: %s",
                 "removed (back to default: off)" if removed else "was not set")
        return 0
    value, source = store.explain(policy.KEY, default=policy.DEFAULT)
    log.info("auto-ingest (key=%s): %s  (from %s)", policy.KEY, value, source)
    return 0


def cmd_sort(args, config: Config, db: ItemStore) -> int:
    """Move loose files from output_dir/unsorted/ into <platform>/<username>/,
    parsing the username out of username_timestamp_… filenames. Pure filesystem
    move — the normal reconcile/upload path picks the files up afterward."""
    from core import sort_unsorted

    rep = sort_unsorted(
        config.output_dir,
        platform         = args.platform,
        dry_run          = args.dry_run,
        unsorted_dirname = args.unsorted_name,
    )
    print(rep)
    for err in rep.errors:
        print("  error:", err)
    return 0


def cmd_auto_sort(args, config: Config, db: ItemStore) -> int:
    """Show/toggle the global sort_unsorted policy (and its target platform)."""
    store  = config.policy_store
    policy = SortPolicy(store)
    action = getattr(args, "as_action", None)
    if action == "set":
        value = args.enabled == "true"
        store.set(policy.KEY, value)
        if args.platform:
            store.set(policy.PLATFORM_KEY, args.platform.strip())
        log.info("auto-sort set: global → %s, platform=%s (key=%s)",
                 value, policy.target_platform(), policy.KEY)
        log.info("Takes effect on the next `archiver start` cycle.")
        return 0
    if action == "unset":
        removed = store.unset(policy.KEY)
        store.unset(policy.PLATFORM_KEY)
        log.info("auto-sort unset: %s",
                 "removed (back to default: off)" if removed else "was not set")
        return 0
    value, source = store.explain(policy.KEY, default=policy.DEFAULT)
    log.info("auto-sort (key=%s): %s  (from %s); target platform=%s",
             policy.KEY, value, source, policy.target_platform())
    return 0


def cmd_auto_retry(args, config: Config, db: ItemStore) -> int:
    """Show/toggle the global auto_retry_failed policy."""
    from core import FailedRetryPolicy

    store  = config.policy_store
    policy = FailedRetryPolicy(store)
    action = getattr(args, "ar_action", None)
    if action == "set":
        value = args.enabled == "true"
        store.set(policy.KEY, value)
        log.info("auto-retry set: global → %s (key=%s)", value, policy.KEY)
        log.info("Applied by the dispatcher's housekeeping (~15 min cadence).")
        return 0
    if action == "unset":
        removed = store.unset(policy.KEY)
        log.info("auto-retry unset: %s",
                 "removed (back to default: on)" if removed else "was not set")
        return 0
    value, source = store.explain(policy.KEY, default=policy.DEFAULT)
    log.info("auto-retry (key=%s): %s  (from %s)", policy.KEY, value, source)
    log.info("  (the dispatcher always cleans up failed rows with a missing "
             "file, regardless of this setting.)")
    return 0


def cmd_local(args, config: Config, db: ItemStore) -> int:
    """Manage the list of user-managed 'local' platforms (no download)."""
    from core import is_chat_id

    store   = config.policy_store
    current = list(store.get("local_platforms", default=[]) or [])
    action  = args.local_cmd

    if action == "add":
        name = args.name.strip().lower()
        if not name or "/" in name or name.startswith("."):
            log.error("local add: invalid folder name %r", args.name)
            return 2
        if name in PLATFORM_CHOICES:
            log.error("local add: '%s' is a built-in platform", name)
            return 2
        if is_chat_id(name):
            log.error("local add: '%s' looks like a chat_id — that's an "
                      "orphaned destination, not a local platform", name)
            return 2
        if name in current:
            print(f"'{name}' is already a local platform")
            return 0
        current.append(name)
        store.set("local_platforms", sorted(current))
        plat_dir = Path(config.output_dir) / name
        print(f"Added local platform '{name}'.")
        print(f"  Put files under: {plat_dir}/<username>/")
        print(f"  Route with env/config TELEGRAM_CHAT_ID_{name.upper()}[_<USER>].")
        return 0
    if action == "remove":
        name = args.name.strip().lower()
        if name not in current:
            print(f"'{name}' is not a local platform")
            return 1
        current.remove(name)
        store.set("local_platforms", sorted(current))
        print(f"Removed local platform '{name}' (files on disk untouched).")
        return 0
    # list
    if current:
        for n in current:
            print(n)
    else:
        print("(no local platforms — add one with `archiver local add <name>`)")
    return 0


def cmd_download(args, config: Config, db: ItemStore) -> int:
    return _cmd_boolpolicy(
        args, config, DownloadPolicy,
        action_attr = "download_action",
        value_attr  = "enabled",
        cmd_label   = "download",
    )


def cmd_backfill(args, config: Config, db: ItemStore) -> int:
    """Fill content_hash for pre-existing rows so move/rename dedup and the
    re-introduction guard apply retroactively. Reads every NULL-hash file."""
    from core import backfill_content_hashes

    def _progress(done: int, total: int) -> None:
        print(f"  backfill: {done}/{total}", end="\r", flush=True)

    report = backfill_content_hashes(
        db, batch_commit=args.batch_commit, progress=_progress)
    print()
    print(report)
    return 0


def cmd_run(args, config: Config, db: ItemStore, *, phase_cb=None,
            ingest_lock=None) -> int:
    user_filter = args.user.lstrip("@") if args.user else None

    if getattr(args, "full_history", False):
        if not user_filter and not args.platform:
            log.error("--full-history requires --user or --platform "
                      "(refusing to re-walk every user's full timeline).")
            return 2
        platforms = build_platforms(config)
        if args.platform:
            platforms = [p for p in platforms if p.name == args.platform]
        rearmed = 0
        for platform in platforms:
            users = (user_filter,) if user_filter else platform.users
            for u in users:
                db.rearm_full_history(platform.name, u)
                rearmed += 1
        log.info("full-history: re-armed %d user(s) — this run walks their "
                 "entire timeline (already-archived posts are skipped).",
                 rearmed)

    arch = Archiver(config, db)
    # Loop mode passes a shared lock so this heavy run's drop-folder reconcile
    # is mutually exclusive with the background ingest sweeper (same process).
    if ingest_lock is not None:
        arch.ingest_lock = ingest_lock
    results = asyncio.run(arch.run(
        platform_filter = args.platform,
        user_filter     = user_filter,
        on_user         = phase_cb,
    ))

    log.info("")
    log.info("══════════════════ Summary ══════════════════════")
    for key, r in results.items():
        status = r.get("status", "?")
        if status == "ok":
            line = (
                f"  ✓ {key:32s} dl={r.get('downloaded',0):>3} "
                f"pending={r.get('pending',0):>3} sent={r.get('sent',0):>3} "
                f"failed={r.get('failed',0):>3}"
            )
        elif status == "partial":
            line = (
                f"  ⚠ {key:32s} dl={r.get('downloaded',0):>3} "
                f"pending={r.get('pending',0):>3} failed={r.get('failed',0)}"
            )
        elif status == "banned":
            line = f"  ⊘ {key:32s} banned/deleted — retired: {r.get('reason','')[:40]}"
        else:
            line = f"  ✗ {key:32s} {status}: {r.get('reason','')[:40]}"
        log.info(line)
    log.info("═════════════════════════════════════════════════")
    # "banned" is a handled terminal outcome, not a failure — a retired account
    # shouldn't count against a loop's consecutive-failure budget.
    return 0 if all(r.get("status") in ("ok", "banned") for r in results.values()) else 1


def cmd_bootstrap(args, config: Config, db: ItemStore) -> int:
    """Absorb existing on-disk archive — no network calls."""
    log.info("Bootstrap: scanning %s and seeding extractor archives…",
             config.output_dir)
    summary = asyncio.run(bootstrap(
        config, db,
        platform_filter = args.platform,
        user_filter     = args.user.lstrip("@") if args.user else None,
    ))

    if not summary:
        log.warning("Bootstrap: no (platform, user) matched. "
                    "Add users via `archiver config add` first.")
        return 1

    log.info("")
    log.info("══════════════════ Bootstrap summary ════════════")
    total_inserted = total_manual = total_seeded = 0
    for key, report in summary.items():
        total_inserted += report.inserted
        total_manual   += report.manual_files
        total_seeded   += report.seeded_archive
        log.info("  %s", report)
    log.info("─────────────────────────────────────────────────")
    log.info("  inserted:        %d", total_inserted)
    log.info("  manual files:    %d", total_manual)
    log.info("  archive entries: %d", total_seeded)
    log.info("═════════════════════════════════════════════════")
    log.info("Next `archiver run` will be incremental from each user's date_floor.")
    return 0


def cmd_reset(args, config: Config, db: ItemStore) -> int:
    sub = args.reset_cmd
    user = args.user.lstrip("@") if getattr(args, "user", None) else None
    if sub == "failed":
        n = db.reset_failed(args.platform, user)
        log.info("reset failed: re-queued %d row(s)", n)
    elif sub == "uploads":
        platforms = build_platforms(config)
        if args.platform:
            platforms = [p for p in platforms if p.name == args.platform]
        for platform in platforms:
            users = (user,) if user else platform.users
            for u in users:
                n = reconcile_user(platform, u, db, config.output_dir).inserted
                if n:
                    log.info("  reconcile [%s] @%s: +%d orphan(s)", platform.name, u, n)
        n = db.reset_uploads(args.platform, user)
        scope = f"[{args.platform or '*'}] @{user or '*'}"
        log.info("reset uploads %s: re-queued %d row(s) (no re-download)", scope, n)
    elif sub == "user":
        n = db.reset_user(args.platform, user)
        log.info("reset user: deleted %d row(s) for [%s] @%s",
                 n, args.platform, user)
        log.info("  Also delete %s/%s/%s/ to force re-download.",
                 config.output_dir, args.platform, user)
    elif sub == "all":
        platforms = build_platforms(config)
        targets: list[tuple[str, str]] = [
            (p.name, u) for p in platforms for u in p.users
        ]
        if not targets:
            log.warning("reset all: no platforms/users configured.")
            return 0

        if not args.yes:
            log.warning("reset all will delete media rows + checkpoints for:")
            for p_name, u in targets:
                log.warning("  • [%s] @%s", p_name, u)
            log.warning("Files on disk are preserved. Next `run` will reconcile "
                        "and (re-)upload them.")

            if not sys.stdin.isatty():
                log.error("reset all: stdin is not a TTY and --yes was not passed. "
                          "Refusing to nuke non-interactively.")
                return 2
            try:
                answer = input("Proceed? [y/N] ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                log.info("reset all: aborted.")
                return 1
            if answer not in ("y", "yes"):
                log.info("reset all: aborted.")
                return 1

        total_deleted = 0
        for p_name, u in targets:
            n = db.reset_user(p_name, u)
            log.info("  reset [%s] @%s: deleted %d row(s)", p_name, u, n)
            total_deleted += n
            db.reset_circuit(p_name)
        log.info("reset all: deleted %d row(s) across %d user(s).",
                 total_deleted, len(targets))
    return 0


def cmd_reconcile(args, config: Config, db: ItemStore) -> int:
    from core import dedup_user

    platforms = build_platforms(config)
    if args.platform:
        platforms = [p for p in platforms if p.name == args.platform]
        if not platforms:
            log.error("reconcile: no matching enabled/configured platform: %s",
                      args.platform)
            return 2

    total_inserted = 0
    total_deleted = 0
    total_bytes_freed = 0
    for platform in platforms:
        if not args.user:
            root_report = reconcile_platform_root(platform, db, config.output_dir)
            if root_report.scanned or root_report.inserted:
                log.info("reconcile: %s", root_report)
            total_inserted += root_report.inserted

        users = _reconcile_users_for_platform(config, platform, args.user)
        for u in users:
            user_dir = Path(config.output_dir) / platform.name / u
            if not args.no_dedup:
                dedup_report = dedup_user(
                    platform.name,
                    u,
                    user_dir,
                    db,
                    dry_run=args.dry_run_dedup,
                )
                log.info("dedup before reconcile: %s", dedup_report)
                total_deleted += dedup_report.deleted
                total_bytes_freed += dedup_report.bytes_freed

            report = reconcile_user(platform, u, db, config.output_dir)
            log.info("reconcile: %s", report)
            total_inserted += report.inserted

    if not args.platform and not args.user:
        for report in reconcile_recordings(db):
            if report.scanned or report.inserted:
                log.info("reconcile recordings: %s", report)
            total_inserted += report.inserted

    log.info("")
    log.info("════════════════ Reconcile summary ══════════════")
    log.info("  queued new file(s): %d", total_inserted)
    if not args.no_dedup:
        mb = total_bytes_freed / (1024 * 1024)
        action = "would delete" if args.dry_run_dedup else "deleted"
        log.info("  dedup %s: %d file(s), %.1f MB", action, total_deleted, mb)
    log.info("═════════════════════════════════════════════════")
    return 0


def cmd_dedup(args, config: Config, db: ItemStore) -> int:
    """
    Content-hash dedup for one or all users. Dry-run by default;
    pass --yes for actual deletion.

    Independent of the dedup-after-download policy — the latter only
    controls the post-download auto-trigger. This command always runs
    when invoked.
    """
    from core import dedup_user

    dry_run = not args.yes
    if dry_run:
        log.info("dedup: DRY RUN — pass --yes to actually delete")

    platforms = build_platforms(config)
    if args.platform:
        platforms = [p for p in platforms if p.name == args.platform]
        if not platforms:
            log.error("dedup: no matching platform: %s", args.platform)
            return 2

    user_filter = args.user.lstrip("@") if args.user else None
    total_deleted     = 0
    total_bytes_freed = 0
    total_groups      = 0

    for platform in platforms:
        users = (user_filter,) if user_filter else platform.users
        for username in users:
            if user_filter and username not in platform.users:
                log.warning("dedup: user %s not configured for %s — skipping",
                            username, platform.name)
                continue
            user_dir = Path(config.output_dir) / platform.name / username
            report = dedup_user(
                platform.name, username, user_dir, db, dry_run=dry_run,
            )
            log.info("%s", report)
            total_deleted     += report.deleted
            total_bytes_freed += report.bytes_freed
            total_groups      += report.confirmed_groups

    mb = total_bytes_freed / (1024 * 1024)
    log.info("")
    log.info("══════════════════ Dedup summary ════════════════")
    log.info("  duplicate groups:  %d", total_groups)
    log.info("  files %s: %d (%.1f MB)",
             "would delete" if dry_run else "deleted", total_deleted, mb)
    log.info("═════════════════════════════════════════════════")
    return 0


def cmd_stats(args, config: Config, db: ItemStore) -> int:
    user = args.user.lstrip("@") if args.user else None
    if args.platform or user:
        s = db.stats(args.platform, user)
        scope = f"[{args.platform or '*'}] @{user or '*'}"
        log.info("%s: total=%d sent=%d pending=%d failed=%d (%.1f MB)",
                 scope, s["total"], s["sent"], s["pending"], s["failed"], s["total_mb"])
    else:
        for p in PLATFORM_CHOICES:
            s = db.stats(p)
            log.info("[%s]: total=%d sent=%d pending=%d failed=%d (%.1f MB)",
                     p, s["total"], s["sent"], s["pending"], s["failed"], s["total_mb"])

    log.info("")
    log.info("date_floor (next incremental cutoff):")
    rows = 0
    for platform in ([args.platform] if args.platform else PLATFORM_CHOICES):
        users = [user] if user else list(config.policy_store.list_users(platform))
        for u in users:
            floor = db.get_date_floor(platform, u)
            log.info("  [%s] @%s → %s", platform, u, floor or "(none — full fetch)")
            rows += 1
    if rows == 0:
        log.info("  (no configured users matched)")
    return 0


def cmd_health(args, config: Config, db: ItemStore) -> int:
    platforms = build_platforms(config)
    if not platforms:
        log.error("No platforms configured.")
        return 2
    bad = 0
    for p in platforms:
        s = p.health_check()
        marker = "✓" if s.healthy else "✗"
        log.info("[%s] %s %s", p.name, marker, "OK" if s.healthy else s.reason)
        if not s.healthy:
            bad += 1
        circuit = db.get_circuit(p.name)
        if circuit["consecutive_fails"] > 0 or circuit["tripped_until_utc"]:
            log.warning("    circuit: fails=%d tripped_until=%s",
                        circuit["consecutive_fails"], circuit["tripped_until_utc"])
    return 0 if bad == 0 else 1


# ── policy commands (generic over any BooleanPolicy) ─────────────────────────
#
# The two policy commands (policy, dedup-policy) share an identical
# dispatch shape: show / set / unset over (platform, user) scopes. The
# only differences are which BooleanPolicy class they wrap and the CLI
# value-arg name (--delete vs --enabled). _cmd_boolpolicy() factors
# that into one function; the two command-entry functions are 3-line
# shims that bind the right class + arg name.

def _cmd_boolpolicy(
    args,
    config:       Config,
    policy_cls:   type,
    action_attr:  str,   # "policy_action" or "dp_action"
    value_attr:   str,   # "delete" or "enabled"
    cmd_label:    str,   # for log lines: "policy" / "dedup-policy"
) -> int:
    store  = config.policy_store
    policy = policy_cls(store)
    action = getattr(args, action_attr, None)

    if action == "set":
        platform = args.platform
        user = getattr(args, "user", None)
        username = user.lstrip("@") if user else None
        if username and not platform:
            log.error("%s set: --user requires --platform", cmd_label)
            return 2
        raw_value = getattr(args, value_attr)
        value = raw_value == "true"
        if username:
            _warn_unknown_user(config, platform, username)
        store.set(policy.KEY, value, platform=platform, username=username)
        scope = _scope_label(platform, username)
        log.info("%s set: %s → %s (key=%s)", cmd_label, scope, value, policy.KEY)
        log.info("Note: a running `archiver loop` won't see this change until it restarts.")
        return 0

    if action == "unset":
        platform = args.platform
        user = getattr(args, "user", None)
        username = user.lstrip("@") if user else None
        if username and not platform:
            log.error("%s unset: --user requires --platform", cmd_label)
            return 2
        removed = store.unset(policy.KEY, platform=platform, username=username)
        scope = _scope_label(platform, username)
        if removed:
            log.info("%s unset: removed %s (key=%s)", cmd_label, scope, policy.KEY)
            log.info("Resolution now falls through to the next level.")
        else:
            log.info("%s unset: %s was not set — nothing to do.", cmd_label, scope)
        return 0

    # No action → resolution print
    log.info("%s resolution:", cmd_label)
    default_value, _ = store.explain(policy.KEY, default=policy.DEFAULT)
    log.info("  default for %s: %s", policy.KEY, default_value)
    log.info("")

    user = getattr(args, "user", None)
    user_filter = user.lstrip("@") if user else None
    rows = 0
    platforms = [args.platform] if args.platform else PLATFORM_CHOICES
    for platform in platforms:
        configured = config.policy_store.list_users(platform)
        users = [u for u in configured if user_filter is None or u == user_filter]
        for u in users:
            log.info("  [%s] @%s → %s", platform, u, policy.explain(platform, u))
            rows += 1
    if rows == 0:
        log.warning("No (platform, user) matched the filter.")
    return 0


def cmd_policy(args, config: Config, db: ItemStore) -> int:
    return _cmd_boolpolicy(
        args, config, DeletePolicy,
        action_attr = "policy_action",
        value_attr  = "delete",
        cmd_label   = "policy",
    )


def cmd_dedup_policy(args, config: Config, db: ItemStore) -> int:
    return _cmd_boolpolicy(
        args, config, DedupPolicy,
        action_attr = "dp_action",
        value_attr  = "enabled",
        cmd_label   = "dedup-policy",
    )


def cmd_safebrake(args, config: Config, db: ItemStore) -> int:
    """The safebrake is just another BooleanPolicy (ProtectionPolicy), so its
    set/unset/show flow is identical to delete-/dedup-policy."""
    return _cmd_boolpolicy(
        args, config, ProtectionPolicy,
        action_attr = "safebrake_action",
        value_attr  = "on",
        cmd_label   = "safebrake",
    )


def cmd_purge_sent(args, config: Config, db: ItemStore) -> int:
    """Delete on-disk copies of already-uploaded ('sent') files to reclaim
    space. Re-runnable; only touches files still present on disk, and routes
    every deletion through the DeletionGuard so safebraked scopes are skipped."""
    username = args.username.lstrip("@") if args.username else None
    if username and not args.platform:
        log.error("purge-sent: --user requires --platform")
        return 2

    guard = DeletionGuard(config.policy_store)
    items = db.sent_items(platform=args.platform, username=username,
                          source=args.source)

    # Partition before touching anything: present-on-disk vs already-gone, and
    # which present ones the safebrake will shield.
    present  = [it for it in items if Path(it.file_path).exists()]
    missing  = len(items) - len(present)
    targets  = [it for it in present
                if not guard.is_protected(it.platform, it.username)]
    shielded = len(present) - len(targets)

    def _size(p: str) -> int:
        try:
            return Path(p).stat().st_size
        except OSError:
            return 0

    target_bytes = sum(_size(it.file_path) for it in targets)
    scope = _scope_label(args.platform, username)
    src = f" source={args.source}" if args.source else ""
    log.info("purge-sent %s%s: %d sent row(s) — on-disk=%d, already-gone=%d, "
             "safebraked=%d, to-delete=%d (%.1f MB)",
             scope, src, len(items), len(present), missing, shielded,
             len(targets), target_bytes / 1_048_576)

    if not targets:
        log.info("purge-sent: nothing to delete.")
        return 0

    if args.dry_run:
        for it in targets:
            log.info("  would delete: id=%d %s/@%s %s",
                     it.id, it.platform, it.username, it.file_path)
        log.info("purge-sent: dry-run — no files were deleted.")
        return 0

    if not args.yes:
        try:
            reply = input(f"Delete {len(targets)} file(s) "
                          f"({target_bytes / 1_048_576:.1f} MB)? [y/N] ")
        except EOFError:
            reply = ""
        if reply.strip().lower() not in ("y", "yes"):
            log.info("purge-sent: aborted (no confirmation).")
            return 1

    deleted = freed = 0
    for it in targets:
        size = _size(it.file_path)
        if guard.delete(it.platform, it.username, it.file_path,
                        reason="purge-sent"):
            deleted += 1
            freed += size
    log.info("purge-sent: deleted %d file(s), freed %.1f MB%s",
             deleted, freed / 1_048_576,
             f" ({shielded} kept by safebrake)" if shielded else "")
    return 0


# ── Helpers ──────────────────────────────────────────────────────────────────

def _scope_label(platform: str | None, username: str | None) -> str:
    if platform and username:
        return f"[{platform}] @{username}"
    if platform:
        return f"[{platform}] (platform-wide)"
    return "(global)"


def _env_path() -> Path:
    return Path.home() / ".config" / "archiver-suite" / ".env"


def _write_enabled_platforms(enabled: set[str]) -> None:
    ordered = [p for p in PLATFORM_CHOICES if p in enabled]
    _set_env_var("ENABLED_PLATFORMS", ",".join(ordered))


def _reconcile_users_for_platform(
    config: Config,
    platform,
    user_filter: str | None,
) -> tuple[str, ...]:
    if user_filter:
        return (user_filter.lstrip("@"),)

    platform_dir = Path(config.output_dir) / platform.name
    disk_users = {
        p.name
        for p in platform_dir.iterdir()
        if p.is_dir()
    } if platform_dir.exists() else set()

    users = set(platform.users) | disk_users
    return tuple(sorted(users))


def _warn_unknown_user(config: Config, platform: str, username: str) -> None:
    """Soft warning if the user isn't currently in the platform's user list.
    Doesn't block writes — operators sometimes pre-stage overrides."""
    if platform not in PLATFORM_CHOICES:
        log.warning(
            "Unknown platform [%s]. The override will be written but no "
            "archiver command currently knows how to use it.",
            platform,
        )
        return
    if username not in config.policy_store.list_users(platform):
        log.warning(
            "User '%s' is not currently in [%s] users. Override will be "
            "written but won't apply until the user is added via "
            "`archiver config add --platform %s --user %s`.",
            username, platform, platform, username,
        )


def _validate_chat_id_format(chat: str) -> str | None:
    """Return None if valid; an error message otherwise."""
    s = chat.strip()
    if not s:
        return "chat ID is empty"
    if s.startswith("@"):
        if len(s) < 2:
            return f"chat ID {chat!r} is just '@' with nothing after"
        return None
    try:
        int(s)
        return None
    except ValueError:
        return (f"chat ID {chat!r} is neither an integer nor a Telegram "
                f"username (must look like -1001234567890 or @somechannel)")


def _set_env_var(key: str, value: str) -> None:
    from dotenv import set_key
    env = _env_path()
    env.parent.mkdir(parents=True, exist_ok=True)
    env.touch(exist_ok=True)
    set_key(str(env), key, value)


def _unset_env_var(key: str) -> bool:
    from dotenv import unset_key
    env = _env_path()
    if not env.exists():
        return False
    removed, _ = unset_key(str(env), key)
    return bool(removed)



# ── config (user-list management, now backed by PolicyStore) ─────────────────

def cmd_config(args, config: Config, db: ItemStore) -> int:
    store = config.policy_store

    if args.config_cmd == "list":
        platforms = [args.platform] if args.platform else PLATFORM_CHOICES
        for plat in platforms:
            users = store.list_users(plat)
            shown = ", ".join(f"@{u}" for u in users) if users else "(none)"
            log.info("[%s] users: %s", plat, shown)
        return 0

    username = args.user.lstrip("@")

    if args.config_cmd == "add":
        # Adding a user explicitly overrides a prior auto-ban (the operator is
        # asserting the account is back); clear it so the two lists stay
        # mutually exclusive.
        was_banned = store.unban_user(args.platform, username)
        added = store.add_user(args.platform, username)
        if added:
            log.info("Added @%s to [%s].", username, args.platform)
            if was_banned:
                log.info("(also removed @%s from the [%s] banned list)",
                         username, args.platform)
            log.info("Note: a running `archiver loop` won't see this change until it restarts.")
        else:
            log.error("@%s already in [%s] list.", username, args.platform)
            return 1
    elif args.config_cmd == "remove":
        removed = store.remove_user(args.platform, username)
        if removed:
            log.info("Removed @%s from [%s].", username, args.platform)
            log.info("Any per-user overrides for this user were also removed.")
        else:
            log.error("@%s not found in [%s] list.", username, args.platform)
            return 1

    return 0


# ── banned (auto-retired accounts) ───────────────────────────────────────────

def cmd_banned(args, config: Config, db: ItemStore) -> int:
    """List banned/deleted accounts or restore one. Bans are written
    automatically during a run when an extractor reports the account is gone;
    this command is the manual inspect/restore side."""
    store  = config.policy_store
    action = getattr(args, "banned_cmd", None)

    if action == "unban":
        username = args.user.lstrip("@")
        if not store.unban_user(args.platform, username):
            log.error("@%s is not on the [%s] banned list.",
                      username, args.platform)
            return 1
        log.info("Removed @%s from the [%s] banned list.", username, args.platform)
        if args.re_add:
            if store.add_user(args.platform, username):
                log.info("Re-added @%s to [%s] active users — it will be fetched "
                         "again next run.", username, args.platform)
            else:
                log.info("@%s was already in the [%s] active user list.",
                         username, args.platform)
        else:
            log.info("Not re-added to active users. To resume archiving, run "
                     "`archiver config add --platform %s --user %s` "
                     "(or re-run unban with --re-add).",
                     args.platform, username)
        log.info("Note: a running `archiver loop` won't see this change until "
                 "it restarts.")
        return 0

    # Default / "list": show the banned roster.
    platforms = [args.platform] if getattr(args, "platform", None) else PLATFORM_CHOICES
    total = 0
    for plat in platforms:
        details = store.banned_details(plat)
        if not details:
            log.info("[%s] banned: (none)", plat)
            continue
        log.info("[%s] banned:", plat)
        for u, meta in sorted(details.items()):
            reason = meta.get("reason", "(no reason recorded)")
            when   = meta.get("detected_at", "?")
            log.info("  ✗ @%s — %s (detected %s)", u, reason, when)
            total += 1
    if total:
        log.info("")
        log.info("Restore one with `archiver banned unban --platform <p> "
                 "--user <u> --re-add`.")
    return 0


# ── platform (run enablement, backed by ENABLED_PLATFORMS in .env) ───────────

def cmd_platform(args, config: Config, db: ItemStore) -> int:
    enabled = set(config.enabled_platforms)

    if args.platform_cmd == "list":
        shown = ", ".join(p for p in PLATFORM_CHOICES if p in enabled) or "(none)"
        log.info("enabled platforms: %s", shown)
        log.info("env: %s", _env_path())
        return 0

    platform = args.platform
    if args.platform_cmd == "add":
        if platform in enabled:
            log.info("[%s] already enabled.", platform)
            return 0
        enabled.add(platform)
        _write_enabled_platforms(enabled)
        log.info("Enabled [%s] for future runs.", platform)
        log.info("Note: a running `archiver loop` won't see this change until it restarts.")
        return 0

    if args.platform_cmd == "remove":
        if platform not in enabled:
            log.info("[%s] already disabled.", platform)
            return 0
        enabled.remove(platform)
        _write_enabled_platforms(enabled)
        log.info("Disabled [%s] for future runs.", platform)
        log.info("Note: configured users and existing queued rows were not removed.")
        log.info("A running `archiver loop` won't see this change until it restarts.")
        return 0

    return 1


# ── run-settings (run-level behavior flags in .env) ─────────────────────────

def cmd_run_settings(args, config: Config, db: ItemStore) -> int:
    if args.run_settings_cmd == "show":
        recorder_delete_policy = RecorderDeletePolicy(config.policy_store)
        log.info("RECONCILE_AFTER_RUN=%s",
                 "true" if config.reconcile_after_run else "false")
        log.info("DELETE_RECORDS_AFTER_UPLOAD=%s",
                 "true" if recorder_delete_policy.should_delete_recording()
                 else "false")
        log.info("env: %s", _env_path())
        log.info("config.toml: %s", config.policy_store._path)
        return 0

    if args.run_settings_cmd == "reconcile-after-run":
        enabled = args.value == "on"
        _set_env_var("RECONCILE_AFTER_RUN", "true" if enabled else "false")
        log.info("RECONCILE_AFTER_RUN=%s", "true" if enabled else "false")
        log.info("Note: a running `archiver loop` won't see this change until it restarts.")
        return 0

    if args.run_settings_cmd == "delete-records-after-upload":
        enabled = args.value == "on"
        config.policy_store.set(
            RecorderDeletePolicy.KEY,
            enabled,
        )
        log.info("DELETE_RECORDS_AFTER_UPLOAD=%s",
                 "true" if enabled else "false")
        log.info("Note: a running `dispatcher start` won't see this change until it restarts.")
        return 0

    return 1


# ── cookies ──────────────────────────────────────────────────────────────────

def cmd_cookies(args, config: Config, db: ItemStore) -> int:
    from . import cookies

    if args.ck_cmd == "list":
        profiles = cookies.list_profiles()
        if not profiles:
            log.info("No Firefox profiles found.")
            return 0
        for name, path in profiles:
            log.info("  %-30s → %s", name, path)
        return 0

    if args.ck_cmd == "refresh":
        if args.platform == "instagram":
            if not config.instagram:
                log.error("Instagram not configured.")
                return 2
            cfg_block = config.instagram
            domain    = "instagram.com"
            required  = {"sessionid", "csrftoken", "ds_user_id"}
        else:
            if not config.tiktok:
                log.error("TikTok not configured.")
                return 2
            cfg_block = config.tiktok
            domain    = "tiktok.com"
            from .platforms import TikTokPlatform
            required  = TikTokPlatform.AUTH_COOKIES

        profile = args.profile or cfg_block.firefox_profile
        if not profile:
            log.error("No profile specified. Pass --profile NAME or set FIREFOX_PROFILE.")
            return 2

        n = cookies.refresh_for_domain(
            domain           = domain,
            profile_name     = profile,
            output_path      = cfg_block.cookies_file,
            required_cookies = required,
        )
        log.info("Refreshed %d cookie(s) → %s", n, cfg_block.cookies_file)
        return 0

    return 1


# ── migrate (.env → config.toml) ─────────────────────────────────────────────

def cmd_migrate(args, config: Config, db: ItemStore) -> int:
    """
    Import legacy state from .env into config.toml:
      - X_USERS / TIKTOK_USERS / INSTAGRAM_USERS → store.add_user(...)
      - DELETE_AFTER_UPLOAD                     → store.set("delete_after_upload", ..., global)
      - DELETE_AFTER_UPLOAD_<PLAT>              → store.set(..., platform=plat)
      - DELETE_AFTER_UPLOAD_<PLAT>_<USER>       → store.set(..., platform=plat, username=user)

    Idempotent. Safe to re-run — add_user / set are both safe on already-present values.

    KNOWN LIMITATION: per-user override parsing splits on the FIRST '_'
    after the platform name. If a user contained '_' in their original
    env var (which the old code uppercased), the split is ambiguous.
    We try the longest user-name match against the current user list
    first to disambiguate; unresolved ones get logged at WARNING.
    """
    import os as _os
    store = config.policy_store

    plat_to_envkey = {
        "x":         "X_USERS",
        "tiktok":    "TIKTOK_USERS",
        "instagram": "INSTAGRAM_USERS",
    }

    # 1. User lists
    added_total = 0
    for plat, envkey in plat_to_envkey.items():
        raw = _os.environ.get(envkey, "")
        users = [u.strip().lstrip("@") for u in raw.split(",") if u.strip()]
        for u in users:
            if store.add_user(plat, u):
                log.info("migrate: + user [%s] @%s", plat, u)
                added_total += 1

    # 2. Global delete-after-upload default
    raw_global = _os.environ.get("DELETE_AFTER_UPLOAD", "").lower().strip()
    if raw_global in ("1", "true", "yes", "on", "y", "t"):
        store.set("delete_after_upload", True)
        log.info("migrate: set global delete_after_upload=True")
    elif raw_global in ("0", "false", "no", "off", "n", "f"):
        store.set("delete_after_upload", False)
        log.info("migrate: set global delete_after_upload=False")

    # 3. Per-platform / per-user overrides.
    # The old keys uppercased platform AND user. Reverse them by looking
    # up against the current (lowercased) user list to disambiguate.
    plat_names_upper = {p.upper(): p for p in PLATFORM_CHOICES}
    override_count = 0
    unresolved: list[str] = []

    for k, v in _os.environ.items():
        if not k.startswith("DELETE_AFTER_UPLOAD_") or k == "DELETE_AFTER_UPLOAD":
            continue
        body = k[len("DELETE_AFTER_UPLOAD_"):]
        # Find the platform prefix
        matched_plat = None
        for plat_upper, plat_name in plat_names_upper.items():
            if body == plat_upper:
                matched_plat = plat_name
                tail = ""
                break
            prefix = plat_upper + "_"
            if body.startswith(prefix):
                matched_plat = plat_name
                tail = body[len(prefix):]
                break
        if matched_plat is None:
            unresolved.append(k)
            continue

        value_str = str(v).strip().lower()
        if value_str in ("1", "true", "yes", "on", "y", "t"):
            value = True
        elif value_str in ("0", "false", "no", "off", "n", "f"):
            value = False
        else:
            log.warning("migrate: %s=%r unparseable, skipping", k, v)
            continue

        if not tail:
            # Platform-scope override
            store.set("delete_after_upload", value, platform=matched_plat)
            log.info("migrate: set %s delete_after_upload=%s", matched_plat, value)
            override_count += 1
        else:
            # Per-user override. The env-var ambiguity is real:
            # DELETE_AFTER_UPLOAD_X_FOO_BAR could be user "FOO_BAR" or
            # user "FOO" with junk. Resolve by case-insensitive match
            # against the current user list.
            current_users = store.list_users(matched_plat)
            matched_user = None
            for u in current_users:
                if u.upper().replace("-", "_") == tail.replace("-", "_"):
                    matched_user = u
                    break
            if matched_user is None:
                # Best-effort fallback: treat tail as the user verbatim,
                # lowercased. The user can edit config.toml afterward.
                matched_user = tail.lower()
                log.warning(
                    "migrate: per-user override %s — no exact match in "
                    "configured users. Writing as @%s; verify in config.toml.",
                    k, matched_user,
                )
            store.set("delete_after_upload", value,
                      platform=matched_plat, username=matched_user)
            log.info("migrate: set [%s] @%s delete_after_upload=%s",
                     matched_plat, matched_user, value)
            override_count += 1

    if unresolved:
        log.warning("migrate: %d DELETE_AFTER_UPLOAD_* var(s) didn't match any "
                    "platform — left in .env: %s",
                    len(unresolved), ", ".join(unresolved))

    log.info("")
    log.info("══════════════════ Migrate summary ══════════════")
    log.info("  users added:        %d", added_total)
    log.info("  overrides imported: %d", override_count)
    log.info("  config.toml path:   %s", store.path)
    log.info("═════════════════════════════════════════════════")
    log.info("You can now remove these from .env:")
    log.info("  X_USERS, TIKTOK_USERS, INSTAGRAM_USERS")
    log.info("  DELETE_AFTER_UPLOAD, DELETE_AFTER_UPLOAD_*")
    log.info("(Telegram chat IDs and credentials stay in .env.)")
    return 0


# ── loop ─────────────────────────────────────────────────────────────────────

def cmd_loop(args, config: Config, db: ItemStore) -> int:
    import random
    import signal
    import threading
    import time
    from datetime import datetime, timezone, timedelta

    from . import loop_state

    if args.min_sleep < 1 or args.max_sleep < args.min_sleep:
        log.error("Invalid sleep bounds: min=%.0f max=%.0f", args.min_sleep, args.max_sleep)
        return 2
    if args.max_fails < 1:
        log.error("--max-fails must be >= 1")
        return 2

    loop_log_path = Path(config.log_file).parent / "loop.log"
    loop_log_path.parent.mkdir(parents=True, exist_ok=True)

    loop_logger = logging.getLogger("archiver.loop")
    loop_handler = logging.FileHandler(loop_log_path, encoding="utf-8")
    loop_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)-7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    loop_logger.addHandler(loop_handler)
    loop_logger.propagate = True

    stop_requested = [False]

    def _on_sigint(signum, frame):
        if stop_requested[0]:
            loop_logger.warning("Second SIGINT received — exiting immediately.")
            sys.exit(130)
        stop_requested[0] = True
        loop_logger.warning("SIGINT received — will exit after current run/sleep. "
                            "Press Ctrl-C again to force-quit.")

    prev_handler = signal.signal(signal.SIGINT, _on_sigint)

    started_at = datetime.now(timezone.utc)
    run_n = 0
    consecutive_fails = 0
    total_failures = 0

    loop_logger.info("════════════════════════════════════════════════════════════")
    loop_logger.info("archiver loop started")
    loop_logger.info("  sleep range: %.0f-%.0f sec (%.1f-%.1f hours)",
                     args.min_sleep, args.max_sleep,
                     args.min_sleep / 3600, args.max_sleep / 3600)
    loop_logger.info("  bail after: %d consecutive failures", args.max_fails)
    if args.platform or args.user:
        loop_logger.info("  filter: platform=%s user=%s",
                         args.platform or "*", args.user or "*")
    loop_logger.info("  loop log: %s", loop_log_path)

    # ── Background ingest sweeper ────────────────────────────────────────────
    # Drop-folder ingestion (record folder, orphaned chat_id dirs, local
    # platforms) otherwise runs only at the TAIL of a full download cycle —
    # hours apart, and never while a long download pass is mid-flight. This
    # daemon thread sweeps those folders every `ingest_interval` seconds on its
    # OWN db connection, decoupled from the download pass, so a dropped file is
    # enqueued within minutes. `ingest_lock` is shared with the heavy in-loop
    # run() (see cmd_run) so the two never media_prep the same file at once.
    ingest_lock = threading.Lock()
    sweeper_stop = threading.Event()
    sweeper_thread = None
    ingest_interval = getattr(args, "ingest_interval", 180.0)
    if ingest_interval and ingest_interval > 0:
        ingest_interval = max(30.0, ingest_interval)

        def _ingest_sweeper():
            try:
                sweep_db = ItemStore.open(config.db_path)
            except Exception as e:
                loop_logger.error("ingest-sweeper: cannot open DB (%s) — "
                                  "disabled; drop folders fall back to the "
                                  "per-cycle reconcile", e)
                return
            sweep_arch = Archiver(config, sweep_db)
            sweep_arch.ingest_lock = ingest_lock
            loop_logger.info("ingest-sweeper started — every %.0fs "
                             "(record folder + orphaned + local platforms)",
                             ingest_interval)
            first = True
            try:
                while not sweeper_stop.is_set():
                    # Short first delay so startup contends less with run #1;
                    # then settle to the configured interval.
                    if sweeper_stop.wait(5.0 if first else ingest_interval):
                        break
                    first = False
                    try:
                        n = asyncio.run(sweep_arch.ingest_sweep())
                        if n:
                            loop_logger.info(
                                "ingest-sweeper: enqueued %d new file(s)", n)
                    except Exception as e:
                        loop_logger.warning(
                            "ingest-sweeper: sweep failed (%s) — continuing",
                            e, exc_info=True)
            finally:
                try:
                    sweep_db.close()
                except Exception:
                    pass
                loop_logger.info("ingest-sweeper stopped")

        sweeper_thread = threading.Thread(
            target=_ingest_sweeper, name="ingest-sweeper", daemon=True)
        sweeper_thread.start()
    else:
        loop_logger.info("ingest-sweeper disabled (--ingest-interval 0) — "
                         "drop folders ingested only at each cycle's reconcile")
    loop_logger.info("════════════════════════════════════════════════════════════")

    exit_code = 0

    try:
        while not stop_requested[0]:
            run_n += 1
            run_start = time.monotonic()
            loop_logger.info("── run #%d starting ────────────────────────────────", run_n)
            loop_state.write_running(run_n)

            try:
                rc = cmd_run(args, config, db, ingest_lock=ingest_lock, phase_cb=(
                    lambda p, u, _n=run_n:
                        loop_state.write_running(_n, platform=p, user=u)))
            except KeyboardInterrupt:
                stop_requested[0] = True
                loop_logger.warning("run #%d interrupted by user", run_n)
                break
            except Exception as e:
                loop_logger.error("run #%d crashed: %s: %s",
                                  run_n, type(e).__name__, e, exc_info=True)
                rc = 1

            duration = time.monotonic() - run_start

            if rc == 0:
                if consecutive_fails > 0:
                    loop_logger.info("run #%d ✓ ok (%.1fs) — recovered from %d failures",
                                     run_n, duration, consecutive_fails)
                else:
                    loop_logger.info("run #%d ✓ ok (%.1fs)", run_n, duration)
                consecutive_fails = 0
            else:
                consecutive_fails += 1
                total_failures += 1
                loop_logger.warning(
                    "run #%d ✗ failed (rc=%d, %.1fs) — consecutive=%d/%d, total=%d",
                    run_n, rc, duration, consecutive_fails, args.max_fails, total_failures,
                )
                if consecutive_fails >= args.max_fails:
                    loop_logger.error("BAILING: %d consecutive failures hit the limit.",
                                      consecutive_fails)
                    exit_code = 1
                    break

            if stop_requested[0]:
                break

            sleep_secs = random.uniform(args.min_sleep, args.max_sleep)
            wake_at = datetime.now(timezone.utc) + timedelta(seconds=sleep_secs)
            loop_logger.info("sleeping %.0f sec (%.2f h) — next run at %s UTC",
                             sleep_secs, sleep_secs / 3600,
                             wake_at.strftime("%Y-%m-%d %H:%M:%S"))
            loop_state.write_sleeping(run_n, time.time() + sleep_secs)

            slept = 0.0
            while slept < sleep_secs and not stop_requested[0]:
                chunk = min(1.0, sleep_secs - slept)
                time.sleep(chunk)
                slept += chunk

    finally:
        signal.signal(signal.SIGINT, prev_handler)
        sweeper_stop.set()                    # tell the sweeper to exit
        if sweeper_thread is not None:
            sweeper_thread.join(timeout=15)   # let an in-flight sweep finish
        loop_state.clear()   # a stopped loop must not read back as 'sleeping'
        ended_at = datetime.now(timezone.utc)
        uptime = ended_at - started_at
        loop_logger.info("════════════════════════════════════════════════════════════")
        loop_logger.info("archiver loop stopped")
        loop_logger.info("  uptime:           %s", _fmt_duration(uptime))
        loop_logger.info("  runs completed:   %d", run_n)
        loop_logger.info("  total failures:   %d", total_failures)
        loop_logger.info("  ended:            %s UTC",
                         ended_at.strftime("%Y-%m-%d %H:%M:%S"))
        loop_logger.info("════════════════════════════════════════════════════════════")
        loop_logger.removeHandler(loop_handler)
        loop_handler.close()

    return exit_code


def _fmt_duration(td) -> str:
    total = int(td.total_seconds())
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:    parts.append(f"{days}d")
    if hours:   parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


# ── Entry point ───────────────────────────────────────────────────────────────

def _check_old_state_files(config: Config) -> None:
    active_db = Path(config.db_path)
    candidates = [Path("./archive.db"), Path("./.archiver/archive.db")]
    for old_db in candidates:
        if old_db.resolve() == active_db.resolve():
            continue
        if old_db.exists() and not active_db.exists():
            log.warning("⚠  Found stale archive.db at %s", old_db.resolve())
            log.warning("   The configured location is %s", active_db.resolve())
            log.warning("   To migrate:")
            log.warning("     mkdir -p '%s'", active_db.parent)
            log.warning("     mv '%s' '%s-wal' '%s-shm' '%s'",
                        old_db, old_db, old_db, active_db.parent)
            return


def main() -> int:
    parser = build_parser()
    args   = parser.parse_args()

    config_only = args.cmd in {
        "config", "platform", "run-settings", "migrate", "policy",
        "dedup-policy", "safebrake", "purge-sent", "stats", "ingest", "queue",
        "backfill", "auto-ingest", "local", "download", "sort", "auto-sort",
        "auto-retry", "banned",
    }
    if args.cmd == "reset" and args.reset_cmd in {"failed", "user"}:
        config_only = True

    try:
        config = Config.load(
            load_platform_configs = not config_only,
            require_platforms     = not config_only,
        )
    except RuntimeError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2

    setup_logging(config.log_file, verbose=args.verbose)

    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║              Media Archiver                              ║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info("Enabled platforms: %s", ", ".join(sorted(config.enabled_platforms)))

    _check_old_state_files(config)

    db = ItemStore.open(config.db_path)
    try:
        dispatch = {
            "start":        cmd_start,
            "run":          cmd_run,
            "queue":        cmd_queue,
            "ingest":       cmd_ingest,
            "auto-ingest":  cmd_auto_ingest,
            "sort":         cmd_sort,
            "auto-sort":    cmd_auto_sort,
            "auto-retry":   cmd_auto_retry,
            "local":        cmd_local,
            "download":     cmd_download,
            "backfill":     cmd_backfill,
            "bootstrap":    cmd_bootstrap,
            "reset":        cmd_reset,
            "reconcile":    cmd_reconcile,
            "dedup":        cmd_dedup,
            "stats":        cmd_stats,
            "health":       cmd_health,
            "cookies":      cmd_cookies,
            "loop":         cmd_loop,
            "config":       cmd_config,
            "banned":       cmd_banned,
            "platform":     cmd_platform,
            "run-settings": cmd_run_settings,
            "policy":       cmd_policy,
            "dedup-policy": cmd_dedup_policy,
            "safebrake":    cmd_safebrake,
            "purge-sent":   cmd_purge_sent,
            "migrate":      cmd_migrate,
        }
        handler = dispatch[args.cmd]
        # Single-instance guard for the long-running / download work commands:
        # two of these in parallel would double-download and race the queue.
        # All share the "archiver" lock so `loop`, `run`, `start`, `bootstrap`
        # are mutually exclusive (the loop runs `run` IN-PROCESS, so no
        # self-deadlock). Quick admin/read commands are never gated.
        if args.cmd in {"loop", "run", "start", "bootstrap"}:
            from core import InstanceLock, InstanceAlreadyRunning
            try:
                with InstanceLock("archiver"):
                    return handler(args, config, db)
            except InstanceAlreadyRunning as exc:
                log.error("%s", exc)
                return 1
        return handler(args, config, db)
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
