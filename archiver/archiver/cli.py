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
  chats                  Show or edit Telegram destination chat per user
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
from .db import ArchiveDB
from .orchestrator import Archiver, bootstrap, build_platforms
from .policies import DeletePolicy, DedupPolicy
from .tg_router import TelegramRouter


PLATFORM_CHOICES = ["x", "tiktok", "instagram"]


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging(log_file: str, verbose: bool = False) -> None:
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level   = level,
        format  = "%(asctime)s [%(levelname)-7s] %(name)s — %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    for lib in ("telethon", "httpx", "httpcore", "urllib3", "asyncio"):
        logging.getLogger(lib).setLevel(logging.WARNING if not verbose else logging.INFO)


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

    # ── run ───
    s_run = sub.add_parser("run", help="Normal archive cycle")
    s_run.add_argument("--platform", choices=PLATFORM_CHOICES,
                       help="Limit to one platform")
    s_run.add_argument("--user", metavar="USERNAME",
                       help="Limit to one username (no @)")

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

    rf = reset_sub.add_parser("failed", help="Re-queue failed uploads")
    rf.add_argument("--platform", choices=PLATFORM_CHOICES)
    rf.add_argument("--user", metavar="USERNAME")

    ru = reset_sub.add_parser("uploads", help="Re-queue ALL uploads (no re-download)")
    ru.add_argument("--platform", choices=PLATFORM_CHOICES)
    ru.add_argument("--user", metavar="USERNAME")

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
    s_rec = sub.add_parser("reconcile", help="Scan disk for files missing from DB")
    s_rec.add_argument("--platform", choices=PLATFORM_CHOICES)
    s_rec.add_argument("--user", metavar="USERNAME")

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

    # ── chats ───
    s_ch = sub.add_parser(
        "chats",
        help="Show or edit Telegram destination chat per (platform, user)",
    )
    s_ch.add_argument("--platform", choices=PLATFORM_CHOICES)
    s_ch.add_argument("--user", metavar="USERNAME")
    ch_sub = s_ch.add_subparsers(dest="chats_action", required=False,
                                  metavar="ACTION",
                                  help="omit to print resolution; "
                                       "'set'/'unset' to mutate .env")

    ch_set = ch_sub.add_parser("set",
        help="Write TELEGRAM_CHAT_ID_<PLAT>[_<USER>]=CHAT into .env")
    ch_set.add_argument("--platform", choices=PLATFORM_CHOICES, required=True)
    ch_set.add_argument("--user", metavar="USERNAME",
                         help="If given, sets a per-user override.")
    ch_set.add_argument("--chat", metavar="CHAT_ID", required=True,
                         help="Chat ID (e.g. -1001234567890) or @username")

    ch_unset = ch_sub.add_parser("unset",
        help="Remove a TELEGRAM_CHAT_ID_<PLAT>[_<USER>] entry from .env")
    ch_unset.add_argument("--platform", choices=PLATFORM_CHOICES, required=True)
    ch_unset.add_argument("--user", metavar="USERNAME",
                           help="If given, removes a per-user override.")

    # ── migrate ───
    sub.add_parser(
        "migrate",
        help="One-shot: import legacy .env user lists + DELETE_AFTER_UPLOAD_* "
             "overrides into config.toml. Idempotent; safe to re-run.",
    )

    return p


# ── Command handlers ──────────────────────────────────────────────────────────

def cmd_run(args, config: Config, db: ArchiveDB) -> int:
    arch = Archiver(config, db)
    results = asyncio.run(arch.run(
        platform_filter = args.platform,
        user_filter     = args.user.lstrip("@") if args.user else None,
    ))

    log.info("")
    log.info("══════════════════ Summary ══════════════════════")
    for key, r in results.items():
        status = r.get("status", "?")
        if status == "ok":
            line = f"  ✓ {key:32s} dl={r.get('downloaded',0):>3} up={r.get('uploaded',0):>3}"
        elif status == "partial":
            line = f"  ⚠ {key:32s} dl={r.get('downloaded',0):>3} up={r.get('uploaded',0):>3} fail={r.get('failed',0)}"
        else:
            line = f"  ✗ {key:32s} {status}: {r.get('reason','')[:40]}"
        log.info(line)
    log.info("═════════════════════════════════════════════════")
    return 0 if all(r.get("status") in ("ok",) for r in results.values()) else 1


def cmd_bootstrap(args, config: Config, db: ArchiveDB) -> int:
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


def cmd_reset(args, config: Config, db: ArchiveDB) -> int:
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
                n = db.reconcile(platform.name, u, config.output_dir)
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


def cmd_reconcile(args, config: Config, db: ArchiveDB) -> int:
    platforms = build_platforms(config)
    if args.platform:
        platforms = [p for p in platforms if p.name == args.platform]

    total = 0
    for platform in platforms:
        users = (args.user.lstrip("@"),) if args.user else platform.users
        for u in users:
            n = db.reconcile(platform.name, u, config.output_dir)
            log.info("[%s] @%s: reconciled %d", platform.name, u, n)
            total += n
    log.info("Total reconciled: %d", total)
    return 0


def cmd_dedup(args, config: Config, db: ArchiveDB) -> int:
    """
    Content-hash dedup for one or all users. Dry-run by default;
    pass --yes for actual deletion.

    Independent of the dedup-after-download policy — the latter only
    controls the post-download auto-trigger. This command always runs
    when invoked.
    """
    from .dedup import dedup_user

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


def cmd_stats(args, config: Config, db: ArchiveDB) -> int:
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

    platforms = build_platforms(config)
    if platforms:
        log.info("")
        log.info("date_floor (next incremental cutoff):")
        for p in platforms:
            users = [user] if user else list(p.users)
            for u in users:
                floor = db.get_date_floor(p.name, u)
                log.info("  [%s] @%s → %s", p.name, u, floor or "(none — full fetch)")
    return 0


def cmd_health(args, config: Config, db: ArchiveDB) -> int:
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
        username = args.user.lstrip("@") if args.user else None
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
        username = args.user.lstrip("@") if args.user else None
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

    platforms = build_platforms(config)
    if args.platform:
        platforms = [p for p in platforms if p.name == args.platform]
        if not platforms:
            log.error("No matching platform configured: %s", args.platform)
            return 2

    user_filter = args.user.lstrip("@") if args.user else None
    rows = 0
    for p in platforms:
        users = [u for u in p.users if user_filter is None or u == user_filter]
        for u in users:
            log.info("  [%s] @%s → %s", p.name, u, policy.explain(p.name, u))
            rows += 1
    if rows == 0:
        log.warning("No (platform, user) matched the filter.")
    return 0


def cmd_policy(args, config: Config, db: ArchiveDB) -> int:
    return _cmd_boolpolicy(
        args, config, DeletePolicy,
        action_attr = "policy_action",
        value_attr  = "delete",
        cmd_label   = "policy",
    )


def cmd_dedup_policy(args, config: Config, db: ArchiveDB) -> int:
    return _cmd_boolpolicy(
        args, config, DedupPolicy,
        action_attr = "dp_action",
        value_attr  = "enabled",
        cmd_label   = "dedup-policy",
    )


# ── chats commands (still env-var-backed; chat IDs are always safe ASCII) ────

def cmd_chats(args, config: Config, db: ArchiveDB) -> int:
    action = getattr(args, "chats_action", None)
    if action == "set":
        return _cmd_chats_set(args, config)
    if action == "unset":
        return _cmd_chats_unset(args, config)

    router = TelegramRouter(default_chat_id=config.telegram.chat_id)
    log.info("Telegram destination resolution:")
    log.info("  global default (TELEGRAM_CHAT_ID): %s", config.telegram.chat_id)
    log.info("")

    platforms = build_platforms(config)
    if args.platform:
        platforms = [p for p in platforms if p.name == args.platform]
        if not platforms:
            log.error("No matching platform configured: %s", args.platform)
            return 2

    user_filter = args.user.lstrip("@") if args.user else None
    rows = 0
    for p in platforms:
        users = [u for u in p.users if user_filter is None or u == user_filter]
        for u in users:
            log.info("  [%s] @%s → %s", p.name, u, router.explain(p.name, u))
            rows += 1
    if rows == 0:
        log.warning("No (platform, user) matched the filter.")
    return 0


# ── Helpers ──────────────────────────────────────────────────────────────────

def _scope_label(platform: str | None, username: str | None) -> str:
    if platform and username:
        return f"[{platform}] @{username}"
    if platform:
        return f"[{platform}] (platform-wide)"
    return "(global)"


def _env_path() -> Path:
    return Path.home() / ".config" / "archiver" / ".env"


def _warn_unknown_user(config: Config, platform: str, username: str) -> None:
    """Soft warning if the user isn't currently in the platform's user list.
    Doesn't block writes — operators sometimes pre-stage overrides."""
    cfg_block = getattr(config, platform, None)
    if cfg_block is None:
        log.warning(
            "Platform [%s] is not currently configured. The override "
            "will be written but inactive until the platform is enabled.",
            platform,
        )
        return
    if username not in cfg_block.users:
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


# ── chats set / unset (still env-backed) ─────────────────────────────────────

def _cmd_chats_set(args, config: Config) -> int:
    from .tg_router import _user_key, _platform_key

    platform = args.platform
    username = args.user.lstrip("@") if args.user else None
    chat     = args.chat.strip()

    err = _validate_chat_id_format(chat)
    if err:
        log.error("chats set: %s", err)
        return 2

    if username:
        _warn_unknown_user(config, platform, username)
        key = _user_key(platform, username)
        scope = f"[{platform}] @{username}"
    else:
        key = _platform_key(platform)
        scope = f"[{platform}] (platform-wide)"

    _set_env_var(key, chat)
    log.info("chats set: %s → %s   [%s=%s]", scope, chat, key, chat)
    log.info("Note: a running `archiver loop` won't see this change until it restarts.")
    return 0


def _cmd_chats_unset(args, config: Config) -> int:
    from .tg_router import _user_key, _platform_key

    platform = args.platform
    username = args.user.lstrip("@") if args.user else None

    if username:
        key = _user_key(platform, username)
        scope = f"[{platform}] @{username}"
    else:
        key = _platform_key(platform)
        scope = f"[{platform}] (platform-wide)"

    removed = _unset_env_var(key)
    if removed:
        log.info("chats unset: removed %s   [%s]", scope, key)
        log.info("Resolution now falls through to the next level.")
    else:
        log.info("chats unset: %s was not set in .env — nothing to do.", scope)
    return 0


# ── config (user-list management, now backed by PolicyStore) ─────────────────

def cmd_config(args, config: Config, db: ArchiveDB) -> int:
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
        added = store.add_user(args.platform, username)
        if added:
            log.info("Added @%s to [%s].", username, args.platform)
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


# ── cookies ──────────────────────────────────────────────────────────────────

def cmd_cookies(args, config: Config, db: ArchiveDB) -> int:
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

def cmd_migrate(args, config: Config, db: ArchiveDB) -> int:
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

def cmd_loop(args, config: Config, db: ArchiveDB) -> int:
    import random
    import signal
    import time
    from datetime import datetime, timezone, timedelta

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
    loop_logger.info("════════════════════════════════════════════════════════════")

    exit_code = 0

    try:
        while not stop_requested[0]:
            run_n += 1
            run_start = time.monotonic()
            loop_logger.info("── run #%d starting ────────────────────────────────", run_n)

            try:
                rc = cmd_run(args, config, db)
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

            slept = 0.0
            while slept < sleep_secs and not stop_requested[0]:
                chunk = min(1.0, sleep_secs - slept)
                time.sleep(chunk)
                slept += chunk

    finally:
        signal.signal(signal.SIGINT, prev_handler)
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

    try:
        config = Config.load()
    except RuntimeError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2

    setup_logging(config.log_file, verbose=args.verbose)

    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║              Media Archiver                              ║")
    log.info("╚══════════════════════════════════════════════════════════╝")
    log.info("Enabled platforms: %s", ", ".join(sorted(config.enabled_platforms)))

    _check_old_state_files(config)

    db = ArchiveDB(config.db_path)
    try:
        dispatch = {
            "run":          cmd_run,
            "bootstrap":    cmd_bootstrap,
            "reset":        cmd_reset,
            "reconcile":    cmd_reconcile,
            "dedup":        cmd_dedup,
            "stats":        cmd_stats,
            "health":       cmd_health,
            "cookies":      cmd_cookies,
            "loop":         cmd_loop,
            "config":       cmd_config,
            "policy":       cmd_policy,
            "dedup-policy": cmd_dedup_policy,
            "chats":        cmd_chats,
            "migrate":      cmd_migrate,
        }
        return dispatch[args.cmd](args, config, db)
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
