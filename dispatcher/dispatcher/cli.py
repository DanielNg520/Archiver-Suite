"""
dispatcher.cli
──────────────
Argparse-based CLI. Subcommands:

  dispatcher start                      Run the drain loop in foreground.
  dispatcher status                     Queue counts + top pending rows.
  dispatcher check-routes [token …]     Verify chat_id/.t<topic> dests exist.
  dispatcher banned-words add <word…>   Add words stripped from names/captions.
  dispatcher banned-words remove <w…>   Remove banned words.  list  Show them.
  dispatcher queue list [--status S]    List rows; default newest 50.
  dispatcher queue retry <id>           Reset failed/sent row to pending.
  dispatcher queue cancel <id>          Force pending/sending row to failed.
  dispatcher config show                Dump effective config + .env path.

Design notes:
  - Subparsers per top-level command. Keeps `--help` output readable.
  - No daemonization. macOS launchd handles backgrounding (slice 5).
    Running in foreground means logs go to stdout/stderr, which launchd
    redirects to files. Don't reinvent.
  - Signal handling: SIGINT/SIGTERM sets stop_event, drain exits cleanly
    between rows. Telethon disconnects via context manager exit.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from pathlib import Path

from core import (
    ItemStore, DeletePolicy, RecorderDeletePolicy, BatchPolicy, DeletionGuard,
    parse_route, load_words,
)
from core import cli as core_cli
from core import termui

from .config import (
    DispatcherConfig, session_name_or_default, banned_words_file_path,
)
from .drain import drain_forever
from .instance_lock import DispatcherAlreadyRunning, DispatcherInstanceLock
from .progress import ProgressReporter, describe, read_progress
from .send import TelethonSendStrategy
from .tg_router import TelegramRouter, Destination

log = logging.getLogger(__name__)


# ── Logging ───────────────────────────────────────────────────────────────

def _setup_logging(verbose: bool) -> None:
    termui.setup_logging(verbose)


# ── Subcommand: start ─────────────────────────────────────────────────────

async def _run_drain(config: DispatcherConfig) -> None:
    if config.telegram is None or config.default_chat_id is None:
        raise RuntimeError("dispatcher start requires Telegram credentials")
    store         = ItemStore.open(config.db_path)
    router        = TelegramRouter(default_chat_id=config.default_chat_id)
    delete_policy = DeletePolicy(config.policy_store)
    recorder_delete_policy = RecorderDeletePolicy(config.policy_store)
    batch_policy  = BatchPolicy(config.policy_store)
    guard         = DeletionGuard(config.policy_store)

    stop_event = asyncio.Event()

    def _on_signal(signum: int) -> None:
        log.info("signal %s — shutting down cleanly", signum, extra={"ev": "stop"})
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _on_signal, sig)

    async with TelethonSendStrategy(
        api_id           = config.telegram.api_id,
        api_hash         = config.telegram.api_hash,
        phone            = config.telegram.phone,
        session_name     = config.telegram.session_name,
        max_retries      = config.max_retries,
        retry_base_delay = config.retry_base_delay,
        max_flood_wait_s = config.max_flood_wait_s,
        stall_base_timeout_s = config.stall_base_timeout_s,
        stall_min_rate_kib_s = config.stall_min_rate_kib_s,
        upload_connections   = config.upload_connections,
        progress         = ProgressReporter(),
        sanitizer        = config.sanitizer,
    ) as send_strategy:
        try:
            await drain_forever(
                config=config,
                store=store,
                send_strategy=send_strategy,
                router=router,
                delete_policy=delete_policy,
                recorder_delete_policy=recorder_delete_policy,
                batch_policy=batch_policy,
                guard=guard,
                stop_event=stop_event,
            )
        finally:
            store.close()


def cmd_start(args: argparse.Namespace) -> int:
    config = DispatcherConfig.load(require_telegram=True)
    assert config.telegram is not None
    conns = config.upload_connections
    termui.banner("dispatcher", [
        ("upload", f"{conns} connection{'' if conns == 1 else 's'} per file"
                   f"{' (serial)' if conns <= 1 else ''}"),
        ("session", config.telegram.session_name),
        ("chat", str(config.default_chat_id)),
        ("queue", config.db_path),
    ], subtitle="telegram uploader")
    try:
        with DispatcherInstanceLock(config.telegram.session_name):
            asyncio.run(_run_drain(config))
    except DispatcherAlreadyRunning as exc:
        log.error("cli: %s", exc)
        return 1
    except KeyboardInterrupt:
        # add_signal_handler should normally swallow SIGINT, but if asyncio
        # is in early startup before the handler is registered, KeyboardInterrupt
        # can still surface. Treat as clean exit.
        log.info("interrupted", extra={"ev": "stop"})
    return 0


# ── Subcommand: check-routes ──────────────────────────────────────────────

async def _run_check_routes(
    config: DispatcherConfig, dests: "list[tuple[str, int | None]]",
) -> int:
    assert config.telegram is not None
    bad = 0
    async with TelethonSendStrategy(
        api_id       = config.telegram.api_id,
        api_hash     = config.telegram.api_hash,
        phone        = config.telegram.phone,
        session_name = config.telegram.session_name,
    ) as strat:
        for chat_id, topic_id in dests:
            # Same peer construction the sender uses, so a green check means the
            # exact value we'd send to resolves.
            dest = Destination(chat_id, topic_id)
            label = chat_id + (f".t{topic_id}" if topic_id is not None else "")
            try:
                ok, detail = await strat.check_destination(
                    peer=dest.peer, topic_id=topic_id)
            except Exception as e:                       # pragma: no cover
                ok, detail = False, f"check errored ({type(e).__name__}: {e})"
            termui.field(label, detail, accent="green" if ok else "red")
            bad += 0 if ok else 1
    return 1 if bad else 0


def cmd_check_routes(args: argparse.Namespace) -> int:
    """Verify chat_id / chat_id.t<topic> destinations actually exist on Telegram.
    With no args, checks every explicit destination in the queue plus the default
    chat; otherwise checks the given tokens (dash-free + `.t<topic>` accepted)."""
    config = DispatcherConfig.load(require_telegram=True)
    assert config.telegram is not None

    dests: list[tuple[str, int | None]] = []
    if args.targets:
        for t in args.targets:
            r = parse_route(t)
            if r is None:
                termui.field(t, "invalid chat_id / route token", accent="red")
                continue
            dests.append((r.chat_id, r.topic_id))
    else:
        store = ItemStore.open(config.db_path)
        try:
            dests = store.distinct_destinations()
        finally:
            store.close()
        if config.default_chat_id and \
                (config.default_chat_id, None) not in dests:
            dests.insert(0, (config.default_chat_id, None))

    if not dests:
        termui.field("check-routes",
                     "nothing to check (no explicit queue destinations)",
                     accent="yellow")
        return 0
    print()
    return asyncio.run(_run_check_routes(config, dests))


# ── Subcommand: banned-words ──────────────────────────────────────────────

def cmd_banned_words(args: argparse.Namespace) -> int:
    """Manage the banned-word list the sanitizer strips from upload filenames +
    captions. Edits BANNED_WORDS_FILE in place, preserving comments/blank lines.
    `add` is idempotent (case-insensitive); `remove` drops matching lines."""
    path = banned_words_file_path()
    action = args.banned_command

    if action == "list":
        words = load_words(path)
        if not words:
            termui.field("banned-words", f"none set ({path})", accent="yellow")
        else:
            print()
            for w in words:
                termui.field("•", w, accent="red")
            termui.field("file", str(path), accent="dim")
        return 0

    # add / remove both mutate the file.
    existing_raw = path.read_text(encoding="utf-8").splitlines() \
        if path.exists() else []
    active = {ln.strip().lower() for ln in existing_raw
              if ln.strip() and not ln.strip().startswith("#")}
    targets = [w.strip() for w in args.words if w.strip()]

    if action == "add":
        added = []
        out = list(existing_raw)
        for w in targets:
            if w.lower() in active:
                continue
            out.append(w)
            active.add(w.lower())
            added.append(w)
        if added:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(out) + "\n", encoding="utf-8")
        termui.field("added", ", ".join(added) if added else "(all already present)",
                     accent="green" if added else "yellow")
        return 0

    if action == "remove":
        drop = {w.lower() for w in targets}
        kept, removed = [], []
        for ln in existing_raw:
            s = ln.strip()
            if s and not s.startswith("#") and s.lower() in drop:
                removed.append(s)
            else:
                kept.append(ln)
        if removed:
            path.write_text(("\n".join(kept) + "\n") if kept else "",
                            encoding="utf-8")
        termui.field("removed", ", ".join(removed) if removed else "(none matched)",
                     accent="green" if removed else "yellow")
        return 0
    return 2


# ── Subcommand: status ────────────────────────────────────────────────────

def cmd_status(args: argparse.Namespace) -> int:
    config = DispatcherConfig.load(require_telegram=False)

    # Liveness first: the question behind most `status` invocations is
    # "is a drain daemon actually running?" — answered via the instance
    # lock's holder, not by guessing from queue counts.
    pid = DispatcherInstanceLock(session_name_or_default()).holder_pid()
    print()
    if pid is not None:
        termui.field("dispatcher", f"running · pid {pid}", accent="green")
        prog = read_progress()
        if prog:
            termui.field("uploading", describe(prog), accent="cyan")
    else:
        termui.field("dispatcher", "not running", accent="yellow")

    store = ItemStore.open(config.db_path)
    try:
        counts = store.counts_by_status()
        last = store.last_sent_at()
        queue = (f"{counts.get('pending', 0)} pending · "
                 f"{counts.get('sending', 0)} sending · "
                 f"{counts.get('sent', 0)} sent · "
                 f"{counts.get('failed', 0)} failed")
        termui.field("queue", queue,
                     accent="yellow" if counts.get("failed") else None)
        termui.field("last sent", termui.age(last))

        pending = store.list_items(status="pending", limit=5)
        if pending:
            print()
            print(f"  {termui.paint('next up (priority order)', 'dim')}")
            for r in pending:
                print(f"    {termui.paint(f'{r.priority:>2}', 'dim')} "
                      f"@{r.username} · {Path(r.file_path).name} "
                      f"{termui.paint(f'[{r.platform}]', 'dim')}")
    finally:
        store.close()
    print()
    return 0


# ── Subcommand: queue ─────────────────────────────────────────────────────

def cmd_queue_list(args: argparse.Namespace) -> int:
    config = DispatcherConfig.load(require_telegram=False)
    store = ItemStore.open(config.db_path)
    try:
        rows = store.list_items(
            status=args.status, limit=args.limit, offset=args.offset,
        )
        for r in rows:
            err = f" ERR={r.last_error[:60]}" if r.last_error else ""
            print(
                f"id={r.id:>5} {r.status:<8} prio={r.priority:>3} "
                f"att={r.attempts} src={r.source:<10} "
                f"{r.platform}/@{r.username} {Path(r.file_path).name}{err}"
            )
        print(f"\n({len(rows)} rows)")
    finally:
        store.close()
    return 0


def cmd_queue_retry(args: argparse.Namespace) -> int:
    config = DispatcherConfig.load(require_telegram=False)
    store = ItemStore.open(config.db_path)
    try:
        if store.retry(args.id):
            print(f"id={args.id} reset to pending (attempts=0)")
            return 0
        print(f"id={args.id} not found", file=sys.stderr)
        return 1
    finally:
        store.close()


def cmd_queue_cancel(args: argparse.Namespace) -> int:
    config = DispatcherConfig.load(require_telegram=False)
    store = ItemStore.open(config.db_path)
    try:
        if store.cancel(args.id):
            print(f"id={args.id} cancelled (status=failed)")
            return 0
        print(
            f"id={args.id} not found, or not in pending/sending",
            file=sys.stderr,
        )
        return 1
    finally:
        store.close()


# ── Subcommand: stats (shared noun — DB counts, distinct from `status`) ───

def cmd_stats(args: argparse.Namespace) -> int:
    config = DispatcherConfig.load(require_telegram=False)
    store = ItemStore.open(config.db_path)
    try:
        return core_cli.handle_stats(store, args)
    finally:
        store.close()


def cmd_config(args: argparse.Namespace) -> int:
    """Settings get/set/unset/list via the shared PolicyStore handler.
    args.config_command (set|get|unset|list) → core_cli's config_cmd."""
    args.config_cmd = args.config_command
    config = DispatcherConfig.load(require_telegram=False)
    return core_cli.handle_config(config.policy_store, args)


# ── Subcommand: config show ───────────────────────────────────────────────

def cmd_config_show(args: argparse.Namespace) -> int:
    config = DispatcherConfig.load(require_telegram=False)
    print(f".env path:        {config.env_path()}")
    print(f"config.toml path: {config.config_toml_path()}")
    print(f"db path:          {config.db_path}")
    print("session:          (load with `dispatcher start`)")
    print("default chat:     (load with `dispatcher start`)")
    print(f"poll interval:    {config.poll_interval_s}s")
    print(f"max retries:      {config.max_retries}")
    print(f"retry base delay: {config.retry_base_delay}s")
    print(f"max flood wait:   {config.max_flood_wait_s}s")
    print(f"stuck claim:      {config.stuck_claim_min}m")
    print(f"stall watchdog:   {config.stall_base_timeout_s:.0f}s base "
          f"+ payload @ {config.stall_min_rate_kib_s:.0f} KiB/s floor")
    return 0


# ── argparse wiring ───────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dispatcher",
        description="Telegram upload dispatcher (drains a shared SQLite queue).",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="enable DEBUG logging",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("start", help="run drain loop in foreground")
    sub.add_parser("status", help="show queue counts + top pending")
    core_cli.add_stats_parser(sub)   # shared `stats` noun (DB counts)

    p_check = sub.add_parser(
        "check-routes",
        help="verify chat_id / chat_id.t<topic> destinations exist on Telegram")
    p_check.add_argument(
        "targets", nargs="*",
        help="chat_id or chat_id.t<topic> to check (dash-free numeric ok); "
             "default = all explicit queue destinations + the default chat")

    p_banned = sub.add_parser(
        "banned-words", help="manage words stripped from filenames/captions")
    banned_sub = p_banned.add_subparsers(dest="banned_command", required=True)
    b_add = banned_sub.add_parser("add", help="add one or more banned words")
    b_add.add_argument("words", nargs="+")
    b_rm = banned_sub.add_parser("remove", help="remove one or more banned words")
    b_rm.add_argument("words", nargs="+")
    banned_sub.add_parser("list", help="list the current banned words")

    p_queue = sub.add_parser("queue", help="queue operations")
    queue_sub = p_queue.add_subparsers(dest="queue_command", required=True)

    p_list = queue_sub.add_parser("list", help="list queue rows")
    p_list.add_argument(
        "--status",
        choices=["pending", "sending", "sent", "failed"],
        default=None,
    )
    p_list.add_argument("--limit", type=int, default=50)
    p_list.add_argument("--offset", type=int, default=0)

    p_retry = queue_sub.add_parser("retry", help="reset row to pending")
    p_retry.add_argument("id", type=int)

    p_cancel = queue_sub.add_parser("cancel", help="force row to failed")
    p_cancel.add_argument("id", type=int)

    p_config = sub.add_parser("config", help="config operations")
    config_sub = p_config.add_subparsers(dest="config_command", required=True)
    config_sub.add_parser("show", help="dump effective config")

    # Settings get/set/unset/list via PolicyStore (config.toml). How the
    # min-batch policy is tuned, e.g.:
    #   dispatcher config set min_batch_size 10 --platform x
    def _scope(sp):
        sp.add_argument("--platform")
        sp.add_argument("--user", dest="username", metavar="USERNAME")
    c_get = config_sub.add_parser("get", help="show a key's effective value")
    c_get.add_argument("key"); _scope(c_get)
    c_set = config_sub.add_parser("set", help="set a key at the given scope")
    c_set.add_argument("key"); c_set.add_argument("value"); _scope(c_set)
    c_unset = config_sub.add_parser("unset", help="remove a key at the given scope")
    c_unset.add_argument("key"); _scope(c_unset)
    config_sub.add_parser("list", help="list all scoped overrides")

    return parser


_DISPATCHERS = {
    ("start", None):                cmd_start,
    ("status", None):               cmd_status,
    ("check-routes", None):         cmd_check_routes,
    ("banned-words", "add"):        cmd_banned_words,
    ("banned-words", "remove"):     cmd_banned_words,
    ("banned-words", "list"):       cmd_banned_words,
    ("stats", None):                cmd_stats,
    ("queue", "list"):              cmd_queue_list,
    ("queue", "retry"):             cmd_queue_retry,
    ("queue", "cancel"):            cmd_queue_cancel,
    ("config", "show"):             cmd_config_show,
    ("config", "get"):              cmd_config,
    ("config", "set"):              cmd_config,
    ("config", "unset"):            cmd_config,
    ("config", "list"):             cmd_config,
}


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    _setup_logging(args.verbose)

    sub = getattr(args, "queue_command", None) \
          or getattr(args, "config_command", None) \
          or getattr(args, "banned_command", None)
    handler = _DISPATCHERS.get((args.command, sub))
    if handler is None:
        log.error("cli: no handler for %s/%s", args.command, sub)
        return 2
    try:
        return handler(args)
    except RuntimeError as e:
        # Config-load errors surface here as RuntimeError; we want a
        # clean message rather than a traceback for "missing env var".
        log.error("cli: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
