"""
dispatcher.cli
──────────────
Argparse-based CLI. Subcommands:

  dispatcher start                      Run the drain loop in foreground.
  dispatcher status                     Queue counts + top pending rows.
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
)
from core import cli as core_cli

from .config import DispatcherConfig, session_name_or_default
from .drain import drain_forever
from .instance_lock import DispatcherAlreadyRunning, DispatcherInstanceLock
from .progress import ProgressReporter, describe, read_progress
from .send import TelethonSendStrategy
from .tg_router import TelegramRouter

log = logging.getLogger(__name__)


# ── Logging ───────────────────────────────────────────────────────────────

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


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
        log.info("cli: received signal %s, requesting clean shutdown", signum)
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
    log.info("cli: db=%s session=%s",
             config.db_path, config.telegram.session_name)
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
        log.info("cli: interrupted")
    return 0


# ── Subcommand: status ────────────────────────────────────────────────────

def cmd_status(args: argparse.Namespace) -> int:
    config = DispatcherConfig.load(require_telegram=False)

    # Liveness first: the question behind most `status` invocations is
    # "is a drain daemon actually running?" — answered via the instance
    # lock's holder, not by guessing from queue counts.
    pid = DispatcherInstanceLock(session_name_or_default()).holder_pid()
    if pid is not None:
        print(f"daemon: running (pid {pid})")
        prog = read_progress()
        if prog:
            print(f"uploading: {describe(prog)}")
    else:
        print("daemon: NOT running")

    store = ItemStore.open(config.db_path)
    try:
        counts = store.counts_by_status()
        total = sum(counts.values())
        last = store.last_sent_at()
        print(f"last sent: {last or 'never'}")
        print(f"db: {config.db_path}")
        print(f"  total: {total}")
        for st in ("pending", "sending", "sent", "failed"):
            print(f"  {st:8s}: {counts.get(st, 0)}")

        pending = store.list_items(status="pending", limit=10)
        if pending:
            print(f"\ntop 10 pending (priority asc):")
            for r in pending:
                print(
                    f"  id={r.id:>5} prio={r.priority:>3} "
                    f"src={r.source:<10} {r.platform}/@{r.username} "
                    f"{Path(r.file_path).name}"
                )
    finally:
        store.close()
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
          or getattr(args, "config_command", None)
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
