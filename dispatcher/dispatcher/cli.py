"""
dispatcher.cli
──────────────
Argparse-based CLI. Subcommands:

  dispatcher start                      Run the drain loop in foreground.
  dispatcher status                     Queue counts + top pending rows.
  dispatcher check-routes [token …]     Verify chat_id/.t<topic> dests exist.
  dispatcher burner login               Register the optional burner account.
  dispatcher burner chats add <id…>     Route chats via the burner. remove/list.
  dispatcher burner status              Show burner config without connecting.
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
import os
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
from .send import TelethonSendStrategy, SessionUnauthorized
from .tg_router import TelegramRouter, Destination

log = logging.getLogger(__name__)


# ── Logging ───────────────────────────────────────────────────────────────

def _setup_logging(verbose: bool) -> None:
    termui.setup_logging(verbose)


def _assert_video_metadata_backend() -> None:
    """Refuse to start if Telethon's video-metadata backend (hachoir) is absent.

    The native video-ALBUM send (send.py) passes no explicit attributes and
    relies on Telethon to derive each item's width/height/duration. Telethon can
    only do that with hachoir; without it every album video uploads with a
    degenerate DocumentAttributeVideo(w=1, h=1, duration=0) and Telegram renders
    it as a 1x1 static IMAGE. That is silent corruption of delivered media, so we
    fail fast (integrity first) rather than drain the queue into broken videos.
    Single sends are immune — they attach explicit ffprobe attributes — but the
    queue is mixed, so a hard stop is the only safe stance."""
    import importlib.util
    if importlib.util.find_spec("hachoir") is None:
        raise RuntimeError(
            "hachoir is not installed — Telethon cannot read video geometry, so "
            "album videos would upload as 1x1 static images. Install it with "
            "`pipx inject dispatcher hachoir` (it is a declared dependency; a "
            "clean reinstall also pulls it in)."
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
        fast_album           = config.fast_album,
        progress         = ProgressReporter(),
        sanitizer        = config.sanitizer,
        burner           = config.burner,
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
        except SessionUnauthorized:
            # Session died mid-run. The drain is serial, so the only rows in
            # 'sending' are the batch that was in flight — revert them to pending
            # immediately (older_than_minutes=0) so the next authorized run
            # retries them, then let the fatal propagate to a clean CLI exit.
            store.reset_stuck_sending(older_than_minutes=0)
            raise
        finally:
            store.close()


def cmd_start(args: argparse.Namespace) -> int:
    config = DispatcherConfig.load(require_telegram=True)
    assert config.telegram is not None
    _assert_video_metadata_backend()
    conns = config.upload_connections
    rows = [
        ("upload", f"{conns} connection{'' if conns == 1 else 's'} per file"
                   f"{' (serial)' if conns <= 1 else ''}"),
        ("session", config.telegram.session_name),
        ("chat", str(config.default_chat_id)),
        ("queue", config.db_path),
    ]
    if config.burner is not None:
        rows.append(("burner", f"{len(config.burner.chat_ids)} chat"
                     f"{'' if len(config.burner.chat_ids) == 1 else 's'} → "
                     f"{config.burner.session_name}"))
    termui.banner("dispatcher", rows, subtitle="telegram uploader")
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
        burner       = config.burner,
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


# ── Subcommand: burner ────────────────────────────────────────────────────

async def _burner_login(session: str, api_id: int, api_hash: str,
                        phone: str) -> str:
    """Interactively authorize the burner session (Telethon prompts for the code
    on this TTY) and return the logged-in account's display name. Kept separate
    from the send strategy: registration is a one-off manual step, not a drain."""
    from telethon import TelegramClient  # local: only the login path needs it
    client = TelegramClient(session, api_id, api_hash)
    await client.start(phone=phone)
    me = await client.get_me()
    await client.disconnect()
    name = " ".join(filter(None, [getattr(me, "first_name", None),
                                  getattr(me, "last_name", None)])) \
        or getattr(me, "username", None) or str(getattr(me, "id", "?"))
    return name


def cmd_burner_login(args: argparse.Namespace) -> int:
    """Register (log in) the optional burner account — the ONLY way to create its
    session. api_id/api_hash default to the primary's TELEGRAM_API_* when not
    given; phone/session are persisted to the dispatcher .env so `start` picks
    the burner up automatically. Requires a TTY for the login code."""
    import os as _os
    from .config import (
        dispatcher_env_path, burner_session_name_or_default, upsert_env_vars,
    )
    from core.env import MissingEnvVar

    api_id_raw = args.api_id or _os.environ.get("TELEGRAM_BURNER_API_ID") \
        or _os.environ.get("TELEGRAM_API_ID")
    api_hash = args.api_hash or _os.environ.get("TELEGRAM_BURNER_API_HASH") \
        or _os.environ.get("TELEGRAM_API_HASH")
    phone = args.phone or _os.environ.get("TELEGRAM_BURNER_PHONE")
    if not api_id_raw or not api_hash:
        raise MissingEnvVar(
            "burner login needs api_id/api_hash — pass --api-id/--api-hash or "
            "set TELEGRAM_API_ID/TELEGRAM_API_HASH in the dispatcher .env")
    if not phone:
        raise RuntimeError("burner login needs a phone — pass --phone")
    try:
        api_id = int(api_id_raw)
    except ValueError:
        raise RuntimeError(f"api_id {api_id_raw!r} is not an integer")

    session = args.session or burner_session_name_or_default()
    _os.makedirs(_os.path.dirname(session) or ".", exist_ok=True)
    if not sys.stdin.isatty():
        raise RuntimeError(
            "burner login is interactive (Telegram sends a code) — run it from "
            "a terminal, not a headless/service context")

    name = asyncio.run(_burner_login(session, api_id, api_hash, phone))

    # Persist so config.BurnerCreds.from_env resolves the burner on next start.
    values = {"TELEGRAM_BURNER_SESSION": session, "TELEGRAM_BURNER_PHONE": phone}
    if args.api_id:
        values["TELEGRAM_BURNER_API_ID"] = str(api_id)
    if args.api_hash:
        values["TELEGRAM_BURNER_API_HASH"] = api_hash
    upsert_env_vars(dispatcher_env_path(), values)

    print()
    termui.field("burner", f"logged in as {name}", accent="green")
    termui.field("session", session, accent="dim")
    if not _os.environ.get("BURNER_CHAT_IDS", "").strip():
        termui.field("next", "add dedicated chats: dispatcher burner chats add "
                     "<chat_id …>", accent="yellow")
    return 0


def _burner_chat_ids() -> list[str]:
    """Current normalized burner chat set from the env, order-stable."""
    raw = os.environ.get("BURNER_CHAT_IDS", "")
    out: list[str] = []
    for tok in raw.replace(",", " ").split():
        r = parse_route(tok)
        if r is not None and r.chat_id not in out:
            out.append(r.chat_id)
    return out


def cmd_burner_chats(args: argparse.Namespace) -> int:
    """Manage the burner's dedicated chats (BURNER_CHAT_IDS) via the .env — the
    chats routed through the burner instead of the primary. CLI-only, so the
    routing set is never hand-edited."""
    from .config import dispatcher_env_path, upsert_env_vars
    action = args.chats_command
    current = _burner_chat_ids()

    if action == "list":
        if not current:
            termui.field("burner chats", "none set", accent="yellow")
        else:
            print()
            for c in current:
                termui.field("•", c, accent="cyan")
        return 0

    targets: list[str] = []
    for tok in args.chat_ids:
        r = parse_route(tok)
        if r is None:
            termui.field(tok, "invalid chat_id / route token", accent="red")
            continue
        targets.append(r.chat_id)

    if action == "add":
        new = current + [c for c in targets if c not in current]
        changed = [c for c in targets if c not in current]
    else:  # remove
        drop = set(targets)
        new = [c for c in current if c not in drop]
        changed = [c for c in targets if c in current]

    upsert_env_vars(dispatcher_env_path(), {"BURNER_CHAT_IDS": ",".join(new)})
    verb = "added" if action == "add" else "removed"
    termui.field(verb, ", ".join(changed) if changed else "(no change)",
                 accent="green" if changed else "yellow")
    termui.field("burner chats", ", ".join(new) if new else "(none)", accent="dim")
    return 0


def cmd_burner_status(args: argparse.Namespace) -> int:
    """Show the burner's configuration WITHOUT connecting: whether it's active,
    its session, and its dedicated chats. `require_telegram=False` so it works
    even when the primary creds aren't loadable."""
    from .config import burner_session_name_or_default
    config = DispatcherConfig.load(require_telegram=False)
    print()
    chats = _burner_chat_ids()
    session = burner_session_name_or_default()
    session_exists = Path(session + ".session").exists()
    phone = os.environ.get("TELEGRAM_BURNER_PHONE", "").strip()

    active = bool(chats) and bool(phone or session_exists)
    termui.field("burner", "active" if active else "inactive (feature off)",
                 accent="green" if active else "yellow")
    termui.field("session", f"{session}"
                 f"{'  ✓ authorized' if session_exists else '  (not logged in)'}",
                 accent="dim")
    termui.field("phone", phone or "(unset)", accent="dim")
    termui.field("chats", ", ".join(chats) if chats else "(none)",
                 accent="cyan" if chats else "yellow")
    if not active:
        termui.field("hint", "register with `dispatcher burner login` then "
                     "`dispatcher burner chats add <chat_id …>`", accent="dim")
    return 0


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

    p_burner = sub.add_parser(
        "burner", help="register/manage the optional burner account (CLI-only)")
    burner_sub = p_burner.add_subparsers(dest="burner_command", required=True)
    b_login = burner_sub.add_parser(
        "login", help="interactively log in the burner account + persist its creds")
    b_login.add_argument("--phone", help="burner phone (else TELEGRAM_BURNER_PHONE)")
    b_login.add_argument("--api-id", dest="api_id",
                         help="burner api_id (else the primary's TELEGRAM_API_ID)")
    b_login.add_argument("--api-hash", dest="api_hash",
                         help="burner api_hash (else the primary's TELEGRAM_API_HASH)")
    b_login.add_argument("--session",
                         help="session file path (else <primary>-burner)")
    b_chats = burner_sub.add_parser(
        "chats", help="manage the burner's dedicated chats (BURNER_CHAT_IDS)")
    chats_sub = b_chats.add_subparsers(dest="chats_command", required=True)
    bc_add = chats_sub.add_parser("add", help="route one or more chats via the burner")
    bc_add.add_argument("chat_ids", nargs="+")
    bc_rm = chats_sub.add_parser("remove", help="stop routing chats via the burner")
    bc_rm.add_argument("chat_ids", nargs="+")
    chats_sub.add_parser("list", help="list the burner's dedicated chats")
    burner_sub.add_parser("status", help="show burner config without connecting")

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
    ("burner", "login"):            cmd_burner_login,
    ("burner", "chats"):            cmd_burner_chats,
    ("burner", "status"):           cmd_burner_status,
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
          or getattr(args, "banned_command", None) \
          or getattr(args, "burner_command", None)
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
