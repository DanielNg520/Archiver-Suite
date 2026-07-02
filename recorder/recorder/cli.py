"""
recorder.cli
────────────
  recorder start                         foreground (watch the priority list)
  recorder start --daemon                fork; pid file at state_dir/pid
  recorder record --user <u>             ONE-SHOT: if @u is live, record it
                                         once and exit (no listening loop)
  recorder stop                          terminate via pid file
  recorder status                        state + queue depth + lock
  recorder config add --user <u>
  recorder config remove --user <u>
  recorder config list
  recorder config priority --user <u> --rank N

config writes go to ~/.config/recorder/config.toml (the priority-ordered
user list). The ordering of the `users` array IS the priority.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
import tomllib
from pathlib import Path

import tomli_w

from core import ItemStore
from core import cli as core_cli
from core import heartbeat

from . import ui, watch
from .config import CONFIG_TOML, RecorderConfig

log = logging.getLogger(__name__)


def cmd_stats(args: argparse.Namespace) -> int:
    """Shared `stats` noun — counts from the one suite DB, identical output to
    archiver/dispatcher `stats`."""
    config = RecorderConfig.load()
    store = ItemStore.open(config.db_path)
    try:
        return core_cli.handle_stats(store, args)
    finally:
        store.close()


def _setup_logging(verbose: bool) -> None:
    ui.setup_logging(verbose)


def _pid_path(config: RecorderConfig) -> Path:
    return Path(config.state_dir).expanduser() / "pid"


def _recorder_running(pid_path: Path) -> bool:
    """True iff the pid file names a live process. Liveness via the suite's one
    primitive (core.heartbeat.pid_alive): a dead/stale pid reads false, one alive
    but owned by another user reads true."""
    if not pid_path.exists():
        return False
    try:
        pid = int(pid_path.read_text().strip())
    except (OSError, ValueError):
        return False
    return heartbeat.pid_alive(pid)


# ── start ─────────────────────────────────────────────────────────────────

def cmd_start(args: argparse.Namespace) -> int:
    config = RecorderConfig.load()
    if not config.tiktok_users:
        log.error("no tiktok users configured. Add some: "
                  "recorder config add --user <username>")
        return 1

    # Lazy imports: only needed to actually run, and they pull in
    # TikTokLive / subprocess machinery.
    from .capture import StreamCapture
    from .enqueue import EnqueueClient
    from .lock import TikTokLock
    from .platforms.tiktok import TikTokLivePlatform
    from .state import StateMachine

    if args.daemon:
        _daemonize(config)

    ui.banner(config)

    pid_path = _pid_path(config)
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()))

    # Startup reconciliation: clear stale logs, requeue un-uploaded recordings,
    # delete already-uploaded leftovers, prune empty folders. Best-effort — a
    # failed sweep must never stop the recorder from coming up.
    from .startup_sweep import sweep
    try:
        report = sweep(config.output_dir, config.db_path,
                       split_threshold_bytes=config.split_threshold_bytes)
        log.info("startup sweep — %s", report, extra={"ev": "sweep"})
    except Exception as e:
        log.warning("startup sweep skipped after error: %s", e)

    platform = TikTokLivePlatform(config.tiktok_cookies_file, config.state_dir)
    capture  = StreamCapture(config.output_dir, config.tiktok_cookies_file)
    enqueue_client = EnqueueClient(
        config.db_path, split_threshold_bytes=config.split_threshold_bytes)
    lock = TikTokLock(config.lock_path, os.getpid())

    def _enqueue(platform_name, username, file_path, caption, group_key=None):
        enqueue_client.enqueue(
            platform=platform_name, username=username,
            file_path=file_path, caption=caption, group_key=group_key,
        )

    machine = StateMachine(config, platform, capture, _enqueue, lock)

    def _on_signal(signum, _frame):
        # First signal → graceful stop. A second one (the user mashing Ctrl-C
        # because an in-flight network probe / browser fallback hasn't yielded
        # yet) → hard exit, so the terminal is always recoverable.
        if machine._stop.is_set():
            log.warning("second signal — forcing exit", extra={"ev": "stop"})
            os._exit(130)
        log.info("signal %s — requesting stop (Ctrl-C again to force)", signum,
                 extra={"ev": "stop"})
        machine.request_stop()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    try:
        machine.run_forever()
    finally:
        pid_path.unlink(missing_ok=True)
    return 0


def _daemonize(config: RecorderConfig) -> None:
    """Minimal double-fork daemonization. launchd (Slice 5) is the real
    backgrounding mechanism; this --daemon flag is for manual use."""
    if os.fork() > 0:
        sys.exit(0)
    os.setsid()
    if os.fork() > 0:
        sys.exit(0)
    log_dir = Path(config.state_dir).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)
    out = open(log_dir / "recorder.out.log", "a")
    os.dup2(out.fileno(), sys.stdout.fileno())
    os.dup2(out.fileno(), sys.stderr.fileno())


# ── record (one-shot manual mode) ──────────────────────────────────────────

def cmd_record(args: argparse.Namespace) -> int:
    """Manual one-shot: check whether one username is live and, if so, record
    that stream to the end, then exit. No listening loop, no priority list —
    just this user, just once.

    Exit codes: 0 recorded, 1 conflict/error, 3 user not live.
    """
    config = RecorderConfig.load()
    username = args.user.lstrip("@")

    # A running recorder owns the TikTok lockfile; a second one would stomp it
    # (the lock is a soft signal file, not a mutex) and could double-record.
    # Manual mode is for ad-hoc grabs when the service isn't running.
    pid_path = _pid_path(config)
    if _recorder_running(pid_path):
        log.error("a recorder is already running (pid file %s). Manual record "
                  "would conflict with its TikTok lock — `recorder stop` first, "
                  "or add @%s to the list and let the service catch it.",
                  pid_path, username)
        return 1

    from .capture import StreamCapture
    from .enqueue import EnqueueClient
    from .lock import TikTokLock
    from .platforms.tiktok import TikTokLivePlatform
    from .state import StateMachine

    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(str(os.getpid()))

    platform = TikTokLivePlatform(config.tiktok_cookies_file, config.state_dir)
    capture  = StreamCapture(config.output_dir, config.tiktok_cookies_file)
    enqueue_client = EnqueueClient(
        config.db_path, split_threshold_bytes=config.split_threshold_bytes)
    lock = TikTokLock(config.lock_path, os.getpid())

    def _enqueue(platform_name, username, file_path, caption, group_key=None):
        enqueue_client.enqueue(
            platform=platform_name, username=username,
            file_path=file_path, caption=caption, group_key=group_key,
        )

    machine = StateMachine(config, platform, capture, _enqueue, lock)

    def _on_signal(signum, _frame):
        # First signal → graceful stop; second → hard exit (see cmd_start).
        if machine._stop.is_set():
            log.warning("second signal — forcing exit", extra={"ev": "stop"})
            os._exit(130)
        log.info("signal %s — requesting stop (Ctrl-C again to force)", signum,
                 extra={"ev": "stop"})
        machine.request_stop()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    try:
        recorded = machine.record_once(username)
    finally:
        pid_path.unlink(missing_ok=True)
    if recorded:
        return 0
    print(f"@{username} is not live — nothing recorded")
    return 3


# ── stop ──────────────────────────────────────────────────────────────────

def cmd_stop(args: argparse.Namespace) -> int:
    config = RecorderConfig.load()
    pid_path = _pid_path(config)
    if not pid_path.exists():
        log.error("no pid file at %s — recorder not running?", pid_path)
        return 1
    try:
        pid = int(pid_path.read_text().strip())
    except (OSError, ValueError):
        log.error("pid file unreadable: %s", pid_path)
        return 1
    try:
        os.kill(pid, signal.SIGTERM)
        log.info("sent SIGTERM to recorder pid=%d", pid)
    except ProcessLookupError:
        log.warning("pid %d not running — clearing stale pid file", pid)
        pid_path.unlink(missing_ok=True)
        return 1
    return 0


# ── status ────────────────────────────────────────────────────────────────

def cmd_status(args: argparse.Namespace) -> int:
    config = RecorderConfig.load()
    pid_path = _pid_path(config)

    running = False
    state_line, accent = "not running", "dim"
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text().strip())
            os.kill(pid, 0)
            running = True
            state_line, accent = f"running · pid {pid}", "green"
        except (OSError, ValueError, ProcessLookupError):
            state_line, accent = "not running (stale pid file)", "yellow"

    lock_held = Path(config.lock_path).expanduser().exists()
    roster = "  ".join(f"@{u}" for u in config.tiktok_users) or "(none)"

    print()
    ui.field("recorder", state_line, accent=accent)
    ui.field("recording", "yes — tiktok.lock held" if lock_held else "no",
             accent="green" if lock_held else None)
    ui.field("users", roster)
    ui.field("output", str(config.output_dir))
    ui.field("queue", str(config.db_path))
    print()
    return 0


# ── watch ─────────────────────────────────────────────────────────────────

def cmd_watch(args: argparse.Namespace) -> int:
    """Live dashboard: clear + re-render every `interval` seconds, the same
    loop as `ops watch`. Successive snapshots are diffed to show the active
    recording's live write-rate."""
    config = RecorderConfig.load()
    prev: tuple[str, int, float] | None = None    # (path, size, wall-clock)
    try:
        while True:
            snap = watch.snapshot(config)
            now = time.monotonic()
            rate = None
            if snap.active and prev and prev[0] == snap.active.path:
                dt = now - prev[2]
                if dt > 0:
                    rate = max(0.0, (snap.active.size - prev[1]) / dt)
            prev = (snap.active.path, snap.active.size, now) if snap.active else None

            print("\033[2J\033[H", end="")        # clear screen, home cursor
            print(f"recorder watch  ({time.strftime('%H:%M:%S')})\n")
            print(watch.render(snap, rate_bps=rate))
            footer = f"refreshing every {args.interval:.0f}s · Ctrl-C to exit"
            print(f"\n  {ui._paint(footer, 'dim', on=ui.color_enabled())}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print()
        return 0


# ── config ────────────────────────────────────────────────────────────────

def _load_toml() -> dict:
    if CONFIG_TOML.exists():
        with CONFIG_TOML.open("rb") as f:
            return tomllib.load(f)
    return {}


def _save_toml(data: dict) -> None:
    CONFIG_TOML.parent.mkdir(parents=True, exist_ok=True)
    tmp = CONFIG_TOML.with_suffix(".toml.tmp")
    with tmp.open("wb") as f:
        f.write(tomli_w.dumps(data).encode("utf-8"))
    os.replace(tmp, CONFIG_TOML)


def _get_users(data: dict) -> list[str]:
    return list(data.get("recorder", {}).get("tiktok", {}).get("users", []))


def _set_users(data: dict, users: list[str]) -> None:
    data.setdefault("recorder", {}).setdefault("tiktok", {})["users"] = users


def cmd_config_add(args: argparse.Namespace) -> int:
    data = _load_toml()
    users = _get_users(data)
    u = args.user.lstrip("@")
    if u in users:
        print(f"@{u} already in list")
        return 0
    users.append(u)
    _set_users(data, users)
    _save_toml(data)
    print(f"added @{u} (priority rank {len(users)})")
    return 0


def cmd_config_remove(args: argparse.Namespace) -> int:
    data = _load_toml()
    users = _get_users(data)
    u = args.user.lstrip("@")
    if u not in users:
        print(f"@{u} not in list")
        return 1
    users.remove(u)
    _set_users(data, users)
    _save_toml(data)
    print(f"removed @{u}")
    return 0


def cmd_config_list(args: argparse.Namespace) -> int:
    users = _get_users(_load_toml())
    if not users:
        print("(no users configured)")
        return 0
    for i, u in enumerate(users, 1):
        print(f"{i}. @{u}")
    return 0


def cmd_config_priority(args: argparse.Namespace) -> int:
    data = _load_toml()
    users = _get_users(data)
    u = args.user.lstrip("@")
    if u not in users:
        print(f"@{u} not in list — add it first")
        return 1
    rank = args.rank
    if not (1 <= rank <= len(users)):
        print(f"rank must be 1..{len(users)}")
        return 1
    users.remove(u)
    users.insert(rank - 1, u)
    _set_users(data, users)
    _save_toml(data)
    print(f"@{u} moved to rank {rank}")
    return 0


# ── parser ──────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="recorder",
                                description="TikTok live recorder.")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    p_start = sub.add_parser("start", help="run the recorder")
    p_start.add_argument("--daemon", action="store_true",
                         help="fork into background (pid file in state_dir)")
    p_record = sub.add_parser(
        "record",
        help="one-shot: record a single user's live now, then exit (no loop)")
    p_record.add_argument("--user", required=True,
                          help="username to check and record once")
    sub.add_parser("stop", help="stop a running recorder via pid file")
    sub.add_parser("status", help="show state + lock + user list")
    p_watch = sub.add_parser("watch", help="live auto-refreshing dashboard")
    p_watch.add_argument("--interval", type=float, default=2.0,
                         help="seconds between refreshes (default 2)")
    core_cli.add_stats_parser(sub)   # shared `stats` noun (DB counts)

    p_cfg = sub.add_parser("config", help="manage the user list")
    cfg_sub = p_cfg.add_subparsers(dest="config_command", required=True)

    p_add = cfg_sub.add_parser("add"); p_add.add_argument("--user", required=True)
    p_rm  = cfg_sub.add_parser("remove"); p_rm.add_argument("--user", required=True)
    cfg_sub.add_parser("list")
    p_pri = cfg_sub.add_parser("priority")
    p_pri.add_argument("--user", required=True)
    p_pri.add_argument("--rank", type=int, required=True)

    return p


_DISPATCH = {
    ("start", None):              cmd_start,
    ("record", None):             cmd_record,
    ("stop", None):               cmd_stop,
    ("status", None):             cmd_status,
    ("watch", None):              cmd_watch,
    ("stats", None):              cmd_stats,
    ("config", "add"):            cmd_config_add,
    ("config", "remove"):         cmd_config_remove,
    ("config", "list"):           cmd_config_list,
    ("config", "priority"):       cmd_config_priority,
}


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    _setup_logging(args.verbose)
    sub = getattr(args, "config_command", None)
    handler = _DISPATCH.get((args.command, sub))
    if handler is None:
        log.error("no handler for %s/%s", args.command, sub)
        return 2
    try:
        # Single-instance guard: only one `recorder start` may run at a time
        # (two would fight over the same TikTok session + capture dirs).
        # InstanceAlreadyRunning is a RuntimeError, so the handler below reports
        # it cleanly. Other subcommands (config, stop, watch, …) are not gated.
        if args.command == "start":
            from core import InstanceLock
            with InstanceLock("recorder"):
                return handler(args)
        return handler(args)
    except RuntimeError as e:
        log.error("%s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
