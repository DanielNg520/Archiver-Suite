"""
core.cli
────────
Shared CLI building blocks so archiver / recorder / dispatcher stop disagreeing
on the cross-cutting NOUNS that act on shared state:

  stats   — counts from the one items table
  queue   — list / retry / cancel rows in that table
  config  — get / set / list / unset settings via PolicyStore (user→platform→global)

Each noun is a (add_*_parser, handle_*) pair. A binary calls add_*_parser(subparsers)
while building its argparse tree, then routes the matching command to handle_*
with its already-open ItemStore / PolicyStore. The handlers are deliberately
store-in / int-out (process exit code) and dependency-free, so they're unit
testable without constructing any app.

This is the layer that lets the three binaries keep separate entry points yet
present an identical interface for everything that touches the shared DB/config.
"""

from __future__ import annotations

import json as _json
from argparse import Namespace, _SubParsersAction
from pathlib import Path

from .store import ItemStore
from .policy_store import PolicyStore

_STATUSES = ("pending", "sending", "sent", "failed")


# ── stats ─────────────────────────────────────────────────────────────────────

def add_stats_parser(sub: _SubParsersAction) -> None:
    p = sub.add_parser("stats", help="Show item counts from the shared DB")
    p.add_argument("--platform")
    p.add_argument("--user", dest="username", metavar="USERNAME")
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON")


def handle_stats(store: ItemStore, args: Namespace) -> int:
    st = store.stats(getattr(args, "platform", None),
                     getattr(args, "username", None))
    if getattr(args, "json", False):
        print(_json.dumps(st))
        return 0
    scope = "global"
    if getattr(args, "platform", None):
        scope = args.platform + (f"/@{args.username}" if getattr(args, "username", None) else "")
    print(f"[{scope}] total={st['total']} "
          f"pending={st['pending']} sending={st['sending']} "
          f"sent={st['sent']} failed={st['failed']} "
          f"({st['total_mb']:.1f} MB)")
    return 0


# ── queue ─────────────────────────────────────────────────────────────────────

def add_queue_parser(sub: _SubParsersAction) -> None:
    p = sub.add_parser("queue", help="Inspect/operate on the shared queue")
    qsub = p.add_subparsers(dest="queue_cmd", required=True)

    p_list = qsub.add_parser("list", help="List queue rows")
    p_list.add_argument("--status", choices=_STATUSES)
    p_list.add_argument("--limit", type=int, default=50)
    p_list.add_argument("--offset", type=int, default=0)
    p_list.add_argument("--json", action="store_true")

    p_retry = qsub.add_parser("retry", help="Reset a row to pending (attempts=0)")
    p_retry.add_argument("id", type=int)

    p_cancel = qsub.add_parser("cancel", help="Force a pending/sending row to failed")
    p_cancel.add_argument("id", type=int)


def handle_queue(store: ItemStore, args: Namespace) -> int:
    cmd = args.queue_cmd
    if cmd == "list":
        rows = store.list_items(status=args.status, limit=args.limit,
                                offset=args.offset)
        if getattr(args, "json", False):
            print(_json.dumps([{
                "id": r.id, "status": r.status, "priority": r.priority,
                "attempts": r.attempts, "source": r.source,
                "platform": r.platform, "username": r.username,
                "chat_id": r.chat_id, "group_key": r.group_key,
                "file": r.file_path, "last_error": r.last_error,
            } for r in rows]))
            return 0
        for r in rows:
            dest = r.chat_id or f"{r.platform}/@{r.username}"
            err = f" ERR={r.last_error[:60]}" if r.last_error else ""
            print(f"id={r.id:>5} {r.status:<8} prio={r.priority:>3} "
                  f"att={r.attempts} src={r.source:<10} {dest} "
                  f"{Path(r.file_path).name}{err}")
        print(f"\n({len(rows)} rows)")
        return 0
    if cmd == "retry":
        if store.retry(args.id):
            print(f"id={args.id} reset to pending (attempts=0)")
            return 0
        print(f"id={args.id} not found")
        return 1
    if cmd == "cancel":
        if store.cancel(args.id):
            print(f"id={args.id} cancelled (status=failed)")
            return 0
        print(f"id={args.id} not found, or not in pending/sending")
        return 1
    return 2


# ── config (settings, NOT the user watch-list) ────────────────────────────────

def add_config_parser(sub: _SubParsersAction) -> None:
    p = sub.add_parser("config", help="Get/set settings (config.toml via PolicyStore)")
    csub = p.add_subparsers(dest="config_cmd", required=True)

    def _scope(sp):
        sp.add_argument("--platform")
        sp.add_argument("--user", dest="username", metavar="USERNAME")

    p_get = csub.add_parser("get", help="Show a key's effective value + source")
    p_get.add_argument("key"); _scope(p_get)

    p_set = csub.add_parser("set", help="Set a key at the given scope")
    p_set.add_argument("key"); p_set.add_argument("value"); _scope(p_set)

    p_unset = csub.add_parser("unset", help="Remove a key at the given scope")
    p_unset.add_argument("key"); _scope(p_unset)

    csub.add_parser("list", help="List all scoped overrides")


def _coerce(value: str):
    """Light scalar coercion so `set x true` / `set x 5` store typed values,
    matching what BooleanPolicy/int settings expect on read-back."""
    low = value.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(value)
    except ValueError:
        return value


def handle_config(policy: PolicyStore, args: Namespace) -> int:
    cmd = args.config_cmd
    platform = getattr(args, "platform", None)
    username = getattr(args, "username", None)
    if cmd == "get":
        val, source = policy.explain(args.key, platform=platform, username=username)
        print(f"{args.key} = {val!r}  (from {source})")
        return 0
    if cmd == "set":
        policy.set(args.key, _coerce(args.value), platform=platform, username=username)
        scope = "global" if not platform else (
            f"{platform}/@{username}" if username else platform)
        print(f"set {args.key} = {_coerce(args.value)!r} at {scope}")
        return 0
    if cmd == "unset":
        removed = policy.unset(args.key, platform=platform, username=username)
        print(f"{'removed' if removed else 'no such key'}: {args.key}")
        return 0 if removed else 1
    if cmd == "list":
        any_shown = False
        for platform_name, username_name, overrides in policy.iter_user_overrides():
            for k, v in overrides.items():
                print(f"{platform_name}/@{username_name}: {k} = {v!r}")
                any_shown = True
        if not any_shown:
            print("(no overrides)")
        return 0
    return 2
