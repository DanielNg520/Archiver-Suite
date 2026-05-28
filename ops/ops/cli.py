"""
ops.cli
───────
  ops health           one-shot system health report
  ops watch            health report refreshed every few seconds
  ops load             launchctl load all three plists
  ops unload           launchctl unload all three plists
  ops restart <name>   kickstart one service (dispatcher|recorder|archiver)

load/unload/restart are thin wrappers over launchctl so you don't have to
remember the plist paths. They operate on whatever plists are present in
~/Library/LaunchAgents/com.duy.*.plist.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

from .health import LABELS, render

LAUNCH_AGENTS = Path("~/Library/LaunchAgents").expanduser()


def cmd_health(_args: argparse.Namespace) -> int:
    print(render())
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    try:
        while True:
            print("\033[2J\033[H", end="")  # clear screen
            print(f"system health  ({time.strftime('%H:%M:%S')})\n")
            print(render())
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


def _plist_path(label: str) -> Path:
    return LAUNCH_AGENTS / f"{label}.plist"


def cmd_load(_args: argparse.Namespace) -> int:
    rc = 0
    for name, label in LABELS.items():
        p = _plist_path(label)
        if not p.exists():
            print(f"{name}: plist missing ({p}) — skipped")
            rc = 1
            continue
        r = subprocess.run(["launchctl", "load", str(p)],
                           capture_output=True, text=True)
        if r.returncode == 0:
            print(f"{name}: loaded")
        else:
            print(f"{name}: load failed — {r.stderr.strip()}")
            rc = 1
    return rc


def cmd_unload(_args: argparse.Namespace) -> int:
    rc = 0
    for name, label in LABELS.items():
        p = _plist_path(label)
        if not p.exists():
            continue
        r = subprocess.run(["launchctl", "unload", str(p)],
                           capture_output=True, text=True)
        if r.returncode == 0:
            print(f"{name}: unloaded")
        else:
            print(f"{name}: unload failed — {r.stderr.strip()}")
            rc = 1
    return rc


def cmd_restart(args: argparse.Namespace) -> int:
    if args.service not in LABELS:
        print(f"unknown service: {args.service} (choose from {list(LABELS)})")
        return 2
    label = LABELS[args.service]
    # `launchctl kickstart -k gui/<uid>/<label>` restarts a running job.
    uid = subprocess.run(["id", "-u"], capture_output=True, text=True).stdout.strip()
    r = subprocess.run(
        ["launchctl", "kickstart", "-k", f"gui/{uid}/{label}"],
        capture_output=True, text=True,
    )
    if r.returncode == 0:
        print(f"{args.service}: restarted")
        return 0
    print(f"{args.service}: restart failed — {r.stderr.strip()}")
    return 1


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ops",
                                description="Ops tooling for the archiver/recorder/dispatcher system.")
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("health", help="one-shot health report")
    w = sub.add_parser("watch", help="auto-refreshing health report")
    w.add_argument("--interval", type=float, default=3.0)
    sub.add_parser("load", help="launchctl load all three plists")
    sub.add_parser("unload", help="launchctl unload all three plists")
    r = sub.add_parser("restart", help="restart one service")
    r.add_argument("service", choices=list(LABELS))
    return p


_DISPATCH = {
    "health":  cmd_health,
    "watch":   cmd_watch,
    "load":    cmd_load,
    "unload":  cmd_unload,
    "restart": cmd_restart,
}


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return _DISPATCH[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
