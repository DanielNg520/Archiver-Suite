"""
ops.cli
───────
  ops install          generate + write the three launchd plists
  ops uninstall        unload + remove the three plists
  ops health           one-shot system health report
  ops watch            health report refreshed every few seconds
  ops load             launchctl load all three plists
  ops unload           launchctl unload all three plists
  ops restart <name>   kickstart one service (dispatcher|recorder|archiver)

load/unload/restart are thin wrappers over launchctl so you don't have to
remember the plist paths. They operate on whatever plists are present in
~/Library/LaunchAgents/com.duy.*.plist — which `ops install` creates. The
plists are GENERATED here (not shipped as static files) so the absolute paths
they embed match THIS machine's home + pipx bin dir, not whoever's repo they
came from.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

from .health import LABELS, render

LAUNCH_AGENTS = Path("~/Library/LaunchAgents").expanduser()
LOG_DIR = Path("~/.local/log").expanduser()

# service name → (CLI command on PATH, subcommand args). Mirrors what each
# launchd job should run: the dispatcher/recorder drain/listen continuously,
# the archiver loops `run` on a random interval.
_SERVICE_CMD: dict[str, tuple[str, list[str]]] = {
    "dispatcher": ("dispatcher", ["start"]),
    "recorder":   ("recorder",   ["start"]),
    "archiver":   ("archiver",   ["loop"]),
}


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


def _resolve_bin(cmd: str) -> str | None:
    """Absolute path to a service CLI. Prefer PATH (pipx puts it there), fall
    back to ~/.local/bin/<cmd>. launchd needs an absolute path — it does not
    source your shell, so a bare name would never resolve."""
    found = shutil.which(cmd)
    if found:
        return found
    fallback = Path.home() / ".local" / "bin" / cmd
    return str(fallback) if fallback.exists() else None


def _plist_xml(label: str, program: str, sub_args: list[str]) -> str:
    """Generate a launchd plist for one service with paths bound to THIS
    machine (program's bin dir on PATH, $HOME workdir, ~/.local/log capture)."""
    tag = label.rsplit(".", 1)[-1]                 # com.duy.dispatcher → dispatcher
    bindir = str(Path(program).parent)
    path_env = ":".join([bindir, "/opt/homebrew/bin", "/usr/local/bin",
                         "/usr/bin", "/bin", "/usr/sbin", "/sbin"])
    prog_lines = "\n".join(f"        <string>{a}</string>"
                           for a in (program, *sub_args))
    home = str(Path.home())
    out = LOG_DIR / f"{tag}.out.log"
    err = LOG_DIR / f"{tag}.err.log"
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" \
"http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>
    <key>ProgramArguments</key>
    <array>
{prog_lines}
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>30</integer>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>{path_env}</string>
    </dict>
    <key>StandardOutPath</key>
    <string>{out}</string>
    <key>StandardErrorPath</key>
    <string>{err}</string>
    <key>WorkingDirectory</key>
    <string>{home}</string>
    <key>ProcessType</key>
    <string>Background</string>
</dict>
</plist>
"""


def cmd_install(_args: argparse.Namespace) -> int:
    """Write the three launchd plists into ~/Library/LaunchAgents, generated
    for this machine. Idempotent — re-running overwrites with fresh paths.
    Run `ops load` afterward to start them (and at every login)."""
    LAUNCH_AGENTS.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    rc = 0
    for name, label in LABELS.items():
        cmd, sub_args = _SERVICE_CMD[name]
        program = _resolve_bin(cmd)
        if program is None:
            print(f"{name}: '{cmd}' not found on PATH or in ~/.local/bin — "
                  f"install it first (pipx install ./{cmd}), then re-run. skipped")
            rc = 1
            continue
        path = _plist_path(label)
        path.write_text(_plist_xml(label, program, sub_args))
        print(f"{name}: wrote {path}  →  {program} {' '.join(sub_args)}")
    if rc == 0:
        print("installed. Now run:  ops load")
    return rc


def cmd_uninstall(args: argparse.Namespace) -> int:
    """Unload (if loaded) and remove the three plists."""
    cmd_unload(args)
    for name, label in LABELS.items():
        path = _plist_path(label)
        if path.exists():
            path.unlink()
            print(f"{name}: removed {path}")
    return 0


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
    sub.add_parser("install", help="generate + write the three launchd plists")
    sub.add_parser("uninstall", help="unload + remove the three plists")
    sub.add_parser("health", help="one-shot health report")
    w = sub.add_parser("watch", help="auto-refreshing health report")
    w.add_argument("--interval", type=float, default=3.0)
    sub.add_parser("load", help="launchctl load all three plists")
    sub.add_parser("unload", help="launchctl unload all three plists")
    r = sub.add_parser("restart", help="restart one service")
    r.add_argument("service", choices=list(LABELS))
    return p


_DISPATCH = {
    "install":   cmd_install,
    "uninstall": cmd_uninstall,
    "health":    cmd_health,
    "watch":     cmd_watch,
    "load":      cmd_load,
    "unload":    cmd_unload,
    "restart":   cmd_restart,
}


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return _DISPATCH[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
