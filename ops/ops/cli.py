"""
ops.cli
───────
  ops install          generate + write the three launchd plists
  ops uninstall        unload + remove the three plists
  ops health           one-shot system health report
  ops watch            health report refreshed every few seconds
  ops load [name]      launchctl load managed plists (all, or just one)
  ops unload [name]    launchctl unload managed plists (all, or just one —
                       stop a single worker while you edit its config,
                       then `ops load <name>` to bring it back)
  ops restart <name>   kickstart one service (dispatcher|recorder|archiver)
  ops logrotate        copytruncate-rotate oversized worker logs (gzip history)

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
from .logrotate import DEFAULT_KEEP, DEFAULT_MAX_BYTES, rotate_logs

LAUNCH_AGENTS = Path("~/Library/LaunchAgents").expanduser()
LOG_DIR = Path("~/.local/log").expanduser()

# Calendar job (not a daemon): rotates the workers' launchd-captured logs
# daily so history survives reboots/truncation. Installed/removed alongside
# the three service plists but never health-checked — it has no liveness.
LOGROTATE_LABEL = "com.duy.logrotate"

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


def _watch_frame() -> str:
    """One repaint of the watch screen, as a SINGLE string written atomically.

    Flicker-free redraw: never blank the screen (the old `\\033[2J` left a blank
    frame between clear and repaint — the visible blink). Instead home the cursor
    and overwrite in place. Each line ends with `\\033[K` (erase-to-end-of-line)
    so a shorter new line leaves no stale tail; a trailing `\\033[J`
    (erase-to-end-of-screen) drops orphaned rows when a frame is shorter than the
    last. Written in one shot so the terminal repaints in a single pass."""
    body = render()   # render() draws its own header + live clock
    return "\033[H" + "\033[K\n".join(body.split("\n")) + "\033[K\033[J"


def cmd_watch(args: argparse.Namespace) -> int:
    out = sys.stdout
    # Alternate screen buffer (like htop/less): watch gets its own screen, so an
    # oversized report can't smear and the user's scrollback is restored on exit.
    # Cursor hidden during the loop to kill its blink too.
    out.write("\033[?1049h\033[?25l")
    out.flush()
    try:
        while True:
            out.write(_watch_frame())
            out.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0
    finally:
        out.write("\033[?25h\033[?1049l")  # restore cursor + leave alt screen
        out.flush()


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


def _logrotate_plist_xml(ops_bin: str) -> str:
    """Daily-04:05 calendar job running `ops logrotate`. launchd runs missed
    intervals on wake, so a sleeping laptop still rotates once a day."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" \
"http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{LOGROTATE_LABEL}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{ops_bin}</string>
        <string>logrotate</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>4</integer>
        <key>Minute</key>
        <integer>5</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>{LOG_DIR / 'logrotate.out.log'}</string>
    <key>StandardErrorPath</key>
    <string>{LOG_DIR / 'logrotate.err.log'}</string>
    <key>ProcessType</key>
    <string>Background</string>
</dict>
</plist>
"""


def cmd_logrotate(args: argparse.Namespace) -> int:
    actions = rotate_logs(
        LOG_DIR,
        max_bytes=int(args.max_mb * 1024 * 1024),
        keep=args.keep,
    )
    for a in actions:
        print(a)
    if not actions:
        print("logrotate: nothing over threshold")
    return 1 if any(a.startswith("ERROR") for a in actions) else 0


def cmd_install(_args: argparse.Namespace) -> int:
    """Write the launchd plists into ~/Library/LaunchAgents, generated
    for this machine: the three services + the daily logrotate calendar job.
    Idempotent — re-running overwrites with fresh paths.
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
    ops_bin = _resolve_bin("ops")
    if ops_bin is None:
        print("logrotate: 'ops' not found on PATH — plist skipped")
        rc = 1
    else:
        path = _plist_path(LOGROTATE_LABEL)
        path.write_text(_logrotate_plist_xml(ops_bin))
        print(f"logrotate: wrote {path}  →  {ops_bin} logrotate (daily 04:05)")
    if rc == 0:
        print("installed. Now run:  ops load")
    return rc


def _all_jobs() -> list[tuple[str, str]]:
    """(display name, launchd label) for every plist ops manages: the three
    services plus the logrotate calendar job."""
    return [*LABELS.items(), ("logrotate", LOGROTATE_LABEL)]


def _selected_jobs(args: argparse.Namespace) -> list[tuple[str, str]]:
    """Jobs a load/unload acts on: the single named one, or all of them.
    The name is validated by argparse choices, so no not-found case here."""
    name = getattr(args, "service", None)
    if name:
        return [(n, l) for n, l in _all_jobs() if n == name]
    return _all_jobs()


def cmd_uninstall(args: argparse.Namespace) -> int:
    """Unload (if loaded) and remove every ops-managed plist."""
    cmd_unload(args)
    for name, label in _all_jobs():
        path = _plist_path(label)
        if path.exists():
            path.unlink()
            print(f"{name}: removed {path}")
    return 0


def cmd_load(args: argparse.Namespace) -> int:
    rc = 0
    for name, label in _selected_jobs(args):
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


def cmd_unload(args: argparse.Namespace) -> int:
    rc = 0
    for name, label in _selected_jobs(args):
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
    job_names = [*LABELS, "logrotate"]
    ld = sub.add_parser("load", help="launchctl load managed plists "
                                     "(all, or just one)")
    ld.add_argument("service", nargs="?", choices=job_names,
                    help="load only this job (default: all)")
    ul = sub.add_parser("unload", help="launchctl unload managed plists "
                                       "(all, or just one — e.g. to edit "
                                       "its config while it's stopped)")
    ul.add_argument("service", nargs="?", choices=job_names,
                    help="unload only this job (default: all)")
    r = sub.add_parser("restart", help="restart one service")
    r.add_argument("service", choices=list(LABELS))
    lr = sub.add_parser(
        "logrotate",
        help="copytruncate-rotate oversized ~/.local/log/*.log (gzip history)")
    lr.add_argument("--max-mb", type=float,
                    default=DEFAULT_MAX_BYTES / (1024 * 1024),
                    help="rotate logs larger than this many MiB")
    lr.add_argument("--keep", type=int, default=DEFAULT_KEEP,
                    help="compressed generations to keep per log")
    return p


_DISPATCH = {
    "install":   cmd_install,
    "uninstall": cmd_uninstall,
    "health":    cmd_health,
    "watch":     cmd_watch,
    "load":      cmd_load,
    "unload":    cmd_unload,
    "restart":   cmd_restart,
    "logrotate": cmd_logrotate,
}


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return _DISPATCH[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
