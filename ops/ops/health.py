"""
ops.health
──────────
Standalone health check for the three-process system. Imports NOTHING
from dispatcher / recorder / archiver — it only reads their on-disk
artifacts and OS process state. This keeps
ops installable and runnable even if one of the services is broken or
uninstalled.

Liveness source of truth is launchd when a job is managed there. For manual
foreground workers, fall back to the process table. Self-written pid files are
not used for liveness because they go stale on hard kills.
"""

from __future__ import annotations

import os
import re
import shutil
import shlex
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from core import db_path as _core_db_path
except ModuleNotFoundError:
    _this_dir = Path(__file__).resolve()
    _repo_root = _this_dir.parents[2]
    _core_pkg = _repo_root / "core"
    if _core_pkg.is_dir():
        sys.path.insert(0, str(_core_pkg))
    from core import db_path as _core_db_path

# core is now importable (installed, or path-injected above). The shared
# heartbeat reader gives ops the SAME liveness/staleness semantics the workers
# write with, so the rules can't drift between writer and monitor. Still no
# *worker* package imported — core is the shared library.
from core import heartbeat as _heartbeat
from core import paths as _paths

# Single source of truth: one DB for the whole suite. ops still imports no
# *service* package (dispatcher/recorder/archiver) — it only borrows the
# canonical DB path from the shared `core` library so the location isn't
# duplicated here and can't drift from what the services actually write.
# Cross-process artifact locations come from core.paths (the single source the
# WORKERS write to), so the monitor can't drift from the writers. ops still
# imports no *worker* package — core is the shared library.
SUITE_DB      = _core_db_path()
RECORDER_PID  = _paths.recorder_pid()
TIKTOK_LOCK   = _paths.tiktok_lock()
# Upload-progress heartbeat written by the dispatcher's send strategy
# (dispatcher/progress.py), read with the same staleness rules via core.heartbeat.
PROGRESS_FILE = _paths.dispatcher_progress()
PROGRESS_STALE_S = 30.0
# Phase heartbeat written by `archiver loop` (archiver/loop_state.py): tells us
# whether the archiver is mid-scan or resting between cycles. No staleness window
# (a rest phase legitimately lasts hours); the writer-pid liveness check is what
# guards against a stale file.
ARCHIVER_LOOP_FILE = _paths.archiver_loop()

LABELS = {
    "dispatcher": "com.duy.dispatcher",
    "recorder":   "com.duy.recorder",
    "archiver":   "com.duy.archiver",
}


# ── launchd liveness ──────────────────────────────────────────────────────

def launchctl_pid(label: str) -> int | None:
    """Return the managed pid if the job is loaded AND running, else None.
    `launchctl list <label>` prints a plist-ish block with a "PID" key
    only while the process is actually alive."""
    try:
        out = subprocess.run(
            ["launchctl", "list", label],
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None  # not loaded
    for line in out.stdout.splitlines():
        s = line.strip()
        if s.startswith('"PID"'):
            # format: "PID" = 1234;
            digits = "".join(c for c in s if c.isdigit())
            return int(digits) if digits else None
    return None


def foreground_pid(command: str, action: str) -> int | None:
    """Find a manually-started worker without matching shell snippets."""
    try:
        out = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if out.returncode != 0:
        return None
    for line in out.stdout.splitlines():
        fields = line.strip().split(maxsplit=1)
        if len(fields) != 2:
            continue
        try:
            pid = int(fields[0])
            argv = shlex.split(fields[1])
        except (ValueError, IndexError):
            continue
        for index, token in enumerate(argv[:-1]):
            if Path(token).name == command and argv[index + 1] == action:
                return pid
    return None


def worker_pid(name: str) -> tuple[int | None, str]:
    """Return a worker PID and whether launchd or a shell owns it."""
    managed = launchctl_pid(LABELS[name])
    if managed is not None:
        return managed, "launchd"
    action = "loop" if name == "archiver" else "start"
    return foreground_pid(name, action), "foreground"


def proc_stats(pid: int) -> str | None:
    """'up 1:10:15, cpu 10.6%, mem 110MB' from ps, or None if it vanished."""
    try:
        out = subprocess.run(
            ["ps", "-p", str(pid), "-o", "etime=,%cpu=,rss="],
            capture_output=True, text=True, timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    fields = out.stdout.split()
    if out.returncode != 0 or len(fields) != 3:
        return None
    etime, pcpu, rss_kb = fields
    try:
        mem_mb = int(rss_kb) // 1024
    except ValueError:
        return None
    return f"up {etime}, cpu {pcpu}%, mem {mem_mb}MB"


# ── DB queries (read-only) ─────────────────────────────────────────────────

def _iso_utc_ago(hours: float) -> str:
    """Cutoff timestamp in the exact ISO-8601-Z format the workers store
    (string comparison only works when both sides share one format)."""
    from datetime import timedelta
    dt = datetime.now(timezone.utc) - timedelta(hours=hours)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def dispatcher_details() -> dict | None:
    """What the drain is DOING, not just that it exists: recent throughput,
    the in-flight claim (who/how big/how long — a multi-GB album legitimately
    holds 'sending' for an hour), and the newest failure."""
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        sent_1h = conn.execute(
            "SELECT COUNT(*) AS n FROM items WHERE status='sent' AND sent_at > ?",
            (_iso_utc_ago(1),)).fetchone()["n"]
        sent_24h = conn.execute(
            "SELECT COUNT(*) AS n FROM items WHERE status='sent' AND sent_at > ?",
            (_iso_utc_ago(24),)).fetchone()["n"]
        inflight = conn.execute(
            "SELECT username, platform, source, claimed_at, file_path "
            "FROM items WHERE status='sending' ORDER BY claimed_at",
        ).fetchall()
        in_bytes = 0
        for r in inflight:
            try:
                in_bytes += Path(r["file_path"]).stat().st_size
            except OSError:
                pass
        last_fail = conn.execute(
            "SELECT last_error FROM items WHERE status='failed' "
            "ORDER BY id DESC LIMIT 1").fetchone()
        return {
            "sent_1h":  sent_1h,
            "sent_24h": sent_24h,
            "inflight": [dict(r) for r in inflight],
            "inflight_bytes": in_bytes,
            "last_fail": last_fail["last_error"] if last_fail else None,
        }
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def recorder_details() -> dict | None:
    """Recorder activity via its rows in the shared table (ops deliberately
    has no IPC with the recorder)."""
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        last = conn.execute(
            "SELECT MAX(discovered_at) AS m FROM items WHERE source='recorder'"
        ).fetchone()["m"]
        n24 = conn.execute(
            "SELECT COUNT(*) AS n FROM items WHERE source='recorder' "
            "AND discovered_at > ?", (_iso_utc_ago(24),)).fetchone()["n"]
        return {"last_enqueue": last, "enqueued_24h": n24}
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def tiktok_lock_info() -> dict | None:
    """Parse the TikTok recording lockfile as a plain artifact: it names who is
    being recorded right now and since when. The in-progress recording isn't in
    the items table until it finishes, so this lockfile is the ONLY live source
    of the current user. None when the file is absent or unreadable; an older
    recorder's lock may lack the 'username' key (handled by the caller)."""
    import json
    try:
        data = json.loads(TIKTOK_LOCK.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def archiver_details() -> dict | None:
    """Archiver activity: discoveries landing in the table + busiest
    pending platforms (where the backlog actually sits)."""
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        n24 = conn.execute(
            "SELECT COUNT(*) AS n FROM items WHERE source='archiver' "
            "AND discovered_at > ?", (_iso_utc_ago(24),)).fetchone()["n"]
        top = conn.execute(
            "SELECT platform, COUNT(*) AS n FROM items "
            "WHERE status='pending' GROUP BY platform ORDER BY n DESC LIMIT 3",
        ).fetchall()
        return {
            "discovered_24h": n24,
            "top_pending": [(r["platform"], r["n"]) for r in top],
        }
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def upload_progress_fields() -> dict | None:
    """Structured live-upload state from the dispatcher's heartbeat file, or None
    when idle/stale/writer-dead. Returns name (+ batch tag), the 0..1 fraction
    complete, and a human transfer detail (bytes · rate · ETA) — the renderer
    turns the fraction into a progress bar."""
    p = _heartbeat.read_live(
        PROGRESS_FILE, stale_after_s=PROGRESS_STALE_S,
        validate=lambda d: "sent" in d and "total" in d,
    )
    if p is None:
        return None
    sent, total = p["sent"], p["total"]
    frac = (sent / total) if total else 0.0
    bits = [f"{_human_bytes(sent)}/{_human_bytes(total)}"]
    elapsed = p.get("updated_at", 0) - p.get("started_at", 0)
    if elapsed >= 1.0 and sent > 0:
        rate = sent / elapsed
        bits.append(f"{_human_bytes(rate)}/s")
        if rate > 0 and total >= sent:
            eta = int((total - sent) / rate)
            bits.append(f"ETA {eta // 60}m{eta % 60:02d}s" if eta >= 60
                        else f"ETA {eta}s")
    batch = ""
    if p.get("batch_total") and p["batch_total"] > 1:
        batch = (f" [file {p['batch_pos']}/{p['batch_total']}]"
                 if p.get("batch_pos") else f" [album of {p['batch_total']}]")
    return {"name": Path(p["file"]).name + batch,
            "frac": frac, "detail": " · ".join(bits)}


def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f}{unit}" if unit != "B" else f"{n}B"
        n /= 1024
    return f"{n}B"

def _connect_ro(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        return None
    try:
        # mode=ro (NOT immutable=1 — the DB has live writers, and immutable
        # would skip the WAL and read torn state): WAL lets this reader run
        # concurrently with the workers' writes.
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=3.0)
        conn.row_factory = sqlite3.Row
        return conn
    except sqlite3.Error:
        return None


def dispatcher_queue_counts() -> dict[str, int] | None:
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM items GROUP BY status"
        ).fetchall()
        return {r["status"]: r["n"] for r in rows}
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def queue_health() -> dict | None:
    """Early-warning signals the counts alone don't show: how long the oldest
    pending row has waited, whether anything is wedged in 'sending', and how
    many rows are invisible to the dedup guarantee (NULL content_hash). All
    read-only, all single indexed queries."""
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        oldest_pending = conn.execute(
            "SELECT MIN(discovered_at) AS m FROM items WHERE status='pending'"
        ).fetchone()["m"]
        oldest_sending = conn.execute(
            "SELECT MIN(claimed_at) AS m FROM items WHERE status='sending'"
        ).fetchone()["m"]
        null_hash = conn.execute(
            "SELECT COUNT(*) AS n FROM items WHERE content_hash IS NULL"
        ).fetchone()["n"]
        # The single best stall signal: a healthy drain keeps this fresh
        # while queue counts look identical whether draining or wedged
        # (2026-06-12: a silent TCP stall froze uploads all night — counts
        # alone showed nothing).
        last_sent = conn.execute(
            "SELECT MAX(sent_at) AS m FROM items WHERE status='sent'"
        ).fetchone()["m"]
        return {
            "oldest_pending": oldest_pending,
            "oldest_sending": oldest_sending,
            "null_hash": null_hash,
            "last_sent": last_sent,
        }
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def archive_volume_path() -> str | None:
    """A path ON the archive volume, so disk-free is measured where the files
    actually live (often an external drive) instead of on /. Derived from the
    file_path of recent items — ops deliberately reads no worker config.

    Walk back through recent rows rather than trusting the single newest one:
    recordings stage under records/<user>/ and that dir is cleaned up after
    the converted upload, so the newest row often points at a vanished path.
    Skip those and land on the first parent that still exists."""
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        for row in conn.execute(
            "SELECT file_path FROM items ORDER BY id DESC LIMIT 50"
        ):
            if not row["file_path"]:
                continue
            parent = Path(row["file_path"]).parent
            if parent.exists():
                return str(parent)
        return None
    except sqlite3.Error:
        return None
    finally:
        conn.close()


def archiver_loop_phase() -> dict | None:
    """The loop's current phase ('running' a scan / 'sleeping' between loops),
    or None when the file is absent, malformed, or its writer pid is gone
    (a foreground `archiver run`, or an old loop that predates this file, has
    no heartbeat — the caller falls back to plain liveness). Mirrors the
    progress-file pattern: ops reads the artifact directly, importing nothing
    from the archiver."""
    return _heartbeat.read_live(
        ARCHIVER_LOOP_FILE,
        validate=lambda d: d.get("phase") in ("running", "sleeping"),
    )


def archiver_last_run() -> str | None:
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT MAX(last_run_utc) AS m FROM checkpoints"
        ).fetchone()
        return row["m"] if row and row["m"] else None
    except sqlite3.Error:
        return None
    finally:
        conn.close()


# ── helpers ────────────────────────────────────────────────────────────────

def _humanize_age(iso_ts: str) -> str:
    """'12m ago' / '3h ago' from an ISO timestamp. Tolerates Z suffix and
    offset-naive strings."""
    try:
        ts = iso_ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        secs = (datetime.now(timezone.utc) - dt).total_seconds()
    except (ValueError, TypeError):
        return "unknown"
    if secs < 90:
        return f"{int(secs)}s ago"
    if secs < 5400:
        return f"{int(secs // 60)}m ago"
    return f"{int(secs // 3600)}h ago"


def _humanize_age_epoch(epoch: float) -> str:
    """'12m ago' / '3h ago' from a past epoch timestamp — the epoch-input twin
    of _humanize_age (which parses ISO strings from the DB)."""
    secs = time.time() - epoch
    if secs < 90:
        return f"{int(secs)}s ago"
    if secs < 5400:
        return f"{int(secs // 60)}m ago"
    return f"{int(secs // 3600)}h ago"


def _humanize_until(epoch: float) -> str:
    """'in 47m' / 'in 1h' from a future epoch timestamp; 'due now' once past.
    The countdown twin of _humanize_age, for the loop's next-run time."""
    secs = epoch - time.time()
    if secs <= 0:
        return "due now"
    if secs < 5400:
        return f"in {max(1, int(secs // 60))}m"
    return f"in {int(secs // 3600)}h"


def _disk_fields(path: str = "/") -> tuple[str, float] | None:
    """(free-space label, fraction-used 0..1) for the volume at `path`, or None
    if it can't be stat'd. The fraction drives the disk gauge."""
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return None
    frac_used = (usage.total - usage.free) / usage.total if usage.total else 0.0
    return f"{usage.free / 1_000_000_000:.0f}GB free", frac_used


# ── presentation: color + bars ───────────────────────────────────────────────
# A small ANSI toolkit. Color is enabled ONLY on a real terminal (and honours
# NO_COLOR / TERM=dumb), so `ops health` piped to a log stays clean plain text
# while `ops watch` and an interactive `ops health` get the full palette. The
# box-drawing glyphs and bars are plain Unicode, so they survive even uncoloured.

_USE_COLOR = (
    sys.stdout.isatty()
    and os.environ.get("TERM", "") not in ("", "dumb")
    and not os.environ.get("NO_COLOR")
)
_ANSI_RE = re.compile(r"\033\[[0-9;]*m")

# 256-colour palette (widely supported; degrades to plain when color is off).
_TITLE, _RULE, _LABEL, _DIM = 81, 238, 245, 240
_TEXT, _WHITE = 252, 255
_GREEN, _AMBER, _RED, _CYAN, _BLUE = 78, 214, 203, 80, 75

_FILL, _TRACK = "█", "░"            # bar: filled cell / empty track
_PART = " ▏▎▍▌▋▊▉█"                 # 8 sub-cell steps for fractional precision


def _c(text: str, color: int, *, bold: bool = False) -> str:
    """Wrap `text` in a 256-colour SGR (and optional bold), or return it bare
    when colour is disabled."""
    if not _USE_COLOR:
        return text
    pre = ("\033[1m" if bold else "") + f"\033[38;5;{color}m"
    return f"{pre}{text}\033[0m"


def _vlen(s: str) -> int:
    """Visible width: string length minus the ANSI escapes (every glyph used
    here is single-width, so len-after-strip is the column count)."""
    return len(_ANSI_RE.sub("", s))


def _pad(s: str, width: int) -> str:
    return s + " " * max(0, width - _vlen(s))


def _bar(frac: float, width: int, color: int) -> str:
    """A horizontal gauge `width` cells wide, filled to `frac` (0..1) with
    sub-cell resolution: the boundary cell uses one of eight partial blocks, so
    62% of a 20-cell bar lands precisely instead of snapping to a whole cell."""
    frac = max(0.0, min(1.0, frac))
    exact = frac * width
    full = int(exact)
    filled = _FILL * full
    if full < width:
        step = int(round((exact - full) * 8))
        if step:
            filled += _PART[step]
    track = _TRACK * (width - _vlen(filled))
    return _c(filled, color) + _c(track, _DIM)


def _stacked_bar(segments: "list[tuple[int, int]]", width: int) -> str:
    """One bar split into proportional coloured runs — (count, colour) pairs —
    so the queue's pending/sending/sent/failed mix is legible at a glance.
    Largest-remainder apportionment fills exactly `width` cells; zero-count
    segments get none."""
    total = sum(c for c, _ in segments)
    if total <= 0:
        return _c(_TRACK * width, _DIM)
    raw = [(c / total) * width for c, _ in segments]
    alloc = [int(x) for x in raw]
    leftover = width - sum(alloc)
    # hand out remaining cells to the largest fractional parts (count>0 only)
    order = sorted((i for i, (c, _) in enumerate(segments) if c > 0),
                   key=lambda i: raw[i] - alloc[i], reverse=True)
    for i in (order * (leftover // len(order) + 1))[:leftover] if order else []:
        alloc[i] += 1
    return "".join(_c(_FILL * n, color)
                   for n, (_, color) in zip(alloc, segments) if n)


# ── report ──────────────────────────────────────────────────────────────────

# Total dashboard width, capped so it stays a tidy card on a wide terminal.
_WIDTH = 64
_BAR_W = 22
# Where a section's value column begins (after the rail + indent + label).
_RAILPAD = "     "          # rail glyph(1) + 4 spaces under it for body rows


def _gauge_color(frac_used: float) -> int:
    """Green plenty / amber tight / red critical — used for the disk gauge."""
    return _GREEN if frac_used < 0.8 else _AMBER if frac_used < 0.93 else _RED


def _section(name: str, pid: int | None, owner: str,
             rows: "list[str]") -> list[str]:
    """A worker block: a coloured status rail + header (●/name/state/vitals)
    followed by its already-formatted body rows, each carried on the rail."""
    color = _GREEN if pid else _RED
    rail = _c("▌", color)
    dot = _c("●", color)
    state = _c("running", _GREEN) if pid else _c("NOT running", _RED)
    header = f" {rail} {_c(_pad(name.upper(), 10), color, bold=True)} {dot} {state}"
    if pid:
        stats = proc_stats(pid)
        # proc_stats joins with ', '; unify on ' · ' so the whole vitals line
        # reads with one consistent separator.
        vit = f"{owner} · pid {pid}" + (
            " · " + stats.replace(", ", " · ") if stats else "")
        header += "  " + _c(vit, _DIM)
    body_rail = _c("│", color)
    out = [header]
    for r in rows:
        out.append(f" {body_rail} {r}")
    return out


def _row(label: str, value: str) -> str:
    """A `label   value` body row, label dimmed and column-aligned."""
    return f"   {_c(_pad(label, 9), _LABEL)} {value}"


def _cont(value: str) -> str:
    """A continuation row aligned under the value column (no label) — used to
    hang a progress bar beneath the line it belongs to."""
    return f"   {' ' * 9} {value}"


def render() -> str:
    cols = shutil.get_terminal_size((80, 24)).columns
    width = min(cols, _WIDTH)

    out: list[str] = []

    # ── header: title · live status · clock ──
    live = {n: worker_pid(n) for n in ("dispatcher", "recorder", "archiver")}
    down = [n for n, (pid, _) in live.items() if not pid]
    title = _c("ARCHIVER SUITE", _TITLE, bold=True) + "  " + _c("health", _LABEL)
    clock = _c(time.strftime("%H:%M:%S"), _WHITE)
    gap = max(2, width - _vlen(title) - _vlen(clock) - 1)
    out.append(" " + title + " " * gap + clock)
    if down:
        status = _c("●", _RED) + _c(f" {len(down)} down: {', '.join(down)}", _RED)
    else:
        status = _c("●", _GREEN) + _c(" all systems nominal", _GREEN)
    out.append(" " + status)
    out.append(_c("─" * width, _RULE))
    out.append("")

    qh = queue_health()

    # ── dispatcher ──
    pid, owner = live["dispatcher"]
    rows: list[str] = []
    counts = dispatcher_queue_counts()
    if counts is not None:
        pend = counts.get("pending", 0); send = counts.get("sending", 0)
        sent = counts.get("sent", 0); fail = counts.get("failed", 0)
        rows.append(_row("queue",
            _c(f"{pend:,}", _AMBER) + _c(" pending  ", _DIM)
            + _c(f"{send:,}", _CYAN) + _c(" sending  ", _DIM)
            + _c(f"{sent:,}", _GREEN) + _c(" sent  ", _DIM)
            + _c(f"{fail:,}", _RED if fail else _DIM) + _c(" failed", _DIM)))
        rows.append(_cont(_stacked_bar(
            [(pend, _AMBER), (send, _CYAN), (sent, _GREEN), (fail, _RED)],
            _BAR_W)))
    dd = dispatcher_details()
    if dd is not None:
        last = qh.get("last_sent") if qh else None
        rows.append(_row("activity",
            _c(f"last send {_humanize_age(last) if last else 'never'}", _TEXT)
            + _c(f"  ·  {dd['sent_1h']:,} in 1h  ·  {dd['sent_24h']:,} in 24h",
                 _DIM)))
        up = upload_progress_fields()
        if up:
            rows.append(_row("upload", _c(up["name"], _WHITE)))
            rows.append(_cont(
                _bar(up["frac"], _BAR_W, _CYAN)
                + _c(f" {up['frac'] * 100:.0f}%", _WHITE, bold=True)
                + _c(f"  {up['detail']}", _DIM)))
        if dd["inflight"]:
            # Rows in 'sending' = the batch actually uploading PLUS any
            # claims stranded by a killed predecessor (watchdog rescues
            # those within ~15 min) — so describe, don't presume one album.
            head = dd["inflight"][0]
            n = len(dd["inflight"])
            what = f"{n} items" if n > 1 else Path(head["file_path"]).name
            rows.append(_row("in-flight",
                _c(f"{what}, {_human_bytes(dd['inflight_bytes'])}", _TEXT)
                + _c(f"  ·  oldest claim {_humanize_age(head['claimed_at'])}"
                     f"  ·  {head['platform']}/@{head['username']}", _DIM)))
        if dd["last_fail"]:
            rows.append(_row("last fail", _c(dd["last_fail"][:80], _RED)))
    out += _section("dispatcher", pid, owner, rows)
    out.append("")

    # ── recorder ──
    pid, owner = live["recorder"]
    rows = []
    # Held only when a LIVE recorder owns it (same liveness gate the archiver
    # uses): a crashed recorder's stale lock reads as not-held, so ops doesn't
    # report a phantom recording and matches what the archiver actually does.
    held = _heartbeat.read_live(TIKTOK_LOCK) is not None
    info = tiktok_lock_info() if held else None
    rd = recorder_details()
    if rd is not None:
        last = rd["last_enqueue"]
        rows.append(_row("activity",
            _c(f"last enqueue {_humanize_age(last) if last else 'never'}", _TEXT)
            + _c(f"  ·  {rd['enqueued_24h']:,} in 24h", _DIM)))
    if held:
        # A capture is in flight — name the user (and how long) from the lock.
        user = (info or {}).get("username")
        who = _c(f"@{user}", _GREEN, bold=True) if user else _c("user unknown", _AMBER)
        line = _c("◉ recording ", _GREEN) + who
        since = (info or {}).get("started_at")
        if since:
            line += _c(f"  ·  started {_humanize_age(since)}", _DIM)
        rows.append(_row("tiktok", line))
    else:
        rows.append(_row("tiktok", _c("○ idle — listening for live streams", _DIM)))
    out += _section("recorder", pid, owner, rows)
    out.append("")

    # ── archiver ──
    pid, owner = live["archiver"]
    rows = []
    # Phase: is the loop mid-scan right now, or resting between cycles? The
    # header's running/NOT-running only proves the process exists — this row
    # says which of the two things an alive loop is actually doing.
    phase = archiver_loop_phase() if pid else None
    if phase:
        run_n = phase.get("run_n")
        tag = f"  ·  run #{run_n}" if run_n else ""
        if phase["phase"] == "running":
            plat, usr = phase.get("platform"), phase.get("user")
            if plat and usr:
                head = _c(f"◉ scanning {plat}/@{usr}", _GREEN)
                extra = tag
            elif plat:
                head = _c(f"◉ scanning {plat}", _GREEN)
                extra = tag
            else:
                # No target yet — between users, or the pre/post-scan phases
                # (reconcile / ingest / backfill). Show how long we've been here.
                since = phase.get("since")
                head = _c("◉ scanning", _GREEN)
                extra = tag + (f"  ·  {_humanize_age_epoch(since)}" if since else "")
            rows.append(_row("phase", head + _c(extra, _DIM)))
        else:
            wake = phase.get("wake_at")
            nxt = f"  ·  next run {_humanize_until(wake)}" if wake else ""
            rows.append(_row("phase",
                _c("○ resting between loops", _TEXT) + _c(tag + nxt, _DIM)))
    lr = archiver_last_run()
    ad = archiver_details()
    if ad is not None:
        rows.append(_row("activity",
            _c(f"last checkpoint {_humanize_age(lr) if lr else 'never'}", _TEXT)
            + _c(f"  ·  {ad['discovered_24h']:,} found in 24h", _DIM)))
        if ad["top_pending"]:
            tp = _c("  ·  ", _DIM).join(
                _c(f"{p} ", _TEXT) + _c(f"{n:,}", _BLUE)
                for p, n in ad["top_pending"])
            rows.append(_row("backlog", tp))
    out += _section("archiver", pid, owner, rows)
    out.append("")

    # ── system: cross-cutting warnings + disk ──
    out.append(f" {_c('▌', _BLUE)} {_c('SYSTEM', _BLUE, bold=True)}")
    srail = _c("│", _BLUE)
    if qh:
        warn: list[str] = []
        if qh["oldest_pending"]:
            warn.append(f"oldest pending {_humanize_age(qh['oldest_pending'])}")
        if qh["oldest_sending"]:
            warn.append(
                f"oldest in-flight claim {_humanize_age(qh['oldest_sending'])}")
        if qh["null_hash"]:
            warn.append(f"{qh['null_hash']:,} rows missing content_hash "
                        f"(run `archiver backfill`)")
        if warn:
            out.append(f" {srail} " + _row("warnings",
                _c("▲ ", _AMBER) + _c("  ·  ".join(warn), _AMBER)))

    vol = archive_volume_path()
    disk = _disk_fields(vol) if vol else _disk_fields()
    if disk is not None:
        free_lbl, used = disk
        where = "archive volume" if vol else "root volume"
        out.append(f" {srail} " + _row("disk",
            _bar(used, _BAR_W, _gauge_color(used))
            + _c(f" {free_lbl}", _WHITE) + _c(f"  ·  {where}", _DIM)))

    return "\n".join(out)
