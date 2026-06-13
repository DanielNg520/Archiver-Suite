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

# Single source of truth: one DB for the whole suite. ops still imports no
# *service* package (dispatcher/recorder/archiver) — it only borrows the
# canonical DB path from the shared `core` library so the location isn't
# duplicated here and can't drift from what the services actually write.
SUITE_DB      = _core_db_path()
RECORDER_PID  = Path("~/.recorder/pid").expanduser()
TIKTOK_LOCK   = Path("~/.config/archiver-suite/locks/tiktok.lock").expanduser()
# Upload-progress heartbeat written by the dispatcher's send strategy
# (dispatcher/progress.py). Read as a plain artifact — same staleness rules,
# duplicated here on purpose so ops keeps importing no worker package.
PROGRESS_FILE = Path("~/.config/dispatcher/progress.json").expanduser()
PROGRESS_STALE_S = 30.0

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


def upload_progress() -> str | None:
    """One-line live-upload description from the dispatcher's heartbeat file,
    or None when idle/stale/writer-dead."""
    import json
    import os as _os
    try:
        p = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(p, dict) or "sent" not in p or "total" not in p:
        return None
    if time.time() - p.get("updated_at", 0) > PROGRESS_STALE_S:
        return None
    try:
        _os.kill(int(p["pid"]), 0)
    except (OSError, ValueError):
        return None
    sent, total = p["sent"], p["total"]
    pct = (100 * sent / total) if total else 0
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
    return f"{Path(p['file']).name}{batch}  {pct:.0f}% ({', '.join(bits)})"


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
    most recent item's file_path — ops deliberately reads no worker config."""
    conn = _connect_ro(SUITE_DB)
    if conn is None:
        return None
    try:
        row = conn.execute(
            "SELECT file_path FROM items ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        parent = Path(row["file_path"]).parent
        return str(parent) if parent.exists() else None
    except sqlite3.Error:
        return None
    finally:
        conn.close()


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


def _disk_free(path: str = "/") -> str:
    try:
        usage = shutil.disk_usage(path)
        return f"{usage.free / 1_000_000_000:.0f}GB free"
    except OSError:
        return "unknown"


# ── report ──────────────────────────────────────────────────────────────────

def _head(name: str, pid: int | None, owner: str) -> str:
    """First line of a worker section: liveness + process vitals."""
    label = f"{name}:".ljust(11)
    if not pid:
        return f"{label} NOT running"
    stats = proc_stats(pid)
    vitals = f", {stats}" if stats else ""
    return f"{label} running ({owner} pid {pid}{vitals})"


def render() -> str:
    lines: list[str] = []

    # ── dispatcher ──
    pid, owner = worker_pid("dispatcher")
    lines.append(_head("dispatcher", pid, owner))
    counts = dispatcher_queue_counts()
    if counts is not None:
        lines.append(
            f"  queue:     {counts.get('pending', 0)} pending / "
            f"{counts.get('sending', 0)} sending / "
            f"{counts.get('sent', 0)} sent / "
            f"{counts.get('failed', 0)} failed")
    qh = queue_health()
    dd = dispatcher_details()
    if dd is not None:
        last = qh.get("last_sent") if qh else None
        lines.append(
            f"  activity:  last send {_humanize_age(last) if last else 'never'}; "
            f"{dd['sent_1h']} sent in last hour, {dd['sent_24h']} in 24h")
        up = upload_progress()
        if up:
            lines.append(f"  uploading: {up}")
        if dd["inflight"]:
            # Rows in 'sending' = the batch actually uploading PLUS any
            # claims stranded by a killed predecessor (watchdog rescues
            # those within ~15 min) — so describe, don't presume one album.
            head = dd["inflight"][0]
            n = len(dd["inflight"])
            what = (f"{n} item(s)" if n > 1 else
                    Path(head["file_path"]).name)
            lines.append(
                f"  in-flight: {what}, {_human_bytes(dd['inflight_bytes'])}, "
                f"oldest claim {_humanize_age(head['claimed_at'])} "
                f"({head['platform']}/@{head['username']})")
        if dd["last_fail"]:
            lines.append(f"  last fail: {dd['last_fail'][:90]}")

    # ── recorder ──
    pid, owner = worker_pid("recorder")
    lines.append(_head("recorder", pid, owner))
    held = TIKTOK_LOCK.exists()
    rd = recorder_details()
    if rd is not None:
        last = rd["last_enqueue"]
        lines.append(
            f"  activity:  last live enqueue "
            f"{_humanize_age(last) if last else 'never'}; "
            f"{rd['enqueued_24h']} recording(s) in last 24h")
    lines.append(
        f"  tiktok.lock: {'HELD (recording now)' if held else 'not held'}")

    # ── archiver ──
    pid, owner = worker_pid("archiver")
    lines.append(_head("archiver", pid, owner))
    lr = archiver_last_run()
    ad = archiver_details()
    if ad is not None:
        lines.append(
            f"  activity:  last checkpoint "
            f"{_humanize_age(lr) if lr else 'never'}; "
            f"{ad['discovered_24h']} item(s) discovered in last 24h")
        if ad["top_pending"]:
            tp = ", ".join(f"{p} {n}" for p, n in ad["top_pending"])
            lines.append(f"  backlog:   {tp}")

    # ── cross-cutting early warnings ──
    if qh:
        warn: list[str] = []
        if qh["oldest_pending"]:
            warn.append(f"oldest pending {_humanize_age(qh['oldest_pending'])}")
        if qh["oldest_sending"]:
            warn.append(f"oldest in-flight claim "
                        f"{_humanize_age(qh['oldest_sending'])}")
        if qh["null_hash"]:
            warn.append(f"{qh['null_hash']} row(s) without content_hash "
                        f"(outside the dedup guarantee — run `archiver "
                        f"backfill` or the next `archiver run` auto-fills)")
        if warn:
            lines.append("queue:      " + "; ".join(warn))

    vol = archive_volume_path()
    if vol:
        lines.append(f"disk:       {_disk_free(vol)} (archive volume)")
    else:
        lines.append(f"disk:       {_disk_free()}")

    return "\n".join(lines)
