"""
recorder.watch
──────────────
A live, auto-refreshing dashboard for `recorder watch` — the same clear-screen
+ re-render loop as `ops watch`, but focused on the recorder: is it up, is it
recording right now (and whom, growing how fast), the priority roster, and the
recorder's slice of the upload queue.

Split into two halves so the formatting is testable without a live system:
  • snapshot(config) → Snapshot   reads pid / lock / filesystem / DB (defensive;
                                   every probe degrades to "unknown" on error).
  • render(snapshot, rate) → str  pure formatting, no I/O.

The CLI loop (cli.cmd_watch) diffs successive snapshots to derive the live
write-rate of the active recording.
"""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from core import heartbeat

from . import ui
from .config import RecorderConfig

# Recording containers yt-dlp may leave on disk (matches state._VIDEO_SUFFIXES).
_VIDEO_SUFFIXES = frozenset({".mp4", ".ts", ".mkv", ".webm", ".flv", ".m4v"})
# A file counts as "being recorded now" if touched within this window.
_ACTIVE_WINDOW_S = 20.0


@dataclass
class Active:
    user: str
    size: int
    path: str


@dataclass
class Snapshot:
    running: bool = False
    pid: int | None = None
    lock_held: bool = False
    lock_since: str | None = None
    active: Active | None = None
    roster: tuple[str, ...] = ()
    pending: int = 0
    sending: int = 0
    sent_24h: int = 0
    failed: int = 0
    last_enqueue: str | None = None
    db_ok: bool = True


# ── probes (each defensive; the watch must never crash) ─────────────────────

def _pid(config: RecorderConfig) -> int | None:
    p = Path(config.state_dir).expanduser() / "pid"
    try:
        pid = int(p.read_text().strip())
    except (OSError, ValueError):
        return None
    return pid if heartbeat.pid_alive(pid) else None


def _lock(config: RecorderConfig) -> tuple[bool, str | None]:
    path = Path(config.lock_path).expanduser()
    if not path.exists():
        return False, None
    try:
        return True, json.loads(path.read_text()).get("started_at")
    except (OSError, ValueError):
        return True, None


def _active_recording(config: RecorderConfig) -> Active | None:
    """The newest video file under output_dir touched within the active window
    — i.e. the stream being written right now. Its parent dir is the username."""
    root = Path(config.output_dir).expanduser()
    cutoff = time.time() - _ACTIVE_WINDOW_S
    newest: tuple[float, Path] | None = None
    try:
        for user_dir in root.iterdir():
            if not user_dir.is_dir():
                continue
            for f in user_dir.iterdir():
                if f.suffix.lower() not in _VIDEO_SUFFIXES:
                    continue
                try:
                    st = f.stat()
                except OSError:
                    continue
                if st.st_mtime >= cutoff and (newest is None or st.st_mtime > newest[0]):
                    newest = (st.st_mtime, f)
    except OSError:
        return None
    if newest is None:
        return None
    f = newest[1]
    try:
        size = f.stat().st_size
    except OSError:
        size = 0
    return Active(user=f.parent.name, size=size, path=str(f))


def _db(config: RecorderConfig) -> dict:
    """Recorder-scoped queue counts via a read-only connection (never blocks a
    busy writer). Returns zeros + db_ok=False if the DB can't be read."""
    out = {"pending": 0, "sending": 0, "sent_24h": 0, "failed": 0,
           "last_enqueue": None, "db_ok": False}
    try:
        conn = sqlite3.connect(f"file:{config.db_path}?mode=ro", uri=True, timeout=2)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return out
    try:
        r = conn.execute(
            """SELECT
                 SUM(status='pending')                              AS pending,
                 SUM(status='sending')                              AS sending,
                 SUM(status='failed')                               AS failed,
                 SUM(status='sent' AND discovered_at >=
                     datetime('now','-1 day'))                      AS sent_24h,
                 MAX(discovered_at)                                 AS last_enq
               FROM items WHERE source='recorder'""").fetchone()
        out.update(pending=r["pending"] or 0, sending=r["sending"] or 0,
                   failed=r["failed"] or 0, sent_24h=r["sent_24h"] or 0,
                   last_enqueue=r["last_enq"], db_ok=True)
    except sqlite3.Error:
        pass
    finally:
        conn.close()
    return out


def snapshot(config: RecorderConfig) -> Snapshot:
    pid = _pid(config)
    held, since = _lock(config)
    db = _db(config)
    return Snapshot(
        running=pid is not None, pid=pid,
        lock_held=held, lock_since=since,
        active=_active_recording(config),
        roster=tuple(config.tiktok_users),
        pending=db["pending"], sending=db["sending"],
        sent_24h=db["sent_24h"], failed=db["failed"],
        last_enqueue=db["last_enqueue"], db_ok=db["db_ok"],
    )


# ── render (pure) ───────────────────────────────────────────────────────────

_RULE = "━" * 58


def _line(label: str, value: str, *, accent: str | None = None) -> str:
    on = ui.color_enabled()
    lbl = ui._paint(f"{label:<10}", "dim", on=on)
    val = ui._paint(value, accent, on=on) if accent else value
    return f"  {lbl} {val}"


def render(snap: Snapshot, *, rate_bps: float | None = None) -> str:
    on = ui.color_enabled()
    lines = [ui._paint(_RULE, "dim", on=on),
             f"  {ui._paint('recorder', 'bold', on=on)}"
             f"{ui._paint(' · tiktok live', 'dim', on=on)}"]

    if snap.running:
        lines.append(_line("status", f"running · pid {snap.pid}", accent="green"))
    else:
        lines.append(_line("status", "not running", accent="yellow"))

    if snap.active:
        rate = f" · +{ui.human_size(int(rate_bps))}/s" if rate_bps else ""
        lines.append(_line(
            "recording",
            f"● @{snap.active.user} · {ui.human_size(snap.active.size)}{rate}",
            accent="green"))
    elif snap.lock_held:
        # Lock held but no growing file yet (just started / between writes).
        lines.append(_line("recording", "● starting…", accent="green"))
    else:
        lines.append(_line("recording", "idle — listening", accent="dim"))

    if snap.lock_held and snap.lock_since:
        lines.append(_line("lock", f"held since {ui._short_time(snap.lock_since)}"))

    roster = "  ".join(f"@{u}" for u in snap.roster[:8])
    extra = f"  +{len(snap.roster) - 8} more" if len(snap.roster) > 8 else ""
    lines.append(_line("users", f"{roster}{extra}  ({len(snap.roster)})"))

    if snap.db_ok:
        q = (f"{snap.pending} pending · {snap.sent_24h} sent (24h)"
             f" · {snap.failed} failed")
        lines.append(_line("queue", q,
                           accent="yellow" if snap.failed else None))
        lines.append(_line("last grab", ui._age(snap.last_enqueue)))
    else:
        lines.append(_line("queue", "db unavailable", accent="yellow"))

    lines.append(ui._paint(_RULE, "dim", on=on))
    return "\n".join(lines)
