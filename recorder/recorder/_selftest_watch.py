"""
Self-test for recorder.watch — the live dashboard's data + rendering.

Covers the pure, system-independent logic: active-recording detection on the
filesystem (newest growing video wins; stale/non-video ignored), defensive DB
handling, and that render() surfaces the right tokens for each state.

Run: PYTHONPATH=core:recorder python3 -m recorder._selftest_watch
"""

from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from recorder import ui, watch                                 # noqa: E402
from recorder.config import RecorderConfig                     # noqa: E402
from recorder.watch import Active, Snapshot                    # noqa: E402

_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    _checks += 1
    if not cond:
        print(f"✗ FAIL: {label}")
        raise SystemExit(1)
    print(f"✓ {label}")


def _cfg(tmp: Path, *, db: str = "/nonexistent/x.db") -> RecorderConfig:
    return RecorderConfig(
        poll_interval_s=2.0, db_path=db, output_dir=str(tmp / "rec"),
        state_dir=str(tmp / "state"), lock_path=str(tmp / "tiktok.lock"),
        tiktok_users=("alice", "bob"), tiktok_cookies_file=None)


def _touch(path: Path, *, age_s: float = 0.0, size: int = 1024) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\0" * size)
    if age_s:
        t = time.time() - age_s
        os.utime(path, (t, t))


def test_active_recording(tmp: Path) -> None:
    print("\n── active-recording detection on disk ──")
    cfg = _cfg(tmp)
    rec = Path(cfg.output_dir)
    check(watch._active_recording(cfg) is None,
          "no recordings dir / no files → None")

    _touch(rec / "alice" / "alice_100.mp4", age_s=0, size=4096)
    _touch(rec / "bob" / "bob_50.mp4", age_s=600)            # stale (10 min old)
    _touch(rec / "alice" / "alice_100_ytdlp.log", age_s=0)   # sidecar, not video
    act = watch._active_recording(cfg)
    check(act is not None and act.user == "alice"
          and act.path.endswith("alice_100.mp4"),
          "newest RECENT video wins; user is its parent dir")
    check(act.size == 4096, "reports the live file size")

    # Make bob's file freshly touched and bigger-mtime → it should take over.
    _touch(rec / "bob" / "bob_77.mp4", age_s=0, size=8192)
    act2 = watch._active_recording(cfg)
    check(act2.user == "bob", "the most-recently-touched recording is chosen")


def test_db_missing_is_defensive(tmp: Path) -> None:
    print("\n── DB probe degrades, never crashes ──")
    snap = watch.snapshot(_cfg(tmp, db="/nope/missing.db"))
    check(snap.db_ok is False and snap.pending == 0,
          "unreadable DB → db_ok=False, zeroed counts, no exception")
    check(snap.running is False and snap.lock_held is False,
          "absent pid/lock files read as not-running / not-held")


def test_render_states() -> None:
    print("\n── render surfaces the right tokens per state ──")
    ui.color_enabled = lambda: False        # strip colour for token assertions

    live = Snapshot(running=True, pid=42, lock_held=True,
                    lock_since="2026-06-16T09:00:00Z",
                    active=Active(user="cng", size=1_200_000_000, path="/r/x.mp4"),
                    roster=tuple(f"u{i}" for i in range(12)),
                    pending=3, sent_24h=11, failed=0,
                    last_enqueue="2026-06-16T09:00:00Z", db_ok=True)
    out = watch.render(live, rate_bps=18_000_000)
    check("running · pid 42" in out, "shows running + pid")
    check("@cng" in out and "1.2 GB" in out and "/s" in out,
          "shows active user, size, and live rate")
    check("+4 more" in out and "(12)" in out, "roster truncates with a count")
    check("3 pending" in out and "11 sent (24h)" in out, "queue line present")

    idle = Snapshot(running=True, pid=42, roster=("a",), db_ok=True)
    check("idle — listening" in watch.render(idle), "idle state rendered")

    down = Snapshot(running=False, roster=("a",), db_ok=False)
    o2 = watch.render(down)
    check("not running" in o2 and "db unavailable" in o2,
          "down state: not running + db unavailable")


def main() -> int:
    print("recorder.watch self-test")
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        test_active_recording(tmp)
        test_db_missing_is_defensive(tmp)
    test_render_states()
    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
