"""
Self-test for recorder.state's reconnect-on-premature-exit loop.

Drives StateMachine._wait_for_recording_done with a scripted fake capture +
platform, asserting the safety contract:
  - a capture exit while still-live → relaunch on a fresh URL and keep recording
  - all segment files of one session are accumulated and handed off once
  - the download-lock is HELD across reconnects, released exactly once at the end
  - terminal exits (stop / dead-stream rc=-2) never reconnect
  - reconnect is bounded (consecutive zero-byte relaunches give up)

No network, no yt-dlp: real temp files stand in for segments so byte tallies and
output discovery behave like production.

Run: PYTHONPATH=core:recorder python3 -m recorder._selftest_reconnect
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from recorder.config import RecorderConfig          # noqa: E402
from recorder.state import StateMachine, RecorderState  # noqa: E402

_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    _checks += 1
    if not cond:
        print(f"✗ FAIL: {label}")
        raise SystemExit(1)
    print(f"✓ {label}")


class FakeCapture:
    """Scripted capture. `script[i]` = {"rc": int, "bytes": int} for the i-th
    start() (run 0 is the initial recording; 1+ are reconnects). Writes a real
    temp file per run that produced bytes so output_files()/size work."""

    def __init__(self, tmp: Path, script: list[dict]):
        self.tmp = tmp
        self.tmp.mkdir(parents=True, exist_ok=True)
        self.script = script
        self.run = -1
        self._files: dict[int, list[Path]] = {}
        self.starts = 0

    def start(self, url: str, username: str) -> None:
        self.run += 1
        self.starts += 1
        spec = self.script[self.run]
        files: list[Path] = []
        if spec.get("bytes", 0) > 0:
            p = self.tmp / f"{username}_{self.run}.ts"
            p.write_bytes(b"\0" * spec["bytes"])
            files.append(p)
        self._files[self.run] = files

    def wait(self, stop_event) -> int:
        return self.script[self.run]["rc"]

    def output_files(self) -> list[Path]:
        return list(self._files.get(self.run, []))

    def finalize(self) -> None:
        pass

    def elapsed_s(self) -> float:
        return 1.0


class FakePlatform:
    name = "tiktok"

    def __init__(self, live_samples: list[bool]):
        self._samples = list(live_samples)
        self.is_live_calls = 0
        self.stream_url_calls = 0

    def is_live(self, username: str) -> bool:
        self.is_live_calls += 1
        return self._samples.pop(0) if self._samples else False

    def stream_url(self, username: str) -> str:
        self.stream_url_calls += 1
        return f"https://fake/{username}/{self.stream_url_calls}.m3u8"


class FakeLock:
    def __init__(self):
        self.held = False
        self.enters = 0
        self.exits = 0

    def __enter__(self):
        self.held = True
        self.enters += 1
        return self

    def __exit__(self, *a):
        self.held = False
        self.exits += 1
        return False


def _sm(tmp: Path, capture: FakeCapture, platform: FakePlatform,
        lock: FakeLock) -> StateMachine:
    cfg = RecorderConfig(
        poll_interval_s=2.0, db_path="/x.db", output_dir=str(tmp),
        state_dir=str(tmp), lock_path=str(tmp / "l"),
        tiktok_users=("alice",), tiktok_cookies_file=None,
        # Fast + deterministic: one confirm sample, no sleeps.
        live_confirm_samples=1, live_confirm_interval_s=0.0,
        reconnect_backoff_base_s=0.0, max_zero_byte_reconnects=3)
    return StateMachine(cfg, platform, capture, lambda *a: None, lock)


def test_reconnect_while_live(tmp: Path) -> None:
    print("\n── still-live capture exit → reconnect, accumulate, one handoff ──")
    cap = FakeCapture(tmp, [
        {"rc": 0, "bytes": 1000},   # run 0: initial segment
        {"rc": 0, "bytes": 2000},   # run 1: after 1st reconnect
        {"rc": 0, "bytes": 3000},   # run 2: after 2nd reconnect
    ])
    plat = FakePlatform([True, True, False])   # live, live, then offline
    lock = FakeLock()
    sm = _sm(tmp, cap, plat, lock)

    sm._start_recording("alice")
    check(sm.state == RecorderState.RECORDING and lock.held,
          "initial start: recording + lock held")
    sm._wait_for_recording_done()

    check(cap.starts == 3, "two reconnects → three total capture starts")
    check(plat.stream_url_calls == 3, "each (re)start resolved a FRESH url")
    check(sm._upload_q.qsize() == 3, "all three segment files handed off once")
    check(not lock.held and lock.exits == 1,
          "lock released exactly once, after the session")
    check(sm.state == RecorderState.HANDOFF, "ends in HANDOFF")


def test_genuine_end_no_reconnect(tmp: Path) -> None:
    print("\n── offline after exit → finalize, no reconnect ──")
    cap = FakeCapture(tmp, [{"rc": 0, "bytes": 1000}])
    plat = FakePlatform([False])               # not live → genuine end
    lock = FakeLock()
    sm = _sm(tmp, cap, plat, lock)
    sm._start_recording("alice")
    sm._wait_for_recording_done()
    check(cap.starts == 1, "no reconnect when offline")
    check(sm._upload_q.qsize() == 1 and sm.state == RecorderState.HANDOFF,
          "single segment handed off; HANDOFF")
    check(not lock.held, "lock released")


def test_dead_stream_never_reconnects(tmp: Path) -> None:
    print("\n── rc=-2 (dead stream) is terminal even if is_live says True ──")
    cap = FakeCapture(tmp, [{"rc": -2, "bytes": 0}])
    plat = FakePlatform([True, True, True])    # would say live, but must be ignored
    lock = FakeLock()
    sm = _sm(tmp, cap, plat, lock)
    sm._start_recording("alice")
    sm._wait_for_recording_done()
    check(cap.starts == 1 and plat.is_live_calls == 0,
          "dead stream short-circuits before any liveness re-check")
    check(sm._upload_q.qsize() == 0, "nothing enqueued for a dead stream")


def test_stop_request_is_terminal(tmp: Path) -> None:
    print("\n── stop request (rc=-1) finalizes without reconnect ──")
    cap = FakeCapture(tmp, [{"rc": -1, "bytes": 500}])
    plat = FakePlatform([True])
    lock = FakeLock()
    sm = _sm(tmp, cap, plat, lock)
    sm._start_recording("alice")
    sm._stop.set()
    sm._wait_for_recording_done()
    check(cap.starts == 1, "no reconnect on stop")
    check(sm.state != RecorderState.HANDOFF,
          "stop does not advance to HANDOFF (clean shutdown)")


def test_zero_byte_budget(tmp: Path) -> None:
    print("\n── flapping live with no new data → bounded, then finalize ──")
    # Every reconnect returns rc=0 but zero bytes while is_live stays True.
    # Budget = max_zero_byte_reconnects (3) → give up after the streak exceeds it.
    script = [{"rc": 0, "bytes": 0} for _ in range(20)]
    cap = FakeCapture(tmp, script)
    plat = FakePlatform([True] * 20)
    lock = FakeLock()
    sm = _sm(tmp, cap, plat, lock)
    sm._start_recording("alice")
    sm._wait_for_recording_done()
    # budget=3 → 3 zero-byte reconnects allowed (streak 1,2,3), the 4th streak
    # exceeds it and we give up: run0 + 3 reconnects = 4 starts.
    check(cap.starts == 4,
          f"zero-byte reconnects bounded by budget=3 (got {cap.starts} starts)")
    check(not lock.held and sm.state == RecorderState.HANDOFF,
          "still finalizes cleanly after giving up")


def main() -> int:
    print("recorder.state reconnect self-test")
    with tempfile.TemporaryDirectory() as d:
        test_reconnect_while_live(Path(d) / "a")
        test_genuine_end_no_reconnect(Path(d) / "b")
        test_dead_stream_never_reconnects(Path(d) / "c")
        test_stop_request_is_terminal(Path(d) / "d")
        test_zero_byte_budget(Path(d) / "e")
    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
