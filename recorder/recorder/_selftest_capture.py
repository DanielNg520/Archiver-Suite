"""
Self-test for the capture-termination + remux/handoff integrity path.

Regression cover for an observed production data-loss bug: yt-dlp runs the real
download via a CHILD ffmpeg (`--downloader ffmpeg`). Terminating only the yt-dlp
pid orphaned that ffmpeg, which kept the recording file open and writing; a later
remux then unlinked the source, draining live footage into a deleted inode (and
leaving the segment "No such file" at remux time). The fixes verified here:

  - StreamCapture launches each capture in its OWN process group and
    _terminate() signals the whole group, so no child survives (no orphan).
  - _remux_for_telegram never trades a present source for a missing/empty
    output, and treats a vanished/empty source as nothing-to-convert.
  - _enqueue_job reports a file that vanished before enqueue as lost instead of
    enqueuing a phantom row.

No network, no yt-dlp: a shell parent that forks a writer child stands in for the
yt-dlp→ffmpeg pair; real temp files stand in for segments.

Run: PYTHONPATH=core:recorder python3 -m recorder._selftest_capture
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import recorder.state as st                              # noqa: E402
from recorder.capture import StreamCapture               # noqa: E402

_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    _checks += 1
    if not cond:
        print(f"✗ FAIL: {label}")
        raise SystemExit(1)
    print(f"✓ {label}")


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def test_terminate_kills_whole_group(tmp: Path) -> None:
    print("\n── _terminate kills the child writer too (no orphan) ──")
    tmp.mkdir(parents=True, exist_ok=True)
    childfile = tmp / "child.out"
    pidfile = tmp / "childpid"
    # Parent (stands in for yt-dlp) forks a writer child (stands in for the
    # ffmpeg downloader) that appends forever. We record the child's pid so we
    # can prove it dies with the group rather than being orphaned.
    script = (
        f'( while true; do echo x >> "{childfile}"; sleep 0.1; done ) & '
        f'echo $! > "{pidfile}"; wait'
    )
    proc = subprocess.Popen(["sh", "-c", script], start_new_session=True)

    cap = StreamCapture(str(tmp), None)
    cap._proc = proc                       # inject the live group as the capture
    time.sleep(0.6)                        # let the child spin up + write its pid
    child_pid = int(pidfile.read_text().strip())
    check(_pid_alive(child_pid), "writer child is running before terminate")

    cap._terminate()

    check(proc.poll() is not None, "parent (yt-dlp stand-in) terminated")
    time.sleep(0.4)
    check(not _pid_alive(child_pid),
          "writer child (ffmpeg stand-in) killed with the group — NO orphan")
    s1 = childfile.stat().st_size
    time.sleep(0.4)
    s2 = childfile.stat().st_size
    check(s1 == s2, "the orphan is not still writing the file after terminate")


def test_remux_missing_or_empty_source(tmp: Path) -> None:
    print("\n── remux: vanished / empty source → nothing-to-convert, no loss ──")
    tmp.mkdir(parents=True, exist_ok=True)
    missing = tmp / "gone.flv"
    out = st._remux_for_telegram(missing)
    check(out == missing and not missing.exists(),
          "missing source returns unchanged path, never raises")

    empty = tmp / "empty.flv"
    empty.write_bytes(b"")
    out = st._remux_for_telegram(empty)
    check(out == empty and empty.exists(),
          "empty source is kept, not converted, not deleted")


def test_remux_keeps_source_on_empty_output(tmp: Path, monkeypatch_run) -> None:
    print("\n── remux: ffmpeg rc=0 but no output → keep the original ──")
    tmp.mkdir(parents=True, exist_ok=True)
    src = tmp / "real.flv"
    src.write_bytes(b"\0" * 4096)          # passes the present+non-empty guard

    # Simulate ffmpeg returning success WITHOUT producing the .mp4.
    class _R:
        returncode = 0
        stderr = b""
    monkeypatch_run(lambda *a, **k: _R())

    out = st._remux_for_telegram(src)
    check(out == src and src.exists(),
          "source preserved when remux yields no real output (no silent loss)")
    check(not (tmp / "real.mp4").exists(),
          "no empty .mp4 left masquerading as the recording")


def test_inplace_remux_temp_is_hidden(tmp: Path, monkeypatch_run) -> None:
    print("\n── remux: in-place .mp4 temp is a dotfile (no phantom on kill) ──")
    tmp.mkdir(parents=True, exist_ok=True)
    src = tmp / "clip_1234.mp4"
    src.write_bytes(b"\0" * 4096)
    seen: dict = {}

    class _R:
        returncode = 0
        stderr = b""

    def _fake_run(cmd, **k):
        # ffmpeg's last positional arg is the output path; record + create it so
        # the remux "succeeds" and we can inspect the temp name it chose.
        out = Path(cmd[-1])
        seen["out"] = out
        out.write_bytes(b"\0" * 2048)
        return _R()

    monkeypatch_run(_fake_run)
    st._remux_for_telegram(src)
    # A hidden temp is excluded by both output_files() and the startup sweep,
    # so an interrupted remux can never resurrect as a phantom recording.
    check(seen["out"].name.startswith("."),
          f"in-place remux temp is hidden ({seen['out'].name})")


def test_enqueue_skips_vanished_file(tmp: Path) -> None:
    print("\n── handoff: a file gone before enqueue is reported lost, not phantomed ──")
    tmp.mkdir(parents=True, exist_ok=True)
    calls: list[tuple] = []

    # Minimal StateMachine stand-in: only _enqueue_job + its enqueue dep matter.
    sm = st.StateMachine.__new__(st.StateMachine)
    sm.enqueue = lambda *a, **k: calls.append((a, k))

    job = st._Job(username="alice", file_path=tmp / "never_existed.flv")
    sm._enqueue_job(job)
    check(calls == [], "vanished file is NOT enqueued (no phantom queue row)")


def main() -> int:
    print("recorder capture-termination + remux integrity self-test")
    with tempfile.TemporaryDirectory() as d:
        root = Path(d)
        test_terminate_kills_whole_group(root / "grp")
        test_remux_missing_or_empty_source(root / "rx")

        # tiny monkeypatch helper scoped to one test
        orig_run = st.subprocess.run

        def _set(fn):
            st.subprocess.run = fn
        try:
            test_remux_keeps_source_on_empty_output(root / "rx2", _set)
            test_inplace_remux_temp_is_hidden(root / "rx3", _set)
        finally:
            st.subprocess.run = orig_run

        test_enqueue_skips_vanished_file(root / "hq")
    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
