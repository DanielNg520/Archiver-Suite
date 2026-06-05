"""
Focused validation for two correctness fixes:

  1. ingest re-arms a previously-FAILED twin instead of silently dropping the
     re-introduced identical-bytes file (so it gets enqueued again).
  2. reconcile runs convertible containers (.ts/.flv/...) through media_prep so
     a crashed raw recording becomes a streamable .mp4 row instead of being
     skipped.

Run: PYTHONPATH=core:archiver python core/core/_selftest_fixes.py
Requires ffmpeg/ffprobe on PATH.
"""

import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import ItemStore                                  # noqa: E402
from core.ingest import register_file, IngestOutcome        # noqa: E402

OK = "✓"
_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    _checks += 1
    if not cond:
        print(f"✗ FAIL: {label}")
        raise SystemExit(1)
    print(f"{OK} {label}")


def make_video(path: Path, *, container_args: list[str] | None = None) -> None:
    args = [
        "ffmpeg", "-y", "-v", "error",
        "-f", "lavfi", "-i", "testsrc=duration=2:size=240x160:rate=10",
        "-f", "lavfi", "-i", "sine=frequency=440:duration=2",
        "-shortest", "-pix_fmt", "yuv420p", "-c:v", "libx264", "-c:a", "aac",
    ]
    if path.suffix.lower() == ".mp4":
        args += ["-movflags", "+faststart"]
    args += (container_args or []) + [str(path)]
    r = subprocess.run(args, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"ffmpeg setup failed: {r.stderr.strip()[:300]}")


def copy_bytes(src: Path, dst: Path) -> None:
    dst.write_bytes(src.read_bytes())


# ── 1. ingest re-arms a failed twin ───────────────────────────────────────────

def test_rearm_failed_twin(tmp: Path) -> None:
    print("\n── ingest re-arms a FAILED twin ──")
    store = ItemStore.open(str(tmp / "rearm.db"))

    p1 = tmp / "first.mp4"
    make_video(p1)
    res1 = register_file(store, p1, source="orphaned", platform="orphaned",
                         username="100", chat_id="100", caption="first.mp4")
    check(res1.outcome is IngestOutcome.INSERTED, "first copy inserted")

    # Burn the row's retry budget → terminal 'failed'.
    claimed = store.claim_next()
    check(claimed is not None and claimed.id == res1.item_id, "row claimed")
    new_status = store.mark_failed(res1.item_id, error="boom", max_retries=0)
    check(new_status == "failed", "row is terminally failed")

    # Re-introduce identical bytes under a different name.
    p2 = tmp / "second.mp4"
    copy_bytes(p1, p2)
    res2 = register_file(store, p2, source="orphaned", platform="orphaned",
                         username="100", chat_id="100", caption="second.mp4")
    check(res2.outcome is IngestOutcome.REARMED,
          f"re-introduced copy re-armed the failed twin (got {res2.outcome})")
    check(res2.inserted, "REARMED counts as newly enqueued (.inserted)")

    row = store.get(res2.item_id)
    check(row is not None and row.status == "pending",
          "the twin row is pending again (claimable)")
    check(row.attempts == 0, "attempts reset on re-arm")

    # Exactly one row for these bytes — no duplicate enqueue.
    all_rows = store.list_items(limit=100)
    check(len(all_rows) == 1, f"still exactly one row (got {len(all_rows)})")
    store.close()


# ── 2. ingest does NOT re-arm a PENDING twin (normal dedup intact) ────────────

def test_pending_twin_still_dedups(tmp: Path) -> None:
    print("\n── pending twin still dedups (no re-arm) ──")
    store = ItemStore.open(str(tmp / "dedup.db"))

    p1 = tmp / "a.mp4"
    make_video(p1)
    register_file(store, p1, source="orphaned", platform="orphaned",
                  username="200", chat_id="200", caption="a.mp4")

    p2 = tmp / "b.mp4"
    copy_bytes(p1, p2)
    res = register_file(store, p2, source="orphaned", platform="orphaned",
                        username="200", chat_id="200", caption="b.mp4")
    check(res.outcome in (IngestOutcome.DEDUP_DROPPED, IngestOutcome.DEDUP_ADOPTED),
          f"pending twin → dedup, not re-arm (got {res.outcome})")
    check(not res.inserted, "dedup does not count as a new enqueue")
    check(len(store.list_items(limit=100)) == 1, "still one row")
    store.close()


# ── 3. reconcile converts a .ts recording instead of skipping it ──────────────

def test_reconcile_converts_ts(tmp: Path) -> None:
    print("\n── reconcile runs convertible containers through media_prep ──")
    from archiver.reconcile import _reconcile_dir, ReconcileReport, _recorder_identifier

    user_dir = tmp / "tiktok-out" / "someuser"
    user_dir.mkdir(parents=True)
    raw = user_dir / "live_clip.ts"
    make_video(raw, container_args=["-f", "mpegts"])
    check(raw.exists(), "raw .ts recording exists on disk")

    store = ItemStore.open(str(tmp / "rec.db"))
    report = ReconcileReport(platform="tiktok", username="someuser")
    _reconcile_dir(
        platform=None, username="someuser", db=store, scan_dir=user_dir,
        recursive=True, seed_extractor_archive=False, report=report,
        source="recorder", identifier_for_path=_recorder_identifier,
        priority=5, guard=None,
    )

    check(report.scanned == 1, f"the .ts was scanned (got {report.scanned})")
    check(report.inserted == 1,
          f"a streamable row was enqueued (got {report.inserted})")
    check(not raw.exists(), "raw .ts original retired after conversion")

    rows = store.list_items(limit=100)
    check(len(rows) == 1, "exactly one row")
    out_path = Path(rows[0].file_path)
    check(out_path.suffix.lower() == ".mp4" and out_path.exists(),
          f"row points at an existing .mp4 ({out_path.name})")
    check(rows[0].status == "pending", "converted recording is pending/claimable")

    # Idempotency: a second pass adds nothing (output already has a row).
    report2 = ReconcileReport(platform="tiktok", username="someuser")
    _reconcile_dir(
        platform=None, username="someuser", db=store, scan_dir=user_dir,
        recursive=True, seed_extractor_archive=False, report=report2,
        source="recorder", identifier_for_path=_recorder_identifier,
        priority=5, guard=None,
    )
    check(report2.inserted == 0, "second pass inserts nothing (idempotent)")
    check(len(store.list_items(limit=100)) == 1, "still exactly one row")
    store.close()


def main() -> None:
    print("fixes self-test")
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        test_rearm_failed_twin(tmp)
        test_pending_twin_still_dedups(tmp)
        test_reconcile_converts_ts(tmp)
    print(f"\nALL PASS ({_checks} checks)")


if __name__ == "__main__":
    main()
