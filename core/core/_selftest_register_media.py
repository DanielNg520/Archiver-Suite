"""
Validation harness for core.ingest.register_media — the media-prep layer over
register_file that the recorder's live enqueue and startup sweep now share, so a
big recording can never be enqueued whole onto Telegram's FilePartsInvalid wall.

The prep step (ffmpeg/AutoSplitter) is exercised by _selftest_media_prep; here
we pin the SEAM logic deterministically by stubbing media_prep.prepare to return
each PrepResult shape, then assert what register_media did with a REAL temp
ItemStore: rows inserted, split parts share one album group_key, per-output
identifier/caption overrides applied, and the original retired only when prep
transformed it AND every output was accounted for.

Run: PYTHONPATH=core python core/core/_selftest_register_media.py
"""

import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import ItemStore, register_media                   # noqa: E402
from core import ingest                                      # noqa: E402
from core.ingest import IngestOutcome                         # noqa: E402
from core.media_prep import PrepResult                        # noqa: E402

OK = "✓"
_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    _checks += 1
    if not cond:
        print(f"✗ FAIL: {label}")
        raise SystemExit(1)
    print(f"{OK} {label}")


def _make_stable_file(path: Path, *, kb: int = 1) -> Path:
    """A real on-disk file big enough and old enough to pass stability.is_stable
    instantly (no 1.5s probe sleep)."""
    path.write_bytes(os.urandom(kb * 1024))
    old = time.time() - 3600
    os.utime(path, (old, old))
    return path


def _rows(store: ItemStore) -> list[dict]:
    cur = store.conn.execute(
        "SELECT identifier, file_path, caption, group_key, source, platform, "
        "username FROM items ORDER BY id")
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _run(tmp: Path) -> None:
    # ── 1. passthrough: an already-streamable in-ceiling file → one row,
    #       no album key, original NOT retired (it IS the upload). ───────────
    store = ItemStore.open(str(tmp / "pass.db"))
    src = _make_stable_file(tmp / "clip.mp4")
    retired: list[Path] = []
    ingest.media_prep.prepare = (                              # type: ignore
        lambda p, split_threshold_bytes=None: PrepResult.passthrough(p))
    res = register_media(
        store, src, source="recorder", platform="tiktok", username="alice",
        caption="cap-alice", retire_original=retired.append,
        identifier_for=lambda o: f"recorder_{o.stem}",
        caption_for=lambda o: f"@alice · {o.stem}",
    )
    rows = _rows(store)
    check(res.prep_ok and res.transformed is False, "passthrough: prep ok, not transformed")
    check(res.outcomes == [IngestOutcome.INSERTED], "passthrough: one INSERTED output")
    check(res.any_inserted, "passthrough: any_inserted True")
    check(len(rows) == 1 and rows[0]["group_key"] is None,
          "passthrough: single row, no album group_key")
    check(rows[0]["identifier"] == "recorder_clip", "passthrough: identifier_for applied")
    check(rows[0]["caption"] == "@alice · clip", "passthrough: caption_for applied")
    check(retired == [], "passthrough: original NOT retired (it is the upload)")
    store.close()

    # ── 2. oversize split: 3 parts → 3 rows sharing ONE album key, and the
    #       original retired exactly once (all parts accounted). ─────────────
    store = ItemStore.open(str(tmp / "split.db"))
    big = _make_stable_file(tmp / "big.mp4")
    parts = [_make_stable_file(tmp / f"big_part{i:03d}.mp4") for i in range(3)]
    retired = []
    ingest.media_prep.prepare = (                             # type: ignore
        lambda p, split_threshold_bytes=None: PrepResult(
            outputs=parts, transformed=True, individual=True))
    res = register_media(
        store, big, source="recorder", platform="tiktok", username="bob",
        caption="cap-bob", retire_original=retired.append,
        identifier_for=lambda o: f"recorder_{o.stem}",
    )
    rows = _rows(store)
    check(res.outcomes == [IngestOutcome.INSERTED] * 3, "split: 3 INSERTED outputs")
    check(len(rows) == 3, "split: 3 rows enqueued")
    gks = {r["group_key"] for r in rows}
    check(len(gks) == 1 and None not in gks,
          "split: all parts share ONE non-null album group_key")
    check({r["identifier"] for r in rows} == {f"recorder_big_part{i:03d}" for i in range(3)},
          "split: per-part identifier_for applied")
    check(res.transformed and retired == [big],
          "split: original retired exactly once (all parts accounted)")
    store.close()

    # ── 3. split with one UNSTABLE part → original NOT retired (bytes would
    #       be lost). The stable parts still enqueue. ────────────────────────
    store = ItemStore.open(str(tmp / "partial.db"))
    big2 = _make_stable_file(tmp / "big2.mp4")
    good = _make_stable_file(tmp / "big2_part000.mp4")
    unstable = tmp / "big2_part001.mp4.part"      # .part suffix → never stable
    unstable.write_bytes(os.urandom(2048))
    retired = []
    ingest.media_prep.prepare = (                             # type: ignore
        lambda p, split_threshold_bytes=None: PrepResult(
            outputs=[good, unstable], transformed=True, individual=True))
    res = register_media(
        store, big2, source="recorder", platform="tiktok", username="cara",
        retire_original=retired.append)
    check(IngestOutcome.UNSTABLE in res.outcomes, "partial: unstable part reported UNSTABLE")
    check(res.all_accounted is False, "partial: not all_accounted (an output failed)")
    check(retired == [], "partial: original KEPT (a part failed to register — no data loss)")
    store.close()

    # ── 4. prep failure → no rows, prep_ok False, original kept. ────────────
    store = ItemStore.open(str(tmp / "fail.db"))
    bad = _make_stable_file(tmp / "bad.mkv")
    retired = []
    ingest.media_prep.prepare = (                             # type: ignore
        lambda p, split_threshold_bytes=None: PrepResult.failed("conversion failed"))
    res = register_media(
        store, bad, source="recorder", platform="tiktok", username="dan",
        retire_original=retired.append)
    check(res.prep_ok is False and res.error == "conversion failed",
          "prep-fail: prep_ok False with error surfaced")
    check(res.outcomes == [] and not res.any_inserted, "prep-fail: nothing enqueued")
    check(_rows(store) == [], "prep-fail: no rows written")
    check(retired == [], "prep-fail: original kept on disk")
    store.close()

    # ── 5. split_threshold_bytes is threaded through to media_prep.prepare ──
    seen: dict = {}

    def _spy(p, split_threshold_bytes=None):
        seen["threshold"] = split_threshold_bytes
        return PrepResult.passthrough(p)

    ingest.media_prep.prepare = _spy                          # type: ignore
    store = ItemStore.open(str(tmp / "thr.db"))
    f = _make_stable_file(tmp / "thr.mp4")
    register_media(store, f, source="recorder", platform="tiktok",
                   username="e", split_threshold_bytes=1234)
    check(seen.get("threshold") == 1234,
          "split mode: split_threshold_bytes forwarded to media_prep.prepare")
    store.close()


def main() -> int:
    _orig = ingest.media_prep.prepare
    try:
        with tempfile.TemporaryDirectory() as d:
            _run(Path(d))
    finally:
        ingest.media_prep.prepare = _orig                     # type: ignore
    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
