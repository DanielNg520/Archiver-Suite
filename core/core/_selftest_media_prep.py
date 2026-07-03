"""
Validation harness for media_prep + the orphaned ingester's pre-flight
(convert non-streamable formats; split oversize files via AutoSplitter).

Generates REAL tiny videos with ffmpeg and drives the actual code paths — no
mocks — asserting the on-disk + DB outcome of each branch:

    passthrough · remux · re-encode · split · idempotency · delete-after-split

Run: PYTHONPATH=core python core/core/_selftest_media_prep.py
Requires ffmpeg/ffprobe on PATH and the AutoSplitter sibling checkout (for the
split test); both are part of the suite's normal toolchain.
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import ItemStore, media_prep                      # noqa: E402
from core.orphaned import (                                  # noqa: E402
    CHAT_ID_PRIORITY, ORPHANED_PLATFORM, _PREPPED_META_KEY, ingest_folder,
)

OK = "✓"
_checks = 0


def check(cond: bool, label: str) -> None:
    global _checks
    _checks += 1
    if not cond:
        print(f"✗ FAIL: {label}")
        raise SystemExit(1)
    print(f"{OK} {label}")


def _ffmpeg(args: list[str]) -> None:
    r = subprocess.run(["ffmpeg", "-y", "-v", "error", *args],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"ffmpeg setup failed: {r.stderr.strip()[:300]}")


def make_video(path: Path, *, seconds: float, vcodec: str, acodec: str,
               bitrate: str | None = None, gop: int | None = None) -> None:
    """Synthesize a tiny test clip with the requested codecs/container."""
    args = [
        "-f", "lavfi", "-i", f"testsrc=duration={seconds}:size=240x160:rate=10",
        "-f", "lavfi", "-i", f"sine=frequency=440:duration={seconds}",
        "-shortest", "-pix_fmt", "yuv420p",
        "-c:v", vcodec, "-c:a", acodec,
    ]
    if gop:
        # Frequent keyframes so a -c copy segment split can actually cut.
        args += ["-g", str(gop), "-force_key_frames", f"expr:gte(t,n_forced*1)"]
    if bitrate:
        args += ["-b:v", bitrate]
    if path.suffix.lower() == ".mp4":
        args += ["-movflags", "+faststart"]
    _ffmpeg([*args, str(path)])


def rows_for(store: ItemStore, chat_id: str):
    return store.pending_items(ORPHANED_PLATFORM, chat_id)


# ── Unit: decision logic ──────────────────────────────────────────────────────

def test_decisions(tmp: Path) -> None:
    print("\n── media_prep.prepare branches ──")
    chat = tmp  # not a chat folder here; we call prepare() directly

    # 1. Compatible mp4 within size → passthrough (untouched).
    good = chat / "good.mp4"
    make_video(good, seconds=2, vcodec="libx264", acodec="aac")
    res = media_prep.prepare(good)
    check(res.ok and not res.transformed and res.outputs == [good],
          "compatible mp4 → passthrough (no transform)")
    check(good.exists(), "passthrough leaves original in place")

    # 2. h264/aac in an mkv → lossless remux to mp4, original superseded.
    mkv = chat / "remuxme.mkv"
    make_video(mkv, seconds=2, vcodec="libx264", acodec="aac")
    res = media_prep.prepare(mkv)
    check(res.ok and res.transformed and not res.individual,
          "mkv(h264/aac) → transformed, single output")
    check(len(res.outputs) == 1 and res.outputs[0].suffix == ".mp4"
          and res.outputs[0].exists(), "remux produced one .mp4 output")
    probe = media_prep._probe(res.outputs[0])
    check(probe is not None and media_prep._is_streamable(probe),
          "remux output is Telegram-streamable")
    media_prep._unlink(res.outputs[0])

    # 3. mpeg4 video in an avi → re-encode to h264 mp4.
    avi = chat / "reencode.avi"
    make_video(avi, seconds=2, vcodec="mpeg4", acodec="mp3")
    res = media_prep.prepare(avi)
    check(res.ok and res.transformed and len(res.outputs) == 1,
          "avi(mpeg4) → transformed single output")
    probe = media_prep._probe(res.outputs[0])
    check(probe is not None and probe.vcodec == "h264"
          and media_prep._is_streamable(probe),
          "re-encode output is h264 / streamable")
    media_prep._unlink(res.outputs[0])

    # 4. Disabled master switch → always passthrough.
    os.environ["ARCHIVER_MEDIA_PREP"] = "0"
    try:
        res = media_prep.prepare(mkv)
        check(not res.transformed and res.outputs == [mkv],
              "ARCHIVER_MEDIA_PREP=0 disables prep (passthrough)")
    finally:
        del os.environ["ARCHIVER_MEDIA_PREP"]

    # 5. Non-video (image extension) → passthrough untouched.
    img = chat / "pic.jpg"
    img.write_bytes(b"not really a jpeg")
    res = media_prep.prepare(img)
    check(res.outputs == [img] and not res.transformed,
          "image extension → passthrough (not a prep candidate)")


# ── Unit: split via AutoSplitter ──────────────────────────────────────────────

def test_split(tmp: Path) -> None:
    print("\n── media_prep split (AutoSplitter) ──")
    if media_prep._load_run_split() is None:
        print("  (AutoSplitter not found — skipping split test)")
        return
    big = tmp / "long.mp4"
    make_video(big, seconds=20, vcodec="libx264", acodec="aac",
               bitrate="300k", gop=10)
    size = big.stat().st_size
    # Force the oversize path: ceiling + chunk a third of the file, so a ~20s
    # clip yields ~3 parts each well over AutoSplitter's 5s chunk floor.
    os.environ["ARCHIVER_TG_MAX_UPLOAD_BYTES"] = str(size // 3)
    os.environ["ARCHIVER_SPLIT_CHUNK_BYTES"] = str(size // 3)
    try:
        res = media_prep.prepare(big)
    finally:
        del os.environ["ARCHIVER_TG_MAX_UPLOAD_BYTES"]
        del os.environ["ARCHIVER_SPLIT_CHUNK_BYTES"]
    check(res.ok and res.transformed and res.individual,
          "oversize mp4 → split, individual=True")
    check(len(res.outputs) >= 2, f"split produced {len(res.outputs)} parts (>=2)")
    check(all(p.exists() and p.stat().st_size > 0 for p in res.outputs),
          "all split parts exist and are non-empty")
    check(not (big.parent / f"{big.stem}_segments.txt").exists(),
          "AutoSplitter segment-list sidecar was cleaned up")
    for p in res.outputs:
        media_prep._unlink(p)


def test_split_threshold(tmp: Path) -> None:
    """Recorder split mode: a streamable mp4 UNDER the default upload ceiling but
    OVER an explicit split_threshold_bytes is split, with no env override — the
    threshold both triggers the split and caps each part."""
    print("\n── media_prep split_threshold (recorder split mode) ──")
    if media_prep._load_run_split() is None:
        print("  (AutoSplitter not found — skipping split-threshold test)")
        return
    big = tmp / "rec_long.mp4"
    make_video(big, seconds=20, vcodec="libx264", acodec="aac",
               bitrate="300k", gop=10)
    size = big.stat().st_size
    # No env tunables touched: the default ceiling (~3.9 GiB) leaves this tiny
    # clip alone. Passing a sub-file-size threshold must force the split anyway.
    threshold = size // 3
    res = media_prep.prepare(big, split_threshold_bytes=threshold)
    check(res.ok and res.transformed and res.individual,
          "under-ceiling mp4 over split_threshold → split, individual=True")
    check(len(res.outputs) >= 2,
          f"threshold split produced {len(res.outputs)} parts (>=2)")
    check(all(p.exists() and 0 < p.stat().st_size <= threshold + (1 << 20)
              for p in res.outputs),
          "all parts exist and respect the threshold cap")
    # A threshold ABOVE the file size is a no-op: nothing to split.
    big2 = tmp / "rec_small.mp4"
    make_video(big2, seconds=3, vcodec="libx264", acodec="aac")
    res2 = media_prep.prepare(big2, split_threshold_bytes=big2.stat().st_size * 4)
    check(res2.ok and not res2.transformed and len(res2.outputs) == 1
          and res2.outputs[0] == big2,
          "streamable file under threshold → passthrough (no split)")
    for p in res.outputs:
        media_prep._unlink(p)


def test_split_via_cli(tmp: Path) -> None:
    """Force the CLI path (how AutoSplitter is reached when installed stand-alone
    via pipx, i.e. not importable into this interpreter)."""
    print("\n── media_prep split via CLI ──")
    if media_prep._find_cli() is None:
        print("  (autosplitter CLI not on PATH — skipping)")
        return
    big = tmp / "clivid.mp4"
    make_video(big, seconds=20, vcodec="libx264", acodec="aac",
               bitrate="300k", gop=10)
    size = big.stat().st_size
    # Disable the in-process import path so _split must shell out to the CLI.
    saved = media_prep._run_split_cache
    media_prep._run_split_cache = False
    os.environ["ARCHIVER_TG_MAX_UPLOAD_BYTES"] = str(size // 3)
    os.environ["ARCHIVER_SPLIT_CHUNK_BYTES"] = str(size // 3)
    try:
        res = media_prep.prepare(big)
    finally:
        media_prep._run_split_cache = saved
        del os.environ["ARCHIVER_TG_MAX_UPLOAD_BYTES"]
        del os.environ["ARCHIVER_SPLIT_CHUNK_BYTES"]
    check(res.ok and res.transformed and res.individual and len(res.outputs) >= 2,
          f"CLI split produced {len(res.outputs)} parts via subprocess")
    check(all(p.exists() and p.stat().st_size > 0 for p in res.outputs),
          "all CLI split parts exist and are non-empty")
    check(not (big.parent / f"{big.stem}_segments.txt").exists(),
          "CLI segment-list sidecar cleaned up")
    for p in res.outputs:
        media_prep._unlink(p)


# ── Integration: ingest_folder end to end ─────────────────────────────────────

def test_ingest_folder(tmp: Path) -> None:
    print("\n── orphaned.ingest_folder integration ──")
    chat_id = "100200300"
    folder = tmp / chat_id
    (folder / "album").mkdir(parents=True)

    # Top-level compatible file → individual message, untouched.
    make_video(folder / "loose.mp4", seconds=2, vcodec="libx264", acodec="aac")
    # Subfolder incompatible container → remux, album-grouped by subfolder.
    make_video(folder / "album" / "clip.mkv", seconds=2,
               vcodec="libx264", acodec="aac")

    store = ItemStore.open(str(tmp / "t.db"))
    rep = ingest_folder(store, folder, chat_id=chat_id, guard=None)

    # mkv is KEPT (uploaded as a document) AND converted, so it yields TWO rows
    # on top of loose.mp4: clip.mp4 (streamable preview) + clip.mkv (original).
    check(rep.scanned == 2, f"scanned both files (got {rep.scanned})")
    check(rep.inserted == 3, f"inserted 3 rows (got {rep.inserted})")
    rows = {Path(r.file_path).name: r for r in rows_for(store, chat_id)}
    check(len(rows) == 3, "three pending rows present (loose + mp4 + kept mkv)")
    check(all(r.priority == CHAT_ID_PRIORITY for r in rows.values()),
          "default ingest priority is second only to live recordings")

    # The mkv is KEPT on disk and the converted .mp4 sits beside it with a CLEAN
    # name (no internal .tgprep tag — that is what Telegram names the upload).
    check((folder / "album" / "clip.mkv").exists(),
          "kept original .mkv stays on disk for the full-quality upload")
    check((folder / "album" / "clip.mp4").exists()
          and not (folder / "album" / "clip.tgprep.mp4").exists(),
          "converted file is the clean clip.mp4 (no .tgprep tag)")
    conv = rows.get("clip.mp4")
    check(conv is not None and conv.group_key == f"{chat_id}/album"
          and conv.caption is None,
          "converted preview keeps subfolder album routing")
    kept = rows.get("clip.mkv")
    check(kept is not None and kept.group_key is None
          and kept.caption == "clip.mkv",
          "kept original sends individually (never albums with its own preview)")
    loose = rows.get("loose.mp4")
    check(loose is not None and loose.group_key is None
          and loose.caption == "loose.mp4",
          "top-level compatible file routes as individual message")

    # Idempotency: a second sweep enqueues nothing new and adds no rows.
    rep2 = ingest_folder(store, folder, chat_id=chat_id, guard=None)
    check(rep2.inserted == 0, "second sweep inserts nothing (idempotent)")
    check(len(rows_for(store, chat_id)) == 3, "still exactly three rows")
    store.close()


def test_delete_after_split_off(tmp: Path) -> None:
    print("\n── non-streamable original kept as a document + memoized ──")
    chat_id = "100200301"
    folder = tmp / chat_id
    folder.mkdir(parents=True)
    # A .ts is non-streamable, so it is converted for the album AND its original
    # is kept and uploaded as a full-quality document — regardless of the
    # delete-after-split policy, which now governs only true oversize splits.
    make_video(folder / "keep.ts", seconds=2, vcodec="libx264", acodec="aac")

    store = ItemStore.open(str(tmp / "k.db"))
    # delete-after-split ON (the default) must NOT delete a converted original.
    os.environ["ARCHIVER_DELETE_AFTER_SPLIT"] = "1"
    try:
        rep = ingest_folder(store, folder, chat_id=chat_id, guard=None)
        check(rep.inserted == 2,
              "two rows: converted .mp4 preview + kept .ts document")
        check((folder / "keep.ts").exists(),
              "non-streamable original KEPT on disk for the document upload")
        rows = {Path(r.file_path).name: r for r in rows_for(store, chat_id)}
        doc = rows.get("keep.ts")
        check(doc is not None and doc.group_key is None
              and doc.caption == "keep.ts",
              "kept .ts is an individual document (never albums with its preview)")
        memo = store.meta_get(_PREPPED_META_KEY)
        check(memo and "keep.ts" in memo,
              "kept original is memoized so it won't be reprocessed")
        # Second sweep must NOT re-convert (memo hit) → no new rows.
        rep2 = ingest_folder(store, folder, chat_id=chat_id, guard=None)
        check(rep2.inserted == 0 and rep2.known >= 1,
              "memoized original skipped on the next sweep")
    finally:
        del os.environ["ARCHIVER_DELETE_AFTER_SPLIT"]
    store.close()


def test_clean_upload_name() -> None:
    print("\n── clean_upload_name strips the internal .tgprep marker ──")
    cun = media_prep.clean_upload_name
    check(cun("clip.tgprep.mp4") == "clip.mp4",
          "the .tgprep marker is stripped from the upload name")
    check(cun("/a/b/My Clip.tgprep.mp4") == "My Clip.mp4",
          "works on a full path, returns basename only")
    check(cun("clip.mp4") == "clip.mp4", "a clean name is returned unchanged")
    check(cun("keep.mkv") == "keep.mkv", "a non-mp4 original is untouched")
    check(cun("a.tgprep.mp4") == cun(cun("a.tgprep.mp4")), "idempotent")


def test_flv_not_kept_as_document(tmp: Path) -> None:
    print("\n── low-value .flv original is converted, NOT kept as a document ──")
    chat_id = "100200302"
    folder = tmp / chat_id
    folder.mkdir(parents=True)
    # .flv is non-streamable but excluded from keep-original: only the converted
    # .mp4 is uploaded; the raw stream dump is deleted, not archived.
    make_video(folder / "stream.flv", seconds=2, vcodec="libx264", acodec="aac")

    store = ItemStore.open(str(tmp / "f.db"))
    rep = ingest_folder(store, folder, chat_id=chat_id, guard=None)
    check(rep.inserted == 1, "only the converted .mp4 is enqueued (no document)")
    rows = {Path(r.file_path).name: r for r in rows_for(store, chat_id)}
    check("stream.flv" not in rows, "the .flv original is NOT registered")
    check((folder / "stream.mp4").exists(), "converted .mp4 sits in the folder")
    check(not (folder / "stream.flv").exists(),
          "the raw .flv original is deleted, not kept on disk")
    store.close()


def test_prep_lock_busy(tmp: Path) -> None:
    print("\n── concurrent prepare() of one file: second is 'busy', not a "
          "second encode ──")
    # A file that needs real work (mkv → remux). While one worker holds the
    # per-file prep lock, a second prepare() of the SAME file must return busy
    # (skip this cycle) instead of launching a clobbering parallel encode.
    mkv = tmp / "contended.mkv"
    make_video(mkv, seconds=2, vcodec="libx264", acodec="aac")

    with media_prep._prep_lock(mkv) as acquired:
        check(acquired, "first holder acquires the per-file prep lock")
        res = media_prep.prepare(mkv)
        check(res.busy and res.ok and not res.transformed and res.outputs == [],
              "prepare() while the lock is held → busy (no output, original kept)")
        check(mkv.exists(), "the contended source is left untouched while busy")

    # Lock released → prepare() now does the real work.
    res = media_prep.prepare(mkv)
    check(res.ok and res.transformed and not res.busy and len(res.outputs) == 1,
          "once the lock is free, prepare() converts normally")
    media_prep._unlink(res.outputs[0])


def main() -> None:
    print("media_prep self-test")
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        test_decisions(tmp)
        test_split(tmp)
        test_split_threshold(tmp)
        test_split_via_cli(tmp)
        test_ingest_folder(tmp)
        test_delete_after_split_off(tmp)
        test_flv_not_kept_as_document(tmp)
        test_prep_lock_busy(tmp)
    test_clean_upload_name()
    print(f"\nALL PASS ({_checks} checks)")


if __name__ == "__main__":
    main()
