"""
tests.test_seams
────────────────
Cross-WORKER integration tests. The per-package `core/core/_selftest*.py`
suites prove each module in isolation; this proves the SEAMS where the four
workers actually meet — the places a refactor in one package can silently break
another:

  Seam 1  recorder.lock  ←→  archiver.lock_reader        (the TikTok soft-lock)
  Seam 2  every producer  →  one items table              (priority + content_hash)
  Seam 3  add_item        →  dispatcher.claim_batch        (album/bucket grouping)
  Seam 4  core.ingest     →  dispatcher dedup guarantee    (global content_hash)
  Seam 5  BatchPolicy     →  claim_batch min-batch gate    (defer + flush-age)
  Seam 6  recorder.startup_sweep over the shared table     (sent/failed/new/dup)
  Seam 7  archiver.reconcile_recordings identifier scheme  (matches live enqueue)
  Seam 8  dispatcher.tg_router resolution chain            (env + explicit chat_id)
  Seam 9  PolicyStore banned roster ↔ active user list     (mutual exclusivity)
  Seam 10 the FULL dispatcher drain loop, fake Telegram    (claim→send→delete)

Run (from repo root):
    PYTHONPATH="core:archiver:recorder:dispatcher:ops" python3 -m tests.test_seams

Style matches the project's `_selftest` scripts: plain asserts, a printed
checkmark per assertion, nonzero exit on first failure. No pytest dependency.
Everything runs against temp dirs / a temp DB / a temp config.toml and a fake
Telegram sender — no network, no real Telegram, no touching the user's config.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import tempfile
from pathlib import Path

# ── tiny test harness ─────────────────────────────────────────────────────────

_checks = 0


def ok(cond: bool, label: str) -> None:
    global _checks
    if not cond:
        raise AssertionError(f"✗ {label}")
    _checks += 1
    print(f"✓ {label}")


def section(title: str) -> None:
    print(f"\n── {title} " + "─" * max(0, 50 - len(title)))


def _write_media(path: Path, payload: bytes) -> Path:
    """Write a >=200-byte 'media' file (stability + min-size gates both pass)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload + b"\0" * max(0, 256 - len(payload)))
    return path


def _fresh_db() -> "object":
    from core import ItemStore
    fd, p = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return ItemStore.open(p)


# ══════════════════════════════════════════════════════════════════════════════
# Seam 1 — the TikTok soft-lock: recorder writes, archiver reads
# ══════════════════════════════════════════════════════════════════════════════

def test_lock_seam(tmp: Path) -> None:
    section("Seam 1: recorder.lock ←→ archiver.lock_reader")
    import archiver.lock_reader as lr
    from recorder.lock import TikTokLock

    lock_path = tmp / "locks" / "tiktok.lock"
    # Point the reader at the same path the writer will use (the production
    # contract is a shared absolute path; here we redirect both to tmp).
    orig = lr.LOCK_PATH
    lr.LOCK_PATH = lock_path
    try:
        ok(not lr.tiktok_lock_held(), "no lock initially → archiver downloads")
        with TikTokLock(str(lock_path), recorder_pid=4321):
            ok(lock_path.exists(), "recorder __enter__ wrote the lockfile")
            ok(lr.tiktok_lock_held(), "archiver SEES the lock while recording")
        ok(not lr.tiktok_lock_held(), "recorder __exit__ removed the lock")
        # Stale lock (recorder SIGKILLed): file persists → still 'held' (the
        # reader does no liveness check, by design — operational concern).
        lock_path.write_text('{"pid": 999}')
        ok(lr.tiktok_lock_held(), "stale lockfile still reads as held (no liveness)")
    finally:
        lr.LOCK_PATH = orig


# ══════════════════════════════════════════════════════════════════════════════
# Seam 2 — every producer writes the ONE items table; priority + content_hash
# ══════════════════════════════════════════════════════════════════════════════

def test_producer_table_seam(tmp: Path) -> None:
    section("Seam 2: producers → one items table (priority + content_hash)")
    from recorder.enqueue import EnqueueClient, RECORDER_PRIORITY, _recorder_identifier
    from core import CHAT_ID_PRIORITY

    db = _fresh_db()
    try:
        # Archiver-style enqueue (priority 10, content_hash stamped by producer).
        from core.hashing import full_hash
        af = _write_media(tmp / "x" / "alice" / "20240101_1_0.jpg", b"ARCHIVER-BYTES")
        db.add_item(source="archiver", platform="x", username="alice",
                    identifier="x_1", file_path=str(af), priority=10,
                    content_hash=full_hash(af))

        # chat_id-folder files are urgent, but live recordings still win.
        of = _write_media(tmp / "-100123" / "loose.mp4", b"CHAT-ID-BYTES")
        db.add_item(source="orphaned", platform="orphaned", username="-100123",
                    identifier="orphaned_1", file_path=str(of),
                    priority=CHAT_ID_PRIORITY, chat_id="-100123",
                    content_hash=full_hash(of))

        # Recorder LIVE enqueue (priority 5). This is the seam the fix touched:
        # the recorder must now stamp content_hash like every other producer.
        rf = _write_media(tmp / "rec" / "bob" / "bob_1700.mp4", b"RECORDING-BYTES")
        # EnqueueClient opens its OWN ItemStore on the same file → use db_path.
        client = EnqueueClient(_db_file(db))
        inserted = client.enqueue(platform="tiktok", username="bob",
                                  file_path=str(rf), caption="@bob · tiktok · live")
        ok(inserted, "recorder live enqueue inserted a row")

        rec = db.get(db.id_of(str(rf)))
        ok(rec is not None, "recorder row is in the shared table")
        ok(rec.content_hash is not None,
           "recorder live enqueue now STAMPS content_hash (seam fix)")
        ok(rec.content_hash == full_hash(rf),
           "stamped hash equals core.hashing.full_hash (one definition of bytes)")
        ok(rec.identifier == _recorder_identifier(str(rf)),
           "recorder identifier scheme is recorder_<stem>")
        ok(RECORDER_PRIORITY < CHAT_ID_PRIORITY < 10,
           "priority order is recorder, chat_id folder, archiver")

        # The dispatcher claims lowest-priority-number first.
        first = db.claim_next()
        ok(first.source == "recorder",
           "claim_next picks the recorder row first")
        second = db.claim_next()
        ok(second.source == "orphaned", "then the chat_id-folder row")
        third = db.claim_next()
        ok(third.source == "archiver", "then the archiver row")
        ok(db.claim_next() is None, "queue drained — nothing left to claim")
    finally:
        db.close()


def test_local_platform_discovery_seam(tmp: Path) -> None:
    section("Seam 13: local-platform discovery excludes reserved routes")
    from types import SimpleNamespace
    from archiver.orchestrator import _local_platform_names

    for name in ("x", "tiktok", "instagram", "unsorted", "-100123", "library"):
        (tmp / name).mkdir(parents=True, exist_ok=True)
    config = SimpleNamespace(output_dir=str(tmp), local_platforms=())

    names = _local_platform_names(config)
    ok(names == ["library"],
       "only a genuine local platform is auto-discovered")


def test_dispatcher_instance_lock_seam(tmp: Path) -> None:
    section("Seam 14: one dispatcher owns each Telethon session")
    from dispatcher.instance_lock import DispatcherInstanceLock

    session = str(tmp / "telegram-session")
    code = (
        "import sys,time;"
        "sys.path.insert(0,'dispatcher');"
        "from dispatcher.instance_lock import DispatcherInstanceLock;"
        f"lock=DispatcherInstanceLock({session!r});"
        "lock.__enter__();print('locked',flush=True);time.sleep(30)"
    )
    child = subprocess.Popen(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        ok(child.stdout.readline().strip() == "locked",
           "first dispatcher process acquires the session lock")
        probe = DispatcherInstanceLock(session)
        ok(probe.holder_pid() == child.pid,
           "holder_pid() probe names the owning process")
        err = ""
        try:
            with DispatcherInstanceLock(session):
                acquired = True
        except RuntimeError as e:
            acquired, err = False, str(e)
        ok(not acquired, "second dispatcher process is rejected")
        ok(str(child.pid) in err,
           "rejection message names the holding pid (diagnosable, not opaque)")
    finally:
        child.terminate()
        child.wait(timeout=5)

    ok(DispatcherInstanceLock(session).holder_pid() is None,
       "holder_pid() reports no owner once the process is gone")
    with DispatcherInstanceLock(session):
        ok(True, "lock is recoverable after the owner exits")


def _db_file(store) -> str:
    """Pull the on-disk path out of an ItemStore's connection (test helper)."""
    row = store.conn.execute("PRAGMA database_list").fetchone()
    return row["file"]


# ══════════════════════════════════════════════════════════════════════════════
# Seam 3 — add_item → claim_batch album grouping (media bucket + group key)
# ══════════════════════════════════════════════════════════════════════════════

def test_album_batching_seam(tmp: Path) -> None:
    section("Seam 3: claim_batch album grouping by bucket + group")
    from core.files import ALBUM_MAX

    db = _fresh_db()
    try:
        # 12 photos, same (platform,user,source,caption) → one album capped at
        # ALBUM_MAX; a video in the same group must NOT mix in.
        for i in range(12):
            f = _write_media(tmp / "x" / "al" / f"p{i}.jpg", f"PH{i}".encode())
            db.add_item(source="archiver", platform="x", username="al",
                        identifier=f"p{i}", file_path=str(f), priority=10,
                        caption="album-A")
        vf = _write_media(tmp / "x" / "al" / "v.mp4", b"VID")
        db.add_item(source="archiver", platform="x", username="al",
                    identifier="v", file_path=str(vf), priority=10,
                    caption="album-A")

        batch = db.claim_batch()
        ok(len(batch) == ALBUM_MAX, f"photo album capped at ALBUM_MAX={ALBUM_MAX}")
        ok(all(Path(it.file_path).suffix == ".jpg" for it in batch),
           "video did not mix into the photo album (bucket-homogeneous)")

        # A 'single'-bucket item (gif) is always sent alone.
        gf = _write_media(tmp / "x" / "al" / "g.gif", b"GIF")
        db.add_item(source="archiver", platform="x", username="al",
                    identifier="g", file_path=str(gf), priority=1,
                    caption="album-A")
        solo = db.claim_batch()
        ok(len(solo) == 1 and solo[0].identifier == "g",
           "gif (single bucket) is claimed alone, never albumed")
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Seam 4 — core.ingest content_hash → dispatcher global-dedup guarantee
# ══════════════════════════════════════════════════════════════════════════════

def test_content_hash_dedup_seam(tmp: Path) -> None:
    section("Seam 4: global content_hash dedup (ingest ↔ dispatcher)")
    from core import register_file
    from core.hashing import full_hash

    db = _fresh_db()
    try:
        same = b"IDENTICAL-MEDIA-CONTENT-FOR-DEDUP-XXXXXXXXXX"
        a = _write_media(tmp / "d" / "a.jpg", same)
        b = _write_media(tmp / "d" / "b.jpg", same)   # byte-identical copy

        r1 = register_file(db, a, source="archiver", platform="x", username="u")
        ok(r1.inserted, "first copy ingested → new row")
        r2 = register_file(db, b, source="archiver", platform="x", username="u")
        ok(not r2.inserted, "byte-identical second copy did NOT create a row")
        ok(r2.outcome.value == "dedup_dropped", "second copy reported dedup_dropped")
        ok(not b.exists(), "redundant on-disk copy was removed (as if never there)")

        # Dispatcher's sent_twin: once one row ships, a DIFFERENT row with the
        # same bytes is suppressed (the guarantee). Add a same-hash row directly.
        c = _write_media(tmp / "d" / "c.jpg", same)
        cid_inserted = db.add_item(source="recorder", platform="tiktok",
                                   username="z", identifier="rec_c",
                                   file_path=str(c), content_hash=full_hash(c))
        ok(cid_inserted, "a same-bytes row from a DIFFERENT (platform,identifier) inserts")
        row1 = db.id_of(str(a))
        # Drive row1 → 'sent' through the real state machine to simulate prior
        # delivery. Claim every pending row ONCE into a list (claim flips them to
        # 'sending'); mark the target sent; requeue the rest exactly once. (Never
        # requeue mid-claim — that resurrects the row and loops forever.)
        claimed_ids = []
        while (it := db.claim_next()) is not None:
            claimed_ids.append(it.id)
        for cid in claimed_ids:
            if cid == row1:
                db.mark_sent(cid)
            else:
                db.requeue(cid)
        ok(row1 in claimed_ids and db.get(row1).status == "sent",
           "row1 marked sent (simulating prior delivery)")
        twin = db.sent_twin(full_hash(c), exclude_id=db.id_of(str(c)))
        ok(twin is not None and twin.id == row1,
           "sent_twin finds the already-delivered bytes (O(log n) index hit)")
        ok(db.sent_twin(None, exclude_id=1) is None,
           "NULL content_hash never matches a twin (never wrongly suppressed)")
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Seam 5 — BatchPolicy → claim_batch min-batch gate (defer + flush-age)
# ══════════════════════════════════════════════════════════════════════════════

def test_min_batch_gate_seam(tmp: Path) -> None:
    section("Seam 5: min-batch gate + anti-starvation flush")
    db = _fresh_db()
    try:
        # 3 photos in a group; require min_batch=5 → group is DEFERRED.
        for i in range(3):
            f = _write_media(tmp / "x" / "g" / f"p{i}.jpg", f"q{i}".encode())
            db.add_item(source="archiver", platform="x", username="g",
                        identifier=f"q{i}", file_path=str(f), priority=10,
                        caption="grp")
        got = db.claim_batch(min_batch=lambda a: 5, flush_age_s=lambda a: None)
        ok(got == [], "under-threshold group is deferred (nothing claimed yet)")

        # Same group, flush-age 0-ish → anti-starvation flush claims the partial.
        flushed = db.claim_batch(min_batch=lambda a: 5,
                                 flush_age_s=lambda a: 0.0001)
        ok(len(flushed) == 3,
           "aged partial is flushed despite being below min_batch")

        # Recorder/orphaned exemption is enforced by the dispatcher's closures
        # (source=='archiver' gate only); verify a recorder anchor bypasses it.
        rf = _write_media(tmp / "rec" / "u" / "u_1.mp4", b"RR")
        db.add_item(source="recorder", platform="tiktok", username="u",
                    identifier="rec_u_1", file_path=str(rf), priority=5)

        def _min(anchor):
            return 9 if anchor["source"] == "archiver" else 1

        claimed = db.claim_batch(min_batch=_min, flush_age_s=lambda a: None)
        ok(claimed and claimed[0].source == "recorder",
           "recorder anchor bypasses the min-batch gate (sends immediately)")
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Seam 6 — recorder.startup_sweep reconciles the shared table with disk
# ══════════════════════════════════════════════════════════════════════════════

def test_startup_sweep_seam(tmp: Path) -> None:
    section("Seam 6: recorder.startup_sweep over the shared table")
    from recorder import startup_sweep
    from core.hashing import full_hash

    out = tmp / "recout"
    db = _fresh_db()
    db_path = _db_file(db)
    try:
        # (a) a SENT-but-not-deleted file → sweep deletes it (policy ON below).
        sent_f = _write_media(out / "alice" / "alice_sent.mp4", b"SENT-BYTES")
        db.add_item(source="recorder", platform="tiktok", username="alice",
                    identifier="rec_sent", file_path=str(sent_f),
                    content_hash=full_hash(sent_f))
        db.mark_sent(db.claim_next().id)   # only row so far → it's this one

        # (b) a FAILED file → sweep re-arms it (failed → pending).
        failed_f = _write_media(out / "alice" / "alice_failed.mp4", b"FAILED-BYTES")
        db.add_item(source="recorder", platform="tiktok", username="alice",
                    identifier="rec_failed", file_path=str(failed_f),
                    content_hash=full_hash(failed_f))
        fid = db.id_of(str(failed_f))
        db.cancel(fid)                     # → failed (terminal)
        ok(db.get(fid).status == "failed", "  precondition: row is failed")

        # (c) a brand-NEW file with no row → sweep registers it.
        _write_media(out / "carol" / "carol_new.mp4", b"NEW-RECORDING-BYTES")

        # (d) a per-recording .log → sweep deletes it.
        (out / "alice" / "alice_sent_ytdlp.log").write_text("yt-dlp log\n")

        # (e) an orphaned RAW .flv: a capture that crashed before its live remux
        #     ran, leaving a non-canonical container with no DB row. It is NOT in
        #     MEDIA_EXTENSIONS, so the sweep must recognise it via the convertible
        #     set or it is stranded forever. Recovered raw here; the dispatcher's
        #     send-time net (Seam 20) makes it streamable at upload.
        orphan_flv = _write_media(out / "dave" / "dave_crash.flv",
                                  b"ORPHANED-RAW-FLV-NEVER-ENQUEUED")

        db.close()   # sweep opens its own ItemStore on the same file

        # Policy ON so the sent leftover is actually removed (uses a temp config).
        from core import PolicyStore, RecorderDeletePolicy
        ps = PolicyStore()   # ARCHIVER_SUITE_CONFIG points at a temp file
        ps.set(RecorderDeletePolicy.KEY, True)

        rep = startup_sweep.sweep(str(out), db_path, policy_store=ps)
        ok(rep.deleted_sent == 1 and not sent_f.exists(),
           "sent-but-present file deleted (delete-after-upload honored)")
        ok(rep.requeued >= 2, "failed re-armed AND new file registered (requeued≥2)")
        ok(rep.logs_deleted == 1, "per-recording .log cleaned up")

        db2 = __import__("core").ItemStore.open(db_path)
        try:
            ok(db2.get(fid).status == "pending", "failed recording re-armed to pending")
            ok(db2.has_file_path(str(out / "carol" / "carol_new.mp4")),
               "brand-new recording registered into the shared table")
            ok(db2.has_file_path(str(orphan_flv)),
               "orphaned raw .flv recovered by the sweep (not stranded on disk)")
        finally:
            db2.close()
    finally:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# Seam 7 — archiver.reconcile_recordings identifier matches live enqueue
# ══════════════════════════════════════════════════════════════════════════════

def test_recordings_reconcile_seam(tmp: Path) -> None:
    section("Seam 7: reconcile_recordings ↔ recorder identifier/priority")
    from archiver.reconcile import (
        reconcile_recordings, _recorder_identifier as arch_ident,
        _RECORDER_PRIORITY,
    )
    from recorder.enqueue import (
        _recorder_identifier as rec_ident, RECORDER_PRIORITY,
    )

    # The two packages MUST agree on the recorder identity + priority, or a
    # live-enqueued recording and the same file reconciled by the archiver
    # would not collide on UNIQUE(platform, identifier).
    probe = "/x/y/bob_1700.mp4"
    ok(arch_ident(Path(probe)) == rec_ident(probe),
       "archiver and recorder derive the SAME recorder identifier")
    ok(_RECORDER_PRIORITY == RECORDER_PRIORITY,
       "archiver and recorder agree on recorder upload priority")

    out = tmp / "recorder-out"
    _write_media(out / "dave" / "dave_42.mp4", b"RECONCILE-RECORDING-BYTES")
    db = _fresh_db()
    try:
        reports = reconcile_recordings(db, str(out))
        total = sum(r.inserted for r in reports)
        ok(total == 1, "reconcile_recordings queued the loose recording")
        row = db.get(db.id_of(str(out / "dave" / "dave_42.mp4")))
        ok(row.source == "recorder" and row.priority == RECORDER_PRIORITY,
           "reconciled recording carries source=recorder + recorder priority")
        ok(row.identifier == rec_ident(str(out / "dave" / "dave_42.mp4")),
           "reconciled identifier == the live-enqueue identifier")
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Seam 8 — dispatcher.tg_router resolution chain
# ══════════════════════════════════════════════════════════════════════════════

def test_routing_seam() -> None:
    section("Seam 8: tg_router resolution chain")
    from dispatcher.tg_router import TelegramRouter, RouteError
    from core.models import Item

    router = TelegramRouter(default_chat_id="-1000000000001")

    def _item(**kw):
        base = dict(id=1, source="archiver", platform="x", username="al",
                    identifier="i", file_path="/f.jpg", upload_date=None,
                    file_size_bytes=None, title="", discovered_at="t",
                    status="pending", priority=10, caption=None, attempts=0,
                    claimed_at=None, sent_at=None, last_error=None,
                    tg_message_id=None, content_hash=None, chat_id=None,
                    group_key=None)
        base.update(kw)
        return Item(**base)

    for k in list(os.environ):
        if k.startswith("TELEGRAM_CHAT_ID"):
            del os.environ[k]

    ok(router.chat_id_for_item(_item()) == "-1000000000001",
       "falls back to the global default chat id")

    os.environ["TELEGRAM_CHAT_ID_X"] = "-1001"
    ok(router.chat_id_for_item(_item()) == "-1001", "per-platform override wins over default")
    os.environ["TELEGRAM_CHAT_ID_X_AL"] = "-1002"
    ok(router.chat_id_for_item(_item()) == "-1002", "per-user override wins over per-platform")

    os.environ["TELEGRAM_CHAT_ID_TIKTOK_LIVE"] = "-9001"
    live = _item(platform="tiktok", username="streamer", source="recorder")
    ok(router.chat_id_for_item(live) == "-9001",
       "tiktok recorder/live routes to the LIVE channel")

    # Explicit chat_id on the row (orphaned folders) overrides everything.
    orphan = _item(source="orphaned", chat_id="-1009999")
    ok(router.chat_id_for_item(orphan) == "-1009999",
       "explicit row chat_id (orphaned) overrides env resolution")
    bad = _item(source="orphaned", chat_id="not-a-chat-id")
    try:
        router.chat_id_for_item(bad)
        raised = False
    except RouteError:
        raised = True
    ok(raised, "an invalid explicit chat_id raises RouteError (fail fast, not mid-send)")

    for k in ("TELEGRAM_CHAT_ID_X", "TELEGRAM_CHAT_ID_X_AL",
              "TELEGRAM_CHAT_ID_TIKTOK_LIVE"):
        os.environ.pop(k, None)


# ══════════════════════════════════════════════════════════════════════════════
# Seam 9 — PolicyStore banned roster ↔ active user list (the new feature)
# ══════════════════════════════════════════════════════════════════════════════

def test_banned_roster_seam() -> None:
    section("Seam 9: banned roster ↔ active users (mutual exclusivity)")
    from core import PolicyStore

    ps = PolicyStore()
    ps.add_user("x", "alice")
    ps.add_user("x", "bob")
    ps.set("delete_after_upload", True, platform="x", username="bob")

    newly = ps.ban_user("x", "bob", reason="account is suspended",
                         detected_at="2026-06-07T00:00:00+00:00")
    ok(newly, "first ban returns newly=True")
    ok("bob" not in ps.list_users("x"), "banned user removed from active list")
    ok("bob" in ps.list_banned("x"), "banned user appears on the banned roster")
    ok(list(ps.iter_user_overrides()) == [],
       "per-user overrides dropped on ban (no stale config)")
    ok(not ps.ban_user("x", "bob"), "re-ban is idempotent (newly=False)")

    # config add un-bans (operator asserting the account is back) — exclusivity.
    ps.unban_user("x", "bob")
    ok("bob" not in ps.list_banned("x"), "unban removes from the roster")
    ok("bob" not in ps.list_users("x"), "unban does NOT silently re-add to active")
    ps.add_user("x", "bob")
    ok("bob" in ps.list_users("x") and "bob" not in ps.list_banned("x"),
       "the two lists stay mutually exclusive")


# ══════════════════════════════════════════════════════════════════════════════
# Seam 11 — identity.resolve gives renamed-account re-downloads ONE identifier
# (so UNIQUE(platform, identifier) dedups them even when bytes/folder differ)
# ══════════════════════════════════════════════════════════════════════════════

def test_identity_ig_pk_dedup_seam(tmp: Path) -> None:
    section("Seam 11: IG media-pk identity → dedup across rename/re-encode")
    from core import identity, ItemStore

    # Same post (media pk 3540317000569885880), two usernames (account renamed),
    # different bytes → historically two manual_ ids → two uploads. The fix:
    # both resolve to the media PK, so the second insert is rejected.
    a = identity.resolve(Path("/o/fit_miness_1736258696_3540317000569885880_50348444507.jpg"))
    b = identity.resolve(Path("/o/gym__ln_1736258696_3540317000569885880_50348444507.jpg"))
    ok(a.identifier == "3540317000569885880", "IG filename → media PK identifier")
    ok(not a.is_manual, "media-pk identity is not a manual hash fallback")
    ok(a.identifier == b.identifier,
       "renamed-account copies resolve to the SAME identifier")

    # Our OWN download naming is untouched (regression guard).
    ours = identity.resolve(Path("/o/20240101_C1a2b3_0.jpg"))
    ok(ours.identifier == "C1a2b3_0" and not ours.is_manual,
       "our YYYYMMDD_<shortcode>_<num> scheme is unchanged")
    rnd = identity.resolve(Path("/o/some_random_clip.mp4"))
    ok(rnd.is_manual, "a non-matching name still falls back to manual_")
    ok(identity.archive_entry_for("instagram", a) is None,
       "numeric IG media-pk is NOT seeded into gallery-dl's shortcode archive")

    # TikTok: same video from yt-dlp (<id>.mp4) and gallery-dl (<id>_0.mp4)
    # must resolve to ONE identifier; photo carousels must stay distinct.
    yt = identity.resolve(Path("/o/20250317_7482670428511538440.mp4")).identifier
    gd = identity.resolve(Path("/o/20250317_7482670428511538440_0.mp4")).identifier
    ok(yt == gd == "7482670428511538440",
       "TikTok <id>.mp4 and <id>_0.mp4 collapse to one identifier")
    c1 = identity.resolve(Path("/o/20250402_7488614368540757303_1.jpg")).identifier
    c2 = identity.resolve(Path("/o/20250402_7488614368540757303_2.jpg")).identifier
    ok(c1 != c2, "TikTok photo carousel _1/_2 stay distinct (not collapsed)")
    img0 = identity.resolve(Path("/o/20250402_555_0.jpg")).identifier
    ok(img0.endswith("_0"), "a non-video _0 is NOT stripped (only videos)")

    # End-to-end at the table seam: the two copies → exactly one row.
    db = _fresh_db()
    try:
        fa = _write_media(tmp / "instagram" / "fit_miness" /
                          "fit_miness_1736258696_3540317000569885880_50348444507.jpg",
                          b"BYTES-V1")
        fb = _write_media(tmp / "instagram" / "gym__ln" /
                          "gym__ln_1736258696_3540317000569885880_50348444507.jpg",
                          b"BYTES-V2-REENCODED")  # different bytes on purpose
        for f in (fa, fb):
            mi = identity.resolve(f)
            db.add_item(source="archiver", platform="instagram",
                        username=f.parent.name, identifier=mi.identifier,
                        file_path=str(f), upload_date=mi.upload_date)
        rows = db.conn.execute(
            "SELECT COUNT(*) n FROM items WHERE platform='instagram'").fetchone()["n"]
        ok(rows == 1,
           "same post under two handles + different bytes → ONE row (no dup upload)")
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Seam 10 — the FULL dispatcher drain loop against a fake Telegram sender
# ══════════════════════════════════════════════════════════════════════════════

class _FakeSend:
    """A SendStrategy stand-in: records calls, always succeeds. Lets us drive
    dispatcher.drain.drain_forever end-to-end with zero network."""
    def __init__(self):
        self.sent_singles: list[str] = []
        self.sent_albums: list[list[str]] = []
        self.sent_ensure_streamable: list[bool] = []

    async def send(self, *, peer, file_path, caption, ensure_streamable=True):
        from dispatcher.send import SendResult
        self.sent_singles.append(file_path)
        self.sent_ensure_streamable.append(ensure_streamable)
        return SendResult(ok=True)

    async def send_album(self, *, peer, file_paths, caption):
        from dispatcher.send import SendResult
        self.sent_albums.append(list(file_paths))
        return SendResult(ok=True)


def test_full_drain_seam(tmp: Path) -> None:
    section("Seam 10: full dispatcher drain (claim→send→mark→delete)")
    from core import (ItemStore, PolicyStore, DeletePolicy, RecorderDeletePolicy,
                      BatchPolicy, DeletionGuard)
    from core.hashing import full_hash
    from dispatcher.drain import drain_forever
    from dispatcher.config import DispatcherConfig
    from dispatcher.tg_router import TelegramRouter

    db = _fresh_db()
    db_path = _db_file(db)
    try:
        # Two archiver photos (album) + one recorder single. delete-after-upload
        # ON globally so the drain's delete gate fires after a successful send.
        ps = PolicyStore()
        ps.set("delete_after_upload", True)
        ps.set(RecorderDeletePolicy.KEY, True)
        # Disable the min-batch gate so the small album sends within the test
        # (the gate itself is covered by Seam 5). Default size is 10.
        ps.set(BatchPolicy.SIZE_KEY, 1)

        p1 = _write_media(tmp / "x" / "al" / "p1.jpg", b"P1")
        p2 = _write_media(tmp / "x" / "al" / "p2.jpg", b"P2")
        for f, ident in ((p1, "p1"), (p2, "p2")):
            db.add_item(source="archiver", platform="x", username="al",
                        identifier=ident, file_path=str(f), priority=10,
                        caption="A", content_hash=full_hash(f))
        rec = _write_media(tmp / "rec" / "bo" / "bo_1.mp4", b"REC")
        db.add_item(source="recorder", platform="tiktok", username="bo",
                    identifier="rec_bo_1", file_path=str(rec), priority=5,
                    content_hash=full_hash(rec))

        # An orphaned single (already prepped at ingest). It must send with the
        # streamable net DISABLED — proves source-keyed net gating end-to-end.
        orph = _write_media(tmp / "orph" / "o1.mp4", b"ORPH")
        db.add_item(source="orphaned", platform="orphaned", username="-100999",
                    identifier="orph_o1", file_path=str(orph), priority=6,
                    caption="o1.mp4", chat_id="-100999",
                    content_hash=full_hash(orph))

        # A byte-duplicate of p1 that must be SUPPRESSED + its copy deleted.
        dup = _write_media(tmp / "x" / "al" / "p1_dup.jpg", b"P1")
        db.add_item(source="archiver", platform="x", username="al",
                    identifier="p1_dup", file_path=str(dup), priority=10,
                    caption="A", content_hash=full_hash(dup))
        db.close()

        cfg = DispatcherConfig(
            telegram=None, default_chat_id="-100123", db_path=db_path,
            policy_store=ps, poll_interval_s=0.01, max_retries=3,
            inter_album_sleep=0.0, stuck_claim_min=10, failed_retention_days=0,
        )
        store = ItemStore.open(db_path)
        fake = _FakeSend()
        router = TelegramRouter(default_chat_id="-100123")
        stop = asyncio.Event()

        async def _run():
            task = asyncio.create_task(drain_forever(
                cfg, store, fake, router,
                DeletePolicy(ps), RecorderDeletePolicy(ps), BatchPolicy(ps),
                DeletionGuard(ps), stop_event=stop,
            ))
            # Poll until everything is terminal (sent/deduped) or timeout.
            for _ in range(400):
                await asyncio.sleep(0.01)
                c = store.counts_by_status()
                if c.get("pending", 0) == 0 and c.get("sending", 0) == 0:
                    break
            stop.set()
            await task

        asyncio.run(_run())

        counts = store.counts_by_status()
        ok(counts.get("pending", 0) == 0, "drain emptied the pending queue")
        ok(counts.get("sent", 0) == 5,
           "all 5 rows terminal as 'sent' (2 album + 2 single + 1 dedup-suppressed)")
        ok(fake.sent_albums and sorted(Path(p).name for p in fake.sent_albums[0])
           == ["p1.jpg", "p2.jpg"],
           "the two photos went up as ONE album (homogeneous batch)")
        ok(sorted(Path(p).name for p in fake.sent_singles) == ["bo_1.mp4", "o1.mp4"],
           "recorder + orphaned files each sent as singles (never albumed)")
        # Source-keyed streamable-net gating: the recorder (fail-soft producer)
        # asks for the net; the orphaned row (prepped at ingest) opts out.
        net = dict(zip((Path(p).name for p in fake.sent_singles),
                       fake.sent_ensure_streamable))
        ok(net.get("bo_1.mp4") is True,
           "recorder single requests the send-time streamable net")
        ok(net.get("o1.mp4") is False,
           "orphaned single (already prepped at ingest) opts out of the net")
        ok(not p1.exists() and not p2.exists() and not rec.exists(),
           "delete-after-upload removed the originals post-send")
        ok(not dup.exists(),
           "dedup-suppressed duplicate's on-disk copy was removed unconditionally")
        dup_row = store.get(store.id_of(str(dup)))
        ok(dup_row.status == "sent" and dup_row.tg_message_id is None
           and "deduped" in (dup_row.last_error or ""),
           "suppressed dup recorded as sent-by-twin (no real send, audited)")
    finally:
        try:
            store.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# Seam 12 — full-history gate: core.store flag ↔ archiver._compute_date_min
# The gate (needs_full_history) and the cutoff computation live in different
# packages; the contract is "armed user ⇒ None cutoff ⇒ whole-timeline walk",
# and "marking done ⇒ fall back to the incremental floor". A regression in
# either side silently turns full-history into a no-op (old posts never come
# down) or makes EVERY run re-walk the timeline (slow + rate-limit risk).
# ══════════════════════════════════════════════════════════════════════════════

def test_full_history_gate_seam() -> None:
    section("Seam 12: full-history gate ↔ _compute_date_min cutoff")
    from archiver.platforms import _compute_date_min

    db = _fresh_db()
    try:
        # Brand-new user: no checkpoint row → needs full history → None cutoff,
        # so gallery-dl/yt-dlp walk the ENTIRE timeline on the first run.
        ok(db.needs_full_history("tiktok", "alice"),
           "brand-new user (no checkpoint) needs full history")
        ok(_compute_date_min(db, "tiktok", "alice", slack_days=2) is None,
           "armed user ⇒ None cutoff (extractor walks whole timeline)")

        # A delivered post gives the incremental path something to anchor on.
        f = _write_media(Path(tempfile.mkdtemp()) / "20240115_1_0.mp4", b"V")
        db.add_item(source="archiver", platform="tiktok", username="alice",
                    identifier="tt_1", file_path=str(f),
                    upload_date="20240115")
        # Drive it through the real state machine (pending → sending → sent) so
        # max_sent_upload_date counts it — mark_sent is guarded on 'sending'.
        for item in db.claim_batch():
            db.mark_sent(item.id)
        ok(db.max_sent_upload_date("tiktok", "alice") == "20240115",
           "  precondition: a delivered post exists with a date floor")

        # Still armed (download hasn't completed yet) → full-history WINS over
        # the floor: the cutoff stays None even though a floor now exists.
        ok(_compute_date_min(db, "tiktok", "alice", slack_days=2) is None,
           "armed user overrides the incremental floor (still None)")

        # Orchestrator closes the gate after the first complete walk.
        db.mark_full_history_done("tiktok", "alice")
        ok(not db.needs_full_history("tiktok", "alice"),
           "mark_full_history_done closes the gate")
        from datetime import datetime, timezone
        cutoff = _compute_date_min(db, "tiktok", "alice", slack_days=2)
        cutoff_day = (datetime.fromtimestamp(cutoff, tz=timezone.utc)
                      .strftime("%Y%m%d") if cutoff is not None else None)
        ok(cutoff_day == "20240113",
           "done user ⇒ incremental cutoff = floor − slack_days (fast path)")

        # `run --full-history` re-opens the gate without touching rows/files;
        # the cutoff goes back to None so old posts are re-walked next run.
        db.rearm_full_history("tiktok", "alice")
        ok(db.needs_full_history("tiktok", "alice"),
           "rearm_full_history re-opens the gate on demand")
        ok(_compute_date_min(db, "tiktok", "alice", slack_days=2) is None,
           "re-armed user ⇒ None cutoff again (old posts re-walked)")

        # Migration semantics: an existing user (checkpoint already present from
        # set_last_run) that was never explicitly armed reads as done — the v3
        # migration backfilled full_history_done=1 so upgrades don't re-walk
        # everyone. Here a fresh checkpoint defaults to needing it, so we assert
        # the inverse contract: a marked-done user is never re-walked silently.
        db.mark_full_history_done("tiktok", "alice")
        ok(_compute_date_min(db, "tiktok", "alice", slack_days=2) is not None,
           "a done user never silently reverts to a full walk")
    finally:
        try:
            db.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# Seam 15 — in-batch dedup must not suppress before its twin DELIVERS.
# Two byte-identical pending files claimed in one batch: the dupe is held back
# from the send. If the send FAILS, the dupe's bytes/file must be untouched
# (its twin never delivered); only after a successful send may it be
# suppressed and its redundant copy removed. Regression guard for the
# file-integrity bug where a dupe was marked 'sent' + deleted pre-send.
# ══════════════════════════════════════════════════════════════════════════════

class _FlakySend(_FakeSend):
    """Fails the first N album/single sends, then succeeds. `on_failure` (if
    set) runs at the moment of each failure — the deterministic point to
    assert what the world looks like while the twin has NOT delivered."""
    def __init__(self, fail_first: int, on_failure=None):
        super().__init__()
        self._failures_left = fail_first
        self._on_failure = on_failure

    def _maybe_fail(self):
        from dispatcher.send import SendResult
        if self._failures_left > 0:
            self._failures_left -= 1
            if self._on_failure:
                self._on_failure()
            return SendResult(ok=False, error="simulated network failure")
        return None

    async def send(self, *, peer, file_path, caption, ensure_streamable=True):
        return self._maybe_fail() or await super().send(
            peer=peer, file_path=file_path, caption=caption,
            ensure_streamable=ensure_streamable)

    async def send_album(self, *, peer, file_paths, caption):
        return self._maybe_fail() or await super().send_album(
            peer=peer, file_paths=file_paths, caption=caption)


def test_in_batch_dedup_integrity_seam(tmp: Path) -> None:
    section("Seam 15: in-batch dup survives a failed twin send")
    from core import (ItemStore, PolicyStore, DeletePolicy, RecorderDeletePolicy,
                      BatchPolicy, DeletionGuard)
    from core.hashing import full_hash
    from dispatcher.drain import drain_forever
    from dispatcher.config import DispatcherConfig
    from dispatcher.tg_router import TelegramRouter

    db = _fresh_db()
    db_path = _db_file(db)
    store = None
    try:
        ps = PolicyStore()
        ps.set(BatchPolicy.SIZE_KEY, 1)

        # Two byte-identical photos in ONE album group → claimed together.
        a = _write_media(tmp / "x" / "al" / "a.jpg", b"SAME")
        b = _write_media(tmp / "x" / "al" / "b.jpg", b"SAME")
        for f, ident in ((a, "a"), (b, "b")):
            db.add_item(source="archiver", platform="x", username="al",
                        identifier=ident, file_path=str(f), priority=10,
                        caption="A", content_hash=full_hash(f))
        db.close()

        cfg = DispatcherConfig(
            telegram=None, default_chat_id="-100123", db_path=db_path,
            policy_store=ps, poll_interval_s=0.01, max_retries=5,
            inter_album_sleep=0.0, stuck_claim_min=10, failed_retention_days=0,
        )
        store = ItemStore.open(db_path)
        # At each failure instant the twin has NOT delivered — both files must
        # still be on disk and no row may be terminal 'sent'. Captured inside
        # the sender so the check is deterministic, not poll-timing-dependent.
        failure_snapshots: list[bool] = []

        def _at_failure():
            c = store.counts_by_status()
            failure_snapshots.append(
                a.exists() and b.exists() and c.get("sent", 0) == 0)

        fake = _FlakySend(fail_first=2, on_failure=_at_failure)
        router = TelegramRouter(default_chat_id="-100123")
        stop = asyncio.Event()

        async def _run():
            task = asyncio.create_task(drain_forever(
                cfg, store, fake, router,
                DeletePolicy(ps), RecorderDeletePolicy(ps), BatchPolicy(ps),
                DeletionGuard(ps), stop_event=stop,
            ))
            for _ in range(600):
                await asyncio.sleep(0.01)
                c = store.counts_by_status()
                if c.get("pending", 0) == 0 and c.get("sending", 0) == 0 \
                        and c.get("sent", 0) == 2:
                    break
            stop.set()
            await task

        asyncio.run(_run())

        ok(len(failure_snapshots) == 2 and all(failure_snapshots),
           "during failed sends, no file was deleted and nothing marked sent")
        counts = store.counts_by_status()
        ok(counts.get("sent", 0) == 2 and counts.get("failed", 0) == 0,
           "after the sender recovered, both rows are terminal 'sent'")
        sent_files = [Path(p).name for batch in fake.sent_albums for p in batch] \
            + [Path(p).name for p in fake.sent_singles]
        ok(len(sent_files) == 1,
           "exactly ONE physical upload happened (the dupe never re-sent)")
        dup_row = store.get(store.id_of(str(b)))
        ok("deduped" in (dup_row.last_error or ""),
           "held-back dupe was suppressed only AFTER its twin delivered")
        ok(not b.exists(),
           "redundant copy removed once (and only once) the bytes shipped")
    finally:
        for s in (store, db):
            try:
                s.close()
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# Seam 16 — instance lock is CWD-independent. A bare session name must resolve
# to the SAME lock file no matter where the process was started (launchd CWD=/
# vs manual CWD=~ previously took two different locks and both ran).
# ══════════════════════════════════════════════════════════════════════════════

def test_lock_cwd_independence_seam(tmp: Path) -> None:
    section("Seam 16: instance lock path is CWD-independent")
    from dispatcher.instance_lock import DispatcherInstanceLock

    tmp.mkdir(parents=True, exist_ok=True)
    cwd = os.getcwd()
    try:
        os.chdir(tmp)
        lock_a = DispatcherInstanceLock("bare-session-name")
        os.chdir("/")
        lock_b = DispatcherInstanceLock("bare-session-name")
    finally:
        os.chdir(cwd)
    ok(lock_a.path == lock_b.path and lock_a.path.is_absolute(),
       "bare session name → one absolute lock path from any CWD")

    abs_session = tmp / "explicit" / "session"
    lock_c = DispatcherInstanceLock(str(abs_session))
    ok(lock_c.path.parent == abs_session.parent,
       "path-style session name keeps the lock beside the session file")


# ══════════════════════════════════════════════════════════════════════════════
# Seam 17 — recorder live enqueue goes through core.ingest with the recorder's
# identifier scheme intact, and inherits ingest's dedup-collapse: bytes already
# tracked under another path never become a second row.
# ══════════════════════════════════════════════════════════════════════════════

def test_recorder_enqueue_ingest_seam(tmp: Path) -> None:
    section("Seam 17: recorder enqueue ↔ core.ingest")
    from recorder.enqueue import EnqueueClient

    db = _fresh_db()
    db_path = _db_file(db)
    try:
        rec = _write_media(tmp / "alice" / "alice_live.mp4", b"LIVE")
        # Age the mtime past the stability quiescent window so the test
        # doesn't pay the 1.5s probe sleep.
        old = __import__("time").time() - 60
        os.utime(rec, (old, old))

        client = EnqueueClient(db_path)
        ok(client.enqueue(platform="tiktok", username="alice",
                          file_path=str(rec), caption="c"),
           "live enqueue registers a fresh recording")
        row = db.get(db.id_of(str(rec)))
        ok(row.identifier == f"recorder_{rec.stem}",
           "recorder identifier scheme preserved through core.ingest")
        ok(row.content_hash is not None,
           "live enqueue stamps content_hash (dedup guarantee intact)")

        # Same bytes under a second path → collapsed, never a second row.
        twin = _write_media(tmp / "alice" / "alice_live_copy.mp4", b"LIVE")
        os.utime(twin, (old, old))
        inserted = client.enqueue(platform="tiktok", username="alice",
                                  file_path=str(twin), caption="c")
        ok(not inserted, "byte-identical second path does not insert")
        ok(db.id_of(str(twin)) is None or db.id_of(str(twin)) == row.id,
           "no second row for identical bytes (dedup-collapse applied)")
    finally:
        try:
            db.close()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
# Seam 18 — the stall watchdog. A silent TCP freeze raises nothing, so without
# a per-attempt deadline the serial drain loop awaits forever (observed: a
# whole night of zero uploads, one row wedged in 'sending'). The retry
# envelope must convert "no progress" into a counted, retryable failure and
# recycle the presumed-wedged connection between attempts.
# ══════════════════════════════════════════════════════════════════════════════

def test_send_stall_watchdog_seam() -> None:
    section("Seam 18: send stall watchdog (deadline + reconnect)")
    from dispatcher.send import TelethonSendStrategy

    strategy = TelethonSendStrategy(
        api_id=0, api_hash="", phone="", session_name="stub",
        max_retries=2, retry_base_delay=0.01,
        stall_base_timeout_s=0.05, stall_min_rate_kib_s=128.0,
    )

    class _StubClient:
        def __init__(self):
            self.disconnects = 0
            self.connects = 0
        async def disconnect(self):
            self.disconnects += 1
        async def connect(self):
            self.connects += 1

    stub = _StubClient()
    strategy._client = stub  # bypass __aenter__: no network in tests

    # deadline math: fixed grace + payload at the floor rate
    ok(strategy._stall_timeout(0) == 0.05,
       "empty payload → base timeout only")
    ok(abs(strategy._stall_timeout(128 * 1024 * 10) - (0.05 + 10.0)) < 1e-6,
       "payload timeout scales by the floor-rate assumption")

    # a send that never completes must fail after max_retries, not hang
    calls = {"n": 0}
    async def _hang():
        calls["n"] += 1
        await asyncio.sleep(60)

    result = asyncio.run(
        strategy._send_with_retries(_hang, what="stub", payload_bytes=0))
    ok(not result.ok and "stalled" in (result.error or ""),
       "eternal stall becomes a counted failure, not an eternal await")
    ok(calls["n"] == 2, "each retry got its own deadline")
    ok(stub.disconnects == 2 and stub.connects == 2,
       "wedged connection is recycled before every retry")

    # first attempt stalls, second succeeds → retry actually recovers
    state = {"n": 0}
    async def _flaky():
        state["n"] += 1
        if state["n"] == 1:
            await asyncio.sleep(60)

    result = asyncio.run(
        strategy._send_with_retries(_flaky, what="stub", payload_bytes=0))
    ok(result.ok, "one stalled attempt then success → SendResult.ok")


# ══════════════════════════════════════════════════════════════════════════════
# Seam 19 — upload-progress heartbeat. The drain's send strategy WRITES a JSON
# heartbeat; `dispatcher status` and `ops health` READ it from other processes.
# The seam contract: atomic, throttled-but-never-misses-the-final-tick, and
# self-expiring (stale timestamp or dead writer pid reads as "idle", so a
# crashed dispatcher can't leave a lying status line behind).
# ══════════════════════════════════════════════════════════════════════════════

def test_upload_progress_seam(tmp: Path) -> None:
    section("Seam 19: upload progress heartbeat (writer ↔ readers)")
    import json
    import subprocess as sp
    from dispatcher.progress import ProgressReporter, read_progress, describe

    tmp.mkdir(parents=True, exist_ok=True)
    pf = tmp / "progress.json"

    rep = ProgressReporter(path=pf, min_interval_s=0.0)
    cb = rep.callback("/x/video.mp4", batch_pos=3, batch_total=10)
    cb(52_428_800, 140_826_032)
    p = read_progress(pf)
    ok(p is not None and p["file"] == "/x/video.mp4" and p["sent"] == 52_428_800,
       "heartbeat written and readable cross-call")
    desc = describe(p)
    ok("video.mp4" in desc and "[file 3/10]" in desc and "37%" in desc,
       f"describe() is human-readable ({desc})")

    # rate + ETA derive from byte/timestamp deltas
    fake = {"file": "/x/a.mp4", "sent": 50, "total": 100,
            "started_at": 0.0, "updated_at": 50.0}
    ok("1.0KB" not in describe(fake) and "ETA 50s" in describe(fake),
       "describe() derives rate and ETA from the heartbeat")

    # throttle: mid ticks suppressed, final tick never dropped
    rep2 = ProgressReporter(path=pf, min_interval_s=9999)
    cb2 = rep2.callback("/x/video.mp4")
    cb2(1, 100)            # first write (throttle window opens)
    cb2(2, 100)            # suppressed
    ok(read_progress(pf)["sent"] == 1, "mid-upload ticks are throttled")
    cb2(100, 100)          # sent == total bypasses the throttle
    ok(read_progress(pf)["sent"] == 100, "final tick always lands (100%)")

    # staleness self-expiry
    data = json.loads(pf.read_text())
    data["updated_at"] -= 3600
    pf.write_text(json.dumps(data))
    ok(read_progress(pf) is None, "stale heartbeat reads as idle")

    # dead-writer self-expiry: a just-exited child's pid is guaranteed dead
    dead_pid = int(sp.run(
        [sys.executable, "-c", "import os; print(os.getpid())"],
        capture_output=True, text=True).stdout.strip())
    data["updated_at"] = __import__("time").time()
    data["pid"] = dead_pid
    pf.write_text(json.dumps(data))
    ok(read_progress(pf) is None, "dead writer pid reads as idle")

    # clear() removes the artifact entirely
    cb(1, 2)
    rep.clear()
    ok(read_progress(pf) is None, "clear() leaves no heartbeat behind")


# ══════════════════════════════════════════════════════════════════════════════
# Seam 20 — the send-time streamable net. A recording whose recorder remux fell
# back to the raw container (.flv/.ts), or any video that bypassed ingest-time
# prep, reaches the dispatcher non-streamable. The send strategy must convert it
# to a streamable .mp4 BEFORE handing it to Telegram, send the converted bytes,
# and clean the temp up — while leaving an already-streamable file untouched
# (no needless re-encode) and never mutating the on-disk original.
# ══════════════════════════════════════════════════════════════════════════════

def _ffmpeg_present() -> bool:
    from shutil import which
    return which("ffmpeg") is not None and which("ffprobe") is not None


def _make_video(path: Path, *, container: str) -> Path:
    """A real, tiny H.264/AAC clip in the requested container. Codecs are always
    Telegram-friendly, so streamability is decided purely by the container —
    .mp4 streams inline, .flv does not (forcing the remux path)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["ffmpeg", "-y", "-v", "error",
         "-f", "lavfi", "-i", "testsrc=duration=1:size=160x120:rate=15",
         "-f", "lavfi", "-i", "sine=frequency=440:duration=1",
         "-c:v", "libx264", "-c:a", "aac", "-shortest", str(path)],
        check=True, capture_output=True)
    return path


def test_send_streamable_net_seam(tmp: Path) -> None:
    section("Seam 20: send-time streamable net (non-streamable video → mp4)")
    if not _ffmpeg_present():
        ok(True, "ffmpeg/ffprobe absent — net seam skipped (toolchain missing)")
        return
    from dispatcher.send import TelethonSendStrategy

    strategy = TelethonSendStrategy(
        api_id=0, api_hash="", phone="", session_name="stub")

    from telethon.tl import types as tg_types

    def _sent_filename(kw) -> str | None:
        for a in (kw.get("attributes") or []):
            if isinstance(a, tg_types.DocumentAttributeFilename):
                return a.file_name
        return None

    class _CaptureClient:
        """Records what path + filename attribute the strategy hands to Telegram."""
        def __init__(self):
            self.files: list[str] = []
            self.names: list[str | None] = []
            self.docs: list[bool] = []
        async def send_file(self, peer, file, **kw):
            self.files.append(str(file))
            self.names.append(_sent_filename(kw))
            self.docs.append(bool(kw.get("force_document")))
        async def disconnect(self): ...
        async def connect(self): ...

    # (a) a non-streamable .flv → Telegram receives a CONVERTED .mp4.
    flv = _make_video(tmp / "u" / "clip.flv", container="flv")
    cap = _CaptureClient()
    strategy._client = cap                      # bypass __aenter__: no network
    res = asyncio.run(strategy.send(peer="p", file_path=str(flv), caption="c"))
    ok(res.ok, "non-streamable .flv send succeeded")
    ok(len(cap.files) == 1 and cap.files[0].endswith(".mp4"),
       "dispatcher converted .flv → streamable .mp4 before the Telegram send")
    ok(cap.files[0] != str(flv),
       "the raw container is NOT what went over the wire")
    ok(cap.names[0] == "clip.mp4",
       "upload filename is the clean original stem + .mp4 (no .tgprep tag)")
    ok(flv.exists() and flv.suffix == ".flv",
       "the on-disk original recording is left untouched (never lose bytes)")
    ok(not (tmp / "u" / "clip.mp4").exists(),
       "the converted temp was cleaned up after the send")

    # (b) an already-streamable .mp4 → passthrough: sent untouched, no temp.
    mp4 = _make_video(tmp / "u" / "ok.mp4", container="mp4")
    cap2 = _CaptureClient()
    strategy._client = cap2
    res2 = asyncio.run(strategy.send(peer="p", file_path=str(mp4), caption="c"))
    ok(res2.ok and cap2.files == [str(mp4)],
       "already-streamable .mp4 is sent as-is (no needless re-encode)")
    ok(sorted(p.name for p in (tmp / "u").iterdir()) == ["clip.flv", "ok.mp4"],
       "no temp artifacts left behind by either send")

    # (c) ensure_streamable=False (a source that prepped at ingest, e.g. an
    # orphaned .mkv kept as a document) → the net is skipped, raw bytes ship.
    mkv = _make_video(tmp / "u" / "keep.mkv", container="matroska")
    cap3 = _CaptureClient()
    strategy._client = cap3
    res3 = asyncio.run(strategy.send(
        peer="p", file_path=str(mkv), caption="c", ensure_streamable=False))
    ok(res3.ok and cap3.files == [str(mkv)],
       "ensure_streamable=False ships the original .mkv as-is (no conversion)")
    ok(not (tmp / "u" / "keep.mp4").exists(),
       "no conversion temp created when the net is skipped")
    ok(cap3.docs == [True],
       "the non-streamable kept .mkv is sent as a DOCUMENT, not a 2nd video "
       "(otherwise Telegram shows the recording twice)")

    # (d) ensure_streamable=False on an ALREADY-streamable .mp4 (the common
    # prepped-at-ingest case) keeps the normal streaming-video path — only
    # non-streamable kept originals become documents.
    mp4b = _make_video(tmp / "u" / "ingested.mp4", container="mp4")
    cap4 = _CaptureClient()
    strategy._client = cap4
    res4 = asyncio.run(strategy.send(
        peer="p", file_path=str(mp4b), caption="c", ensure_streamable=False))
    ok(res4.ok and cap4.docs == [False],
       "a streamable as-is .mp4 still ships as a normal video, not a document")


# ══════════════════════════════════════════════════════════════════════════════
# Seam 21 — keep-original documents end-to-end: orphaned.ingest_folder (core)
# → claim_batch grouping → the full drain → fake send. Proves the cross-worker
# contract for a mixed folder: a non-streamable original ships as its OWN single
# (so send() documents it) while its converted preview albums with the sibling
# streamable videos, and an excluded .flv contributes only its converted copy.
# ══════════════════════════════════════════════════════════════════════════════

def test_keep_original_document_seam(tmp: Path) -> None:
    section("Seam 21: keep-original documents (ingest → drain → send)")
    if not _ffmpeg_present():
        ok(True, "ffmpeg/ffprobe absent — keep-original seam skipped")
        return
    from core import (ItemStore, PolicyStore, DeletePolicy,
                      RecorderDeletePolicy, BatchPolicy, DeletionGuard)
    from core.orphaned import ingest_folder
    from dispatcher.drain import drain_forever
    from dispatcher.config import DispatcherConfig
    from dispatcher.tg_router import TelegramRouter

    chat_id = "-100555"
    folder = tmp / chat_id
    album = folder / "album"
    album.mkdir(parents=True)
    # A subfolder so the streamable copies album together. Three sources:
    #   keep.mkv  — non-streamable → converted (album) + kept as a DOCUMENT
    #   plain.mp4 — already streamable → album as-is
    #   raw.flv   — non-streamable but EXCLUDED → only its converted copy ships
    _make_video(album / "keep.mkv", container="matroska")
    _make_video(album / "plain.mp4", container="mp4")
    _make_video(album / "raw.flv", container="flv")

    db_file = str(tmp / "seam21.db")
    store = ItemStore.open(db_file)
    rep = ingest_folder(store, folder, chat_id=chat_id, guard=None)
    ok(rep.inserted == 4,
       "4 rows: keep.mp4 + plain.mp4 + raw.mp4 (album) + keep.mkv (document)")
    ok((album / "keep.mkv").exists() and not (album / "raw.flv").exists(),
       "kept .mkv stays on disk; excluded .flv original is deleted")
    store.close()

    ps = PolicyStore()
    ps.set("delete_after_upload", False)        # keep originals; we assert sends
    ps.set(BatchPolicy.SIZE_KEY, 1)             # don't defer the small album
    cfg = DispatcherConfig(
        telegram=None, default_chat_id=chat_id, db_path=db_file,
        policy_store=ps, poll_interval_s=0.01, max_retries=3,
        inter_album_sleep=0.0, stuck_claim_min=10, failed_retention_days=0,
    )
    store = ItemStore.open(db_file)
    fake = _FakeSend()
    router = TelegramRouter(default_chat_id=chat_id)
    stop = asyncio.Event()

    async def _run():
        task = asyncio.create_task(drain_forever(
            cfg, store, fake, router,
            DeletePolicy(ps), RecorderDeletePolicy(ps), BatchPolicy(ps),
            DeletionGuard(ps), stop_event=stop,
        ))
        for _ in range(400):
            await asyncio.sleep(0.01)
            c = store.counts_by_status()
            if c.get("pending", 0) == 0 and c.get("sending", 0) == 0:
                break
        stop.set()
        await task

    asyncio.run(_run())

    # The converted previews (keep/plain/raw → .mp4) went up as ONE album.
    album_names = sorted(Path(p).name for p in fake.sent_albums[0]) \
        if fake.sent_albums else []
    ok(album_names == ["keep.mp4", "plain.mp4", "raw.mp4"],
       "the three streamable copies ship as one album (converted + native)")
    # The kept .mkv shipped as its OWN single with the streamable net DISABLED,
    # so send() takes the force_document branch — never albumed with its preview.
    singles = {Path(p).name: net for p, net in
               zip(fake.sent_singles, fake.sent_ensure_streamable)}
    ok(singles == {"keep.mkv": False},
       "only the kept .mkv sent as a single, net off (→ document at send)")
    ok(all("raw.flv" != Path(p).name for p in
            fake.sent_singles + [f for a in fake.sent_albums for f in a]),
       "the excluded .flv original is never sent (convert-only)")
    store.close()


# ══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    print("cross-worker seam integration tests")
    # Each test gets an isolated temp config.toml so the real user config is
    # never read or written.
    cfgfd, cfgpath = tempfile.mkstemp(suffix=".toml")
    os.close(cfgfd)
    os.environ["ARCHIVER_SUITE_CONFIG"] = cfgpath

    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        test_lock_seam(tmp / "s1")
        test_producer_table_seam(tmp / "s2")
        test_local_platform_discovery_seam(tmp / "s13")
        test_dispatcher_instance_lock_seam(tmp / "s14")
        test_album_batching_seam(tmp / "s3")
        test_content_hash_dedup_seam(tmp / "s4")
        test_min_batch_gate_seam(tmp / "s5")
        # fresh config per config-touching test so rosters don't bleed across
        for sub, fn in (("s6", test_startup_sweep_seam),
                        ("s7", test_recordings_reconcile_seam)):
            fn(tmp / sub)
        test_routing_seam()
        test_identity_ig_pk_dedup_seam(tmp / "s11")
        # banned-roster test wants a clean config
        _reset_config()
        test_banned_roster_seam()
        _reset_config()
        test_full_history_gate_seam()
        test_full_drain_seam(tmp / "s10")
        _reset_config()
        test_in_batch_dedup_integrity_seam(tmp / "s15")
        test_lock_cwd_independence_seam(tmp / "s16")
        test_recorder_enqueue_ingest_seam(tmp / "s17")
        test_send_stall_watchdog_seam()
        test_upload_progress_seam(tmp / "s19")
        test_send_streamable_net_seam(tmp / "s20")
        test_keep_original_document_seam(tmp / "s21")

    print(f"\nALL PASS ({_checks} checks)")
    return 0


def _reset_config() -> None:
    """Point ARCHIVER_SUITE_CONFIG at a brand-new empty file."""
    fd, path = tempfile.mkstemp(suffix=".toml")
    os.close(fd)
    os.environ["ARCHIVER_SUITE_CONFIG"] = path


if __name__ == "__main__":
    import sys
    sys.exit(main())
