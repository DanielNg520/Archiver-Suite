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


def _dead_pid() -> int:
    """A pid guaranteed not to be alive right now (for stale-heartbeat tests)."""
    p = 999_999
    while True:
        try:
            os.kill(p, 0)
        except ProcessLookupError:
            return p
        except OSError:
            pass        # alive (or not ours) — try a higher number
        p += 1


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
        # Default pid = this live test process, so the lock reads as held.
        with TikTokLock(str(lock_path)):
            ok(lock_path.exists(), "recorder __enter__ wrote the lockfile")
            ok(lr.tiktok_lock_held(), "archiver SEES the lock while recording")
        ok(not lr.tiktok_lock_held(), "recorder __exit__ removed the lock")
        # Stale lock (recorder SIGKILLed without cleanup): file persists but its
        # pid is dead → the reader's liveness gate SELF-HEALS it to not-held, so
        # TikTok archiving resumes instead of starving forever.
        lock_path.write_text(f'{{"pid": {_dead_pid()}}}')
        ok(not lr.tiktok_lock_held(),
           "stale lock (dead writer pid) self-heals to not-held")
        # A lock owned by a LIVE process still blocks (no false resume).
        lock_path.write_text(f'{{"pid": {os.getpid()}}}')
        ok(lr.tiktok_lock_held(), "lock with a live writer pid still reads held")
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

    # '-100123.t42' is a TOPIC-suffixed route: it must be excluded too. Using
    # is_chat_id here (bare-only) instead of parse_route would MISS the `.t`
    # suffix, auto-adopt it as a platform, and upload to the default chat.
    for name in ("x", "tiktok", "instagram", "unsorted",
                 "-100123", "-100123.t42", "1003547920321.t41478", "library"):
        (tmp / name).mkdir(parents=True, exist_ok=True)
    config = SimpleNamespace(output_dir=str(tmp), local_platforms=())

    names = _local_platform_names(config)
    ok(names == ["library"],
       "only a genuine local platform is auto-discovered "
       "(bare AND topic-suffixed routes excluded)")


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
        # A REAL terminal failure (burn the retry budget), NOT a manual cancel:
        # cancel is now durable and must never be swept back to pending, so using
        # it here would wrongly assert the sweep re-arms an abort. claim_next is
        # this row (only pending left after (a) was sent).
        db.mark_failed(db.claim_next().id, error="send failed", max_retries=0)
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

    # Regression: a DASH-FREE numeric channel id on the row (a legacy row, or one
    # from `archiver ingest --chat 100…`) must re-sign to -100… and resolve as a
    # PeerChannel — NOT a PeerUser, which Telegram can't find an input entity for.
    from telethon.tl.types import PeerChannel, PeerUser
    bare = _item(source="orphaned", chat_id="1001733273713")
    ok(router.chat_id_for_item(bare) == "-1001733273713",
       "dash-free channel id re-signed to its canonical -100… form")
    peer = router.peer_for_item(bare)
    ok(isinstance(peer, PeerChannel) and peer.channel_id == 1733273713,
       "dash-free channel id resolves to PeerChannel (not PeerUser → entity error)")
    ok(not isinstance(router.peer_for_item(orphan), PeerUser),
       "a -100… channel id never resolves to PeerUser")

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
    dispatcher.drain.drain_forever end-to-end with zero network. Captures the
    caption and topic_id of every send so seams that assert on the destination
    forum-topic (Seam 22) or the sanitized caption (Seam 24) can read them back."""
    def __init__(self):
        self.sent_singles: list[str] = []
        self.sent_albums: list[list[str]] = []
        self.sent_ensure_streamable: list[bool] = []
        self.single_captions: list[str] = []
        self.album_captions: list[str] = []
        self.single_topics: list[int | None] = []
        self.album_topics: list[int | None] = []
        self.album_as_documents: list[bool] = []

    async def send(self, *, peer, file_path, caption, ensure_streamable=True,
                   filetype_tag=False, topic_id=None):
        from dispatcher.send import SendResult
        self.sent_singles.append(file_path)
        self.sent_ensure_streamable.append(ensure_streamable)
        self.single_captions.append(caption)
        self.single_topics.append(topic_id)
        return SendResult(ok=True)

    async def send_album(self, *, peer, file_paths, caption, topic_id=None, as_documents=False):
        from dispatcher.send import SendResult
        self.sent_albums.append(list(file_paths))
        self.album_captions.append(caption)
        self.album_topics.append(topic_id)
        self.album_as_documents.append(as_documents)
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
        ok(counts.get("sent", 0) == 4,
           "4 rows terminal as 'sent' (2 album + recorder single + dedup-suppressed)")
        ok(store.id_of(str(orph)) is None and not orph.exists(),
           "orphaned chat_id-folder item left NO trace: row deleted AND file removed "
           "(ship-and-delete)")
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
# Seam 11 — MediaEmptyError ↔ fail-fast quarantine (queue can't head-of-line jam)
# Telegram occasionally rejects uploaded media for a destination (MediaEmptyError),
# often transiently. The send envelope must NOT retry it 4x (a backoff storm), and
# the drain must NOT let it cycle attempts at the head of the queue — one poison
# album would otherwise starve the whole drain (the real 3-hour outage). Contract:
# media_empty ⇒ terminal 'failed' on the FIRST hit, no CANCELLED_MARKER, so the
# drain moves on AND `reset failed` can recover it once the cause clears.
# ══════════════════════════════════════════════════════════════════════════════

class _MediaEmptySend:
    """Fake strategy modeling the real poison: an ALBUM send hits MediaEmptyError
    (album atomicity — one bad item fails all). The per-item fallback then sends
    each single: a file whose name contains 'bad' stays MediaEmptyError (a truly
    undeliverable clip media_prep can't fix); everything else delivers (a good
    H.264 item, or a VP9 the net re-encoded). Proves: deliver the good, isolate
    the bad — never write off the whole album."""
    def __init__(self):
        self.album_attempts = 0
        self.single_sends: list[str] = []

    async def send(self, *, peer, file_path, caption, ensure_streamable=True,
                   filetype_tag=False, topic_id=None):
        from dispatcher.send import SendResult
        self.single_sends.append(file_path)
        if "bad" in Path(file_path).name:
            return SendResult(ok=False, error="MediaEmptyError: rejected",
                              media_empty=True)
        return SendResult(ok=True)

    async def send_album(self, *, peer, file_paths, caption, topic_id=None, as_documents=False):
        from dispatcher.send import SendResult
        self.album_attempts += 1
        return SendResult(ok=False, error="MediaEmptyError: rejected",
                          media_empty=True)


def test_media_empty_quarantine_seam(tmp: Path) -> None:
    section("Seam 11: MediaEmptyError ↔ per-item fallback (deliver good, isolate bad)")
    from core import (ItemStore, PolicyStore, DeletePolicy, RecorderDeletePolicy,
                      BatchPolicy, DeletionGuard, CANCELLED_MARKER)
    from core.hashing import full_hash
    from dispatcher.drain import drain_forever
    from dispatcher.config import DispatcherConfig
    from dispatcher.tg_router import TelegramRouter

    db = _fresh_db()
    db_path = _db_file(db)
    try:
        ps = PolicyStore()
        ps.set(BatchPolicy.SIZE_KEY, 1)   # let the small album send immediately
        # A 3-photo album whose send hits MediaEmptyError: two good items + one
        # undeliverable ('bad'). Plus a recorder single that must still flow.
        good1 = _write_media(tmp / "x" / "al" / "good1.jpg", b"G1")
        good2 = _write_media(tmp / "x" / "al" / "good2.jpg", b"G2")
        bad   = _write_media(tmp / "x" / "al" / "bad_vp9.jpg", b"BAD")
        for f, ident in ((good1, "g1"), (good2, "g2"), (bad, "b1")):
            db.add_item(source="archiver", platform="x", username="al",
                        identifier=ident, file_path=str(f), priority=10,
                        content_hash=full_hash(f))
        rec = _write_media(tmp / "rec" / "bo" / "bo_1.mp4", b"REC")
        db.add_item(source="recorder", platform="tiktok", username="bo",
                    identifier="rec_bo_1", file_path=str(rec), priority=5,
                    content_hash=full_hash(rec))
        db.close()

        cfg = DispatcherConfig(
            telegram=None, default_chat_id="-100123", db_path=db_path,
            policy_store=ps, poll_interval_s=0.01, max_retries=4,
            inter_album_sleep=0.0, stuck_claim_min=10, failed_retention_days=0)
        store = ItemStore.open(db_path)
        fake = _MediaEmptySend()
        stop = asyncio.Event()

        async def _run():
            task = asyncio.create_task(drain_forever(
                cfg, store, fake, TelegramRouter(default_chat_id="-100123"),
                DeletePolicy(ps), RecorderDeletePolicy(ps), BatchPolicy(ps),
                DeletionGuard(ps), stop_event=stop))
            for _ in range(400):
                await asyncio.sleep(0.01)
                c = store.counts_by_status()
                if c.get("pending", 0) == 0 and c.get("sending", 0) == 0:
                    break
            stop.set(); await task

        asyncio.run(_run())

        ok(fake.album_attempts == 1,
           "album attempted once, then fell back to per-item (no retry storm)")
        ok(store.get(store.id_of(str(good1))).status == "sent" and
           store.get(store.id_of(str(good2))).status == "sent",
           "good album items DELIVERED individually (not lost with the bad one)")
        bad_row = store.get(store.id_of(str(bad)))
        ok(bad_row.status == "failed" and bad_row.attempts <= 1,
           "only the undeliverable item quarantined (no retry-budget churn)")
        ok(not (bad_row.last_error or "").startswith(CANCELLED_MARKER),
           "quarantine leaves a plain failure (reset failed can recover it)")
        ok(store.get(store.id_of(str(rec))).status == "sent",
           "deliverable recorder single still sent — poison didn't block it")
        n = store.reset_failed(None, None)
        ok(n == 1 and store.get(store.id_of(str(bad))).status == "pending",
           "reset failed re-arms only the quarantined item (recovery path)")
    finally:
        try: store.close()
        except Exception: pass


class _AlwaysFailSend:
    """Every send fails with a SYSTEMIC (network) error — models Telegram down."""
    def __init__(self):
        self.calls = 0

    async def send(self, *, peer, file_path, caption, ensure_streamable=True,
                   filetype_tag=False, topic_id=None):
        from dispatcher.send import SendResult
        self.calls += 1
        return SendResult(ok=False, error="network down")

    async def send_album(self, *, peer, file_paths, caption, topic_id=None, as_documents=False):
        from dispatcher.send import SendResult
        self.calls += 1
        return SendResult(ok=False, error="network down")


def test_circuit_breaker_seam(tmp: Path) -> None:
    section("Seam 11b: dispatcher circuit breaker pauses on systemic failure")
    import dispatcher.drain as drain_mod
    from core import (ItemStore, PolicyStore, DeletePolicy, RecorderDeletePolicy,
                      BatchPolicy, DeletionGuard)
    from core.hashing import full_hash
    from dispatcher.config import DispatcherConfig
    from dispatcher.tg_router import TelegramRouter

    orig_trip, orig_cd = drain_mod._CIRCUIT_TRIP_AT, drain_mod._CIRCUIT_COOLDOWN_S
    drain_mod._CIRCUIT_TRIP_AT = 3
    drain_mod._CIRCUIT_COOLDOWN_S = 30.0   # long: we catch the drain mid-cooldown
    db = _fresh_db()
    db_path = _db_file(db)
    try:
        ps = PolicyStore(); ps.set(BatchPolicy.SIZE_KEY, 1)
        for i in range(10):
            f = _write_media(tmp / "rec" / "u" / f"u_{i}.mp4", bytes([i]) * 50)
            db.add_item(source="recorder", platform="tiktok", username="u",
                        identifier=f"rec_{i}", file_path=str(f), priority=5,
                        content_hash=full_hash(f))
        db.close()
        cfg = DispatcherConfig(
            telegram=None, default_chat_id="-100123", db_path=db_path,
            policy_store=ps, poll_interval_s=0.01, max_retries=4,
            inter_album_sleep=0.0, stuck_claim_min=10, failed_retention_days=0)
        store = ItemStore.open(db_path)
        fake = _AlwaysFailSend()
        stop = asyncio.Event()

        async def _run():
            task = asyncio.create_task(drain_mod.drain_forever(
                cfg, store, fake, TelegramRouter(default_chat_id="-100123"),
                DeletePolicy(ps), RecorderDeletePolicy(ps), BatchPolicy(ps),
                DeletionGuard(ps), stop_event=stop))
            for _ in range(500):
                await asyncio.sleep(0.01)
                if fake.calls >= 3:
                    break
            await asyncio.sleep(0.15)   # let it reach the cooldown gate
            stop.set(); await task

        asyncio.run(_run())
        ok(fake.calls == 3,
           f"breaker tripped at threshold: {fake.calls} sends then paused, not "
           f"all 10 churned through")
    finally:
        drain_mod._CIRCUIT_TRIP_AT = orig_trip
        drain_mod._CIRCUIT_COOLDOWN_S = orig_cd
        try: store.close()
        except Exception: pass


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

    async def send(self, *, peer, file_path, caption, ensure_streamable=True,
                   filetype_tag=False, topic_id=None):
        return self._maybe_fail() or await super().send(
            peer=peer, file_path=file_path, caption=caption,
            ensure_streamable=ensure_streamable, filetype_tag=filetype_tag,
            topic_id=topic_id)

    async def send_album(self, *, peer, file_paths, caption, topic_id=None, as_documents=False):
        return self._maybe_fail() or await super().send_album(
            peer=peer, file_paths=file_paths, caption=caption, topic_id=topic_id)


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

    # A NETWORK error must trigger an explicit reconnect. Telethon's own
    # auto_reconnect is OFF (it raced our _force_reconnect → 'NoneType'.connect
    # hangs), so the dispatcher is the SOLE reconnect authority: without this,
    # every retry would hit the same dead socket. First attempt errors → reconnect
    # → second succeeds.
    stub2 = _StubClient()
    strategy._client = stub2
    netstate = {"n": 0}
    async def _neterr():
        netstate["n"] += 1
        if netstate["n"] == 1:
            raise ConnectionError("Connection to Telegram failed 5 time(s)")

    result = asyncio.run(
        strategy._send_with_retries(_neterr, what="stub", payload_bytes=0))
    ok(result.ok, "network error then success → SendResult.ok")
    ok(stub2.disconnects == 1 and stub2.connects == 1,
       "ConnectionError triggers ONE explicit _force_reconnect before the retry "
       "(sole reconnect authority; Telethon auto_reconnect is off)")


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

    # Video sends now funnel through the parallel uploader: the strategy
    # upload_file()s the bytes (→ a handle) and send_file()s an
    # InputMediaUploadedDocument. The fake records every upload and the media so
    # we can introspect what actually went to Telegram.
    class _Handle:
        def __init__(self, path): self.path = str(path)

    class _CaptureClient:
        def __init__(self):
            self.uploaded: list[str] = []   # every upload_file path (file+thumbs)
            self.sent: list = []            # InputMedia objects → send_file
        async def upload_file(self, file, **kw):
            self.uploaded.append(str(file))
            return _Handle(file)
        async def send_file(self, peer, file, **kw):
            self.sent.append(file)
        async def disconnect(self): ...
        async def connect(self): ...

    def media_file(m) -> str | None:        # the uploaded bytes behind a media
        return getattr(getattr(m, "file", None), "path", None)

    def media_name(m) -> str | None:        # explicit DocumentAttributeFilename
        for a in (getattr(m, "attributes", None) or []):
            if isinstance(a, tg_types.DocumentAttributeFilename):
                return a.file_name
        return None

    def is_document(m) -> bool:             # a document has NO video attribute
        attrs = getattr(m, "attributes", None) or []
        return not any(isinstance(a, tg_types.DocumentAttributeVideo)
                       for a in attrs)

    # (a) a non-streamable .flv → Telegram receives a CONVERTED .mp4.
    flv = _make_video(tmp / "u" / "clip.flv", container="flv")
    cap = _CaptureClient()
    strategy._client = cap                      # bypass __aenter__: no network
    res = asyncio.run(strategy.send(peer="p", file_path=str(flv), caption="c"))
    ok(res.ok and len(cap.sent) == 1, "non-streamable .flv send succeeded")
    wire = media_file(cap.sent[0])
    ok(wire and wire.endswith(".mp4") and wire != str(flv),
       "dispatcher converted .flv → streamable .mp4 before the Telegram send")
    eff_name = media_name(cap.sent[0]) or Path(wire).name
    ok(eff_name == "clip.mp4",
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
    ok(res2.ok and media_file(cap2.sent[0]) == str(mp4),
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
    ok(res3.ok and media_file(cap3.sent[0]) == str(mkv),
       "ensure_streamable=False ships the original .mkv as-is (no conversion)")
    ok(not (tmp / "u" / "keep.mp4").exists(),
       "no conversion temp created when the net is skipped")
    ok(is_document(cap3.sent[0]),
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
    ok(res4.ok and not is_document(cap4.sent[0]),
       "a streamable as-is .mp4 still ships as a normal video, not a document")

    # (e) an as-is streamable file stored with the internal ".tgprep" marker
    # (an incompatible-codec .mp4 converted in place at ingest) must upload with
    # a CLEAN name — the tag never reaches Telegram, even on the as-is path.
    tagged = _make_video(tmp / "u" / "show.tgprep.mp4", container="mp4")
    cap5 = _CaptureClient()
    strategy._client = cap5
    res5 = asyncio.run(strategy.send(
        peer="p", file_path=str(tagged), caption="c", ensure_streamable=False))
    ok(res5.ok and media_file(cap5.sent[0]) == str(tagged),
       "the real .tgprep file on disk is what gets uploaded")
    ok(media_name(cap5.sent[0]) == "show.mp4",
       "but Telegram is told the clean name 'show.mp4' (no .tgprep leak)")

    # (f) ALBUM path: _send_video_album_fast uploads each item with
    # attributes=None (an explicit DocumentAttributeFilename would break Telegram
    # grouping), so the ".tgprep" strip must happen on the DERIVED filename attr
    # inside _upload_document — otherwise a split album's ".tgprep" part leaks its
    # on-disk name to the chat. Drive _upload_document directly with attributes=
    # None (exactly how the album path calls it) and confirm the wire name is clean.
    cap6 = _CaptureClient()
    strategy._client = cap6
    album_item = asyncio.run(strategy._upload_document(
        str(tagged), attributes=None, thumb_path=None,
        supports_streaming=True, force_document=False,
        progress_cb=None))
    ok(media_name(album_item) == "show.mp4",
       "album item (attributes=None) also gets the clean name — no .tgprep leak")


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
# Shared drain runner for the destination/grouping seams below (22–24). Drives
# the real drain_forever to quiescence against a _FakeSend, same pattern as
# Seam 10/21 but factored out so each new seam reads as just its setup+asserts.
# ══════════════════════════════════════════════════════════════════════════════

def _drain_once(db_file: str, ps, fake: "_FakeSend", *,
                default_chat_id: str, sanitizer=None) -> None:
    from core import (ItemStore, DeletePolicy, RecorderDeletePolicy,
                      BatchPolicy, DeletionGuard)
    from dispatcher.drain import drain_forever
    from dispatcher.config import DispatcherConfig
    from dispatcher.tg_router import TelegramRouter

    cfg_kwargs = dict(
        telegram=None, default_chat_id=default_chat_id, db_path=db_file,
        policy_store=ps, poll_interval_s=0.01, max_retries=3,
        inter_album_sleep=0.0, stuck_claim_min=10, failed_retention_days=0,
    )
    if sanitizer is not None:
        cfg_kwargs["sanitizer"] = sanitizer
    cfg = DispatcherConfig(**cfg_kwargs)
    store = ItemStore.open(db_file)
    router = TelegramRouter(default_chat_id=default_chat_id)
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

    try:
        asyncio.run(_run())
    finally:
        store.close()


# ══════════════════════════════════════════════════════════════════════════════
# Seam 22 — forum-topic routing end-to-end: a `<chat>.t<topic>` folder name →
# core.parse_route → add_item(topic_id) → claim_batch's destination discriminator
# (chat_id + topic_id) → drain dest resolution → fake send's reply_to. The trap
# this guards: two folders for the SAME chat but DIFFERENT topics whose subfolders
# share a name produce the SAME group_key ('<chat>/<sub>'), so ONLY the topic_id
# discriminator keeps them from wrongly albuming into one cross-topic message.
# ══════════════════════════════════════════════════════════════════════════════

def test_topic_routing_seam(tmp: Path) -> None:
    section("Seam 22: forum-topic routing (.t<topic> folder → reply_to)")
    from core import ItemStore, PolicyStore, BatchPolicy
    from core.orphaned import ingest_chat_id_dirs

    # Same chat, two topics, identically-named subfolders ('g') → colliding
    # group_key '<chat>/g'. Topic is the only thing that separates them.
    _write_media(tmp / "-100222.t77" / "g" / "a.jpg", b"AA")
    _write_media(tmp / "-100222.t77" / "g" / "b.jpg", b"BB")
    _write_media(tmp / "-100222.t88" / "g" / "c.jpg", b"CC")
    _write_media(tmp / "-100222.t88" / "g" / "d.jpg", b"DD")

    db_file = str(tmp / "seam22.db")
    store = ItemStore.open(db_file)
    reports = ingest_chat_id_dirs(store, tmp, known_platforms=set())
    inserted = sum(r.inserted for r in reports)
    ok(inserted == 4, "4 loose files ingested from two .t<topic> folders")
    paths = [tmp / "-100222.t77" / "g" / "a.jpg",
             tmp / "-100222.t77" / "g" / "b.jpg",
             tmp / "-100222.t88" / "g" / "c.jpg",
             tmp / "-100222.t88" / "g" / "d.jpg"]
    rows = [store.get(store.id_of(str(p))) for p in paths]
    topics = sorted(r.topic_id for r in rows)
    ok(topics == [77, 77, 88, 88],
       "parse_route carried each file's topic_id onto its row (77/77, 88/88)")
    ok(all(r.chat_id == "-100222" for r in rows),
       "the .t<topic> suffix is stripped from chat_id (dest is the bare chat)")
    store.close()

    ps = PolicyStore()
    ps.set(BatchPolicy.SIZE_KEY, 1)
    fake = _FakeSend()
    _drain_once(db_file, ps, fake, default_chat_id="-100999")

    ok(len(fake.sent_albums) == 2,
       "two albums sent — the shared group_key did NOT merge across topics")
    by_topic = {t: sorted(Path(p).name for p in a)
                for t, a in zip(fake.album_topics, fake.sent_albums)}
    ok(by_topic.get(77) == ["a.jpg", "b.jpg"],
       "topic 77 album = a.jpg+b.jpg, sent with reply_to=77")
    ok(by_topic.get(88) == ["c.jpg", "d.jpg"],
       "topic 88 album = c.jpg+d.jpg, sent with reply_to=88")
    ok(None not in fake.album_topics,
       "every topic-routed album carried a non-None reply_to (no General leak)")


# ══════════════════════════════════════════════════════════════════════════════
# Seam 23 — split-part albums: an oversize original split into parts shares ONE
# core.split_group_key, so claim_batch albums the parts together (despite each
# part carrying a different per-part caption), and the drain's min-batch gate is
# EXEMPT for a split group (flush the complete unit immediately, never hold it
# waiting for the archiver batch size). Guards the contract between core.grouping,
# core.store.claim_batch, and dispatcher.drain.
# ══════════════════════════════════════════════════════════════════════════════

def test_split_album_seam(tmp: Path) -> None:
    section("Seam 23: split-part albums (shared group_key, gate-exempt flush)")
    from core import ItemStore, PolicyStore, BatchPolicy, split_group_key
    from core.hashing import full_hash

    gk = split_group_key("x", "alice", "bigvideo")
    db_file = str(tmp / "seam23.db")
    store = ItemStore.open(db_file)
    parts = []
    for n in range(3):
        # Part 1 carries the internal ".tgprep" marker on disk (a remuxed copy):
        # it must ship as that real file but be NAMED cleanly in the caption.
        stem = f"bigvideo_part{n:03d}" + (".tgprep" if n == 1 else "")
        f = _write_media(tmp / "x" / "alice" / f"{stem}.mp4", bytes([n]) * 300)
        parts.append(f)
        # Distinct per-part captions on purpose: only the shared group_key may
        # bind them, never a coincidentally-equal caption.
        store.add_item(source="archiver", platform="x", username="alice",
                       identifier=f"bigvideo_p{n}", file_path=str(f),
                       priority=10, group_key=gk, caption=f"part {n}",
                       content_hash=full_hash(f))
    store.close()

    ps = PolicyStore()
    # Archiver min-batch gate set HIGH: a non-split group of 3 would be deferred.
    # The split exemption is the only reason these flush.
    ps.set(BatchPolicy.SIZE_KEY, 10)
    fake = _FakeSend()
    _drain_once(db_file, ps, fake, default_chat_id="-100123")

    ok(len(fake.sent_albums) == 1 and len(fake.sent_albums[0]) == 3,
       "all 3 split parts shipped as ONE album despite min_batch=10 (gate-exempt)")
    ok(sorted(Path(p).name for p in fake.sent_albums[0]) ==
       ["bigvideo_part000.mp4", "bigvideo_part001.tgprep.mp4",
        "bigvideo_part002.mp4"],
       "the album ships the real files incl. the .tgprep-marked part")
    cap = fake.album_captions[0]
    ok(cap == "bigvideo_part000\nbigvideo_part001\nbigvideo_part002",
       "caption lists clean part stems — the .tgprep marker is stripped, not leaked")
    store = ItemStore.open(db_file)
    ok(store.counts_by_status().get("sent", 0) == 3,
       "all three part rows are terminal 'sent'")
    store.close()


# ══════════════════════════════════════════════════════════════════════════════
# Seam 24 — banned-word sanitizer at send: a configured Sanitizer on the
# dispatcher config strips banned words from the caption the drain builds, before
# the send sees it — while the on-disk file is left untouched. Guards the contract
# between core.sanitize and dispatcher.drain's caption path.
# ══════════════════════════════════════════════════════════════════════════════

def test_banned_word_sanitizer_seam(tmp: Path) -> None:
    section("Seam 24: banned-word sanitizer strips caption at send")
    from core import ItemStore, PolicyStore, Sanitizer, ProtectionPolicy
    from core.hashing import full_hash

    db_file = str(tmp / "seam24.db")
    store = ItemStore.open(db_file)
    # An orphaned single → caption is its own stem (drain's orphaned_caption).
    photo = _write_media(tmp / "-100777" / "meetup badword tonight.jpg", b"PH")
    store.add_item(source="orphaned", platform="orphaned", username="-100777",
                   identifier="orph_1", file_path=str(photo), priority=6,
                   caption="meetup badword tonight", chat_id="-100777",
                   content_hash=full_hash(photo))
    store.close()

    ps = PolicyStore()
    # Protect this orphaned scope so the orphaned ship-and-delete (file + row)
    # is suppressed — this seam is about the sanitizer NOT mutating disk, which
    # is orthogonal to the post-send cleanup. With the safebrake on, the file
    # persists, so the `photo.exists()` check still genuinely catches a sanitizer
    # that would rename or rewrite the source.
    ps.set(ProtectionPolicy.KEY, True, platform="orphaned", username="-100777")
    fake = _FakeSend()
    _drain_once(db_file, ps, fake, default_chat_id="-100777",
                sanitizer=Sanitizer(["badword"]))

    ok(len(fake.single_captions) == 1, "the orphaned photo sent as one single")
    cap = fake.single_captions[0]
    ok("badword" not in cap,
       "the banned word was stripped from the caption before the send")
    ok("meetup" in cap and "tonight" in cap,
       "only the banned token was removed; the rest of the caption survives")
    ok(photo.exists(),
       "the on-disk file is untouched (sanitizer rewrites the message, not disk)")


# ══════════════════════════════════════════════════════════════════════════════
# Seam 25 — dispatcher housekeeping owns failed-queue maintenance
# Failed-upload lifecycle (retire dead tombstones + re-queue the rest) lives
# ENTIRELY in the dispatcher's periodic housekeeping (drain.run_housekeeping),
# the queue owner — not in the archiver loop. The contract, three ways:
#   • a 'failed' row whose file is GONE is deleted every pass (always, no policy)
#   • the REMAINING (present-file) failed rows are re-queued only when
#     auto_retry_failed is on (default OFF — opt-in, so a poison row can't
#     re-arm itself into a perpetual re-upload storm)
#   • delete-first ordering ⇒ a missing-file row is never re-armed to 'pending'
#     and so never costs a wasted send attempt on a vanished path
# Plus the retention backstop still caps present-file rows stuck past the window.
# A regression here either resurrects vanished files into wasted sends or
# silently stops failed uploads from ever retrying.
# ══════════════════════════════════════════════════════════════════════════════

def test_failed_housekeeping_seam(tmp: Path) -> None:
    section("Seam 25: dispatcher housekeeping ↔ failed-queue maintenance")
    from core import PolicyStore, FailedRetryPolicy
    from dispatcher.drain import run_housekeeping
    from dispatcher.config import DispatcherConfig

    def _cfg(db_path: str, ps: PolicyStore, retention: float):
        # Only telegram/default_chat_id/db_path/policy_store are required; the
        # rest default. failed_retention_days drives the retention backstop.
        return DispatcherConfig(
            telegram=None, default_chat_id="-1", db_path=db_path,
            policy_store=ps, failed_retention_days=retention)

    def _make_failed(db, where: Path, *, present: bool, ident: str) -> str:
        fp = where / f"{ident}.mp4"
        if present:
            _write_media(fp, ident.encode())
        db.add_item(source="archiver", platform="x", username="al",
                    identifier=ident, file_path=str(fp), priority=10)
        db.conn.execute("UPDATE items SET status='failed', attempts=3 "
                        "WHERE id=?", (db.id_of(str(fp)),))
        db.conn.commit()
        return str(fp)

    def _status(db, fp):
        r = db.get(db.id_of(fp)) if db.id_of(fp) is not None else None
        return r.status if r else None

    # ── auto_retry ON (opt-in): present → re-queued, missing → deleted ──
    db = _fresh_db()
    db_path = _db_file(db)
    try:
        present = _make_failed(db, tmp / "on", present=True, ident="keep")
        missing = _make_failed(db, tmp / "on", present=False, ident="gone")
        ps = PolicyStore(); ps.set(FailedRetryPolicy.KEY, True)
        run_housekeeping(db, _cfg(db_path, ps, retention=0))  # ON, prune off
        ok(_status(db, present) == "pending",
           "auto_retry ON: present-file failed row re-queued to pending")
        ok(db.id_of(missing) is None,
           "missing-file failed row DELETED (can never deliver), not re-queued")
    finally:
        db.close()

    # ── auto_retry OFF: present stays failed, missing still deleted ──
    db = _fresh_db()
    db_path = _db_file(db)
    try:
        present = _make_failed(db, tmp / "off", present=True, ident="keep")
        missing = _make_failed(db, tmp / "off", present=False, ident="gone")
        ps = PolicyStore(); ps.set(FailedRetryPolicy.KEY, False)
        run_housekeeping(db, _cfg(db_path, ps, retention=0))
        ok(_status(db, present) == "failed",
           "auto_retry OFF: present-file failed row left for a manual reset")
        ok(db.id_of(missing) is None,
           "missing-file cleanup runs even with auto_retry OFF (unconditional)")
    finally:
        db.close()

    # ── retention backstop: an old present-file failed row is pruned ──
    db = _fresh_db()
    db_path = _db_file(db)
    try:
        old = _make_failed(db, tmp / "ret", present=True, ident="stale")
        # Backdate discovered_at past the window so prune_failed catches it;
        # auto_retry OFF keeps it 'failed' (else it'd be re-queued, not pruned).
        db.conn.execute("UPDATE items SET discovered_at='2000-01-01T00:00:00Z' "
                        "WHERE id=?", (db.id_of(old),))
        db.conn.commit()
        ps = PolicyStore(); ps.set(FailedRetryPolicy.KEY, False)
        run_housekeeping(db, _cfg(db_path, ps, retention=7))
        ok(db.id_of(old) is None,
           "retention backstop prunes a present-file row stuck past the window")
    finally:
        db.close()

    # ── backstop wins over auto_retry: an old row is PRUNED, not resurrected ──
    # The conflict guard. With auto_retry ON, prune MUST run before the re-queue;
    # otherwise reset_failed would move the row to pending first and the cap
    # would never fire (a permanent failure cycling forever — the storm).
    db = _fresh_db()
    db_path = _db_file(db)
    try:
        old = _make_failed(db, tmp / "storm", present=True, ident="stale")
        db.conn.execute("UPDATE items SET discovered_at='2000-01-01T00:00:00Z' "
                        "WHERE id=?", (db.id_of(old),))
        db.conn.commit()
        ps = PolicyStore(); ps.set(FailedRetryPolicy.KEY, True)
        run_housekeeping(db, _cfg(db_path, ps, retention=7))
        ok(db.id_of(old) is None,
           "auto_retry ON: a row failing past the window is pruned, not re-armed "
           "(prune-before-reorder prevents the perpetual-retry storm)")
    finally:
        db.close()

    # ── a manually-cancelled row survives auto_retry; retry(id) forces it back ──
    # cancel() parks the row in 'failed' with CANCELLED_MARKER. A deliberate
    # abort must NOT be resurrected by auto_retry's bulk reset_failed — only the
    # targeted retry(id) override (which clears last_error) brings it back.
    db = _fresh_db()
    db_path = _db_file(db)
    try:
        fp = tmp / "cancel" / "abort.mp4"
        _write_media(fp, b"abort")
        db.add_item(source="archiver", platform="x", username="al",
                    identifier="abort", file_path=str(fp), priority=10)
        cid = db.id_of(str(fp))
        ok(db.cancel(cid), "cancel: pending row parked as a manual abort")
        ps = PolicyStore(); ps.set(FailedRetryPolicy.KEY, True)
        run_housekeeping(db, _cfg(db_path, ps, retention=0))  # auto_retry ON
        ok(db.get(cid).status == "failed",
           "auto_retry ON does NOT resurrect a cancelled row (CANCELLED_MARKER)")
        ok(db.retry(cid) and db.get(cid).status == "pending",
           "targeted retry(id) overrides cancel and re-arms the row")
    finally:
        db.close()

    # ── TRANSIENT auto-recovery (default ON, storm-safe) ──────────────────────
    # A failure with a transient cause (network / upload corruption) heals on the
    # ~15-min cadence with NO opt-in, while a PERMANENT one (Telegram rejecting
    # the media) stays quarantined — so the poison-row storm that forced
    # auto_retry_failed off never happens. This is the safe-by-default self-heal.
    from core import is_transient_failure, CANCELLED_MARKER
    ok(is_transient_failure("ConnectionError: Connection to Telegram failed 5 time(s)"),
       "classifier: ConnectionError is transient")
    ok(not is_transient_failure("FilePartsInvalidError: The number of file parts is invalid"),
       "classifier: FilePartsInvalidError is PERMANENT (removed from transient "
       "signatures — auto-retry was storming; see upload-ceiling fix)")
    ok(not is_transient_failure("ImageProcessFailedError: Failure while processing image"),
       "classifier: ImageProcessFailedError is PERMANENT (poison — never auto-armed)")
    ok(not is_transient_failure("file missing on disk: /x/y.mp4"),
       "classifier: missing-file is PERMANENT")
    ok(not is_transient_failure(None) and not is_transient_failure(""),
       "classifier: unknown/empty defaults to PERMANENT (conservative)")
    ok(not is_transient_failure(CANCELLED_MARKER + " by user"),
       "classifier: a manual abort is never transient")

    def _fail_with(db, where: Path, ident: str, err: str) -> str:
        fp = where / f"{ident}.mp4"
        _write_media(fp, ident.encode())
        db.add_item(source="archiver", platform="x", username="al",
                    identifier=ident, file_path=str(fp), priority=10)
        db.conn.execute("UPDATE items SET status='failed', attempts=3, last_error=? "
                        "WHERE id=?", (err, db.id_of(str(fp))))
        db.conn.commit()
        return str(fp)

    db = _fresh_db()
    db_path = _db_file(db)
    try:
        trans = _fail_with(db, tmp / "tr", "neterr",
                           "ConnectionError: Connection to Telegram failed 5 time(s)")
        perm  = _fail_with(db, tmp / "tr", "imgerr",
                           "ImageProcessFailedError: Failure while processing image")
        ps = PolicyStore(); ps.set(FailedRetryPolicy.KEY, False)   # opt-in OFF
        run_housekeeping(db, _cfg(db_path, ps, retention=7))
        ok(_status(db, trans) == "pending",
           "auto_retry OFF: a TRANSIENT failure self-heals to pending (default on)")
        ok(_status(db, perm) == "failed",
           "auto_retry OFF: a PERMANENT failure stays quarantined (no poison storm)")
    finally:
        db.close()

    # The cancelled row must survive the transient sweep too (belt-and-braces:
    # CANCELLED_MARKER is excluded by the classifier, not just _reset_to_pending).
    db = _fresh_db()
    db_path = _db_file(db)
    try:
        fp = tmp / "trc" / "abort.mp4"
        _write_media(fp, b"abort")
        db.add_item(source="archiver", platform="x", username="al",
                    identifier="abort2", file_path=str(fp), priority=10)
        cid = db.id_of(str(fp))
        db.cancel(cid)
        ps = PolicyStore(); ps.set(FailedRetryPolicy.KEY, False)
        run_housekeeping(db, _cfg(db_path, ps, retention=0))
        ok(db.get(cid).status == "failed",
           "transient sweep does NOT resurrect a cancelled row")
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Seam 26 — producer enqueue order ↔ dispatcher claim_batch SEND ORDER
# A user's media must drain CONTIGUOUSLY, not interleave with other users'.
# The queue's raw (priority, discovered_at) order scatters a user whose files
# arrive across multiple runs: files downloaded later get a later discovered_at,
# so another user who appeared in between would sort ahead of them. claim_batch
# instead anchors each (platform, username) cluster on its FIRST-APPEARANCE time
# (core.store._CLUSTER_COLS), so later-enqueued files join the user's existing
# block. The contract holds across the cases that actually broke — single-bucket
# items (one send each) and >ALBUM_MAX runs (multiple albums) — while priority
# still dominates (a live recorder cluster drains before an archiver backlog).
# A regression here re-scatters a user's uploads across the timeline.
# ══════════════════════════════════════════════════════════════════════════════

def test_send_order_clustering_seam(tmp: Path) -> None:
    section("Seam 26: enqueue order ↔ claim_batch send-order clustering")
    from core import ItemStore

    def _seed(plan, ext: str) -> ItemStore:
        """plan: [(username, count, discovered_at)] — simulates same-user files
        arriving in separate runs, interleaved with another user's run."""
        db = _fresh_db()
        k = 0
        for user, count, ts in plan:
            for _ in range(count):
                f = _write_media(tmp / f"{ext[1:]}/{user}/{user}_{k}{ext}",
                                 f"{user}{k}".encode())
                db.add_item(source="archiver", platform="x", username=user,
                            identifier=f"{user}_{k}", file_path=str(f), priority=10)
                db.conn.execute("UPDATE items SET discovered_at=? WHERE id=?",
                                (ts, db.id_of(str(f))))
                k += 1
        db.conn.commit()
        return db

    def _drain(db) -> list:
        out = []
        while (b := db.claim_batch(max_items=10)):
            out.append((b[0].username, len(b)))
        return out

    # alice appears first (run @:01), bob runs in between (@:02), alice runs
    # again (@:03). Without clustering, alice's :03 files sort after bob's :02.
    interleaved = [("alice", 2, "2024-01-01T00:00:01Z"),
                   ("bob",   2, "2024-01-01T00:00:02Z"),
                   ("alice", 2, "2024-01-01T00:00:03Z")]

    # Single-bucket (.gif): each file is its own send; the recompute-prone case.
    db = _seed(interleaved, ".gif")
    try:
        ok(_drain(db) == [("alice", 1)] * 4 + [("bob", 1)] * 2,
           "single-bucket: all of alice's files drain before bob's (contiguous)")
    finally:
        db.close()

    # Album bucket, over the 10-item cap: alice 8@:01 + 5@:03 = 13 photos, bob
    # 3@:02 between. Alice's two albums must stay adjacent, ahead of bob.
    db = _seed([("alice", 8, "2024-01-01T00:00:01Z"),
                ("bob",   3, "2024-01-01T00:00:02Z"),
                ("alice", 5, "2024-01-01T00:00:03Z")], ".jpg")
    try:
        ok(_drain(db) == [("alice", 10), ("alice", 3), ("bob", 3)],
           "over-cap: alice's two albums drain back-to-back, then bob")
    finally:
        db.close()

    # Priority still dominates the cluster anchor: a recorder single (priority 5)
    # for a user who appeared LATE (@09:00) must still precede alice's pri-10
    # backlog that appeared at :01.
    db = _seed([("alice", 2, "2024-01-01T00:00:01Z")], ".jpg")
    try:
        rec = _write_media(tmp / "rec" / "zoe.mp4", b"REC")
        db.add_item(source="recorder", platform="tiktok", username="zoe",
                    identifier="rec_1", file_path=str(rec), priority=5)
        db.conn.execute("UPDATE items SET discovered_at='2024-01-01T09:00:00Z' "
                        "WHERE id=?", (db.id_of(str(rec)),))
        db.conn.commit()
        ok(_drain(db) == [("zoe", 1), ("alice", 2)],
           "priority wins over first-appearance: recorder pri-5 cluster drains first")
    finally:
        db.close()


def test_video_metadata_backend_seam(tmp: Path) -> None:
    section("Seam 27: video-metadata backend (album videos must NOT ship as 1x1 images)")
    # ROOT-CAUSE REGRESSION GUARD. The native video-ALBUM send (send.send_album)
    # passes NO explicit attributes and relies on Telethon to derive each item's
    # width/height/duration itself. Telethon can only do that with `hachoir`
    # installed; without it it emits DocumentAttributeVideo(w=1, h=1, duration=0)
    # and Telegram renders every album video as a 1x1 static IMAGE. That bug
    # shipped silently for days because no test exercised Telethon's own metadata
    # path — single sends attach explicit ffprobe attributes and so masked it.
    from telethon import utils
    from telethon.tl import types as tg_types

    # 1. The dispatcher refuses to start without the backend (integrity-first).
    from dispatcher.cli import _assert_video_metadata_backend
    _assert_video_metadata_backend()
    ok(True, "startup guard passes when the video-metadata backend is present")

    import importlib.util
    real_find_spec = importlib.util.find_spec
    importlib.util.find_spec = lambda n, *a, **k: (
        None if n == "hachoir" else real_find_spec(n, *a, **k))
    try:
        raised = False
        try:
            _assert_video_metadata_backend()
        except RuntimeError:
            raised = True
        ok(raised, "startup guard FAILS FAST when the backend is missing")
    finally:
        importlib.util.find_spec = real_find_spec

    # 2. End-to-end proof: Telethon derives REAL geometry for a real mp4 — the
    #    exact call the album path leans on. A 160x120/1s clip must come back as
    #    a video attribute with those non-degenerate dims, never the 1x1/0s stub.
    if not _ffmpeg_present():
        ok(True, "ffmpeg absent — real-geometry probe skipped (toolchain missing)")
        return
    mp4 = _make_video(tmp / "clip.mp4", container="mp4")
    attrs, mime = utils.get_attributes(str(mp4))
    vattr = next((a for a in attrs
                  if isinstance(a, tg_types.DocumentAttributeVideo)), None)
    ok(vattr is not None, "Telethon attaches a DocumentAttributeVideo to the mp4")
    ok(mime == "video/mp4", "mime resolves to video/mp4 (sent as video, not photo)")
    ok(not (vattr.w <= 1 and vattr.h <= 1 and vattr.duration <= 0),
       f"geometry is real, not the 1x1/0s stub (w={vattr.w} h={vattr.h} "
       f"dur={vattr.duration}) — album videos render as videos")
    ok(vattr.w == 160 and vattr.h == 120,
       f"derived dimensions match the source (160x120, got {vattr.w}x{vattr.h})")


# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# Seam 28 — `#hashtag` folders are VIRTUAL ROOTS: a file directly inside a
# `chat_id/#tag/` folder uploads INDIVIDUALLY (like a file in the chat_id root),
# captioned `#tag file_name`; a deeper subfolder `chat_id/#tag/sub/` is still an
# album (`#tag sub` header). A non-hashtag subfolder is unchanged (albums).
# Guards the contract between core.orphaned._route_for and drain.orphaned_caption.
# ══════════════════════════════════════════════════════════════════════════════

def test_hashtag_root_seam(tmp: Path) -> None:
    section("Seam 28: #hashtag folders are virtual roots (individual vs album)")
    from core import ItemStore, PolicyStore, BatchPolicy
    from core.orphaned import ingest_chat_id_dirs

    # Directly under #JAV → individual; under #Asian/Eli Shaw → album; under a
    # plain (non-hashtag) Nainoi folder → album (existing behavior, unchanged).
    _write_media(tmp / "-100333" / "#JAV" / "one.jpg", b"J1")
    _write_media(tmp / "-100333" / "#JAV" / "two.jpg", b"J2")
    _write_media(tmp / "-100333" / "#Asian" / "Eli Shaw" / "a.jpg", b"AA")
    _write_media(tmp / "-100333" / "#Asian" / "Eli Shaw" / "b.jpg", b"BB")
    _write_media(tmp / "-100333" / "Nainoi" / "x.jpg", b"XX")
    _write_media(tmp / "-100333" / "Nainoi" / "y.jpg", b"YY")

    db_file = str(tmp / "seam28.db")
    store = ItemStore.open(db_file)
    ingest_chat_id_dirs(store, tmp, known_platforms=set())
    jav = [store.get(store.id_of(str(tmp / "-100333" / "#JAV" / n)))
           for n in ("one.jpg", "two.jpg")]
    ok(all(r.group_key is None for r in jav),
       "files directly in #JAV have NO group_key (upload individually)")
    eli = store.get(store.id_of(str(tmp / "-100333" / "#Asian" / "Eli Shaw" / "a.jpg")))
    ok(eli.group_key == "-100333/#Asian/Eli Shaw",
       "a file under #Asian/Eli Shaw albums by its full subpath")
    nai = store.get(store.id_of(str(tmp / "-100333" / "Nainoi" / "x.jpg")))
    ok(nai.group_key == "-100333/Nainoi",
       "a plain (non-hashtag) subfolder still albums — behavior unchanged")
    store.close()

    ps = PolicyStore()
    ps.set(BatchPolicy.SIZE_KEY, 1)
    fake = _FakeSend()
    _drain_once(db_file, ps, fake, default_chat_id="-100999")

    ok(sorted(Path(p).name for p in fake.sent_singles) == ["one.jpg", "two.jpg"],
       "the two #JAV files were sent as individual singles, not an album")
    ok(sorted(fake.single_captions) == ["#JAV one", "#JAV two"],
       "each individual caption is '#JAV <file>' (hashtag stays clickable)")
    album_caps = sorted(fake.album_captions)
    ok(any(c.startswith("#Asian Eli Shaw") for c in album_caps),
       "#Asian/Eli Shaw album header is space-joined '#Asian Eli Shaw'")
    ok(any(c.startswith("Nainoi") for c in album_caps),
       "the plain Nainoi album header is just 'Nainoi'")


def test_orphaned_mixed_album_seam(tmp: Path) -> None:
    section("Seam 30: chat_id folders — mixed photo+video album, grouped docs, media-first")
    from core import ItemStore, PolicyStore, BatchPolicy

    db_file = str(tmp / "seam30.db")
    db = ItemStore.open(db_file)
    gk = "-100777/trip"

    def _add(name: str, payload: bytes) -> None:
        f = _write_media(tmp / "-100777" / "trip" / name, payload)
        stem = Path(name).stem
        db.add_item(source="orphaned", platform="orphaned", username="orphaned",
                    identifier=f"trip_{stem}", file_path=str(f),
                    chat_id="-100777", group_key=gk, priority=50)

    # Documents (.mkv) enqueued FIRST → earlier discovered_at. The media-first
    # ordering must still hold them behind the subfolder's inline media.
    _add("orig0.mkv", b"MKV0")
    _add("orig1.mkv", b"MKV1")
    # Inline media: 3 photos + 2 videos → must collapse into ONE mixed album
    # (NOT split into a photo album + a video album).
    for i in range(3):
        _add(f"p{i}.jpg", f"PH{i}".encode())
    for i in range(2):
        _add(f"v{i}.mp4", f"VID{i}".encode())

    ps = PolicyStore()
    ps.set(BatchPolicy.SIZE_KEY, 1)
    fake = _FakeSend()
    _drain_once(db_file, ps, fake, default_chat_id="-100999")
    db.close()

    ok(len(fake.sent_albums) == 2,
       "exactly two albums: one mixed media album + one document album (not 3)")
    media_suffixes = {Path(p).suffix for p in fake.sent_albums[0]}
    ok(len(fake.sent_albums[0]) == 5 and media_suffixes == {".jpg", ".mp4"},
       "media album mixes 3 photos + 2 videos in ONE group of 5")
    ok(all(Path(p).suffix == ".mkv" for p in fake.sent_albums[1])
       and len(fake.sent_albums[1]) == 2,
       "the two .mkv documents are grouped into one document album")
    ok(fake.album_as_documents == [False, True],
       "media shipped inline FIRST, documents shipped as_documents SECOND "
       "(even though the .mkv were enqueued earlier)")


def test_burner_account_seam() -> None:
    section("Seam 29: optional burner account (config → routing → send seam)")
    from dispatcher.config import BurnerCreds, TelegramCreds
    from dispatcher.send import TelethonSendStrategy
    from dispatcher import tg_router

    # ── config seam: env round-trip through BurnerCreds.from_env ───────────
    primary = TelegramCreds(api_id=111, api_hash="ph", phone="+1",
                            session_name="/tmp/claude-seam-primary")
    for k in ("BURNER_CHAT_IDS", "TELEGRAM_BURNER_SESSION",
              "TELEGRAM_BURNER_PHONE", "TELEGRAM_BURNER_API_ID",
              "TELEGRAM_BURNER_API_HASH"):
        os.environ.pop(k, None)
    ok(BurnerCreds.from_env(primary) is None,
       "no burner env → None (pipeline untouched when unconfigured)")

    os.environ["BURNER_CHAT_IDS"] = "-100555, 777"     # dash-free 777 → -777
    os.environ["TELEGRAM_BURNER_SESSION"] = "/tmp/claude-seam-burner"
    os.environ["TELEGRAM_BURNER_PHONE"] = "+2"
    burner = BurnerCreds.from_env(primary)
    ok(burner is not None and burner.chat_ids == frozenset({"-100555", "-777"}),
       "configured burner normalizes its dedicated chat set")
    ok(burner.api_id == 111 and burner.api_hash == "ph",
       "burner inherits the primary's api creds when its own are unset")

    # ── routing seam: send()/check_destination pick the right client ──────
    class _FakeClient:
        def __init__(self, tag):
            self.tag = tag
            self.entities = 0
            self.disconnects = 0
            self.connects = 0
        async def get_entity(self, peer):
            self.entities += 1
            return type("E", (), {"title": self.tag, "id": 1})()
        async def disconnect(self):
            self.disconnects += 1
        async def connect(self):
            self.connects += 1

    primary_client = _FakeClient("primary")
    burner_client = _FakeClient("burner")

    strat = TelethonSendStrategy(
        api_id=111, api_hash="ph", phone="+1",
        session_name="/tmp/claude-seam-primary", burner=burner)
    strat._client = primary_client
    # stub the burner build+connect so no real Telethon/network is touched
    strat._build_client = lambda *a, **k: burner_client
    async def _noconnect(client, session, phone):
        await client.connect()
    strat._connect_authorized = _noconnect

    # dedicated chat → burner comes up lazily and answers
    res_ok, _ = asyncio.run(
        strat.check_destination(peer=tg_router.Destination("-100555").peer))
    ok(res_ok and burner_client.entities == 1 and primary_client.entities == 0,
       "dedicated chat routes through the burner client")
    ok(strat._burner_client is burner_client and burner_client.connects == 1,
       "burner built + connected lazily on first dedicated use")

    # non-dedicated chat → primary, burner untouched by the new call
    res_ok, _ = asyncio.run(
        strat.check_destination(peer=tg_router.Destination("-100999").peer))
    ok(res_ok and primary_client.entities == 1 and burner_client.entities == 1,
       "non-dedicated chat stays on the primary client")

    # _force_reconnect re-homes whichever account the in-flight send uses
    strat._active_client = burner_client
    asyncio.run(strat._force_reconnect())
    ok(burner_client.disconnects == 1 and primary_client.disconnects == 0,
       "reconnect during a burner-routed send recycles the burner socket")

    # ── fallback seam: a burner that can't authorize falls back to primary ─
    strat2 = TelethonSendStrategy(
        api_id=111, api_hash="ph", phone="+1",
        session_name="/tmp/claude-seam-primary", burner=burner)
    strat2._client = primary_client
    strat2._build_client = lambda *a, **k: _FakeClient("deadburner")
    async def _fail(client, session, phone):
        raise RuntimeError("unauthorized")
    strat2._connect_authorized = _fail
    chosen = asyncio.run(strat2._client_for(tg_router.Destination("-100555").peer))
    ok(chosen is primary_client and strat2._burner_client is None,
       "unauthorized burner falls back to the primary (delivery never blocked)")

    for k in ("BURNER_CHAT_IDS", "TELEGRAM_BURNER_SESSION",
              "TELEGRAM_BURNER_PHONE"):
        os.environ.pop(k, None)


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
        test_media_empty_quarantine_seam(tmp / "s11")
        test_circuit_breaker_seam(tmp / "s11b")
        _reset_config()
        test_in_batch_dedup_integrity_seam(tmp / "s15")
        test_lock_cwd_independence_seam(tmp / "s16")
        test_recorder_enqueue_ingest_seam(tmp / "s17")
        test_send_stall_watchdog_seam()
        test_upload_progress_seam(tmp / "s19")
        test_send_streamable_net_seam(tmp / "s20")
        test_keep_original_document_seam(tmp / "s21")
        test_topic_routing_seam(tmp / "s22")
        test_split_album_seam(tmp / "s23")
        test_banned_word_sanitizer_seam(tmp / "s24")
        test_failed_housekeeping_seam(tmp / "s25")
        test_send_order_clustering_seam(tmp / "s26")
        test_video_metadata_backend_seam(tmp / "s27")
        test_hashtag_root_seam(tmp / "s28")
        test_orphaned_mixed_album_seam(tmp / "s30")
        _reset_config()
        test_burner_account_seam()

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
