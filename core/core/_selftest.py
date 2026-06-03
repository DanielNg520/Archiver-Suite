"""
Validation harness (not shipped). Builds synthetic copies of the REAL
legacy schemas, runs the migration, and drives the state machine, asserting
invariants at each step. Run: python core/_selftest.py
"""
import sqlite3
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core import ItemStore, Status          # noqa: E402
from core.migrate import migrate            # noqa: E402

OK = "✓"


def build_legacy(tmp: Path):
    """Recreate the exact legacy archive.db + dispatcher.db schemas."""
    adb_p, ddb_p = tmp / "archive.db", tmp / "dispatcher.db"
    a = sqlite3.connect(adb_p); a.row_factory = sqlite3.Row
    a.executescript("""
        CREATE TABLE media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            platform TEXT NOT NULL, username TEXT NOT NULL,
            identifier TEXT NOT NULL, file_path TEXT NOT NULL UNIQUE,
            upload_date TEXT, file_size_bytes INTEGER, title TEXT,
            downloaded_at TEXT NOT NULL, telegram_sent INTEGER, sent_at TEXT,
            UNIQUE(platform, identifier));
        CREATE TABLE checkpoints (platform TEXT, username TEXT,
            last_run_utc TEXT, date_floor TEXT, PRIMARY KEY(platform, username));
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE circuit (platform TEXT PRIMARY KEY,
            consecutive_fails INTEGER NOT NULL DEFAULT 0,
            tripped_until_utc TEXT, last_error TEXT);
    """)
    # 4 archiver files spanning every delivery state.
    rows = [
        # identifier, file_path, upload_date, telegram_sent  (+queue below)
        ("x1", "/m/x_20240101_x1.mp4", "20240101", 1),     # sent (queue done)
        ("x2", "/m/x_20240115_x2.mp4", "20240115", 2),     # queued→pending(claimed)
        ("x3", "/m/x_20240201_x3.mp4", "20240201", None),  # pending, no queue row
        ("x4", "/m/x_20240210_x4.mp4", "20240210", 0),     # failed, no queue row
    ]
    for ident, fp, ud, ts in rows:
        a.execute("INSERT INTO media (platform,username,identifier,file_path,"
                  "upload_date,file_size_bytes,title,downloaded_at,telegram_sent,"
                  "sent_at) VALUES ('x','alice',?,?,?,?,?,?,?,?)",
                  (ident, fp, ud, 1000, "t", "2024-01-01T00:00:00Z", ts,
                   "2024-01-02T00:00:00Z" if ts == 1 else None))
    a.execute("INSERT INTO checkpoints VALUES ('x','alice','2024-02-11T00:00:00Z','20240101')")
    a.execute("INSERT INTO metadata VALUES ('cookie_refresh','2024-02-01')")
    a.execute("INSERT INTO circuit VALUES ('x',0,NULL,NULL)")
    a.commit(); a.close()

    d = sqlite3.connect(ddb_p); d.row_factory = sqlite3.Row
    d.executescript("""
        CREATE TABLE upload_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL,
            platform TEXT NOT NULL, username TEXT NOT NULL,
            file_path TEXT NOT NULL, caption TEXT, priority INTEGER NOT NULL DEFAULT 100,
            submitted_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT, claimed_at TEXT,
            sent_at TEXT, seen_by_archiver INTEGER NOT NULL DEFAULT 0,
            UNIQUE(source, file_path));
    """)
    # archiver queue rows that mirror media x1 (done) and x2 (claimed)
    d.execute("INSERT INTO upload_queue (source,platform,username,file_path,"
              "caption,priority,submitted_at,status,attempts,sent_at) VALUES "
              "('archiver','x','alice','/m/x_20240101_x1.mp4','cap1',10,"
              "'2024-01-01T00:00:00Z','done',1,'2024-01-02T00:00:00Z')")
    d.execute("INSERT INTO upload_queue (source,platform,username,file_path,"
              "caption,priority,submitted_at,status,attempts,claimed_at) VALUES "
              "('archiver','x','alice','/m/x_20240115_x2.mp4','cap2',10,"
              "'2024-01-15T00:00:00Z','claimed',1,'2024-01-15T00:05:00Z')")
    # recorder-only rows (no media counterpart)
    d.execute("INSERT INTO upload_queue (source,platform,username,file_path,"
              "caption,priority,submitted_at,status,attempts) VALUES "
              "('recorder','tiktok','bob','/r/tt_live_001.mp4','live1',20,"
              "'2024-03-01T00:00:00Z','pending',0)")
    d.execute("INSERT INTO upload_queue (source,platform,username,file_path,"
              "caption,priority,submitted_at,status,attempts,sent_at) VALUES "
              "('recorder','tiktok','bob','/r/tt_live_002.mp4','live2',20,"
              "'2024-03-02T00:00:00Z','done',1,'2024-03-02T01:00:00Z')")
    d.commit(); d.close()
    return adb_p, ddb_p


def _selftest_sorter(tmp: Path) -> None:
    """core.sorter: username parse + the move/collision/dry-run contract."""
    from core.sorter import sort_unsorted, extract_username

    # username = segments before the first 17/18-led 10-digit Unix timestamp.
    assert extract_username("1stagram_0406_1780186897_3915641126") == "1stagram_0406"
    assert extract_username("bob_1780186897") == "bob"
    assert extract_username("1780186897_only") is None          # timestamp leads
    assert extract_username("no_timestamp") is None
    assert extract_username("x_1234567890") is None             # not 17/18-led
    print(OK, "sorter: username extraction")

    out = tmp / "sortroot"
    uns = out / "unsorted"
    uns.mkdir(parents=True)
    (uns / "1stagram_0406_1780186897_3915641126.mp4").write_bytes(b"v")
    (uns / "1stagram_0406_1780186897_3915641126.mp4.json").write_bytes(b"{}")
    (uns / "garbage.mp4").write_bytes(b"g")           # no timestamp → stays put
    (uns / "._appledouble_1780186897.mp4").write_bytes(b"d")  # dotfile → ignored

    dry = sort_unsorted(out, platform="instagram", dry_run=True)
    assert dry.moved == 1 and dry.skipped_no_username == 1, dry
    assert not (out / "instagram").exists(), "dry-run must not touch disk"

    rep = sort_unsorted(out, platform="instagram")
    dst = out / "instagram" / "1stagram_0406" / "1stagram_0406_1780186897_3915641126.mp4"
    assert rep.moved == 1 and rep.created_dirs == 1 and rep.skipped_no_username == 1, rep
    assert dst.exists() and (dst.parent / (dst.name + ".json")).exists(), "sidecar follows"
    assert (uns / "garbage.mp4").exists(), "unparseable file left in place"

    # Re-running with the same source name present at dst → collision, no clobber.
    (uns / "1stagram_0406_1780186897_3915641126.mp4").write_bytes(b"new")
    rep2 = sort_unsorted(out, platform="instagram")
    assert rep2.skipped_collision == 1 and rep2.moved == 0, rep2
    assert dst.read_bytes() == b"v", "collision must not overwrite"
    print(OK, "sorter: move + sidecar + dry-run + collision contract")


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    adb, ddb = build_legacy(tmp)
    out = tmp / "suite.db"

    stats = migrate(adb, ddb, out)
    assert stats["media"] == 4, stats
    assert stats["recorder"] == 2, stats
    assert stats["queue_orphans"] == 0, stats
    assert stats["checkpoints"] == 1 and stats["circuit"] == 1 and stats["metadata"] == 1
    print(OK, "migration counts:", stats)

    # Idempotency: a second run against an already-populated DB inserts nothing.
    stats2 = migrate(adb, ddb, out)
    assert stats2["media"] == 0 and stats2["recorder"] == 0, stats2
    print(OK, "migration is idempotent")

    s = ItemStore.open(str(out))
    by_id = {i.file_path: i for i in
             [s.get(r["id"]) for r in s.conn.execute("SELECT id FROM items")]}

    # Status resolution: queue truth beats the telegram_sent mirror.
    assert by_id["/m/x_20240101_x1.mp4"].status == "sent"      # queue done
    assert by_id["/m/x_20240115_x2.mp4"].status == "pending"   # claimed→reset
    assert by_id["/m/x_20240201_x3.mp4"].status == "pending"   # ts NULL
    assert by_id["/m/x_20240210_x4.mp4"].status == "failed"    # ts 0
    assert by_id["/r/tt_live_001.mp4"].status == "pending"
    assert by_id["/r/tt_live_002.mp4"].status == "sent"
    # recorder identifier synthesized, source preserved
    assert by_id["/r/tt_live_001.mp4"].identifier == "recorder_tt_live_001"
    assert by_id["/r/tt_live_001.mp4"].source == "recorder"
    print(OK, "status resolution + recorder synthesis correct")

    # date_floor reads the one table — only the SENT x1 counts.
    assert s.max_sent_upload_date("x", "alice") == "20240101"
    print(OK, "date_floor =", s.max_sent_upload_date("x", "alice"))

    # ── State machine ────────────────────────────────────────────────
    # Highest priority (archiver=10) pending claims before recorder=20.
    # attempts increments from whatever the row carried in (some migrated
    # rows carry a prior claim count), so assert the delta, not an absolute.
    pre = {i.file_path: i.attempts for i in
           [s.get(r["id"]) for r in s.conn.execute("SELECT id FROM items WHERE status='pending'")]}
    c1 = s.claim_next()
    assert c1.priority == 10 and c1.status == "sending"
    assert c1.attempts == pre[c1.file_path] + 1
    s.mark_sent(c1.id, tg_message_id=555)
    assert s.get(c1.id).status == "sent" and s.get(c1.id).tg_message_id == 555
    print(OK, "claim(priority order)→sending→sent")

    # Retry path: fail under budget → pending; fail at budget → failed.
    c2 = s.claim_next()
    budget_max = c2.attempts + 1          # one more attempt allowed
    st = s.mark_failed(c2.id, error="boom", max_retries=budget_max)
    assert st == "pending" and s.get(c2.id).status == "pending"
    c2b = s.claim_next()                   # re-claim same row
    assert c2b.id == c2.id and c2b.attempts == c2.attempts + 1
    st = s.mark_failed(c2b.id, error="boom2", max_retries=budget_max)
    assert st == "failed" and s.get(c2b.id).status == "failed"
    print(OK, "retry budget: pending then failed at max")

    # FloodWait requeue refunds the attempt.
    c3 = s.claim_next()
    a_before = c3.attempts
    s.requeue(c3.id, reason="floodwait 30s")
    assert s.get(c3.id).status == "pending"
    assert s.get(c3.id).attempts == a_before - 1
    print(OK, "requeue refunds attempt")

    # Watchdog: a row stuck 'sending' with an old claim reverts to pending.
    c4 = s.claim_next()
    old = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    s.conn.execute("UPDATE items SET claimed_at=? WHERE id=?", (old, c4.id))
    s.conn.commit()
    n = s.reset_stuck_sending(older_than_minutes=10)
    assert n == 1 and s.get(c4.id).status == "pending"
    print(OK, "watchdog reset stuck sending")

    # Reset: failed → pending (single write, no second DB).
    moved = s.reset_failed("x", "alice")
    assert moved >= 1
    assert all(i.status != "failed"
               for i in s.pending_items("x", "alice") + [])  # none failed now
    assert s.conn.execute("SELECT COUNT(*) c FROM items WHERE status='failed'").fetchone()["c"] == 0
    print(OK, "reset_failed → pending")

    # ── Guarded transitions ──────────────────────────────────────────
    # A lifecycle write from a disallowed state is a no-op (the WHERE-clause
    # guard matches 0 rows), so a terminal row can never be silently rewritten
    # by a stray call — the gap the state-machine-as-comment used to leave open.
    s.add_item(source="archiver", platform="x", username="zed",
               identifier="z1", file_path="/m/z1.mp4")
    zid = s.id_of("/m/z1.mp4")
    s.mark_sent(zid)                                   # pending → (guard) no-op
    assert s.get(zid).status == "pending", s.get(zid).status
    s.conn.execute("UPDATE items SET status='sent' WHERE id=?", (zid,))
    s.conn.commit()
    st = s.mark_failed(zid, error="late", max_retries=0)  # sent → no-op
    assert st == "sent" and s.get(zid).status == "sent"
    print(OK, "guarded transition: disallowed write is a no-op")

    # ── Unit of Work (batch) ─────────────────────────────────────────
    # A batch commits its inserts as a group; an exception rolls the in-flight
    # (un-flushed) inserts back.
    with s.batch():
        s.add_item(source="archiver", platform="x", username="bat",
                   identifier="b1", file_path="/m/b1.mp4")
        s.add_item(source="archiver", platform="x", username="bat",
                   identifier="b2", file_path="/m/b2.mp4")
    assert s.id_of("/m/b1.mp4") and s.id_of("/m/b2.mp4")
    try:
        with s.batch():
            s.add_item(source="archiver", platform="x", username="bat",
                       identifier="b3", file_path="/m/b3.mp4")
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert s.id_of("/m/b3.mp4") is None, "rollback should drop the in-flight insert"
    print(OK, "batch commits as a group and rolls back on error")

    # Nested batch folds into the outer one: the inner must not commit on
    # entry/exit, so an outer rollback still discards BOTH levels' writes.
    try:
        with s.batch():
            s.add_item(source="archiver", platform="x", username="bat",
                       identifier="n1", file_path="/m/n1.mp4")
            with s.batch():
                s.add_item(source="archiver", platform="x", username="bat",
                           identifier="n2", file_path="/m/n2.mp4")
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert s.id_of("/m/n1.mp4") is None and s.id_of("/m/n2.mp4") is None, \
        "nested batch must roll back both levels"
    print(OK, "nested batch folds into outer; rollback discards both levels")

    # reset_user wipes rows + checkpoint.
    before = s.stats("x", "alice")["total"]
    deleted = s.reset_user("x", "alice")
    assert deleted == before
    assert s.get_checkpoint("x", "alice") is None
    print(OK, "reset_user wipes rows + checkpoint")
    s.close()

    _selftest_sorter(tmp)

    print("\nALL PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
