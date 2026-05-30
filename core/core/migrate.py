"""
core.migrate
────────────
One-time fold of the two legacy databases into the single suite DB.

    archive.db.media          ┐
    archive.db.checkpoints    ├─▶  suite.db.items / checkpoints / circuit / metadata
    archive.db.circuit        │
    archive.db.metadata       │
    dispatcher.db.upload_queue┘

JOIN KEY: file_path — the same key the old reconcile bridge used, so the
two halves line up exactly.

STATUS RESOLUTION (the whole point — collapse two truths into one):
  For an archiver media row, the AUTHORITATIVE delivery state is the
  dispatcher queue row if one exists (the dispatcher actually did the
  send); otherwise fall back to the media row's telegram_sent mirror.

    queue.status: pending→pending  claimed→pending(reset)  done→sent  failed→failed
    telegram_sent (no queue row): 1→sent  0→failed  2→pending  NULL→pending

  'claimed' and telegram_sent=2 both mean "in flight / handed off, outcome
  unknown" → reset to 'pending' so the unified dispatcher re-sends. Safe:
  worst case is one duplicate upload, never a lost file.

RECORDER rows live ONLY in dispatcher.db (the recorder never wrote
archive.db). They become standalone items with a synthesized identifier.

IDEMPOTENT: INSERT OR IGNORE on the unique keys, so re-running can't
duplicate. Run it once with the daemons stopped.

Usage:
    python -m core.migrate \
        --archive-db    ~/.config/archiver-suite/archive.db \
        --dispatcher-db ~/.config/dispatcher/dispatcher.db \
        --out           ~/.config/archiver-suite/suite.db
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
from pathlib import Path

from . import schema
from .store import now_iso

log = logging.getLogger("core.migrate")

_QUEUE_TO_STATUS = {
    "pending": "pending",
    "claimed": "pending",   # in-flight at migration → re-send
    "done":    "sent",
    "failed":  "failed",
}
_SENT_TO_STATUS = {1: "sent", 0: "failed", 2: "pending", None: "pending"}


def _open_ro(path: Path) -> sqlite3.Connection | None:
    if not path.exists():
        log.info("migrate: %s absent — skipping that source", path)
        return None
    conn = sqlite3.connect(str(path), timeout=10.0)
    conn.row_factory = sqlite3.Row
    return conn


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def _insert_item(out: sqlite3.Connection, **f) -> bool:
    """INSERT OR IGNORE one item. On a synthetic-identifier collision,
    suffix it and retry once."""
    cols = ("source", "platform", "username", "identifier", "file_path",
            "upload_date", "file_size_bytes", "title", "discovered_at",
            "status", "priority", "caption", "attempts", "sent_at",
            "last_error")
    vals = tuple(f.get(c) for c in cols)
    cur = out.execute(
        f"INSERT OR IGNORE INTO items ({','.join(cols)}) "
        f"VALUES ({','.join('?' * len(cols))})", vals,
    )
    return cur.rowcount > 0


def migrate(archive_db: Path, dispatcher_db: Path, out_db: Path) -> dict:
    out = schema.connect(out_db)            # creates items/checkpoints/... if new
    adb = _open_ro(archive_db)
    ddb = _open_ro(dispatcher_db)

    stats = {"media": 0, "recorder": 0, "queue_orphans": 0,
             "checkpoints": 0, "circuit": 0, "metadata": 0}

    # Index dispatcher rows by file_path for the join.
    queue_by_path: dict[str, sqlite3.Row] = {}
    if ddb is not None:
        for q in ddb.execute("SELECT * FROM upload_queue"):
            queue_by_path[q["file_path"]] = q

    matched_paths: set[str] = set()

    # 1. Archiver catalog rows — delivery status from the queue if present.
    if adb is not None:
        for m in adb.execute("SELECT * FROM media"):
            fp = m["file_path"]
            q = queue_by_path.get(fp)
            if q is not None:
                matched_paths.add(fp)
                status   = _QUEUE_TO_STATUS.get(q["status"], "pending")
                attempts = q["attempts"]
                caption  = q["caption"]
                sent_at  = q["sent_at"]
                last_err = q["last_error"]
                priority = q["priority"]
            else:
                status   = _SENT_TO_STATUS.get(m["telegram_sent"], "pending")
                attempts = 0
                caption  = None
                sent_at  = m["sent_at"]
                last_err = None
                priority = 10   # archiver default priority
            if _insert_item(
                out, source="archiver", platform=m["platform"],
                username=m["username"], identifier=m["identifier"],
                file_path=fp, upload_date=m["upload_date"],
                file_size_bytes=m["file_size_bytes"], title=m["title"],
                discovered_at=m["downloaded_at"] or now_iso(),
                status=status, priority=priority, caption=caption,
                attempts=attempts, sent_at=sent_at, last_error=last_err,
            ):
                stats["media"] += 1

    # 2. Queue rows with no media counterpart (recorder lives, or orphans).
    if ddb is not None:
        for fp, q in queue_by_path.items():
            if fp in matched_paths:
                continue
            src = q["source"]
            stem = Path(fp).stem or "item"
            identifier = f"{src}_{stem}"
            status = _QUEUE_TO_STATUS.get(q["status"], "pending")
            # collision-proof the synthetic identifier
            n = 0
            base = identifier
            while out.execute(
                "SELECT 1 FROM items WHERE platform=? AND identifier=?",
                (q["platform"], identifier),
            ).fetchone():
                n += 1
                identifier = f"{base}_{n}"
            size = None
            if Path(fp).exists():
                try: size = Path(fp).stat().st_size
                except OSError: pass
            inserted = _insert_item(
                out, source=src, platform=q["platform"],
                username=q["username"], identifier=identifier, file_path=fp,
                upload_date=None, file_size_bytes=size, title=None,
                discovered_at=q["submitted_at"] or now_iso(),
                status=status, priority=q["priority"], caption=q["caption"],
                attempts=q["attempts"], sent_at=q["sent_at"],
                last_error=q["last_error"],
            )
            if inserted:
                stats["recorder" if src == "recorder" else "queue_orphans"] += 1

    # 3. Checkpoints / circuit / metadata — archiver-private, copied verbatim.
    if adb is not None:
        cp_cols = _columns(adb, "checkpoints")
        for c in adb.execute("SELECT * FROM checkpoints"):
            out.execute(
                """INSERT OR IGNORE INTO checkpoints
                     (platform, username, last_run_utc, date_floor)
                   VALUES (?,?,?,?)""",
                (c["platform"], c["username"],
                 c["last_run_utc"] if "last_run_utc" in cp_cols else None,
                 c["date_floor"] if "date_floor" in cp_cols else None),
            )
            stats["checkpoints"] += 1
        for c in adb.execute("SELECT * FROM circuit"):
            out.execute(
                """INSERT OR IGNORE INTO circuit
                     (platform, consecutive_fails, tripped_until_utc, last_error)
                   VALUES (?,?,?,?)""",
                (c["platform"], c["consecutive_fails"],
                 c["tripped_until_utc"], c["last_error"]),
            )
            stats["circuit"] += 1
        for c in adb.execute("SELECT * FROM metadata"):
            out.execute(
                "INSERT OR IGNORE INTO metadata (key, value) VALUES (?,?)",
                (c["key"], c["value"]),
            )
            stats["metadata"] += 1

    out.commit()
    out.close()
    if adb: adb.close()
    if ddb: ddb.close()
    return stats


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    p = argparse.ArgumentParser(prog="core.migrate")
    p.add_argument("--archive-db",    default="~/.config/archiver-suite/archive.db")
    p.add_argument("--dispatcher-db", default="~/.config/dispatcher/dispatcher.db")
    p.add_argument("--out",           default=schema.DEFAULT_DB_PATH)
    a = p.parse_args(argv)

    stats = migrate(
        Path(a.archive_db).expanduser(),
        Path(a.dispatcher_db).expanduser(),
        Path(a.out).expanduser(),
    )
    log.info("migrate complete → %s", Path(a.out).expanduser())
    for k, v in stats.items():
        log.info("  %-13s %d", k, v)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
