"""
archiver.db
───────────
SQLite-backed state. One schema for all platforms.

Tables
──────
  media         — one row per downloaded file.
                  (platform, identifier) is unique per file.
                  identifier = tweet_id (X) / video_id (TikTok) /
                               shortcode (Instagram) / manual_<hash> for
                               user-added files.
                  telegram_sent: NULL=pending, 1=sent, 0=failed,
                                 2=queued (handed to dispatcher.db,
                                 outcome pending; reconciled back to 1/0
                                 by reconcile_dispatch_outcomes()).
                  upload_date: YYYYMMDD if known (sidecar/filename/mtime).

  checkpoints   — per (platform, username):
                    last_run_utc   : when the run last completed cleanly
                    date_floor     : YYYYMMDD upload_date of the newest
                                     successfully-uploaded media. This is
                                     the value used to compute the
                                     `date-min`/`dateafter` for the next
                                     extractor invocation — survives
                                     `delete_after_upload=true`.
                  date_floor is recomputed from MAX(upload_date WHERE
                  telegram_sent=1) at the end of every clean run AND at
                  the end of bootstrap, so a freshly-reconciled archive
                  immediately becomes incremental.

  metadata      — generic key/value (cookie refresh timestamps, etc.)

  circuit       — circuit-breaker state for self-healing.

Schema migration:
  v1 of `checkpoints` had only (platform, username, last_run_utc).
  v2 adds `date_floor TEXT`. `_init_schema` adds the column if missing —
  safe to run on every startup. No data migration needed; existing rows
  get NULL for date_floor and the orchestrator falls back to last_run_utc.

Design notes
────────────
- WAL mode for concurrent reads while we write.
- Foreign keys ON so we could later add cascading.
- All write operations are short transactions so locks release fast.
- `IF NOT EXISTS` in DDL → safe to call _init_schema on every startup.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

MEDIA_EXTENSIONS = {
    ".mp4", ".mov", ".webm", ".mkv",
    ".jpg", ".jpeg", ".png", ".webp",
    ".gif",
}


class ArchiveDB:

    def __init__(self, db_path: str):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        self._migrate_checkpoints_v2()

    def _init_schema(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS media (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                platform        TEXT    NOT NULL,
                username        TEXT    NOT NULL,
                identifier      TEXT    NOT NULL,
                file_path       TEXT    NOT NULL UNIQUE,
                upload_date     TEXT,
                file_size_bytes INTEGER,
                title           TEXT,
                downloaded_at   TEXT    NOT NULL,
                telegram_sent   INTEGER,
                sent_at         TEXT,
                UNIQUE(platform, identifier)
            );
            CREATE INDEX IF NOT EXISTS idx_media_platform_user
                ON media(platform, username);
            CREATE INDEX IF NOT EXISTS idx_media_pending
                ON media(platform, username, telegram_sent);
            CREATE INDEX IF NOT EXISTS idx_media_upload_date
                ON media(platform, username, upload_date);

            CREATE TABLE IF NOT EXISTS checkpoints (
                platform     TEXT NOT NULL,
                username     TEXT NOT NULL,
                last_run_utc TEXT NOT NULL,
                PRIMARY KEY (platform, username)
            );

            CREATE TABLE IF NOT EXISTS metadata (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS circuit (
                platform           TEXT PRIMARY KEY,
                consecutive_fails  INTEGER NOT NULL DEFAULT 0,
                tripped_until_utc  TEXT,
                last_error         TEXT
            );
        """)
        self.conn.commit()

    def _migrate_checkpoints_v2(self) -> None:
        """Add `date_floor` to checkpoints if not present. Idempotent."""
        cols = {r["name"] for r in
                self.conn.execute("PRAGMA table_info(checkpoints)")}
        if "date_floor" not in cols:
            log.info("db: migrating checkpoints v1 → v2 (adding date_floor)")
            self.conn.execute("ALTER TABLE checkpoints ADD COLUMN date_floor TEXT")
            self.conn.commit()

    # ── Media CRUD ────────────────────────────────────────────────────────────

    def seen(self, platform: str, identifier: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM media WHERE platform=? AND identifier=?",
            (platform, identifier),
        ).fetchone() is not None

    def has_file_path(self, file_path: str) -> bool:
        """Fast existence check on file_path. Used by reconcile to skip
        the stability+identity work for files we already know about."""
        return self.conn.execute(
            "SELECT 1 FROM media WHERE file_path=?", (file_path,),
        ).fetchone() is not None

    def add_file(
        self,
        *,
        platform:        str,
        username:        str,
        identifier:      str,
        file_path:       str,
        upload_date:     str | None     = None,
        file_size_bytes: int | None     = None,
        title:           str            = "",
    ) -> bool:
        """
        INSERT OR IGNORE on (platform, identifier). Returns True if a row
        was actually inserted.
        """
        now = datetime.now(timezone.utc).isoformat()
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO media
                 (platform, username, identifier, file_path, upload_date,
                  file_size_bytes, title, downloaded_at, telegram_sent)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (platform, username, identifier, file_path, upload_date,
             file_size_bytes, title, now),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def mark_sent(self, file_path: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE media SET telegram_sent=1, sent_at=? WHERE file_path=?",
            (now, file_path),
        )
        self.conn.commit()

    def mark_failed(self, file_path: str) -> None:
        self.conn.execute(
            "UPDATE media SET telegram_sent=0 WHERE file_path=?", (file_path,),
        )
        self.conn.commit()

    def mark_queued(self, file_path: str) -> None:
        """telegram_sent=2: handed to dispatcher, outcome not yet known.
        Excluded from pending_uploads (which matches NULL/0 only), so a
        second archiver run won't re-enqueue an in-flight file."""
        self.conn.execute(
            "UPDATE media SET telegram_sent=2 WHERE file_path=?", (file_path,),
        )
        self.conn.commit()

    def pending_uploads(self, platform: str, username: str) -> list[sqlite3.Row]:
        """Returns NULL and failed rows together so --reset-failed isn't strictly necessary."""
        return self.conn.execute(
            """SELECT * FROM media
               WHERE platform=? AND username=?
                 AND (telegram_sent IS NULL OR telegram_sent=0)
               ORDER BY upload_date ASC, id ASC""",
            (platform, username),
        ).fetchall()

    def sent_file_paths(self, platform: str, username: str) -> list[str]:
        rows = self.conn.execute(
            "SELECT file_path FROM media "
            "WHERE platform=? AND username=? AND telegram_sent=1",
            (platform, username),
        ).fetchall()
        return [r["file_path"] for r in rows]

    def reconcile_dispatch_outcomes(self, dispatcher_db_path: str) -> tuple[int, int]:
        """
        The archiver-side bridge (guide §3.3 option b). Pull terminal
        outcomes the dispatcher recorded for archiver-sourced rows, apply
        them to this archive.db, then mark them seen in dispatcher.db so
        they aren't re-processed next run.

        One-way knowledge: archiver reads dispatcher's schema. Dispatcher
        never reads archive.db.

        Matching is by file_path. The archiver enqueued with source=
        'archiver', and dispatcher preserves file_path verbatim, so it's
        the stable join key across the two databases.

        Returns (n_sent, n_failed) applied this pass.
        """
        import sqlite3 as _sqlite3
        from pathlib import Path as _Path

        if not _Path(dispatcher_db_path).expanduser().exists():
            log.debug("reconcile: dispatcher.db not found at %s — skipping",
                      dispatcher_db_path)
            return 0, 0

        dconn = _sqlite3.connect(str(_Path(dispatcher_db_path).expanduser()),
                                 timeout=10.0)
        dconn.row_factory = _sqlite3.Row
        dconn.execute("PRAGMA busy_timeout=5000")
        try:
            rows = dconn.execute(
                """SELECT id, file_path, status FROM upload_queue
                   WHERE source='archiver'
                     AND status IN ('done','failed')
                     AND seen_by_archiver=0""",
            ).fetchall()

            n_sent = n_failed = 0
            seen_ids: list[int] = []
            for r in rows:
                if r["status"] == "done":
                    self.mark_sent(r["file_path"])
                    n_sent += 1
                else:
                    self.mark_failed(r["file_path"])
                    n_failed += 1
                seen_ids.append(r["id"])

            if seen_ids:
                dconn.executemany(
                    "UPDATE upload_queue SET seen_by_archiver=1 WHERE id=?",
                    [(i,) for i in seen_ids],
                )
                dconn.commit()

            if n_sent or n_failed:
                log.info("reconcile: dispatcher outcomes applied — "
                         "sent=%d failed=%d", n_sent, n_failed)
            return n_sent, n_failed
        finally:
            dconn.close()

    def telegram_sent_state(self, file_path: str) -> int | None:
        """Return the telegram_sent value (NULL / 0 / 1) for the path,
        or None if no row exists. Used by the cleanup gate."""
        row = self.conn.execute(
            "SELECT telegram_sent FROM media WHERE file_path=?", (file_path,),
        ).fetchone()
        return row["telegram_sent"] if row else None

    def max_upload_date(self, platform: str, username: str,
                        sent_only: bool = True) -> str | None:
        """
        Newest upload_date for this (platform, user). Defaults to
        considering only telegram_sent=1 rows — this is what drives the
        incremental download cutoff, and we don't want a pending-but-not-
        yet-uploaded post moving the floor and accidentally skipping it
        on the next run.

        Used by checkpoint logic AND by reconcile's report.
        """
        sql = "SELECT MAX(upload_date) AS m FROM media WHERE platform=? AND username=?"
        if sent_only:
            sql += " AND telegram_sent=1"
        row = self.conn.execute(sql, (platform, username)).fetchone()
        return row["m"] if row and row["m"] else None

    # ── Checkpoints ───────────────────────────────────────────────────────────

    def get_last_run(self, platform: str, username: str) -> datetime | None:
        row = self.conn.execute(
            "SELECT last_run_utc FROM checkpoints WHERE platform=? AND username=?",
            (platform, username),
        ).fetchone()
        return datetime.fromisoformat(row["last_run_utc"]) if row else None

    def get_date_floor(self, platform: str, username: str) -> str | None:
        """Stored date_floor (YYYYMMDD) for this (platform, user)."""
        row = self.conn.execute(
            "SELECT date_floor FROM checkpoints WHERE platform=? AND username=?",
            (platform, username),
        ).fetchone()
        return row["date_floor"] if row and row["date_floor"] else None

    def set_last_run(self, platform: str, username: str, dt: datetime) -> None:
        self.conn.execute(
            """INSERT INTO checkpoints (platform, username, last_run_utc)
               VALUES (?, ?, ?)
               ON CONFLICT(platform, username) DO UPDATE
                 SET last_run_utc=excluded.last_run_utc""",
            (platform, username, dt.isoformat()),
        )
        self.conn.commit()

    def set_date_floor(self, platform: str, username: str,
                       floor: str | None) -> None:
        """
        Update the date_floor for this (platform, user). Creates the
        checkpoint row if needed (with last_run_utc=now to keep the
        NOT NULL constraint happy).
        """
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO checkpoints (platform, username, last_run_utc, date_floor)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(platform, username) DO UPDATE
                 SET date_floor=excluded.date_floor""",
            (platform, username, now, floor),
        )
        self.conn.commit()

    def clear_checkpoint(self, platform: str, username: str) -> None:
        self.conn.execute(
            "DELETE FROM checkpoints WHERE platform=? AND username=?",
            (platform, username),
        )
        self.conn.commit()

    # ── Metadata (generic key/value) ──────────────────────────────────────────

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM metadata WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            """INSERT INTO metadata (key, value) VALUES (?, ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, value),
        )
        self.conn.commit()

    # ── Circuit breaker state ─────────────────────────────────────────────────

    def get_circuit(self, platform: str) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM circuit WHERE platform=?", (platform,),
        ).fetchone()
        if row is None:
            return {"platform": platform, "consecutive_fails": 0,
                    "tripped_until_utc": None, "last_error": None}
        return dict(row)

    def bump_circuit_fail(self, platform: str, error: str) -> int:
        self.conn.execute(
            """INSERT INTO circuit (platform, consecutive_fails, last_error)
               VALUES (?, 1, ?)
               ON CONFLICT(platform) DO UPDATE SET
                 consecutive_fails = consecutive_fails + 1,
                 last_error        = excluded.last_error""",
            (platform, error[:500]),
        )
        self.conn.commit()
        return self.conn.execute(
            "SELECT consecutive_fails FROM circuit WHERE platform=?", (platform,),
        ).fetchone()["consecutive_fails"]

    def trip_circuit(self, platform: str, until_utc: datetime) -> None:
        self.conn.execute(
            """INSERT INTO circuit (platform, consecutive_fails, tripped_until_utc)
               VALUES (?, 0, ?)
               ON CONFLICT(platform) DO UPDATE SET
                 tripped_until_utc = excluded.tripped_until_utc""",
            (platform, until_utc.isoformat()),
        )
        self.conn.commit()

    def reset_circuit(self, platform: str) -> None:
        self.conn.execute(
            """INSERT INTO circuit (platform, consecutive_fails, tripped_until_utc, last_error)
               VALUES (?, 0, NULL, NULL)
               ON CONFLICT(platform) DO UPDATE SET
                 consecutive_fails = 0,
                 tripped_until_utc = NULL,
                 last_error        = NULL""",
            (platform,),
        )
        self.conn.commit()

    # ── Reset operations ──────────────────────────────────────────────────────

    def reset_failed(self, platform: str | None, username: str | None) -> int:
        sql = "UPDATE media SET telegram_sent=NULL WHERE telegram_sent=0"
        params: list = []
        if platform:
            sql += " AND platform=?"; params.append(platform)
        if username:
            sql += " AND username=?"; params.append(username)
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur.rowcount

    def reset_uploads(self, platform: str | None, username: str | None) -> int:
        sql = "UPDATE media SET telegram_sent=NULL, sent_at=NULL WHERE 1=1"
        params: list = []
        if platform:
            sql += " AND platform=?"; params.append(platform)
        if username:
            sql += " AND username=?"; params.append(username)
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur.rowcount

    def reset_user(self, platform: str, username: str) -> int:
        cur = self.conn.execute(
            "DELETE FROM media WHERE platform=? AND username=?",
            (platform, username),
        )
        self.conn.commit()
        self.clear_checkpoint(platform, username)
        return cur.rowcount

    # ── Reconcile (delegated to archiver.reconcile) ───────────────────────────

    def reconcile(self, platform: str, username: str, output_dir: str) -> int:
        """
        Legacy-compatible reconcile entry point. Delegates to the v2
        implementation in `archiver.reconcile`. Returns count inserted.

        Callers that want the full ReconcileReport should call
        `archiver.reconcile.reconcile_user` directly with a Platform
        instance — that gives them stability/manual/seed counts too.

        We accept a string platform name here for backward compatibility
        with code that doesn't have a Platform instance handy (e.g. the
        `reset uploads` reconcile pass in cli.py).
        """
        # Late import — `reconcile` imports `db` indirectly for types.
        from . import reconcile as _r
        from .platforms import build_platform_by_name

        platform_obj = build_platform_by_name(platform)
        if platform_obj is None:
            # Platform not configured (e.g. reconcile called for a user
            # whose platform is currently disabled). Fall back to a
            # walk-only pass with no archive seeding.
            return _legacy_reconcile(self, platform, username, output_dir)

        report = _r.reconcile_user(platform_obj, username, self, output_dir,
                                    seed_extractor_archive=True)
        return report.inserted

    # ── Stats ─────────────────────────────────────────────────────────────────

    def stats(self, platform: str | None = None,
              username: str | None = None) -> dict:
        where, params = [], []
        if platform: where.append("platform=?"); params.append(platform)
        if username: where.append("username=?"); params.append(username)
        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        row = self.conn.execute(
            f"""SELECT
                  COUNT(*)                                              AS total,
                  SUM(CASE WHEN telegram_sent=1       THEN 1 ELSE 0 END) AS sent,
                  SUM(CASE WHEN telegram_sent IS NULL THEN 1 ELSE 0 END) AS pending,
                  SUM(CASE WHEN telegram_sent=0       THEN 1 ELSE 0 END) AS failed,
                  ROUND(SUM(COALESCE(file_size_bytes,0))/1048576.0, 1)   AS total_mb
                FROM media {where_sql}""",
            params,
        ).fetchone()
        return {k: (row[k] or 0) for k in row.keys()}

    def close(self) -> None:
        self.conn.close()


def _legacy_reconcile(db: ArchiveDB, platform: str, username: str,
                      output_dir: str) -> int:
    """
    Fallback used when reconcile() is called for a platform that doesn't
    have a build_platform_by_name() match (e.g. disabled in current
    config). Just registers stable files with identity.resolve(); doesn't
    try to seed any extractor archive.
    """
    from . import identity as _id, stability as _stab

    user_dir = Path(output_dir) / platform / username
    if not user_dir.exists():
        return 0

    added = 0
    for f in sorted(user_dir.rglob("*")):
        if not f.is_file() or f.suffix.lower() not in MEDIA_EXTENSIONS:
            continue
        if db.has_file_path(str(f)):
            continue
        if not _stab.is_stable(f):
            continue
        ident = _id.resolve(f)
        try:
            size = f.stat().st_size
        except OSError:
            continue
        if db.add_file(
            platform=platform, username=username,
            identifier=ident.identifier, file_path=str(f),
            upload_date=ident.upload_date, file_size_bytes=size,
            title=ident.title,
        ):
            added += 1
    return added
