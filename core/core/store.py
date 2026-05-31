"""
core.store
──────────
The one data-access layer. Every process goes through ItemStore; nobody
writes raw SQL against the suite DB. This is where the status state
machine lives (see core.models), so the legal transitions are enforced
in exactly one place instead of being re-implemented per package.

Replaces, in a single class:
  - archiver.db.ArchiveDB  (media CRUD, checkpoints, circuit, stats)
  - dispatcher.db.QueueDB  (claim/mark/requeue/watchdog)
and deletes outright:
  - archiver.dispatch_client.DispatchClient  (no cross-DB handoff exists)
  - archiver.db.reconcile_dispatch_outcomes  (no second DB to reconcile)

CONCURRENCY:
  Claim uses the compare-and-swap pattern SQLite forces on us (no row
  locks): SELECT a candidate id, then UPDATE ... WHERE id=? AND
  status='pending'. If a racing claimer already flipped it, our UPDATE
  matches 0 rows and we try the next candidate. Serial drain makes
  contention nil today, but the watchdog + a future parallel drain both
  rely on this being correct.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

from . import schema
from .models import Item, Status
from .files import media_bucket, ALBUM_MAX

log = logging.getLogger(__name__)

_CLAIM_RETRIES = 5


class ClaimContentionError(RuntimeError):
    """claim_* lost every CAS retry to a concurrent claimer."""

    def __init__(self, retries: int = _CLAIM_RETRIES) -> None:
        self.retries = retries
        super().__init__(
            f"claim exhausted after {retries} retries under contention"
        )
_ERROR_CAP = 1000


def now_iso() -> str:
    """Single canonical timestamp format across the whole suite.

    Trailing 'Z', no offset. claimed_at is compared as a STRING by the
    watchdog, so every writer MUST use this exact format — lexical order
    equals chronological order only when the encoding is identical.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _age_seconds(iso: str) -> float:
    """Seconds since an item's discovered_at (now_iso format). Used by the
    min-batch flush. A malformed/empty timestamp reads as age 0 (not stale),
    so a bad row can never force a premature partial flush."""
    try:
        t = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return 0.0
    return (datetime.now(timezone.utc) - t).total_seconds()


class ItemStore:
    """One instance per process. Wraps a single sqlite3 connection."""

    def __init__(self, conn: sqlite3.Connection | None = None,
                 *, db_path: str | None = None):
        self.conn = conn if conn is not None else schema.connect(db_path)

    @classmethod
    def open(cls, db_path: str | None = None) -> "ItemStore":
        return cls(schema.connect(db_path))

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "ItemStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @contextmanager
    def _immediate(self) -> Iterator[sqlite3.Cursor]:
        """BEGIN IMMEDIATE so the write lock is taken up front, making the
        read-then-write inside a claim atomic against other writers."""
        cur = self.conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            yield cur
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    # ── Producer side (archiver / recorder) ───────────────────────────────

    def add_item(
        self,
        *,
        source:          str,
        platform:        str,
        username:        str,
        identifier:      str,
        file_path:       str,
        upload_date:     str | None = None,
        file_size_bytes: int | None = None,
        title:           str        = "",
        caption:         str | None = None,
        priority:        int        = 100,
        content_hash:    str | None = None,
        chat_id:         str | None = None,
        group_key:       str | None = None,
    ) -> bool:
        """
        Register a downloaded/recorded file as a pending upload. This IS
        the enqueue — there is no separate handoff step anymore. Writing
        the row makes it claimable by the dispatcher on its next poll.

        INSERT OR IGNORE on (platform, identifier): re-running a download
        before the dispatcher has sent won't create a duplicate. Returns
        True iff a row was actually inserted.

        content_hash / chat_id / group_key are the redesign columns:
          - content_hash → global dedup key (stamped by ingest)
          - chat_id      → explicit Telegram destination (orphaned folders)
          - group_key    → explicit album batch identity (else NULL → the
                           dispatcher falls back to caption-based grouping)
        """
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO items
                 (source, platform, username, identifier, file_path,
                  upload_date, file_size_bytes, title, discovered_at,
                  status, priority, caption, attempts,
                  content_hash, chat_id, group_key)
               VALUES (?,?,?,?,?,?,?,?,?, 'pending', ?, ?, 0, ?, ?, ?)""",
            (source, platform, username, identifier, file_path,
             upload_date, file_size_bytes, title, now_iso(),
             priority, caption, content_hash, chat_id, group_key),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def seen(self, platform: str, identifier: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM items WHERE platform=? AND identifier=?",
            (platform, identifier),
        ).fetchone() is not None

    def has_file_path(self, file_path: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM items WHERE file_path=?", (file_path,),
        ).fetchone() is not None

    def find_by_content_hash(self, content_hash: str) -> Item | None:
        """First existing row sharing these exact bytes, or None. Drives
        ingest-time global dedup: if a row already holds this content_hash,
        the incoming file is a duplicate and never gets a second row."""
        r = self.conn.execute(
            "SELECT * FROM items WHERE content_hash=? LIMIT 1", (content_hash,),
        ).fetchone()
        return Item.from_row(r) if r else None

    def relink_file(self, item_id: int, new_file_path: str) -> None:
        """Re-point an existing row at a different physical file (the dedup
        ADOPT case: the incoming copy has a better/canonical name, so we keep
        it and retire the old file, preserving the row's delivery history)."""
        with self._immediate() as cur:
            cur.execute(
                "UPDATE items SET file_path=? WHERE id=?",
                (new_file_path, item_id),
            )

    # ── Dispatcher side: the state machine ────────────────────────────────

    def claim_next(self) -> Item | None:
        """Atomically claim the highest-priority pending item (pending →
        sending). Returns None when nothing is pending. Raises
        ClaimContentionError when retries are exhausted under contention."""
        for _ in range(_CLAIM_RETRIES):
            with self._immediate() as cur:
                row = cur.execute(
                    """SELECT id FROM items WHERE status='pending'
                       ORDER BY priority ASC, discovered_at ASC LIMIT 1"""
                ).fetchone()
                if row is None:
                    return None
                cur.execute(
                    """UPDATE items
                          SET status='sending', claimed_at=?, attempts=attempts+1
                        WHERE id=? AND status='pending'""",
                    (now_iso(), row["id"]),
                )
                if cur.rowcount == 1:
                    full = cur.execute(
                        "SELECT * FROM items WHERE id=?", (row["id"],),
                    ).fetchone()
                    return Item.from_row(full)
                # else: lost the race; loop and try the next candidate
        log.warning("claim_next: %d retries exhausted under contention",
                    _CLAIM_RETRIES)
        raise ClaimContentionError(_CLAIM_RETRIES)

    def claim_batch(
        self,
        max_items: int = ALBUM_MAX,
        *,
        min_batch:   "Callable[[sqlite3.Row], int] | None" = None,
        flush_age_s: "Callable[[sqlite3.Row], float | None] | None" = None,
    ) -> list[Item]:
        """Atomically claim a homogeneous group of pending items for one
        album send. Returns [] when nothing is pending (or nothing is yet
        eligible — see the gate below). Raises ClaimContentionError when
        retries are exhausted under contention.

        The group is defined by the highest-priority pending row (the
        "anchor") and everything sharing its (platform, username, source,
        group_key/caption, media-bucket):

          - source in the key  → an album is never mixed across producers.
          - media-bucket in the key → photos batch with photos, videos with
            videos. The 'single' bucket (gifs/other) yields just the anchor.

        BATCH-IDENTITY: grouping keys on COALESCE(group_key, caption, '') so a
        producer's explicit group_key (orphaned subfolders) beats the displayed
        caption text. When neither is set, falls back to caption (unchanged for
        existing producers).

        MINIMUM-BATCH GATE (optional): when `min_batch` is given, a group whose
        in-bucket pending count is below min_batch(anchor) is DEFERRED — we scan
        past it to the next eligible group and leave it pending to accumulate.
        `flush_age_s(anchor)` is the escape hatch: a deferred group is claimed
        anyway once its oldest item has waited that many seconds. 'single' items
        bypass the gate. When neither callable is passed, the original cheap
        LIMIT-1 path runs unchanged.

        All claimed rows flip pending→sending in ONE transaction (BEGIN
        IMMEDIATE / CAS discipline), so a crash mid-claim commits nothing.
        """
        if max_items < 1:
            return []

        GROUP_DISC = "COALESCE(group_key, caption, '')"
        gated = min_batch is not None or flush_age_s is not None

        for _ in range(_CLAIM_RETRIES):
            with self._immediate() as cur:
                if not gated:
                    anchor = cur.execute(
                        f"""SELECT id, platform, username, source, file_path,
                                  {GROUP_DISC} AS group_disc
                             FROM items WHERE status='pending'
                            ORDER BY priority ASC, discovered_at ASC LIMIT 1"""
                    ).fetchone()
                    if anchor is None:
                        return []
                    chosen = self._gather_group(cur, anchor, GROUP_DISC, max_items)
                else:
                    pending = cur.execute(
                        f"""SELECT id, platform, username, source, file_path,
                                  discovered_at, {GROUP_DISC} AS group_disc
                             FROM items WHERE status='pending'
                            ORDER BY priority ASC, discovered_at ASC"""
                    ).fetchall()
                    chosen = self._select_eligible_group(
                        cur, pending, GROUP_DISC, max_items, min_batch, flush_age_s,
                    )
                    if not chosen:
                        return []

                ids = [r["id"] for r in chosen]
                placeholders = ",".join("?" * len(ids))
                cur.execute(
                    f"""UPDATE items
                          SET status='sending', claimed_at=?, attempts=attempts+1
                        WHERE id IN ({placeholders}) AND status='pending'""",
                    (now_iso(), *ids),
                )
                if cur.rowcount == len(ids):
                    full = cur.execute(
                        f"SELECT * FROM items WHERE id IN ({placeholders})"
                        " ORDER BY priority ASC, discovered_at ASC",
                        ids,
                    ).fetchall()
                    return [Item.from_row(r) for r in full]
        log.warning("claim_batch: %d retries exhausted under contention",
                    _CLAIM_RETRIES)
        raise ClaimContentionError(_CLAIM_RETRIES)

    def _gather_group(self, cur, anchor, group_disc_sql: str,
                      max_items: int) -> list:
        """The anchor's album: all same-bucket pending rows sharing its
        (platform, username, source, group_disc), capped at max_items. A
        'single'-bucket anchor yields just itself (gifs/other never album)."""
        bucket = media_bucket(anchor["file_path"])
        if bucket == "single":
            return [anchor]
        candidates = cur.execute(
            f"""SELECT * FROM items
                 WHERE status='pending'
                   AND platform=? AND username=? AND source=?
                   AND {group_disc_sql}=?
                 ORDER BY priority ASC, discovered_at ASC""",
            (anchor["platform"], anchor["username"], anchor["source"],
             anchor["group_disc"]),
        ).fetchall()
        chosen = []
        for row in candidates:
            if media_bucket(row["file_path"]) == bucket:
                chosen.append(row)
            if len(chosen) >= max_items:
                break
        return chosen

    def _select_eligible_group(
        self, cur, pending, group_disc_sql: str, max_items: int,
        min_batch, flush_age_s,
    ) -> list:
        """Scan pending rows (already priority-ordered) and return the first
        group that clears the min-batch gate; [] if none is ready yet.

        A non-'single' group is eligible when it has >= min_batch(anchor)
        in-bucket items, OR its oldest item has aged past flush_age_s(anchor)
        (the anti-starvation flush). Under-threshold groups are deferred and
        skipped so a lower-priority ready group can still drain."""
        deferred: set = set()
        for anchor in pending:
            bucket = media_bucket(anchor["file_path"])
            gkey = (anchor["platform"], anchor["username"], anchor["source"],
                    anchor["group_disc"], bucket)
            if gkey in deferred:
                continue
            if bucket == "single":
                return [anchor]          # singles bypass the gate
            group = self._gather_group(cur, anchor, group_disc_sql, max_items)
            required = min_batch(anchor) if min_batch is not None else 1
            if len(group) >= required:
                return group
            age_limit = flush_age_s(anchor) if flush_age_s is not None else None
            if age_limit and age_limit > 0:
                oldest = min(r["discovered_at"] for r in group)
                if _age_seconds(oldest) >= age_limit:
                    return group
            deferred.add(gkey)
        return []

    def mark_sent(self, item_id: int, *, tg_message_id: int | None = None) -> None:
        with self._immediate() as cur:
            cur.execute(
                """UPDATE items
                      SET status='sent', sent_at=?, last_error=NULL,
                          tg_message_id=?
                    WHERE id=?""",
                (now_iso(), tg_message_id, item_id),
            )

    def sent_twin(self, content_hash: str | None, exclude_id: int) -> Item | None:
        """A different row with the SAME bytes already delivered, or None.
        Powers the dispatcher's global-dedup guarantee — an O(log n) hit on
        the partial idx_items_hash_sent index, never a re-scan. NULL hash
        (rows enqueued without ingest) never matches, so they're never
        wrongly suppressed."""
        if not content_hash:
            return None
        r = self.conn.execute(
            """SELECT * FROM items
                WHERE content_hash=? AND status='sent' AND id<>? LIMIT 1""",
            (content_hash, exclude_id),
        ).fetchone()
        return Item.from_row(r) if r else None

    def mark_deduplicated(self, item_id: int, *, twin_id: int) -> None:
        """Suppress a row whose bytes were already sent: record it as 'sent'
        (delivered by its twin) so the dispatcher won't re-send. The reason is
        kept in last_error for auditability; tg_message_id stays NULL (nothing
        was actually sent). The dispatcher deletes the redundant on-disk copy
        unconditionally after calling this."""
        with self._immediate() as cur:
            cur.execute(
                """UPDATE items
                      SET status='sent', sent_at=?, claimed_at=NULL,
                          last_error=?
                    WHERE id=?""",
                (now_iso(), f"deduped: bytes already sent by id={twin_id}",
                 item_id),
            )

    def mark_failed(self, item_id: int, *, error: str, max_retries: int) -> str:
        """Record a failed attempt. attempts was already incremented at
        claim, so attempts>=max_retries means we've used the budget →
        'failed' (terminal). Otherwise → 'pending' for another go.
        Returns the resulting status."""
        with self._immediate() as cur:
            r = cur.execute(
                "SELECT attempts FROM items WHERE id=?", (item_id,),
            ).fetchone()
            if r is None:
                log.warning("mark_failed: id=%d not found", item_id)
                return "missing"
            new_status = (Status.FAILED.value
                          if r["attempts"] >= max_retries
                          else Status.PENDING.value)
            cur.execute(
                """UPDATE items
                      SET status=?, last_error=?, claimed_at=NULL
                    WHERE id=?""",
                (new_status, (error or "")[:_ERROR_CAP], item_id),
            )
            return new_status

    def requeue(self, item_id: int, *, reason: str | None = None) -> None:
        """sending → pending WITHOUT burning a retry (FloodWait: we waited
        the server-requested time; the request itself wasn't a failure).
        Decrement attempts to undo the claim's increment."""
        with self._immediate() as cur:
            cur.execute(
                """UPDATE items
                      SET status='pending', claimed_at=NULL,
                          attempts=MAX(0, attempts-1), last_error=?
                    WHERE id=?""",
                (reason, item_id),
            )

    def reset_stuck_sending(self, older_than_minutes: int = 10) -> int:
        """Startup watchdog: revert items stuck in 'sending' (a previous
        dispatcher crashed mid-send) back to 'pending', refunding the
        claim's attempt increment. Returns rows reset.

        Cutoff is built with now_iso()'s exact format because claimed_at
        is compared as a string."""
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(minutes=older_than_minutes)
                  ).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._immediate() as cur:
            cur.execute(
                """UPDATE items
                      SET status='pending', claimed_at=NULL,
                          attempts=MAX(0, attempts-1),
                          last_error='startup watchdog: reset stuck send'
                    WHERE status='sending' AND claimed_at < ?""",
                (cutoff,),
            )
            n = cur.rowcount
        if n:
            log.warning("watchdog: reset %d stuck-sending item(s) older than %dm",
                        n, older_than_minutes)
        return n

    def get(self, item_id: int) -> Item | None:
        r = self.conn.execute(
            "SELECT * FROM items WHERE id=?", (item_id,),
        ).fetchone()
        return Item.from_row(r) if r else None

    def status_of(self, file_path: str) -> str | None:
        """Authoritative delivery status for a path — the single read the
        delete gate makes before unlinking."""
        r = self.conn.execute(
            "SELECT status FROM items WHERE file_path=?", (file_path,),
        ).fetchone()
        return r["status"] if r else None

    # ── Archiver queries (download cutoff, lists, stats, purge) ────────────

    def max_sent_upload_date(self, platform: str, username: str) -> str | None:
        """date_floor input: newest post date among DELIVERED items. Reads
        the one table directly — no reconcile bridge."""
        r = self.conn.execute(
            """SELECT MAX(upload_date) AS m FROM items
               WHERE platform=? AND username=? AND status='sent'""",
            (platform, username),
        ).fetchone()
        return r["m"] if r and r["m"] else None

    def max_upload_date(self, platform: str, username: str) -> str | None:
        """Newest post date among ALL items for this user, regardless of
        delivery status. Used only by bootstrap/reconcile to seed the
        initial date_floor when absorbing an existing on-disk archive —
        the normal run path uses max_sent_upload_date (delivered only)."""
        r = self.conn.execute(
            """SELECT MAX(upload_date) AS m FROM items
               WHERE platform=? AND username=?""",
            (platform, username),
        ).fetchone()
        return r["m"] if r and r["m"] else None

    def pending_items(self, platform: str, username: str) -> list[Item]:
        rows = self.conn.execute(
            """SELECT * FROM items
               WHERE platform=? AND username=? AND status='pending'
               ORDER BY priority ASC, discovered_at ASC""",
            (platform, username),
        ).fetchall()
        return [Item.from_row(r) for r in rows]

    def sent_file_paths(self, platform: str, username: str) -> list[str]:
        rows = self.conn.execute(
            """SELECT file_path FROM items
               WHERE platform=? AND username=? AND status='sent'""",
            (platform, username),
        ).fetchall()
        return [r["file_path"] for r in rows]

    def stats(self, platform: str | None = None,
              username: str | None = None) -> dict:
        """Aggregate counts. Both filters optional: pass neither for a
        global rollup, platform-only for a per-platform total, or both
        for a single user. Built dynamically so the cli's `stats <plat>`
        (platform-wide) and `stats` (global) paths share one method."""
        where, params = [], []
        if platform is not None:
            where.append("platform=?")
            params.append(platform)
        if username is not None:
            where.append("username=?")
            params.append(username)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        r = self.conn.execute(
            f"""SELECT
                 COUNT(*)                                            AS total,
                 SUM(CASE WHEN status='sent'    THEN 1 ELSE 0 END)   AS sent,
                 SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END)   AS pending,
                 SUM(CASE WHEN status='sending' THEN 1 ELSE 0 END)   AS sending,
                 SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END)   AS failed,
                 COALESCE(SUM(file_size_bytes),0)                    AS bytes
               FROM items {clause}""",
            params,
        ).fetchone()
        return {
            "total":   r["total"]   or 0,
            "sent":    r["sent"]    or 0,
            "pending": r["pending"] or 0,
            "sending": r["sending"] or 0,
            "failed":  r["failed"]  or 0,
            "total_mb": (r["bytes"] or 0) / 1_048_576,
        }

    def counts_by_status(self) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) AS n FROM items GROUP BY status",
        ).fetchall()
        return {r["status"]: r["n"] for r in rows}

    def list_items(self, *, status: str | None = None,
                   limit: int = 50, offset: int = 0) -> list[Item]:
        sql = "SELECT * FROM items"
        params: list = []
        if status:
            sql += " WHERE status=?"; params.append(status)
        sql += " ORDER BY priority ASC, discovered_at ASC LIMIT ? OFFSET ?"
        params += [limit, offset]
        return [Item.from_row(r) for r in self.conn.execute(sql, params)]

    def retry(self, item_id: int) -> bool:
        """Any status → pending, attempts=0. CLI manual requeue."""
        cur = self.conn.execute(
            """UPDATE items SET status='pending', attempts=0, claimed_at=NULL,
                   sent_at=NULL, last_error=NULL WHERE id=?""",
            (item_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    def cancel(self, item_id: int) -> bool:
        """pending|sending → failed. CLI manual abort."""
        cur = self.conn.execute(
            """UPDATE items SET status='failed', claimed_at=NULL
                   WHERE id=? AND status IN ('pending','sending')""",
            (item_id,),
        )
        self.conn.commit()
        return cur.rowcount > 0

    # ── Reset operations (one write each — no second DB to also reset) ─────

    def reset_failed(self, platform: str | None, username: str | None) -> int:
        """failed → pending. The dispatcher re-sends on its next poll; no
        re-enqueue, no cross-DB cleanup, no idempotency key to fight."""
        return self._reset_to_pending(("failed",), platform, username)

    def reset_uploads(self, platform: str | None, username: str | None) -> int:
        """Re-send everything (sent + failed) → pending. WARNING: 'sent'
        rows re-sent will duplicate on Telegram; intended for deliberate
        re-delivery."""
        return self._reset_to_pending(("sent", "failed"), platform, username)

    def _reset_to_pending(self, statuses: tuple[str, ...],
                          platform: str | None, username: str | None) -> int:
        marks = ",".join("?" * len(statuses))
        sql = (f"UPDATE items SET status='pending', claimed_at=NULL, "
               f"sent_at=NULL, attempts=0, last_error=NULL "
               f"WHERE status IN ({marks})")
        params: list = list(statuses)
        if platform:
            sql += " AND platform=?"; params.append(platform)
        if username:
            sql += " AND username=?"; params.append(username)
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur.rowcount

    def reset_user(self, platform: str, username: str) -> int:
        """Full wipe: delete the user's item rows + checkpoint so the next
        run re-downloads and re-sends from scratch. Single table, single
        delete — no orphaned queue rows left in a second DB."""
        cur = self.conn.execute(
            "DELETE FROM items WHERE platform=? AND username=?",
            (platform, username),
        )
        self.conn.execute(
            "DELETE FROM checkpoints WHERE platform=? AND username=?",
            (platform, username),
        )
        self.conn.commit()
        return cur.rowcount

    # ── Checkpoints ────────────────────────────────────────────────────────

    def set_last_run(self, platform: str, username: str, when: datetime) -> None:
        self.conn.execute(
            """INSERT INTO checkpoints (platform, username, last_run_utc)
               VALUES (?,?,?)
               ON CONFLICT(platform, username)
               DO UPDATE SET last_run_utc=excluded.last_run_utc""",
            (platform, username, when.strftime("%Y-%m-%dT%H:%M:%SZ")),
        )
        self.conn.commit()

    def set_date_floor(self, platform: str, username: str,
                       floor: str | None) -> None:
        self.conn.execute(
            """INSERT INTO checkpoints (platform, username, date_floor)
               VALUES (?,?,?)
               ON CONFLICT(platform, username)
               DO UPDATE SET date_floor=excluded.date_floor""",
            (platform, username, floor),
        )
        self.conn.commit()

    def get_checkpoint(self, platform: str, username: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM checkpoints WHERE platform=? AND username=?",
            (platform, username),
        ).fetchone()

    def get_last_run(self, platform: str, username: str) -> "datetime | None":
        r = self.get_checkpoint(platform, username)
        if not r or not r["last_run_utc"]:
            return None
        try:
            dt = datetime.fromisoformat(r["last_run_utc"].replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def get_date_floor(self, platform: str, username: str) -> str | None:
        r = self.get_checkpoint(platform, username)
        return r["date_floor"] if r else None

    def clear_checkpoint(self, platform: str, username: str) -> None:
        self.conn.execute(
            "DELETE FROM checkpoints WHERE platform=? AND username=?",
            (platform, username),
        )
        self.conn.commit()

    # ── Circuit breaker ──────────────────────────────────────────────────

    def bump_circuit_fail(self, platform: str, error: str) -> int:
        with self._immediate() as cur:
            cur.execute(
                """INSERT INTO circuit (platform, consecutive_fails, last_error)
                   VALUES (?, 1, ?)
                   ON CONFLICT(platform) DO UPDATE
                     SET consecutive_fails = consecutive_fails + 1,
                         last_error = excluded.last_error""",
                (platform, (error or "")[:_ERROR_CAP]),
            )
            n = cur.execute(
                "SELECT consecutive_fails FROM circuit WHERE platform=?",
                (platform,),
            ).fetchone()["consecutive_fails"]
        return n

    def trip_circuit(self, platform: str, until: datetime) -> None:
        self.conn.execute(
            """INSERT INTO circuit (platform, tripped_until_utc)
               VALUES (?,?)
               ON CONFLICT(platform) DO UPDATE
                 SET tripped_until_utc=excluded.tripped_until_utc""",
            (platform, until.strftime("%Y-%m-%dT%H:%M:%SZ")),
        )
        self.conn.commit()

    def reset_circuit(self, platform: str) -> None:
        self.conn.execute(
            """UPDATE circuit
                  SET consecutive_fails=0, tripped_until_utc=NULL, last_error=NULL
                WHERE platform=?""",
            (platform,),
        )
        self.conn.commit()

    def circuit_state(self, platform: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM circuit WHERE platform=?", (platform,),
        ).fetchone()

    def get_circuit(self, platform: str) -> dict:
        """circuit_state as a dict, with a zeroed default when no row
        exists yet. Mirrors the old ArchiveDB contract so callers can
        index the result unconditionally (no None-guard at every site)."""
        r = self.circuit_state(platform)
        if r is None:
            return {"platform": platform, "consecutive_fails": 0,
                    "tripped_until_utc": None, "last_error": None}
        return {"platform": r["platform"],
                "consecutive_fails": r["consecutive_fails"],
                "tripped_until_utc": r["tripped_until_utc"],
                "last_error": r["last_error"]}

    # ── Metadata k/v ───────────────────────────────────────────────────────

    def meta_get(self, key: str) -> str | None:
        r = self.conn.execute(
            "SELECT value FROM metadata WHERE key=?", (key,),
        ).fetchone()
        return r["value"] if r else None

    def meta_set(self, key: str, value: str) -> None:
        self.conn.execute(
            """INSERT INTO metadata (key, value) VALUES (?,?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (key, value),
        )
        self.conn.commit()
