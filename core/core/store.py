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
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

from . import schema
from .models import Item, Status
from .files import media_bucket, album_bucket, ORPHANED_SOURCE_NAME, ALBUM_MAX

log = logging.getLogger(__name__)

_CLAIM_RETRIES = 5

# SEND-ORDER CLUSTERING. The queue drains by (priority, discovered_at), which
# alone scatters a user's media: files downloaded in a later run get a later
# discovered_at, so another user who started in between sorts ahead of them and
# the first user's uploads end up split across the timeline. We instead order by
# the *cluster's anchor* — the FIRST-APPEARANCE time per (platform, username),
# i.e. MIN(discovered_at) over ALL that user's rows — so a user's whole block
# stays contiguous, positioned where that user first entered the queue, and
# files enqueued for them later join that block instead of going to the tail.
#
# First-appearance (not "oldest pending") is deliberate: a per-pending anchor
# shifts as items drain, so once a user's earliest items are sent their later
# items would lose their place and interleave again. Anchoring on the stable
# first-appearance keeps singles and >ALBUM_MAX runs contiguous across claims.
# It's a correlated MIN over (platform, username) — cheap via idx_items_user_disc.
#
# cl_prio keeps priority authoritative at cluster granularity (a higher-priority
# recorder/orphaned cluster still drains first); within a cluster rows keep
# (priority, discovered_at) order. Album membership is unchanged — this only
# decides which anchor is claimed next, never what gathers into the album.
_CLUSTER_COLS = (
    "(SELECT MIN(priority) FROM items c "
    " WHERE c.platform=items.platform AND c.username=items.username) AS cl_prio, "
    "(SELECT MIN(discovered_at) FROM items c "
    " WHERE c.platform=items.platform AND c.username=items.username) AS cl_disc"
)
_CLUSTER_ORDER = "cl_prio ASC, cl_disc ASC, priority ASC, discovered_at ASC"

# busy_timeout (schema.connect) already blocks-and-retries inside SQLite for
# brief writer contention. But a bulk producer can hold the single WAL writer
# lock past that window, and a momentary lock must never kill a long-running
# daemon. So acquiring the write lock backs off and retries beyond busy_timeout
# rather than letting OperationalError('database is locked') escape. A failed
# BEGIN leaves no open transaction, so each retry is clean.
_BEGIN_RETRIES = 6


class ClaimContentionError(RuntimeError):
    """claim_* lost every CAS retry to a concurrent claimer."""

    def __init__(self, retries: int = _CLAIM_RETRIES) -> None:
        self.retries = retries
        super().__init__(
            f"claim exhausted after {retries} retries under contention"
        )


class IllegalTransition(RuntimeError):
    """A lifecycle write was attempted from a status the state machine
    doesn't allow it from (see core.models and _ALLOWED)."""


# The status state machine as data, not prose. This is the executable form of
# the diagram in core.models: the set of states a row may legally MOVE TO from
# each current state via the automated lifecycle. 'sent'/'failed' are terminal
# — they have no automated successor and are left only by an explicit admin
# reset (reset_*/retry), which is deliberately NOT modelled here. Lifecycle
# writers pass the allowed predecessor set as a SQL guard, so an out-of-state
# row is never silently rewritten.
_ALLOWED: dict[str, frozenset[str]] = {
    "pending": frozenset({"sending"}),
    "sending": frozenset({"sent", "failed", "pending"}),
    "sent":    frozenset(),
    "failed":  frozenset(),
}

_ERROR_CAP = 1000

# A manually `cancel`-led row is parked in status='failed' (no schema room for a
# distinct 'cancelled' status without a lockstep version bump), so it must be
# told apart from a genuine delivery failure: a deliberate abort, unlike a
# transient failure, must NOT be resurrected by the bulk reset paths
# (auto_retry_failed housekeeping or `reset failed`). We stamp this sentinel as
# the row's last_error and the reset chokepoint (_reset_to_pending) skips any row
# carrying it. The targeted `retry(id)` clears last_error, so it remains the
# explicit way to force one cancelled row back. Kept short and bracketed so it
# can't be mistaken for (or collide with) a real server error string, and it
# contains no LIKE wildcards (% _) so the prefix match is literal.
CANCELLED_MARKER = "[cancelled]"

# Substrings (matched case-insensitively against last_error) that mark a failure
# as TRANSIENT — a network / upload-corruption / server-side cause that heals on
# its own, so the row is safe to auto-re-arm. The list is deliberately SMALL and
# specific: the classifier (is_transient_failure) defaults to PERMANENT for
# anything not listed, so a poison row (media Telegram rejects, oversized,
# missing file, unroutable) is never resurrected into a retry storm. Grow this
# list only for causes proven to recover unattended.
# NOTE: "filepartsinvalid" is deliberately NOT listed. It looks transient (a bad
# part can re-upload cleanly), but the dominant cause is an OVERSIZE file whose
# 512 KiB part count exceeds Telegram's ~8000-part ceiling — which can NEVER
# succeed on retry, so auto-re-arming it produced an endless re-upload storm
# (re-pushing multi-GB every cycle). The real fix is upstream: media_prep's
# upload ceiling now splits such files before they queue (see max_upload_bytes).
# A residual FilePartsInvalid is thus a poison row — left for a deliberate
# `reset failed` once its oversize source is split or removed.
_TRANSIENT_FAILURE_SIGNATURES = (
    "connection",         # ConnectionError / "Connection to Telegram failed N time(s)"
    "floodwait",          # residual flood surfacing as an error string
    "timed out", "timeout",
    "stall",              # send stall-watchdog deadline
    "reconnect",
    "servererror", "rpcerror", "internalservererror",  # transient server-side
    "temporaryfailure", "network is unreachable", "broken pipe",
)


def is_transient_failure(last_error: str | None) -> bool:
    """True only for failures KNOWN to be transient (network / upload corruption
    / server-side) and therefore safe to auto-re-arm. Conservative by design:
    unknown OR permanent causes (media rejected, oversized, missing file,
    unroutable) → False, so the poison rows that make a blanket reset dangerous
    are exactly the ones this skips. A manual abort (CANCELLED_MARKER) is never
    transient. The bias is toward NOT re-arming — a misclassified permanent error
    just stays quarantined for a deliberate `reset failed`, never worse than the
    opt-in-off status quo."""
    if not last_error:
        return False
    low = last_error.lower()
    if low.startswith(CANCELLED_MARKER):
        return False
    return any(sig in low for sig in _TRANSIENT_FAILURE_SIGNATURES)


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
        # Unit-of-Work state. _batch_depth>0 means an open batch() owns the
        # transaction, so the autocommit methods defer their commit to it.
        self._batch_depth = 0
        self._batch_ops = 0
        self._batch_flush_every = 0

    @classmethod
    def open(cls, db_path: str | None = None) -> "ItemStore":
        return cls(schema.connect(db_path))

    def close(self) -> None:
        # PRAGMA optimize before closing a long-lived connection is SQLite's
        # recommended discipline: it refreshes the stat tables the query
        # planner uses (e.g. the partial pending/hash indexes), so the next
        # process to open the DB plans against current statistics. Best-effort
        # — never let housekeeping block a clean shutdown.
        try:
            self.conn.execute("PRAGMA optimize")
        except sqlite3.Error:
            pass
        self.conn.close()

    def __enter__(self) -> "ItemStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    @contextmanager
    def _immediate(self) -> Iterator[sqlite3.Cursor]:
        """BEGIN IMMEDIATE so the write lock is taken up front, making the
        read-then-write inside a claim atomic against other writers.

        Inside an open batch() the surrounding Unit-of-Work already owns the
        transaction, so we neither BEGIN nor commit here — the write simply
        joins the batch and is flushed with it. (Producers don't call claim
        mid-batch; this is just so any nested write stays correct.)"""
        if self._batch_depth:
            yield self.conn.cursor()
            return
        cur = self.conn.cursor()
        self._begin_immediate(cur)
        try:
            yield cur
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _begin_immediate(self, cur: sqlite3.Cursor) -> None:
        """Take the WAL write lock up front, tolerating transient cross-process
        contention. A producer holding the writer lock past busy_timeout would
        otherwise surface as OperationalError('database is locked') and, with no
        retry here, propagate out of claim_* and crash the dispatcher. Retrying
        with backoff turns that momentary lock into a brief wait. Only 'locked'
        errors retry; any other OperationalError is re-raised immediately."""
        for i in range(_BEGIN_RETRIES):
            try:
                cur.execute("BEGIN IMMEDIATE")
                return
            except sqlite3.OperationalError as e:
                if "locked" not in str(e).lower() or i == _BEGIN_RETRIES - 1:
                    raise
                log.warning("write lock contended, retrying (%d/%d)",
                            i + 1, _BEGIN_RETRIES)
                time.sleep(0.2 * (2 ** i))

    def _commit(self) -> None:
        """Commit one autocommit-style write — UNLESS a batch() is open, in
        which case the write is folded into the batch transaction and flushed
        either every flush_every ops (to keep the cross-process write lock
        short) or when the batch closes."""
        if not self._batch_depth:
            self.conn.commit()
            return
        self._batch_ops += 1
        if self._batch_flush_every and self._batch_ops >= self._batch_flush_every:
            self.conn.commit()
            self._batch_ops = 0

    @contextmanager
    def batch(self, *, flush_every: int = 500) -> "Iterator[ItemStore]":
        """Unit of Work: fold many small writes into few transactions.

        Bulk producers (reconcile, bootstrap, orphaned ingest) call add_item
        hundreds–thousands of times; one commit each is one fsync each. Under
        a batch() those commits are deferred and flushed in groups of
        flush_every, turning N transactions into ~N/flush_every. flush_every
        is bounded ON PURPOSE: a single giant transaction would hold the
        write lock for the whole scan and stall the dispatcher's claim; periodic
        flushes keep each lock window short while still amortizing the syncs.

        Re-entrant: only the outermost batch begins/commits. A nested batch()
        folds into the outer one — it must NOT commit on entry/exit, or it would
        prematurely flush (and thereby make un-rollback-able) the outer batch's
        in-flight writes. On any exception the in-flight (un-flushed) writes roll
        back; already-flushed groups are durable — the same resumable semantics
        the backfill relies on."""
        outer = self._batch_depth == 0
        if outer:
            self.conn.commit()             # outer only: start from a clean slate
            self._batch_flush_every = max(1, flush_every)
            self._batch_ops = 0
        self._batch_depth += 1
        try:
            yield self
            if outer:
                self.conn.commit()
                # A bulk batch can append a lot to the WAL; fold it back into
                # the main DB now so the file doesn't stay bloated between the
                # default auto-checkpoints. PASSIVE never blocks — it moves
                # whatever frames it can and silently does nothing if a reader
                # (e.g. ops) is mid-scan. Best-effort housekeeping only.
                try:
                    self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
                except sqlite3.Error:
                    pass
        except Exception:
            if outer:
                self.conn.rollback()
            raise
        finally:
            self._batch_depth -= 1
            if outer:
                self._batch_flush_every = 0
                self._batch_ops = 0

    def _guarded_set(self, cur, item_id: int, *, to: str,
                     allowed_from: "frozenset[str] | set[str]",
                     set_sql: str = "", params: tuple = ()) -> int:
        """Apply a lifecycle status write guarded by the legal predecessor
        set. The `AND status IN (...)` clause is what actually enforces the
        state machine: a row not in `allowed_from` matches 0 rows and is left
        untouched. Returns the affected row count so callers can detect (and
        log) an unexpected out-of-state row. `set_sql`/`params` carry any extra
        columns to write alongside status."""
        marks = ",".join("?" * len(allowed_from))
        cur.execute(
            f"UPDATE items SET status=?{set_sql} "
            f"WHERE id=? AND status IN ({marks})",
            (to, *params, item_id, *allowed_from),
        )
        return cur.rowcount

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
        topic_id:        int | None = None,
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
          - topic_id     → explicit forum-topic thread (twin of chat_id; NULL →
                           the chat's General topic)
        """
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO items
                 (source, platform, username, identifier, file_path,
                  upload_date, file_size_bytes, title, discovered_at,
                  status, priority, caption, attempts,
                  content_hash, chat_id, group_key, topic_id)
               VALUES (?,?,?,?,?,?,?,?,?, 'pending', ?, ?, 0, ?, ?, ?, ?)""",
            (source, platform, username, identifier, file_path,
             upload_date, file_size_bytes, title, now_iso(),
             priority, caption, content_hash, chat_id, group_key, topic_id),
        )
        self._commit()
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

    def id_of(self, file_path: str) -> int | None:
        """Row id for a path, or None. Lets a producer learn the id of the row
        it just inserted without reaching into the connection directly (so the
        ingest primitive can depend on the narrow ProducerStore role)."""
        r = self.conn.execute(
            "SELECT id FROM items WHERE file_path=?", (file_path,),
        ).fetchone()
        return r["id"] if r else None

    def distinct_destinations(self) -> "list[tuple[str, int | None]]":
        """Every distinct explicit (chat_id, topic_id) currently in the queue —
        the rows whose chat_id is set (orphaned / topic-routed items). Powers
        `dispatcher check-routes`: the set of explicit destinations whose
        existence on Telegram is worth pre-verifying. Platform/recorder rows
        (chat_id NULL, resolved from env at send) are not enumerable here."""
        rows = self.conn.execute(
            "SELECT DISTINCT chat_id, topic_id FROM items "
            "WHERE chat_id IS NOT NULL ORDER BY chat_id, topic_id"
        ).fetchall()
        return [(r["chat_id"], r["topic_id"]) for r in rows]

    def find_by_content_hash(self, content_hash: str) -> Item | None:
        """Existing row sharing these exact bytes, or None. Drives ingest-time
        global dedup: if a row already holds this content_hash, the incoming
        file is a duplicate and never gets a second row.

        Prefers a DELIVERABLE twin over a permanently-failed one: a 'failed'
        row will never be sent, so collapsing onto it would silently drop the
        re-introduced bytes. Order sent → sending → pending → failed so a live
        twin is returned when one exists (and ingest re-arms a lone failed twin
        instead of dropping the copy). Also makes reconcile's already-sent
        detection deterministic when both a sent and a pending twin exist."""
        r = self.conn.execute(
            """SELECT * FROM items WHERE content_hash=?
               ORDER BY CASE status
                          WHEN 'sent'    THEN 0
                          WHEN 'sending' THEN 1
                          WHEN 'pending' THEN 2
                          ELSE 3
                        END
               LIMIT 1""",
            (content_hash,),
        ).fetchone()
        return Item.from_row(r) if r else None

    def rearm_failed(self, item_id: int) -> bool:
        """failed → pending for one row (attempts reset), or False if the row
        is no longer 'failed'. Used by ingest when an identical-bytes file is
        re-introduced but its only twin had permanently failed: those bytes
        were never delivered, so the row is re-armed rather than the incoming
        copy being dropped as a dedup. Guarded on 'failed' so a racing delivery
        (a twin reset/resent out from under us) is never overwritten.

        Also skips a deliberately-cancelled row (CANCELLED_MARKER): a manual
        abort is durable across the automatic re-arm paths just as it is across
        the bulk resets (see _reset_to_pending) — re-introducing the file must
        not resurrect it. The incoming copy is then dedup-collapsed against the
        kept file (byte-identical, no data lost); retry(id) remains the explicit
        single-row override."""
        cur = self.conn.execute(
            """UPDATE items SET status='pending', attempts=0, claimed_at=NULL,
                   sent_at=NULL, last_error=NULL
               WHERE id=? AND status='failed'
                 AND (last_error IS NULL OR last_error NOT LIKE ?)""",
            (item_id, CANCELLED_MARKER + "%"),
        )
        self._commit()
        return cur.rowcount > 0

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
        """Atomically claim the next pending item in send order (pending →
        sending). Returns None when nothing is pending. Raises
        ClaimContentionError when retries are exhausted under contention.

        Send order is cluster-aware (see _CLUSTER_ORDER): the next item is the
        head of the earliest-anchored (platform, username) cluster, so a user's
        items drain contiguously rather than interleaving with others'."""
        for _ in range(_CLAIM_RETRIES):
            with self._immediate() as cur:
                row = cur.execute(
                    f"""SELECT id FROM (
                            SELECT id, priority, discovered_at, {_CLUSTER_COLS}
                              FROM items WHERE status='pending'
                        ) ORDER BY {_CLUSTER_ORDER} LIMIT 1"""
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

        The anchor is the head of the earliest-anchored (platform, username)
        cluster in send order (see _CLUSTER_ORDER) — so a user's media drains
        contiguously instead of interleaving with other users'. The album is
        then everything sharing the anchor's (platform, username, source,
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
        # DESTINATION is part of the batch identity: an album must never mix two
        # chats OR two forum topics. chat_id/topic_id are NULL for platform rows
        # (so NULLs must group together) → IFNULL sentinels. Without this, two
        # `.t1`/`.t2` topic folders that share a chat AND a subfolder name would
        # collide on group_disc and one topic's files would land in the other.
        DEST_DISC = "IFNULL(chat_id,''), IFNULL(topic_id,-1)"
        gated = min_batch is not None or flush_age_s is not None

        for _ in range(_CLAIM_RETRIES):
            with self._immediate() as cur:
                if not gated:
                    anchor = cur.execute(
                        f"""SELECT id, platform, username, source, file_path,
                                  group_disc, chat_disc, topic_disc
                             FROM (
                                SELECT id, platform, username, source, file_path,
                                       priority, discovered_at,
                                       {GROUP_DISC} AS group_disc,
                                       IFNULL(chat_id,'') AS chat_disc,
                                       IFNULL(topic_id,-1) AS topic_disc,
                                       {_CLUSTER_COLS}
                                  FROM items WHERE status='pending'
                             ) ORDER BY {_CLUSTER_ORDER} LIMIT 1"""
                    ).fetchone()
                    if anchor is None:
                        return []
                    chosen = self._gather_group(cur, anchor, GROUP_DISC, max_items)
                else:
                    pending = cur.execute(
                        f"""SELECT id, platform, username, source, file_path,
                                  discovered_at, group_disc, chat_disc, topic_disc
                             FROM (
                                SELECT id, platform, username, source, file_path,
                                       priority, discovered_at,
                                       {GROUP_DISC} AS group_disc,
                                       IFNULL(chat_id,'') AS chat_disc,
                                       IFNULL(topic_id,-1) AS topic_disc,
                                       {_CLUSTER_COLS}
                                  FROM items WHERE status='pending'
                             ) ORDER BY {_CLUSTER_ORDER}"""
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
        (platform, username, source, group_disc, chat_id, topic_id), capped at
        max_items. A 'single'-bucket anchor yields just itself (gifs/other never
        album). chat_id/topic_id are in the key so an album is homogeneous in
        DESTINATION — never two chats, never two forum topics.

        BUCKET is source-aware (album_bucket): chat_id folders (orphaned) group
        by 'media' (mixed photo+video) vs 'document' (.mkv/.gif grouped with each
        other); every other producer keeps the photo/video/single split."""
        bucket = album_bucket(anchor["source"], anchor["file_path"])
        if bucket == "single":
            return [anchor]
        candidates = cur.execute(
            f"""SELECT * FROM items
                 WHERE status='pending'
                   AND platform=? AND username=? AND source=?
                   AND {group_disc_sql}=?
                   AND IFNULL(chat_id,'')=? AND IFNULL(topic_id,-1)=?
                 ORDER BY priority ASC, discovered_at ASC""",
            (anchor["platform"], anchor["username"], anchor["source"],
             anchor["group_disc"], anchor["chat_disc"], anchor["topic_disc"]),
        ).fetchall()
        chosen = []
        for row in candidates:
            if album_bucket(row["source"], row["file_path"]) == bucket:
                chosen.append(row)
            if len(chosen) >= max_items:
                break
        return chosen

    def _select_eligible_group(
        self, cur, pending, group_disc_sql: str, max_items: int,
        min_batch, flush_age_s,
    ) -> list:
        """Scan pending rows (already in cluster send order) and return the
        first group that clears the min-batch gate; [] if none is ready yet.

        A non-'single' group is eligible when it has >= min_batch(anchor)
        in-bucket items, OR its oldest item has aged past flush_age_s(anchor)
        (the anti-starvation flush). Under-threshold groups are deferred and
        skipped so a lower-priority ready group can still drain.

        ORDERING (chat_id folders): within one subfolder, a 'document' group
        (grouped .mkv/.gif) is held back while ANY 'media' sibling is still
        pending, so every inline photo/video album of the subfolder ships before
        its documents. Once the media has drained (no longer pending), the
        document group becomes eligible on a later claim."""
        deferred: set = set()
        for anchor in pending:
            bucket = album_bucket(anchor["source"], anchor["file_path"])
            gkey = (anchor["platform"], anchor["username"], anchor["source"],
                    anchor["group_disc"], anchor["chat_disc"],
                    anchor["topic_disc"], bucket)
            if gkey in deferred:
                continue
            if (anchor["source"] == ORPHANED_SOURCE_NAME and bucket == "document"
                    and self._has_pending_media_sibling(pending, anchor)):
                continue                 # media of this subfolder must go first
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

    @staticmethod
    def _has_pending_media_sibling(pending, doc_anchor) -> bool:
        """True iff another pending row in the SAME chat_id subfolder (matching
        platform/username/source/group_disc/chat/topic) is 'media'-kind — i.e.
        an inline photo/video still waiting to ship. Drives the media-before-
        documents ordering for chat_id folders. Cheap: `pending` is already in
        memory (the gated scan loaded it once)."""
        for r in pending:
            if (r["source"] == doc_anchor["source"]
                    and r["platform"] == doc_anchor["platform"]
                    and r["username"] == doc_anchor["username"]
                    and r["group_disc"] == doc_anchor["group_disc"]
                    and r["chat_disc"] == doc_anchor["chat_disc"]
                    and r["topic_disc"] == doc_anchor["topic_disc"]
                    and album_bucket(r["source"], r["file_path"]) == "media"):
                return True
        return False

    def mark_sent(self, item_id: int, *, tg_message_id: int | None = None) -> None:
        """sending → sent. Guarded on 'sending': a row that isn't in flight
        (already terminal, or reset out from under us) is never overwritten."""
        with self._immediate() as cur:
            n = self._guarded_set(
                cur, item_id, to="sent", allowed_from={"sending"},
                set_sql=", sent_at=?, last_error=NULL, tg_message_id=?",
                params=(now_iso(), tg_message_id),
            )
        if n == 0:
            log.warning("mark_sent: id=%d not in 'sending' — no-op", item_id)

    def delete(self, item_id: int) -> int:
        """Hard-delete one row by id; returns rows deleted (0 or 1).

        The orphaned ship-and-delete path uses this: once a chat_id-folder file
        is uploaded AND removed from disk, the row is pure trace with no
        re-ingest or dedup value, so it's dropped entirely. The CALLER must
        ensure the file is gone first — a row deleted while its file remains
        would be re-ingested (ingest's ALREADY_KNOWN / content_hash guards both
        key off the row existing)."""
        with self._immediate() as cur:
            cur.execute("DELETE FROM items WHERE id=?", (item_id,))
            return cur.rowcount

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
        unconditionally after calling this. Guarded on 'sending' (the row was
        just claimed) so a dedup verdict can't resurrect a terminal row."""
        with self._immediate() as cur:
            n = self._guarded_set(
                cur, item_id, to="sent", allowed_from={"sending"},
                set_sql=", sent_at=?, claimed_at=NULL, last_error=?",
                params=(now_iso(),
                        f"deduped: bytes already sent by id={twin_id}"),
            )
        if n == 0:
            log.warning("mark_deduplicated: id=%d not in 'sending' — no-op",
                        item_id)

    def mark_failed(self, item_id: int, *, error: str, max_retries: int) -> str:
        """Record a failed attempt. attempts was already incremented at
        claim, so attempts>=max_retries means we've used the budget →
        'failed' (terminal). Otherwise → 'pending' for another go.
        Returns the resulting status. Guarded on 'sending' (the only state a
        send outcome can legally arrive from) — a stray failure for an already
        terminal/reset row is logged and ignored, never written through."""
        with self._immediate() as cur:
            r = cur.execute(
                "SELECT attempts, status FROM items WHERE id=?", (item_id,),
            ).fetchone()
            if r is None:
                log.warning("mark_failed: id=%d not found", item_id)
                return "missing"
            new_status = (Status.FAILED.value
                          if r["attempts"] >= max_retries
                          else Status.PENDING.value)
            n = self._guarded_set(
                cur, item_id, to=new_status, allowed_from={"sending"},
                set_sql=", last_error=?, claimed_at=NULL",
                params=((error or "")[:_ERROR_CAP],),
            )
            if n == 0:
                log.warning("mark_failed: id=%d not in 'sending' (was %s) — no-op",
                            item_id, r["status"])
                return r["status"]
            return new_status

    def quarantine(self, item_id: int, *, error: str) -> str:
        """sending → failed TERMINALLY on the first hit, regardless of the retry
        budget. For deterministic-per-moment rejections (MediaEmptyError) where
        retrying within the budget is futile AND keeps a head-of-line row cycling,
        blocking the queue. Unlike cancel() this leaves NO CANCELLED_MARKER, so a
        quarantined row is a plain failure: `reset failed` re-arms it once the
        (often transient) cause clears. Guarded on 'sending'. Returns the status."""
        with self._immediate() as cur:
            n = self._guarded_set(
                cur, item_id, to=Status.FAILED.value, allowed_from={"sending"},
                set_sql=", last_error=?, claimed_at=NULL",
                params=((error or "")[:_ERROR_CAP],),
            )
        if n == 0:
            log.warning("quarantine: id=%d not in 'sending' — no-op", item_id)
            return "no-op"
        return Status.FAILED.value

    def requeue(self, item_id: int, *, reason: str | None = None) -> None:
        """sending → pending WITHOUT burning a retry (FloodWait: we waited
        the server-requested time; the request itself wasn't a failure).
        Decrement attempts to undo the claim's increment. Guarded on
        'sending': only an in-flight send can be requeued."""
        with self._immediate() as cur:
            n = self._guarded_set(
                cur, item_id, to="pending", allowed_from={"sending"},
                set_sql=", claimed_at=NULL, attempts=MAX(0, attempts-1), "
                        "last_error=?",
                params=(reason,),
            )
        if n == 0:
            log.warning("requeue: id=%d not in 'sending' — no-op", item_id)

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

    def prune_failed(self, older_than_days: float) -> int:
        """Retention GC: delete terminal 'failed' rows older than the window.
        Returns rows deleted. older_than_days <= 0 disables (returns 0).

        A 'failed' row is inert — the drain loop only ever claims 'pending' —
        but it otherwise lives forever, so this caps unbounded growth (e.g. the
        tombstones left by files deleted off disk and never restored).

        Anchored on discovered_at: there is no failed_at column (adding one
        would bump the schema version and force every component to upgrade in
        lockstep). For the common case — a missing file that fails on its first
        claim — discovered_at ≈ the failure time; more generally 'in the system
        N+ days and still undelivered' is exactly what we want to collect.

        TRADE-OFF: pruning a row drops the tombstone that stopped a reconcile
        sweep from re-enqueuing that exact path/identity. With a multi-day
        window that's intended — by then the file is genuinely gone — and if it
        does reappear, re-queuing it is the correct behavior anyway."""
        if older_than_days <= 0:
            return 0
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(days=older_than_days)
                  ).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._immediate() as cur:
            cur.execute(
                "DELETE FROM items WHERE status='failed' AND discovered_at < ?",
                (cutoff,),
            )
            n = cur.rowcount
        if n:
            log.info("retention: pruned %d failed row(s) older than %.1f day(s)",
                     n, older_than_days)
        return n

    def delete_failed_missing(self) -> int:
        """Delete terminal 'failed' rows whose file is gone from disk. Returns
        rows deleted.

        A failed row whose bytes no longer exist can never be delivered — it's a
        pure tombstone. Re-queuing it (auto_retry_failed) would just have the
        dispatcher claim a vanished path and burn the retry budget re-failing it,
        so this cleanup runs UNCONDITIONALLY and BEFORE any auto-retry sweep.

        Same tombstone trade-off as prune_failed: dropping the row lets a future
        reconcile re-enqueue that exact path if the file ever reappears — which
        is the correct behavior. A blank/NULL file_path is treated as missing.

        stat() per failed row, in Python rather than SQL (SQLite can't see the
        filesystem); 'failed' is a small, inert set so this stays cheap."""
        from pathlib import Path as _Path

        rows = self.conn.execute(
            "SELECT id, file_path FROM items WHERE status='failed'"
        ).fetchall()
        gone = [r["id"] for r in rows
                if not r["file_path"] or not _Path(r["file_path"]).exists()]
        if not gone:
            return 0
        with self._immediate() as cur:
            cur.executemany("DELETE FROM items WHERE id=?",
                            [(i,) for i in gone])
        log.info("cleanup: deleted %d failed row(s) whose file is missing",
                 len(gone))
        return len(gone)

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

    def sent_items(
        self,
        *,
        platform: str | None = None,
        username: str | None = None,
        source:   str | None = None,
    ) -> list[Item]:
        """Every delivered ('sent') row, with optional scope filters. Backs the
        `purge-sent` command (delete on-disk copies of already-uploaded files).
        Built dynamically so a bare call returns the whole sent backlog while
        any combination of platform/user/source narrows it."""
        where = ["status='sent'"]
        params: list = []
        for col, val in (("platform", platform), ("username", username),
                         ("source", source)):
            if val is not None:
                where.append(f"{col}=?")
                params.append(val)
        rows = self.conn.execute(
            f"SELECT * FROM items WHERE {' AND '.join(where)} "
            f"ORDER BY platform, username, id",
            params,
        ).fetchall()
        return [Item.from_row(r) for r in rows]

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

    def last_sent_at(self) -> str | None:
        """ISO-8601 UTC timestamp of the most recent successful send, or None.
        The one-line liveness signal for `status` displays: a healthy drain
        keeps this fresh, a wedged one lets it age — which is how a stalled
        pipeline gets noticed before the queue depth does."""
        row = self.conn.execute(
            "SELECT MAX(sent_at) AS t FROM items WHERE status='sent'",
        ).fetchone()
        return row["t"]

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
        self._commit()
        return cur.rowcount > 0

    def cancel(self, item_id: int) -> bool:
        """pending|sending → failed. CLI manual abort.

        Stamps CANCELLED_MARKER into last_error so the bulk reset paths
        (auto_retry_failed housekeeping and `reset failed`) treat this as a
        deliberate abort and never resurrect it. Force one back with retry(id),
        which clears last_error."""
        cur = self.conn.execute(
            """UPDATE items SET status='failed', claimed_at=NULL, last_error=?
                   WHERE id=? AND status IN ('pending','sending')""",
            (CANCELLED_MARKER + " manual abort", item_id),
        )
        self._commit()
        return cur.rowcount > 0

    # ── Reset operations (one write each — no second DB to also reset) ─────

    def reset_failed(self, platform: str | None, username: str | None) -> int:
        """failed → pending. The dispatcher re-sends on its next poll; no
        re-enqueue, no cross-DB cleanup, no idempotency key to fight."""
        return self._reset_to_pending(("failed",), platform, username)

    def reset_failed_transient(self) -> int:
        """failed → pending for ONLY the rows whose last_error is_transient_failure
        — network / upload-corruption / server-side causes that recover on their
        own. Permanent failures (media rejected, oversized, unroutable) and
        manually-cancelled rows are left untouched. This is the storm-SAFE,
        default-on companion to the opt-in auto_retry_failed (which re-arms
        everything): the poison rows that force that policy off are exactly the
        ones this skips. Returns the number of rows re-armed.

        Classification is in Python (single source of truth, testable) rather than
        SQL LIKEs; the failed set is tiny, so the SELECT-then-UPDATE is cheap. The
        UPDATE re-checks status='failed' so a row delivered out from under us
        between the read and the write is never clobbered."""
        rows = self.conn.execute(
            "SELECT id, last_error FROM items WHERE status='failed'"
        ).fetchall()
        ids = [r["id"] for r in rows if is_transient_failure(r["last_error"])]
        if not ids:
            return 0
        marks = ",".join("?" * len(ids))
        cur = self.conn.execute(
            f"UPDATE items SET status='pending', claimed_at=NULL, sent_at=NULL, "
            f"attempts=0, last_error=NULL "
            f"WHERE id IN ({marks}) AND status='failed'",
            ids,
        )
        self._commit()
        return cur.rowcount

    def reset_uploads(self, platform: str | None, username: str | None) -> int:
        """Re-send everything (sent + failed) → pending. WARNING: 'sent'
        rows re-sent will duplicate on Telegram; intended for deliberate
        re-delivery."""
        return self._reset_to_pending(("sent", "failed"), platform, username)

    def _reset_to_pending(self, statuses: tuple[str, ...],
                          platform: str | None, username: str | None) -> int:
        # Skip deliberately-cancelled rows: cancel() parks them in 'failed' with
        # CANCELLED_MARKER, and a manual abort must survive every bulk reset
        # (auto_retry housekeeping AND `reset failed`/`reset uploads`). retry(id)
        # remains the explicit single-row override (it clears last_error).
        marks = ",".join("?" * len(statuses))
        sql = (f"UPDATE items SET status='pending', claimed_at=NULL, "
               f"sent_at=NULL, attempts=0, last_error=NULL "
               f"WHERE status IN ({marks}) "
               f"AND (last_error IS NULL OR last_error NOT LIKE ?)")
        params: list = list(statuses) + [CANCELLED_MARKER + "%"]
        if platform:
            sql += " AND platform=?"; params.append(platform)
        if username:
            sql += " AND username=?"; params.append(username)
        cur = self.conn.execute(sql, params)
        self._commit()
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
        self._commit()
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
        self._commit()

    def set_date_floor(self, platform: str, username: str,
                       floor: str | None) -> None:
        self.conn.execute(
            """INSERT INTO checkpoints (platform, username, date_floor)
               VALUES (?,?,?)
               ON CONFLICT(platform, username)
               DO UPDATE SET date_floor=excluded.date_floor""",
            (platform, username, floor),
        )
        self._commit()

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
        self._commit()

    # ── Full-history gate ────────────────────────────────────────────────
    #
    # A newly added user has no checkpoint row → needs_full_history is True,
    # so _compute_date_min returns None and the extractor walks the WHOLE
    # timeline. After the first successful download the orchestrator calls
    # mark_full_history_done, flipping the user to incremental forever after.
    # rearm_full_history re-opens the gate on demand (`run --full-history`)
    # without deleting any rows or files — the gallery-dl/yt-dlp archive still
    # skips everything already fetched, so only missing old posts come down.

    def needs_full_history(self, platform: str, username: str) -> bool:
        r = self.get_checkpoint(platform, username)
        if r is None:
            return True                       # brand-new user, never run
        return not r["full_history_done"]

    def mark_full_history_done(self, platform: str, username: str) -> None:
        self.conn.execute(
            """INSERT INTO checkpoints (platform, username, full_history_done)
               VALUES (?,?,1)
               ON CONFLICT(platform, username)
               DO UPDATE SET full_history_done=1""",
            (platform, username),
        )
        self._commit()

    def rearm_full_history(self, platform: str, username: str) -> None:
        self.conn.execute(
            """INSERT INTO checkpoints (platform, username, full_history_done)
               VALUES (?,?,0)
               ON CONFLICT(platform, username)
               DO UPDATE SET full_history_done=0""",
            (platform, username),
        )
        self._commit()

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
        self._commit()

    def reset_circuit(self, platform: str) -> None:
        self.conn.execute(
            """UPDATE circuit
                  SET consecutive_fails=0, tripped_until_utc=NULL, last_error=NULL
                WHERE platform=?""",
            (platform,),
        )
        self._commit()

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
        self._commit()
