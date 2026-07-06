"""
dispatcher.drain
────────────────
The drain loop, now reading/writing the ONE shared table via core.ItemStore.
No QueueDB, no second database, no reconcile bridge.

TEMPLATE METHOD: claim → send → finalize → cleanup. Each is one call.

CLAIM-THEN-SEND: we flip pending→sending before the send. A crash mid-send
leaves the row 'sending'; the startup watchdog (reset_stuck_sending) reverts
it to pending and we re-send. The only duplicate window is a crash between
send-success and mark_sent — unavoidable without Telegram idempotency keys,
and now at least auditable via tg_message_id once recorded.

CAPTION: items may carry a producer-set caption. If absent, the dispatcher
formats a default — caption is a presentation concern of the sender, so it
lives here, not duplicated into every producer.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from pathlib import Path

from core import (
    ClaimContentionError, QueueStore, Item, DeletePolicy, RecorderDeletePolicy,
    BatchPolicy, FailedRetryPolicy, ORPHANED_SOURCE, subfolder_of,
    DeletionGuard, is_split_group,
)
from core.files import media_bucket, orphaned_kind
from core import media_prep
from .tg_router import TelegramRouter, RouteError

from .config import DispatcherConfig
from .delete import maybe_delete
from .send import SendStrategy

log = logging.getLogger(__name__)

# Producers whose videos are already run through media_prep.prepare() at INGEST
# (archiver reconcile, orphaned chat_id folders). The dispatcher's send-time
# streamable net is the backstop for producers that DON'T prep — chiefly the
# recorder, whose remux is fail-soft. So for these sources a non-streamable file
# reaching the queue is intentional (e.g. an .mkv kept as a full-quality document
# beside its .mp4 preview) and must ship as-is, not be re-converted at send.
_PREPPED_AT_INGEST_SOURCES = {"archiver", ORPHANED_SOURCE}

# How often the drain loop re-runs its housekeeping: the failed-queue
# maintenance chain (drop missing-file tombstones → retention GC → auto-retry
# the survivors) and the stuck-'sending' watchdog. The retention WINDOW is
# config.failed_retention_days, auto-retry is gated by the auto_retry_failed
# policy, and the stuck threshold is config.stuck_claim_min; this is just how
# often we check. All of it lives here, in the queue owner, so failed-row
# lifecycle doesn't depend on the archiver loop being alive. The watchdog used
# to run only at startup,
# which left a row wedged in 'sending' (e.g. by a manual `queue cancel` race)
# stranded until the next dispatcher restart — self-healing should not require
# a restart. Safe mid-loop: the drain is serial, so at the top of an iteration
# this process has nothing in flight; only genuinely stale claims match.
# Cadence: both queries are trivial (indexed UPDATE/DELETE on a local SQLite),
# so checking every 15 min costs nothing and caps the worst-case time a
# wedged row waits for rescue at minutes, not the rest of the day (a SIGKILLed
# upload — e.g. launchd restart mid-send — strands exactly one such row).
_HOUSEKEEPING_EVERY_S = 15 * 60

# Circuit breaker: a SYSTEMIC fault (Telegram unreachable, auth lost, a wedged
# connection) makes every send fail the same way. Without a breaker the drain
# would churn the queue at full speed, burning each item's retry budget and
# marking thousands 'failed' during what is really a transient outage. So we
# count CONSECUTIVE systemic failures (network/stall/unknown — NOT item-specific
# ones like MediaEmptyError or a missing file, which say nothing about Telegram's
# health) and, once the threshold trips, pause the whole drain for a cooldown
# instead of hammering. Any single success resets the count. This mirrors the
# archiver's per-platform circuit breaker (core.store.bump_circuit_fail), kept
# in-memory here because the dispatcher's fault domain is the one shared session,
# not per-platform. Tuned conservative: a real backlog never sees 8 genuine
# systemic failures back-to-back, but an outage trips within seconds.
_CIRCUIT_TRIP_AT   = 8
_CIRCUIT_COOLDOWN_S = 60.0


def is_tiktok_live(item: Item) -> bool:
    return item.platform.lower() == "tiktok" and item.source.lower() == "recorder"


def _is_document_batch(head: Item) -> bool:
    """True iff this claimed batch is a chat_id-folder DOCUMENT album (grouped
    .mkv/.gif originals). The batch is homogeneous in kind — claim_batch groups
    orphaned rows by orphaned_kind — so the head decides for the whole album."""
    return (head.source == ORPHANED_SOURCE
            and orphaned_kind(head.file_path) == "document")


def with_live_tag(caption: str) -> str:
    return caption if "#live" in caption.split() else f"{caption} #live"


def orphaned_caption(batch: list[Item]) -> str:
    """Caption for a chat_id-folder batch: the subfolder name as a header,
    then one line per file (its stem). Works for a single file or an album —
    Telegram shows only the first item's caption, so packing every filename
    into that one caption is how all of them stay visible. Matches the
    requested 'Beach day / John / Jess' shape (newline-separated). A top-level
    loose file (no subfolder) → just its stem."""
    head = batch[0]
    # Space-join the subpath components so a leading `#tag` folder renders as a
    # real (clickable) Telegram hashtag: `#Asian/Eli Shaw` → `#Asian Eli Shaw`.
    # A `/` in the header would break the hashtag link (`#Asian/Eli` isn't one).
    sub = subfolder_of(head.chat_id, head.group_key).replace("/", " ")
    stems = [media_prep.clean_upload_stem(it.file_path) for it in batch]
    if not sub:
        # Individual file (no album group_key). A file sitting directly in a
        # `#hashtag` root carries no group_key, so recover the tag from its
        # parent dir → `#tag file_name` (one line, tag stays clickable). A plain
        # top-level loose file (numeric/@ chat_id parent) → just its stem.
        parent = Path(head.file_path).parent.name
        if parent.startswith("#"):
            return f"{parent} {stems[0]}"
        return "\n".join(stems)
    return "\n".join([sub] + stems)


def caption_for(item: Item) -> str:
    """Producer-set caption wins; else the default single-file format
    (identical to what the archiver used to store at enqueue time)."""
    if item.source == ORPHANED_SOURCE:
        return orphaned_caption([item])
    if item.caption:
        caption = item.caption
    elif item.identifier and not item.identifier.startswith(("manual_", "recorder_")):
        caption = f"@{item.username} · {item.platform} · {item.identifier}"
    else:
        caption = f"@{item.username} · {item.platform}"
    return with_live_tag(caption) if is_tiktok_live(item) else caption


def album_caption_for(batch: list[Item]) -> str:
    """A1 album header. Telegram shows a caption only on the album's first
    item, so per-file captions can't all be displayed; we use a single
    header describing the group, matching the old uploader's behavior:
    '📷 @user · platform' (📷 photos / 🎬 videos)."""
    head = batch[0]
    if is_split_group(head.group_key):
        # A split original's parts: list every part name so all are visible
        # (Telegram shows only the first item's caption). Works for both
        # producers regardless of how each part is otherwise captioned.
        return "\n".join(media_prep.clean_upload_stem(it.file_path)
                         for it in batch)
    if head.source == ORPHANED_SOURCE:
        return orphaned_caption(batch)
    if head.caption:
        caption = head.caption
        return with_live_tag(caption) if is_tiktok_live(head) else caption
    icon = {"photo": "📷", "video": "🎬"}.get(media_bucket(head.file_path), "📦")
    caption = f"{icon} @{head.username} · {head.platform}"
    return with_live_tag(caption) if is_tiktok_live(head) else caption


def _suppress_duplicate(store: QueueStore, guard: DeletionGuard,
                        it: Item, twin_id: int) -> None:
    """Mark a claimed row as delivered-by-twin and remove its redundant copy.
    Only legal when the twin's bytes are CONFIRMED delivered (status='sent') —
    callers own that check. The safebrake still wins: a protected scope keeps
    even its duplicates."""
    store.mark_deduplicated(it.id, twin_id=twin_id)
    try:
        removed = guard.delete(it.platform, it.username, it.file_path,
                               reason="dedup-suppressed-duplicate")
    except Exception as e:
        log.exception("drain: id=%d dedup-cleanup raised: %s", it.id, e)
        removed = False
    log.info("@%s · %s suppressed as duplicate (bytes already sent)",
             it.username, Path(it.file_path).name, extra={"ev": "dedup"})
    log.debug("drain: id=%d suppressed as duplicate of id=%d (bytes already "
             "sent) — redundant copy %s", it.id, twin_id,
             "deleted" if removed else "kept (safebrake)")


def run_housekeeping(store: QueueStore, config: DispatcherConfig) -> None:
    """One pass of the drain's periodic maintenance. Extracted from the loop so
    it can be exercised directly (Seam 25) — the loop just calls it on a timer.

    Failed-queue maintenance, in this order on purpose:
      1. delete failed rows whose file is gone — they can never deliver, so
         re-queuing one would only waste the send retry budget claiming a
         vanished path. Always runs, regardless of policy.
      2. retention backstop: drop present-file rows still failing after the
         window — caps unbounded growth of genuinely-stuck rows. Runs BEFORE
         the auto-retry re-queue on purpose: a row failing past the window is
         permanent, so it must be retired rather than re-armed. If auto-retry
         ran first it would move EVERY failed row to pending, leaving this sweep
         nothing to act on — the cap would silently never fire, and a poison row
         would cycle pending→failed→pending forever (the retry storm this
         backstop exists to prevent).
      3. re-arm TRANSIENT failures unconditionally (default on): rows whose
         last_error is_transient_failure — network / upload-corruption /
         server-side causes that recover unattended. Storm-SAFE because the
         poison rows (media rejected, oversized, unroutable) that make a blanket
         re-arm dangerous are exactly the ones the classifier skips, so they stay
         quarantined for a deliberate `reset failed`. This is the self-healing
         the opt-in below used to be the only way to get.
      4. re-queue the REMAINING (recent, present-file) failed rows — including
         permanent ones — ONLY when auto_retry_failed is opted in. Delete-first +
         prune-first mean only present, within-window rows are re-armed.

    Then the stuck-'sending' watchdog: recover rows wedged in 'sending' by a
    crashed predecessor without waiting for a restart. Safe mid-loop — the drain
    is serial, so nothing of ours is in flight here; only stale claims match."""
    store.delete_failed_missing()
    store.prune_failed(config.failed_retention_days)
    store.reset_failed_transient()
    if FailedRetryPolicy(config.policy_store).enabled():
        store.reset_failed(None, None)
    store.reset_stuck_sending(older_than_minutes=config.stuck_claim_min)


async def recover_media_empty(
    *, send_strategy: SendStrategy, store: QueueStore, guard: DeletionGuard,
    config: DispatcherConfig, peer, topic_id,
    present: list[Item], batch_dupes: list[tuple[Item, int]],
    delete_policy: DeletePolicy, recorder_delete_policy: RecorderDeletePolicy,
) -> tuple[int, int]:
    """Recover from an album-level MediaEmptyError by re-sending each item
    INDIVIDUALLY with the streamable net forced ON.

    Album sends are atomic, so a single undeliverable item (an Instagram VP9
    clip Telegram won't take as-is; a xiaohongshu single-frame mjpeg/webp "video")
    rejects the whole 10-item batch. Per-item, with ensure_streamable=True:
      • good H.264 items deliver,
      • convertible ones (VP9 → H.264) are re-encoded by media_prep and deliver,
      • only genuinely-dead media quarantines.
    Turns 'lose all 10' into 'deliver the 9, isolate the 1'. Returns
    (delivered, quarantined). Extracted for direct testing (Seam 11)."""
    delivered = quarantined = 0
    for it in present:
        # A chat_id-folder DOCUMENT (.mkv/.gif) is deliberately kept as a
        # downloadable original — never re-encode it into a streaming video on
        # recovery; send() ships it as a document when ensure_streamable is off.
        # Every other item forces the streamable net so a convertible clip
        # (VP9 → H.264) is re-encoded and delivered.
        keep_as_document = (it.source == ORPHANED_SOURCE
                            and orphaned_kind(it.file_path) == "document")
        r = await send_strategy.send(
            peer=peer, file_path=it.file_path,
            caption=config.sanitizer.sanitize(caption_for(it)),
            ensure_streamable=not keep_as_document,
            filetype_tag=it.source == ORPHANED_SOURCE,
            topic_id=topic_id,
        )
        if r.ok:
            store.mark_sent(it.id)
            try:
                maybe_delete(store, it.id, delete_policy=delete_policy,
                             recorder_delete_policy=recorder_delete_policy,
                             guard=guard)
            except Exception as e:
                log.exception("drain: id=%d cleanup raised: %s", it.id, e)
            delivered += 1
        elif r.flood_wait_s is not None:
            store.requeue(it.id, reason=f"floodwait {r.flood_wait_s}s (fallback)")
            await asyncio.sleep(r.flood_wait_s + 1)
        elif r.media_empty:
            store.quarantine(it.id, error=r.error or "MediaEmptyError")
            quarantined += 1
        else:
            store.mark_failed(it.id, error=r.error or "unknown",
                              max_retries=config.max_retries)
    # Held-back in-batch dupes: re-evaluate next claim (a now-delivered twin will
    # be suppressed by sent_twin; otherwise they get their own turn).
    for it, _twin_id in batch_dupes:
        store.requeue(it.id, reason="album fell back to per-item sends")
    log.warning("@%s album media-rejected → per-item fallback: %d delivered, "
                "%d quarantined", present[0].username, delivered, quarantined,
                extra={"ev": "album-fallback"})
    return delivered, quarantined


async def drain_forever(
    config:        DispatcherConfig,
    store:         QueueStore,
    send_strategy: SendStrategy,
    router:        TelegramRouter,
    delete_policy: DeletePolicy,
    recorder_delete_policy: RecorderDeletePolicy,
    batch_policy:  BatchPolicy,
    guard:         DeletionGuard,
    *,
    stop_event:    asyncio.Event | None = None,
) -> None:
    log.info("draining the upload queue (poll %.0fs)", config.poll_interval_s,
             extra={"ev": "start"})

    # Min-batch gate, applied to PLATFORM (archiver) groups only. Recorder
    # (live) and orphaned (chat_id folders) are exempt — they send as soon as
    # they're ready. The callables receive the anchor row and resolve the
    # policy per (platform, user).
    def _min_batch(anchor) -> int:
        # A split original's parts are a complete unit — flush immediately,
        # never hold them behind the archiver min-batch gate waiting for more.
        if is_split_group(anchor["group_disc"]):
            return 1
        if anchor["source"] == "archiver":
            return batch_policy.min_batch_size(anchor["platform"], anchor["username"])
        return 1

    def _flush_age_s(anchor):
        if anchor["source"] == "archiver":
            return batch_policy.max_wait_hours(
                anchor["platform"], anchor["username"]) * 3600.0
        return None

    # Startup watchdog: revert rows left 'sending' by a crashed predecessor.
    store.reset_stuck_sending(older_than_minutes=config.stuck_claim_min)

    # Housekeeping cadence. last_housekeeping=0 makes the first loop iteration
    # run it immediately (covering startup), then every _HOUSEKEEPING_EVERY_S.
    last_housekeeping = 0.0

    # Circuit breaker state: consecutive SYSTEMIC send failures (see constant).
    consecutive_fails = 0

    while True:
        if stop_event is not None and stop_event.is_set():
            log.info("drain: stop requested, exiting cleanly")
            return

        if consecutive_fails >= _CIRCUIT_TRIP_AT:
            log.error("circuit: %d consecutive systemic send failures — pausing "
                      "%.0fs before retrying (Telegram unreachable / session "
                      "problem?)", consecutive_fails, _CIRCUIT_COOLDOWN_S,
                      extra={"ev": "circuit"})
            # Interruptible cooldown so a stop request is still honored promptly.
            try:
                if stop_event is not None:
                    await asyncio.wait_for(stop_event.wait(),
                                           timeout=_CIRCUIT_COOLDOWN_S)
                    return  # woke because stop was set
                else:
                    await asyncio.sleep(_CIRCUIT_COOLDOWN_S)
            except asyncio.TimeoutError:
                pass        # cooldown elapsed → try again with a clean slate
            consecutive_fails = 0

        now = time.monotonic()
        if now - last_housekeeping >= _HOUSEKEEPING_EVERY_S:
            try:
                run_housekeeping(store, config)
            except Exception as exc:
                # Housekeeping must never kill the daemon.
                log.exception("drain: housekeeping raised: %s", exc)
            last_housekeeping = now

        try:
            batch = store.claim_batch(
                min_batch=_min_batch, flush_age_s=_flush_age_s)
        except ClaimContentionError as exc:
            log.warning("drain: %s — backing off", exc)
            await asyncio.sleep(config.poll_interval_s)
            continue
        except sqlite3.OperationalError as exc:
            # Last-resort net: claim retries the write lock past busy_timeout
            # (core.store._begin_immediate), so reaching here means contention
            # outlasted even that. A transient lock must never kill the daemon —
            # back off and poll again rather than letting it propagate to exit.
            log.warning("drain: claim hit a locked DB (%s) — backing off", exc)
            await asyncio.sleep(config.poll_interval_s)
            continue
        if not batch:
            await asyncio.sleep(config.poll_interval_s)
            continue

        # Decision B: drop files missing on disk BEFORE sending. A claimed
        # row whose file vanished can't go in the album; mark it failed
        # individually and album-send the survivors. (mark_failed here is
        # terminal-ish per its retry budget — a vanished file won't come
        # back, but the operator can `queue retry` if they restore it.)
        present: list[Item] = []
        for it in batch:
            if Path(it.file_path).exists():
                present.append(it)
            else:
                store.mark_failed(
                    it.id, error=f"file missing on disk: {it.file_path}",
                    max_retries=config.max_retries,
                )
                log.warning("drain: id=%d file missing, marked failed: %s",
                            it.id, it.file_path)
        if not present:
            continue

        # Dedup guarantee (global): never upload bytes that already shipped.
        # Cheap indexed check per row (sent_twin → idx_items_hash_sent), plus a
        # within-batch collapse. A suppressed row is marked 'sent' (delivered by
        # its twin); its redundant on-disk copy is then deleted UNCONDITIONALLY
        # — it's a duplicate whose bytes were already delivered, so removing it
        # is not subject to delete_after_upload (that governs the ORIGINAL).
        # Rows without a content_hash are never gated, so nothing is ever
        # wrongly suppressed.
        #
        # ORDERING (file-integrity contract): a SENT twin justifies suppressing
        # immediately — its bytes are already delivered. A twin that is merely
        # in THIS batch has not delivered anything yet, so an in-batch dupe is
        # only set aside (not sent), and is suppressed/deleted strictly AFTER
        # the batch send succeeds. If the send fails or floodwaits, the dupe
        # follows the same transition as the rest of the batch — its bytes and
        # file are never given up on the strength of an undelivered twin.
        survivors: list[Item] = []
        batch_dupes: list[tuple[Item, int]] = []   # (dupe, in-batch twin id)
        batch_hashes: dict[str, int] = {}
        for it in present:
            twin = store.sent_twin(it.content_hash, it.id)
            if twin is not None:
                _suppress_duplicate(store, guard, it, twin.id)
                continue
            if it.content_hash in batch_hashes:
                batch_dupes.append((it, batch_hashes[it.content_hash]))
                continue
            if it.content_hash:
                batch_hashes[it.content_hash] = it.id
            survivors.append(it)
        present = survivors
        if not present:
            # Only in-batch dupes remained (their anchors were all suppressed
            # against sent twins, so the dupes now have sent twins too).
            for it, _twin_id in batch_dupes:
                twin = store.sent_twin(it.content_hash, it.id)
                if twin is not None:
                    _suppress_duplicate(store, guard, it, twin.id)
                else:
                    store.requeue(it.id, reason="in-batch twin not delivered")
            continue

        head = present[0]
        # Resolve the destination once per batch. An explicit chat_id (orphaned
        # folders) wins; an unresolvable one fails the whole batch cleanly
        # rather than throwing mid-send. Routing is by the ANCHOR, so a batch is
        # always homogeneous in destination — and claim_batch keys on chat_id +
        # topic_id, so every row here shares the anchor's chat AND forum topic.
        try:
            dest = router.destination_for_item(head)
            peer, topic_id = dest.peer, dest.topic_id
        except RouteError as exc:
            for it in present:
                store.mark_failed(it.id, error=str(exc),
                                  max_retries=config.max_retries)
            for it, _twin_id in batch_dupes:   # held dupes share the route
                store.mark_failed(it.id, error=str(exc),
                                  max_retries=config.max_retries)
            log.error("drain: %d item(s) unroutable — %s",
                      len(present) + len(batch_dupes), exc)
            continue

        if len(present) == 1:
            # single send (gif/other bucket, or a group that filtered to one)
            it = present[0]
            log.info("@%s uploading %s [%s]", it.username,
                     Path(it.file_path).name, it.platform, extra={"ev": "upload"})
            log.debug("drain: id=%d src=%s prio=%d attempt=%d file=%s",
                      it.id, it.source, it.priority, it.attempts, it.file_path)
            result = await send_strategy.send(
                peer=peer, file_path=it.file_path,
                caption=config.sanitizer.sanitize(caption_for(it)),
                ensure_streamable=it.source not in _PREPPED_AT_INGEST_SOURCES,
                filetype_tag=it.source == ORPHANED_SOURCE,
                topic_id=topic_id,
            )
        else:
            # album send. Homogeneous by claim: a same-producer photo/video
            # batch, a MIXED photo+video chat_id-folder album, or a grouped
            # chat_id-folder DOCUMENT album (.mkv/.gif) — the last flagged so
            # send_album ships it as documents, never inline.
            as_docs = _is_document_batch(head)
            log.info("@%s uploading %s of %d [%s]", head.username,
                     "documents" if as_docs else "album",
                     len(present), head.platform, extra={"ev": "album"})
            log.debug("drain: album src=%s prio=%d docs=%s ids=%s",
                      head.source, head.priority, as_docs,
                      [it.id for it in present])
            result = await send_strategy.send_album(
                peer=peer,
                file_paths=[it.file_path for it in present],
                caption=config.sanitizer.sanitize(album_caption_for(present)),
                topic_id=topic_id,
                as_documents=as_docs,
            )

        if result.ok:
            consecutive_fails = 0          # a success clears the circuit breaker
            # All-or-nothing: the whole batch went up as one atomic send,
            # so mark every row sent together, then run delete gate per row.
            for it in present:
                store.mark_sent(it.id)
            # In-batch dupes were held back from the send; their twin's bytes
            # are NOW confirmed delivered, so suppression is finally legal.
            for it, twin_id in batch_dupes:
                _suppress_duplicate(store, guard, it, twin_id)
            if len(present) > 1:
                log.info("@%s album sent (%d items)", head.username,
                         len(present), extra={"ev": "sent"})
            else:
                log.info("@%s sent %s", head.username,
                         Path(head.file_path).name, extra={"ev": "sent"})
            for it in present:
                try:
                    maybe_delete(
                        store,
                        it.id,
                        delete_policy=delete_policy,
                        recorder_delete_policy=recorder_delete_policy,
                        guard=guard,
                    )
                except Exception as e:
                    log.exception("drain: id=%d cleanup raised: %s", it.id, e)
            # Decision C: pace between album sends to avoid FloodWait.
            if len(present) > 1:
                await asyncio.sleep(config.inter_album_sleep)

        elif result.flood_wait_s is not None:
            log.warning("FloodWait %ds — requeued %d item(s), pausing",
                        result.flood_wait_s, len(present), extra={"ev": "flood"})
            for it in present:
                store.requeue(it.id, reason=f"floodwait {result.flood_wait_s}s")
            # Held-back dupes never went out either; requeue without burning
            # a retry, same as the rest of the batch.
            for it, _twin_id in batch_dupes:
                store.requeue(it.id, reason=f"floodwait {result.flood_wait_s}s "
                                            "(held as in-batch duplicate)")
            await asyncio.sleep(result.flood_wait_s + 1)

        elif result.media_empty:
            # Telegram rejected the media. Don't burn the retry budget and don't
            # let it head-of-line block — but DON'T write off the whole batch
            # either: album atomicity means one bad item (a VP9 clip / a
            # single-frame mjpeg "video") fails all 10. Re-send each item
            # individually with the streamable net ON, so good and convertible
            # (VP9 → H.264) items deliver and only truly-dead media quarantines
            # ('failed', no CANCELLED_MARKER → `reset failed` can recover it).
            await recover_media_empty(
                send_strategy=send_strategy, store=store, guard=guard,
                config=config, peer=peer, topic_id=topic_id,
                present=present, batch_dupes=batch_dupes,
                delete_policy=delete_policy,
                recorder_delete_policy=recorder_delete_policy,
            )
            if len(present) > 1:
                await asyncio.sleep(config.inter_album_sleep)

        else:
            # Whole-batch failure: every row gets an attempt counted. Since
            # the album is atomic, none were posted — all are eligible to
            # retry (or hit failed at max_retries) together. This arm is the
            # SYSTEMIC bucket (network/stall/unknown), so it advances the circuit
            # breaker; an item-specific outcome (media_empty above, missing file,
            # unroutable) never reaches here and so never trips it.
            consecutive_fails += 1
            statuses: set[str] = set()
            for it in present:
                statuses.add(store.mark_failed(
                    it.id, error=result.error or "unknown",
                    max_retries=config.max_retries,
                ))
            # A held-back dupe's twin did NOT deliver — requeue it untouched
            # (no attempt burned: it was never sent). Next claim re-evaluates;
            # if the twin eventually delivers, sent_twin suppresses it then.
            for it, _twin_id in batch_dupes:
                store.requeue(it.id, reason="in-batch twin failed to deliver")
            log.warning("@%s upload failed (%d item%s, %s): %s", head.username,
                        len(present), "" if len(present) == 1 else "s",
                        "/".join(sorted(statuses)), result.error)
