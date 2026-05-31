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
from pathlib import Path

from core import (
    ClaimContentionError, QueueStore, Item, DeletePolicy, RecorderDeletePolicy,
    BatchPolicy, ORPHANED_SOURCE, subfolder_of, cleanup_sidecars,
)
from core.files import media_bucket
from .tg_router import TelegramRouter, RouteError

from .config import DispatcherConfig
from .delete import maybe_delete
from .send import SendStrategy

log = logging.getLogger(__name__)


def is_tiktok_live(item: Item) -> bool:
    return item.platform.lower() == "tiktok" and item.source.lower() == "recorder"


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
    sub = subfolder_of(head.chat_id, head.group_key)
    lines = ([sub] if sub else []) + [Path(it.file_path).stem for it in batch]
    return "\n".join(lines)


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
    if head.source == ORPHANED_SOURCE:
        return orphaned_caption(batch)
    if head.caption:
        caption = head.caption
        return with_live_tag(caption) if is_tiktok_live(head) else caption
    icon = {"photo": "📷", "video": "🎬"}.get(media_bucket(head.file_path), "📦")
    caption = f"{icon} @{head.username} · {head.platform}"
    return with_live_tag(caption) if is_tiktok_live(head) else caption


async def drain_forever(
    config:        DispatcherConfig,
    store:         QueueStore,
    send_strategy: SendStrategy,
    router:        TelegramRouter,
    delete_policy: DeletePolicy,
    recorder_delete_policy: RecorderDeletePolicy,
    batch_policy:  BatchPolicy,
    *,
    stop_event:    asyncio.Event | None = None,
) -> None:
    log.info("drain: starting (poll=%.1fs, max_retries=%d)",
             config.poll_interval_s, config.max_retries)

    # Min-batch gate, applied to PLATFORM (archiver) groups only. Recorder
    # (live) and orphaned (chat_id folders) are exempt — they send as soon as
    # they're ready. The callables receive the anchor row and resolve the
    # policy per (platform, user).
    def _min_batch(anchor) -> int:
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

    while True:
        if stop_event is not None and stop_event.is_set():
            log.info("drain: stop requested, exiting cleanly")
            return

        try:
            batch = store.claim_batch(
                min_batch=_min_batch, flush_age_s=_flush_age_s)
        except ClaimContentionError as exc:
            log.warning("drain: %s — backing off", exc)
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
        survivors: list[Item] = []
        batch_hashes: dict[str, int] = {}
        for it in present:
            twin = store.sent_twin(it.content_hash, it.id)
            if twin is None and it.content_hash in batch_hashes:
                twin_id = batch_hashes[it.content_hash]
            else:
                twin_id = twin.id if twin is not None else None
            if twin_id is not None:
                store.mark_deduplicated(it.id, twin_id=twin_id)
                try:
                    cleanup_sidecars(it.file_path)
                except Exception as e:
                    log.exception("drain: id=%d dedup-cleanup raised: %s", it.id, e)
                log.info("drain: id=%d suppressed as duplicate of id=%d (bytes "
                         "already sent) — redundant copy deleted", it.id, twin_id)
                continue
            if it.content_hash:
                batch_hashes[it.content_hash] = it.id
            survivors.append(it)
        present = survivors
        if not present:
            continue

        head = present[0]
        # Resolve the destination once per batch. An explicit chat_id (orphaned
        # folders) wins; an unresolvable one fails the whole batch cleanly
        # rather than throwing mid-send. Routing is by the ANCHOR, so a batch is
        # always homogeneous in destination.
        try:
            peer = router.peer_for_item(head)
        except RouteError as exc:
            for it in present:
                store.mark_failed(it.id, error=str(exc),
                                  max_retries=config.max_retries)
            log.error("drain: %d item(s) unroutable — %s", len(present), exc)
            continue

        if len(present) == 1:
            # single send (gif/other bucket, or a group that filtered to one)
            it = present[0]
            log.info("drain: id=%d src=%s prio=%d @%s [%s] file=%s attempt=%d",
                     it.id, it.source, it.priority, it.username,
                     it.platform, it.file_path, it.attempts)
            result = await send_strategy.send(
                peer=peer, file_path=it.file_path, caption=caption_for(it),
            )
        else:
            # album send (homogeneous photo/video batch, all same producer)
            log.info("drain: album n=%d src=%s prio=%d @%s [%s] ids=%s",
                     len(present), head.source, head.priority, head.username,
                     head.platform, [it.id for it in present])
            result = await send_strategy.send_album(
                peer=peer,
                file_paths=[it.file_path for it in present],
                caption=album_caption_for(present),
            )

        if result.ok:
            # All-or-nothing: the whole batch went up as one atomic send,
            # so mark every row sent together, then run delete gate per row.
            for it in present:
                store.mark_sent(it.id)
            log.info("drain: %s sent (%d item(s))",
                     "album" if len(present) > 1 else f"id={head.id}",
                     len(present))
            for it in present:
                try:
                    maybe_delete(
                        store,
                        it.id,
                        delete_policy=delete_policy,
                        recorder_delete_policy=recorder_delete_policy,
                    )
                except Exception as e:
                    log.exception("drain: id=%d cleanup raised: %s", it.id, e)
            # Decision C: pace between album sends to avoid FloodWait.
            if len(present) > 1:
                await asyncio.sleep(config.inter_album_sleep)

        elif result.flood_wait_s is not None:
            log.warning("drain: FloodWait %ds — requeue %d item(s), then sleep",
                        result.flood_wait_s, len(present))
            for it in present:
                store.requeue(it.id, reason=f"floodwait {result.flood_wait_s}s")
            await asyncio.sleep(result.flood_wait_s + 1)

        else:
            # Whole-batch failure: every row gets an attempt counted. Since
            # the album is atomic, none were posted — all are eligible to
            # retry (or hit failed at max_retries) together.
            statuses: set[str] = set()
            for it in present:
                statuses.add(store.mark_failed(
                    it.id, error=result.error or "unknown",
                    max_retries=config.max_retries,
                ))
            log.warning("drain: %d item(s) failed (%s): %s",
                        len(present), "/".join(sorted(statuses)), result.error)
