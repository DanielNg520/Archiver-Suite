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

from core import ClaimContentionError, ItemStore, Item, DeletePolicy
from core.files import media_bucket
from .tg_router import TelegramRouter

from .config import DispatcherConfig
from .delete import maybe_delete
from .send import SendStrategy

log = logging.getLogger(__name__)


def is_tiktok_live(item: Item) -> bool:
    return item.platform.lower() == "tiktok" and item.source.lower() == "recorder"


def with_live_tag(caption: str) -> str:
    return caption if "#live" in caption.split() else f"{caption} #live"


def caption_for(item: Item) -> str:
    """Producer-set caption wins; else the default single-file format
    (identical to what the archiver used to store at enqueue time)."""
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
    icon = {"photo": "📷", "video": "🎬"}.get(media_bucket(head.file_path), "📦")
    caption = f"{icon} @{head.username} · {head.platform}"
    return with_live_tag(caption) if is_tiktok_live(head) else caption


async def drain_forever(
    config:        DispatcherConfig,
    store:         ItemStore,
    send_strategy: SendStrategy,
    router:        TelegramRouter,
    delete_policy: DeletePolicy,
    *,
    stop_event:    asyncio.Event | None = None,
) -> None:
    log.info("drain: starting (poll=%.1fs, max_retries=%d)",
             config.poll_interval_s, config.max_retries)

    # Startup watchdog: revert rows left 'sending' by a crashed predecessor.
    store.reset_stuck_sending(older_than_minutes=config.stuck_claim_min)

    while True:
        if stop_event is not None and stop_event.is_set():
            log.info("drain: stop requested, exiting cleanly")
            return

        try:
            batch = store.claim_batch()
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

        head = present[0]
        peer = router.peer_for(head.platform, head.username, source=head.source)

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
                    maybe_delete(store, it.id, delete_policy=delete_policy)
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
            for it in present:
                new_status = store.mark_failed(
                    it.id, error=result.error or "unknown",
                    max_retries=config.max_retries,
                )
            log.warning("drain: %d item(s) failed (%s): %s",
                        len(present), new_status, result.error)
