"""
dispatcher.drain
────────────────
The drain loop. Claims one row at a time, sends it, finalizes the
outcome, repeats.

TEMPLATE METHOD:
  drain_forever is the skeleton. Each step is a single method call you
  could override:
    1. claim     — QueueDB.claim_next
    2. send      — SendStrategy.send
    3. finalize  — mark_done / requeue / mark_failed
    4. cleanup   — maybe_delete (only on success)
  If you ever need a fifth step (e.g. notify on failure), add it here.
  Don't sprinkle if-statements through the loop body.

CLAIM-THEN-SEND vs SEND-THEN-MARK:
  We claim BEFORE sending. If we crash mid-send, the watchdog will
  revert the row to pending and we'll re-send on next startup. That
  risks a duplicate Telegram upload — but the alternative (send first,
  then mark done) also risks duplicates on crash. There's no
  exactly-once without Telegram-side idempotency keys (they don't
  provide them).

  Tradeoff: claim-first MINIMIZES the duplicate window (only crashes
  between send-success and mark_done cause dupes). Accepted.

SERIAL DRAIN:
  One send at a time. Telethon's send_file from a single session can
  hit per-method rate limits if parallelized, complicating FloodWait
  handling. If throughput becomes a problem, that's a later slice.
"""

from __future__ import annotations

import asyncio
import logging

from .config import DispatcherConfig
from .db import QueueDB
from .delete import maybe_delete
from .policies import DeletePolicy
from .send import SendStrategy
from .tg_router import TelegramRouter

log = logging.getLogger(__name__)


async def drain_forever(
    config:         DispatcherConfig,
    queue_db:       QueueDB,
    send_strategy:  SendStrategy,
    router:         TelegramRouter,
    delete_policy:  DeletePolicy,
    *,
    stop_event:     asyncio.Event | None = None,
) -> None:
    """
    Main loop. Runs until stop_event is set (or forever if None).

    Per iteration:
      - If a row is pending: claim it, send it, finalize.
      - If nothing pending: sleep poll_interval_s.

    `stop_event` lets the CLI / signal handler ask for a clean shutdown
    without killing mid-send. The loop checks it between rows, so the
    longest a shutdown can wait is one send's worth of time.
    """
    log.info("drain: starting (poll=%.1fs, max_retries=%d)",
             config.poll_interval_s, config.max_retries)

    # Startup watchdog: any rows stuck in 'claimed' from a previous
    # process get reverted to 'pending'. See db.reset_stuck_claimed.
    queue_db.reset_stuck_claimed(older_than_minutes=config.stuck_claim_min)

    while True:
        if stop_event is not None and stop_event.is_set():
            log.info("drain: stop requested, exiting cleanly")
            return

        row = queue_db.claim_next()
        if row is None:
            await asyncio.sleep(config.poll_interval_s)
            continue

        log.info(
            "drain: id=%d src=%s prio=%d @%s [%s] file=%s attempt=%d",
            row.id, row.source, row.priority, row.username, row.platform,
            row.file_path, row.attempts,
        )

        peer = router.peer_for(row.platform, row.username)
        result = await send_strategy.send(
            peer=peer,
            file_path=row.file_path,
            caption=row.caption,
        )

        if result.ok:
            queue_db.mark_done(row.id)
            log.info("drain: id=%d done", row.id)
            # Delete AFTER mark_done — see delete.py safety contract.
            try:
                maybe_delete(queue_db, row.id, delete_policy=delete_policy)
            except Exception as e:
                # Never let a cleanup error mask a successful send.
                log.exception("drain: id=%d cleanup raised: %s", row.id, e)

        elif result.flood_wait_s is not None:
            # Server-side rate limit beyond our inline cap. Sleep here in
            # the drain loop (not inside send) so OTHER work stays queued
            # — though with single-threaded drain that's moot today.
            log.warning(
                "drain: id=%d FloodWait %ds — sleeping then requeueing",
                row.id, result.flood_wait_s,
            )
            queue_db.requeue(
                row.id,
                reason=f"floodwait {result.flood_wait_s}s",
            )
            await asyncio.sleep(result.flood_wait_s + 1)

        else:
            new_status = queue_db.mark_failed(
                row.id,
                error=result.error or "unknown",
                max_retries=config.max_retries,
            )
            log.warning(
                "drain: id=%d failed (%s): %s",
                row.id, new_status, result.error,
            )
