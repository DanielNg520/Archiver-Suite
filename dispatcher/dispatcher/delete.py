"""
dispatcher.delete
─────────────────
Delete-after-upload safety gate, now reading the ONE shared table.

SAFETY CONTRACT (order is the point of the module):
  A local file is deleted ONLY when, IN ORDER:
    (1) SendStrategy.send returned ok=True
    (2) ItemStore.mark_sent committed status='sent'
    (3) DeletePolicy.should_delete(platform, username) is True
    (4) the DeletionGuard safebrake does NOT shield (platform, username)

  drain.py owns step (1)->(2)->maybe_delete. This module owns (3)+(4)+unlink.

  Defense-in-depth: maybe_delete RE-READS the row and refuses to delete
  unless status=='sent'. If a future refactor calls delete before
  mark_sent, this fires an ERROR instead of losing the file silently.
  With one table the check is a single authoritative read — no risk of
  reading a stale mirror.

  The safebrake (4) is a hard override: even with delete-after-upload ON, a
  protected scope's file is kept. The guard owns that decision so the same
  rule applies identically to every deletion path in the suite.
"""

from __future__ import annotations

import logging
from pathlib import Path

from core import (
    QueueStore, Status, DeletePolicy, RecorderDeletePolicy, DeletionGuard,
)

log = logging.getLogger(__name__)


def maybe_delete(store: QueueStore, item_id: int, *,
                 delete_policy: DeletePolicy,
                 recorder_delete_policy: RecorderDeletePolicy,
                 guard: DeletionGuard) -> None:
    """Gated cleanup. Caller must have already called mark_sent(item_id)."""
    item = store.get(item_id)
    if item is None:
        log.error("maybe_delete: item id=%d not found", item_id)
        return
    if item.status != Status.SENT.value:
        log.error(
            "maybe_delete: refusing to delete %s — status=%r (expected 'sent'). "
            "Possible regression in drain ordering.",
            Path(item.file_path).name, item.status,
        )
        return
    if item.source.lower() == "recorder":
        should_delete = recorder_delete_policy.should_delete_recording()
    else:
        should_delete = delete_policy.should_delete(item.platform, item.username)
    if not should_delete:
        return
    # The guard re-checks the safebrake and skips (logging) if protected.
    guard.delete(item.platform, item.username, item.file_path,
                 reason="delete-after-upload")
