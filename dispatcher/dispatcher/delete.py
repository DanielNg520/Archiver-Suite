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
    ORPHANED_SOURCE,
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
    source = item.source.lower()
    is_orphaned = source == ORPHANED_SOURCE
    if source == "recorder":
        should_delete = recorder_delete_policy.should_delete_recording()
    elif is_orphaned:
        # A chat_id folder is a pure drop-zone: once a file is uploaded, both it
        # and its row are useless, so orphaned items ship-and-delete (file + row)
        # UNCONDITIONALLY — independent of the platform-archive delete_after_upload
        # policy. The DeletionGuard safebrake below is still honored.
        should_delete = True
    else:
        should_delete = delete_policy.should_delete(item.platform, item.username)
    if not should_delete:
        return
    # The guard re-checks the safebrake and returns False (logging) if protected.
    removed = guard.delete(
        item.platform, item.username, item.file_path,
        reason="ship-and-delete (orphaned)" if is_orphaned else "delete-after-upload",
    )
    # Orphaned rows leave NO trace — but ONLY once the file is actually gone.
    # Coupled to a confirmed-absent file, not just `removed`: if the safebrake
    # KEPT the file (removed=False) or an unlink silently failed, we MUST keep
    # the row, because it is the sole guard stopping the ingester from
    # re-uploading that still-present file on its next scan (ingest's
    # ALREADY_KNOWN / content_hash dedup both key off the row existing).
    if is_orphaned and removed and not Path(item.file_path).exists():
        store.delete(item_id)
