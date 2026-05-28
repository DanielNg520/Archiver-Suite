"""
dispatcher.delete
─────────────────
Delete-after-upload safety gate.

SAFETY CONTRACT (do not reorder these checks; this is the whole point of
the module):

  A local file is deleted ONLY when ALL of the following are true, IN ORDER:
    (1) SendStrategy.send returned ok=True
    (2) QueueDB.mark_done(row.id) committed status='done'
    (3) DeletePolicy.should_delete(platform, username) returns True

  The orchestration is in drain.py, which calls send -> mark_done ->
  maybe_delete. This module only owns step (3) + the actual unlink.

  Defense-in-depth: maybe_delete() RE-CHECKS the DB state before unlinking.
  If a future refactor moves the delete call BEFORE mark_done, the re-read
  fires an ERROR log and refuses to delete — silent data loss becomes a
  loud "file didn't delete, why?" question.

SIDECAR CLEANUP:
  gallery-dl and yt-dlp leave .json / .info.json sidecars next to media
  files. We delete those too when removing the media. Pattern lifted
  verbatim from archiver.telegram._cleanup.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .db import QueueDB
from .policies import DeletePolicy

log = logging.getLogger(__name__)


def _cleanup_sidecars(file_path: str) -> None:
    """
    Raw delete of media file + its known sidecars. No gating here —
    gating is maybe_delete's job. Do NOT add direct callers from outside
    this module.
    """
    p = Path(file_path)
    try:
        p.unlink(missing_ok=True)
    except OSError as e:
        log.warning("cleanup: unlink %s failed: %s", p.name, e)
        return

    # yt-dlp sidecars: <stem>.info.json
    for suffix in (".json", ".info.json"):
        try:
            p.with_suffix(suffix).unlink(missing_ok=True)
        except OSError:
            pass
    # gallery-dl sidecar: <full_name>.json (note: full name, not stem)
    try:
        (p.parent / (p.name + ".json")).unlink(missing_ok=True)
    except OSError:
        pass


def maybe_delete(
    queue_db: QueueDB,
    row_id: int,
    *,
    delete_policy: DeletePolicy,
) -> None:
    """
    Gated cleanup. Caller must have already called mark_done(row_id).

    We re-read the row fresh from the DB to:
      (a) verify status='done' (defense against ordering regressions)
      (b) get the authoritative file_path (in case caller had a stale ref)
      (c) get platform/username for the policy lookup
    """
    row = queue_db.get(row_id)
    if row is None:
        log.error("maybe_delete: row id=%d not found", row_id)
        return

    if row.status != "done":
        log.error(
            "maybe_delete: refusing to delete %s — DB status=%r (expected 'done'). "
            "Possible regression in drain ordering.",
            Path(row.file_path).name, row.status,
        )
        return

    if not delete_policy.should_delete(row.platform, row.username):
        return

    _cleanup_sidecars(row.file_path)
