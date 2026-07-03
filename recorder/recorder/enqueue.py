"""
recorder.enqueue
────────────────
Registers finished recordings in the shared suite DB via core.ItemStore.

Single source of truth: there is no separate dispatcher.db and no raw SQL
here anymore. Writing the item row IS the enqueue — the dispatcher claims
it from the same `items` table on its next poll. The recorder no longer
needs to know the dispatcher's schema; `core` owns it.

source='recorder'. Priority defaults to 5 so recordings drain BEFORE the
archiver's VOD backlog (archiver enqueues at 10; the dispatcher claims
lowest-priority-number first). Recordings are also exempt from the platform
min-batch gate, so each finished stream uploads immediately as a single file.
Override with $RECORDER_UPLOAD_PRIORITY (lower = sooner).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from core import ItemStore, cleanup_sidecars, media_prep, register_media
from core.ingest import IngestOutcome

log = logging.getLogger(__name__)

# Lower number drains first. Default 5 = ahead of archiver's 10. Env-tunable
# so you can re-order without a code change (e.g. set to 25 to deprioritize).
RECORDER_PRIORITY = int(os.environ.get("RECORDER_UPLOAD_PRIORITY", "5"))


def _recorder_identifier(file_path: str) -> str:
    """Synthesize the (platform, identifier) key for a recording.

    Recordings have no upstream post id, so we derive a stable identifier
    from the filename stem. This MUST match core.migrate's scheme
    (`recorder_<stem>`) so a recording migrated from the legacy queue and
    the same file re-enqueued live collide on UNIQUE(platform, identifier)
    instead of duplicating. The real per-file dedup guarantee is the
    separate UNIQUE(file_path) constraint; this key just has to be present
    and stable.
    """
    return f"recorder_{Path(file_path).stem or 'item'}"


def _retire_after_split(original: Path) -> None:
    """Delete a recording that media_prep replaced with streamable/split output
    (the bytes now live in the registered outputs). Gated by delete-after-split
    (default on); off → the original is kept and harmlessly re-prepped next pass
    (idempotent: the outputs' file_paths already have rows). The live path has
    no DeletionGuard, so this is the suite's legacy unconditional cleanup — the
    same fallback archiver.reconcile uses when guard is None."""
    if not media_prep.delete_after_split():
        return
    cleanup_sidecars(str(original))


class EnqueueClient:
    """Opens a short-lived ItemStore per enqueue call.

    A recording runs for minutes-to-hours; we deliberately do NOT hold a
    DB handle open across that window. Enqueues happen once per finished
    stream, so per-call connect/close churn is irrelevant, and a short-
    lived connection avoids keeping a WAL handle (and any lock) alive
    while nothing is being written.
    """

    def __init__(self, db_path: str | None = None,
                 *, split_threshold_bytes: int | None = None):
        # None → core resolves the default suite DB ($ARCHIVER_DB or the
        # packaged default). No "db not found" guard: core.connect() runs
        # CREATE TABLE IF NOT EXISTS idempotently, so whichever process
        # connects first creates the schema. This is what removes the old
        # install-order requirement.
        self._db_path = db_path
        # Recorder split mode (config.toml): when set, a recording over this
        # size is split into <=this-size parts at enqueue instead of only above
        # the ~3.9 GiB upload ceiling. None → ceiling only. Either way, an
        # oversize recording is never enqueued whole onto the FilePartsInvalid
        # wall (see core.register_media).
        self._split_threshold_bytes = split_threshold_bytes

    def enqueue(
        self,
        *,
        platform:  str,
        username:  str,
        file_path: str,
        caption:   str | None,
        group_key: str | None = None,
        priority:  int = RECORDER_PRIORITY,
    ) -> bool:
        """Register one finished recording. Returns True if it became newly
        claimable (inserted, or a failed twin was re-armed).

        `group_key` (optional) albums this recording with others sharing the key
        — the state machine passes one broadcast's reconnect-stitched segments a
        shared key so they ship as a single ordered batch instead of scattered
        clips. None → the recording sends on its own (or, if oversize, its split
        parts get their own album).

        Goes through core.register_media — the media-prep layer over the SAME
        register_file primitive the startup sweep and every other producer use.
        A still-streamable in-ceiling recording passes straight through (one
        cheap ffprobe) exactly as a bare register_file would; a non-streamable
        one is converted and an oversize one is split into parts (recorder split
        mode lowers that trigger), so a big recording is never enqueued whole
        onto Telegram's FilePartsInvalid wall. Each output keeps the recorder's
        `recorder_<stem>` identity and live caption; split parts share one album
        key so they ship as a single ordered batch, and the replaced original is
        deleted (gated by delete-after-split).

        Self-healing contract: an UNSTABLE / HASH_FAILED outcome leaves the
        file on disk untouched — the startup sweep re-registers it on the
        next `recorder start`, so a refused enqueue can delay an upload but
        never lose a recording. A prep failure (bad/oversize file ffmpeg or
        AutoSplitter refused) likewise keeps the original for the next pass."""
        path = Path(file_path)
        store = ItemStore.open(self._db_path)
        try:
            result = register_media(
                store, path,
                source     = "recorder",
                platform   = platform,
                username   = username,
                priority   = priority,
                group_key  = group_key,
                split_threshold_bytes = self._split_threshold_bytes,
                identifier_for = lambda out: _recorder_identifier(str(out)),
                caption_for    = lambda out: (
                    caption if out == path
                    else f"@{username} · tiktok · live · {out.stem}"),
                retire_original = _retire_after_split,
            )
        finally:
            store.close()

        if result.busy:
            # Another worker (an archiver/recorder sweep) is already preparing
            # this exact recording — NOT a failure. Leave it on disk untouched;
            # the holder registers it, or the next startup sweep retries. Never
            # launch a second clobbering encode of the same file.
            log.info(
                "enqueue: %s @%s %s already being prepared by another worker — "
                "left on disk, will be picked up by whichever holds it",
                platform, username, path.name,
            )
            return False
        if not result.prep_ok:
            log.warning(
                "enqueue: %s @%s %s prep failed (%s) — file kept on disk; the "
                "startup sweep will retry it on the next recorder start",
                platform, username, path.name, result.error,
            )
            return False
        if not result.outcomes or all(
                o in (IngestOutcome.UNSTABLE, IngestOutcome.HASH_FAILED)
                for o in result.outcomes):
            log.warning(
                "enqueue: %s @%s %s refused (%s) — file kept on disk; the "
                "startup sweep will register it on the next recorder start",
                platform, username, path.name,
                ", ".join(o.value for o in result.outcomes) or "no outputs",
            )
            return False
        log.info("@%s queued for upload (%d part(s): %s)", username,
                 len(result.outcomes),
                 ", ".join(o.value for o in result.outcomes),
                 extra={"ev": "queued"})
        return result.any_inserted
