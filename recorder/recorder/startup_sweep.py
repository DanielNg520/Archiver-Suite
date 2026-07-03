"""
recorder.startup_sweep
──────────────────────
A reconciliation pass run ONCE at the top of `recorder start`, before the
state machine begins listening. It makes the on-disk output dir and the shared
queue agree, so nothing a previous run left behind is silently stranded:

  1. delete every *.log under the output dir — the per-recording yt-dlp logs
     (recorder.capture writes one beside each stream). They are never uploaded
     and accumulate forever otherwise; a fresh start wants a clean tree.

  2. for every media file, make the queue reflect reality:
       - already SENT (uploaded, but the file wasn't deleted)  → delete it now
         (file + sidecars), the dispatcher already shipped these bytes.
       - PENDING / SENDING                                     → leave it; it's
         already queued / in flight.
       - FAILED                                                → re-arm it
         (failed → pending) so the next drain retries it.
       - no row at all                                         → register it via
         core.ingest (which also content-hash-dedups: bytes already sent under
         another path are deleted rather than re-uploaded).

  3. delete any directory left empty AFTER the above — recordings that were
     sent-and-deleted leave behind empty per-user folders.

Self-contained: imports only `core` (never archiver), matching the suite's
producer-shares-core rule. The heavy requeue/dedup logic is core.ingest, reused
verbatim rather than re-derived here.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from core import (
    DeletionGuard, ItemStore, PolicyStore, RecorderDeletePolicy, Status,
    cleanup_sidecars, register_media,
)
from core.ingest import IngestOutcome

from .enqueue import RECORDER_PRIORITY

log = logging.getLogger(__name__)

# "What we consider a recording" on a recovery sweep. Canonical media (core.files)
# PLUS the raw capture containers a crashed recording can leave behind before its
# live remux ran (.flv/.ts/…). Without the second set those orphans match no
# filter and are stranded on disk forever; with it they are re-enqueued raw and
# the dispatcher's send-time net converts them to streamable .mp4. Mirrors the
# archiver reconcile policy (MEDIA_EXTENSIONS | CONVERTIBLE_VIDEO_EXTS) so both
# recovery paths recognise exactly the same files. Sidecars (.json) and logs
# (.log) are excluded by construction.
from core.files import MEDIA_EXTENSIONS  # noqa: E402
from core import media_prep  # noqa: E402

_RECORDING_EXTS = MEDIA_EXTENSIONS | media_prep.CONVERTIBLE_VIDEO_EXTS


@dataclass
class SweepReport:
    logs_deleted:   int = 0
    requeued:       int = 0   # new rows + re-armed failed twins
    already_queued: int = 0   # pending/sending — left alone
    deleted_sent:   int = 0   # uploaded-but-not-deleted files removed
    kept_sent:      int = 0   # uploaded files kept (policy off / safebraked)
    dedup_dropped:  int = 0   # bytes already sent under another path — removed
    skipped:        int = 0   # unstable / unreadable — retried next start
    dirs_removed:   int = 0

    def __str__(self) -> str:
        return (
            f"logs -{self.logs_deleted}, requeued {self.requeued}, "
            f"already-queued {self.already_queued}, sent-deleted "
            f"{self.deleted_sent}, sent-kept {self.kept_sent}, dedup-dropped "
            f"{self.dedup_dropped}, skipped {self.skipped}, "
            f"empty-dirs -{self.dirs_removed}"
        )


def _username_for(media: Path, root: Path) -> str:
    """Recorder layout is {output_dir}/{username}/file. A file directly in the
    root has no owning user folder → '_root', matching reconcile_recordings."""
    rel = media.relative_to(root)
    return rel.parts[0] if len(rel.parts) > 1 else "_root"


def _caption(username: str, media: Path) -> str:
    # Same shape recorder.state._enqueue builds for a live recording.
    return f"@{username} · tiktok · live · {media.stem}"


def _handle_sent(store: ItemStore, media: Path, may_delete: bool,
                 guard: "DeletionGuard | None", report: SweepReport) -> None:
    """Delete an already-uploaded leftover file, gated by the delete policy and
    the safebrake. Resolves the owning (platform, username) from the DB row so
    the protection check matches what the dispatcher would have applied."""
    if not may_delete:
        report.kept_sent += 1
        return
    item_id = store.id_of(str(media))
    item = store.get(item_id) if item_id is not None else None
    # The row is the authority on the owning scope. If it vanished mid-sweep
    # (race), fall back to the parent folder name — never a path-relative call
    # that could raise on an absolute path.
    platform = item.platform if item else "tiktok"
    username = item.username if item else media.parent.name
    # guard is None only when policy/guard couldn't be built; may_delete would
    # be False in that case, so reaching here means we have a guard.
    if guard is not None and not guard.delete(
            platform, username, str(media), reason="startup-sweep-already-sent"):
        report.kept_sent += 1   # safebrake shielded this scope
        return
    if guard is None:
        cleanup_sidecars(str(media))
    report.deleted_sent += 1


def _delete_policy_and_guard(
    policy_store: "PolicyStore | None",
) -> "tuple[RecorderDeletePolicy | None, DeletionGuard | None]":
    """Build the recorder delete-policy + safebrake guard from the shared
    config.toml. Best-effort: if config is unreadable we return (None, None),
    and the caller treats that as 'cannot confirm deletion is allowed' — i.e.
    it KEEPS uploaded files rather than risk deleting ones the user wanted to
    retain. Failing safe is the point."""
    try:
        store = policy_store or PolicyStore()
        return RecorderDeletePolicy(store), DeletionGuard(store)
    except Exception as e:  # malformed config.toml, etc.
        log.warning("startup-sweep: policy/guard unavailable (%s) — "
                    "uploaded files will be KEPT this pass", e)
        return None, None


def sweep(output_dir: str, db_path: str | None = None,
          *, priority: int = RECORDER_PRIORITY,
          policy_store: "PolicyStore | None" = None,
          split_threshold_bytes: int | None = None) -> SweepReport:
    """Run the startup reconciliation over `output_dir`. Never raises for an
    expected condition — a single bad file logs and the sweep continues — so a
    cluttered tree can never stop the recorder from starting.

    `split_threshold_bytes` (recorder split mode) lowers the size at which a
    recording is split into parts below the ~3.9 GiB upload ceiling; None keeps
    the ceiling. Either way an oversize recording is split rather than enqueued
    whole onto Telegram's FilePartsInvalid wall (see core.register_media)."""
    report = SweepReport()
    root = Path(output_dir).expanduser()
    if not root.exists():
        return report

    # Deleting an already-uploaded file is a deletion like any other: it must
    # honor the user's delete-after-upload setting AND the safebrake, exactly
    # as the dispatcher's delete-after-upload path does. We never bypass them.
    delete_policy, guard = _delete_policy_and_guard(policy_store)
    may_delete_sent = (delete_policy is not None
                       and delete_policy.should_delete_recording())

    def _retire_after_split(original: Path) -> None:
        """Delete a recording media_prep replaced with streamable/split output.
        Gated by delete-after-split (default on) and the safebrake (guard); the
        bytes survive in the registered outputs. Distinct from the sent-leftover
        cleanup above (delete-after-UPLOAD): a split original was never uploaded,
        it was superseded pre-queue."""
        if not media_prep.delete_after_split():
            return
        username = _username_for(original, root)
        if guard is not None:
            guard.delete("tiktok", username, str(original),
                         reason="startup-sweep media-prep replaced original "
                                "with streamable copy")
        else:
            cleanup_sidecars(str(original))

    # 1. delete every per-recording log.
    for f in root.rglob("*.log"):
        if not f.is_file():
            continue
        try:
            f.unlink()
            report.logs_deleted += 1
        except OSError as e:
            log.warning("startup-sweep: could not delete log %s: %s", f.name, e)

    # 2. reconcile media against the queue.
    store = ItemStore.open(db_path)
    try:
        for f in sorted(root.rglob("*")):
            if not f.is_file() or f.name.startswith("."):
                continue
            if f.suffix.lower() not in _RECORDING_EXTS:
                continue

            path_str = str(f)
            status = store.status_of(path_str)

            if status == Status.SENT.value:
                # Uploaded already, but the file is still here (the dispatcher's
                # delete was missed, or delete-after-upload is off). Clean it up
                # ONLY if the policy says recordings may be deleted and the
                # safebrake doesn't shield this scope — same gate the dispatcher
                # uses. Read the owning scope from the row, not the folder name.
                _handle_sent(store, f, may_delete_sent, guard, report)
                continue

            if status in (Status.PENDING.value, Status.SENDING.value):
                report.already_queued += 1
                continue

            if status == Status.FAILED.value:
                item_id = store.id_of(path_str)
                if item_id is not None and store.rearm_failed(item_id):
                    report.requeued += 1
                continue

            # No row → media-prep + register it (content-hash dedup happens
            # inside register_file). A big recording is converted/split here so
            # it can never be enqueued whole onto the FilePartsInvalid wall; the
            # replaced original is retired (gated) once its parts are accounted.
            username = _username_for(f, root)
            result = register_media(
                store, f,
                source="recorder", platform="tiktok",
                username=username,
                priority=priority,
                split_threshold_bytes=split_threshold_bytes,
                identifier_for=lambda out: f"recorder_{out.stem or 'item'}",
                caption_for=lambda out: _caption(username, out),
                retire_original=_retire_after_split,
            )
            if result.busy:
                # Another worker's sweep is already preparing this exact file —
                # NOT a failure. Skip it this pass (retried next start); never
                # launch a second clobbering encode of the same source.
                report.skipped += 1
                log.info("startup-sweep: %s already being prepared by another "
                         "worker — skipped this pass", f.name)
                continue
            if not result.prep_ok:
                # Couldn't prepare safely (bad/oversize file ffmpeg/AutoSplitter
                # refused). Leave the original on disk; retried next start.
                report.skipped += 1
                log.warning("startup-sweep: media_prep failed for %s: %s",
                            f.name, result.error)
                continue
            for outcome in result.outcomes:
                if outcome in (IngestOutcome.INSERTED, IngestOutcome.REARMED):
                    report.requeued += 1
                elif outcome == IngestOutcome.DEDUP_DROPPED:
                    report.dedup_dropped += 1
                elif outcome in (IngestOutcome.DEDUP_ADOPTED,
                                 IngestOutcome.ALREADY_KNOWN):
                    report.already_queued += 1
                else:  # UNSTABLE / HASH_FAILED
                    report.skipped += 1
    finally:
        store.close()

    # 3. prune directories left empty by the deletions above. Deepest first so
    #    a parent emptied only by removing its now-empty children is also caught.
    for d in sorted((p for p in root.rglob("*") if p.is_dir()),
                    key=lambda p: len(p.parts), reverse=True):
        try:
            d.rmdir()  # raises OSError if not empty — exactly what we want
            report.dirs_removed += 1
        except OSError:
            pass

    return report
