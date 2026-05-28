"""
archiver.telegram
─────────────────
Telegram MTProto uploader.

Key design choices:
  - ONE TelegramClient per run (async context manager lifecycle).
    All uploads (every user, every platform) reuse the same connection.
  - Per-(platform, username) DESTINATION routed via TelegramRouter.
    Low-level _send_with_retries doesn't know about platforms; it just
    gets handed a resolved peer.
  - FloodWaitError honored exactly (sleep e.seconds, retry).
  - Per-file failures don't burn the whole run.

Album semantics (Telethon v1):
  send_file(chat, [path1, path2, ...], caption=[cap1, None, ...])
    → SendMultiMediaRequest = Telegram album.
    → Only the first item shows the caption.
  send_file(chat, [single_path], ...) still calls SendMultiMedia with
  one item and can fail oddly — special-case len==1 → single send.

Delete-after-upload safety contract:
  A local file is deleted ONLY when ALL of the following hold:
    (1) Telethon send returned no exception
    (2) db.mark_sent(file_path) has committed telegram_sent=1
    (3) delete_policy.should_delete(platform, username) returns True
  Sequenced as (1) → (2) → (3) → unlink. Any exception in between
  leaves the file on disk — the only safe failure mode.

  The single gate is `_maybe_cleanup`. Two callers exist:
    - this module's album-batch and single-file paths (gated)
    - orchestrator._purge_sent_files (disk-full emergency on rows
      already known sent — bypasses policy because the row is sent)
  Search for `_cleanup` references before adding more callers.
"""

from __future__ import annotations

import asyncio
import logging
from itertools import islice
from pathlib import Path
from typing import Iterable, Sequence, Any

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from .config import Config
from .db import ArchiveDB
from .policies import DeletePolicy
from .tg_router import TelegramRouter

log = logging.getLogger(__name__)

PHOTO_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
VIDEO_EXTS = {".mp4", ".mov", ".webm", ".mkv"}
GIF_EXTS   = {".gif"}
ALBUM_MAX  = 10  # Telegram's hard limit on items per album

MAX_FLOOD_WAIT_S = 600   # cap on per-request FloodWait we'll honor inline


# ── Sorting & bucketing ───────────────────────────────────────────────────────

def _sort_key(file_path: str) -> str:
    """Chronological key. Filenames starting with YYYYMMDD sort correctly."""
    stem = Path(file_path).stem
    if len(stem) >= 8 and stem[:8].isdigit():
        return stem
    try:
        return f"{Path(file_path).stat().st_mtime:020.6f}"
    except OSError:
        return stem


def _bucket(file_path: str) -> str:
    ext = Path(file_path).suffix.lower()
    if ext in PHOTO_EXTS: return "photos"
    if ext in VIDEO_EXTS: return "videos"
    if ext in GIF_EXTS:   return "gifs"
    return "other"


def _split_by_type(rows: Sequence[Any]) -> dict[str, list]:
    sorted_rows = sorted(rows, key=lambda r: _sort_key(r["file_path"]))
    buckets: dict[str, list] = {"photos": [], "videos": [], "gifs": [], "other": []}
    for row in sorted_rows:
        buckets[_bucket(row["file_path"])].append(row)
    return buckets


def _chunked(it: Iterable, n: int):
    it = iter(it)
    while chunk := list(islice(it, n)):
        yield chunk


def _album_caption_list(header: str, idx: int, total: int,
                        batch_size: int) -> list[str | None]:
    text = f"{header} [{idx}/{total}]" if total > 1 else header
    return [text] + [None] * (batch_size - 1)


# ── Uploader ──────────────────────────────────────────────────────────────────

class TelegramUploader:
    """
    Async context manager. Opens once at run start, closes at run end.

    Constructor takes both a DeletePolicy (per-row delete decision) and
    a TelegramRouter (per-platform destination chat).
    """

    def __init__(self, config: Config, db: ArchiveDB,
                 delete_policy: DeletePolicy, router: TelegramRouter):
        self.config = config
        self.db     = db
        self.delete_policy = delete_policy
        self.router = router
        self.client: TelegramClient | None = None

    async def __aenter__(self) -> "TelegramUploader":
        self.client = TelegramClient(
            self.config.telegram.session_name,
            self.config.telegram.api_id,
            self.config.telegram.api_hash,
        )
        await self.client.start(phone=self.config.telegram.phone)
        log.info("Telegram client connected (session=%s)",
                 self.config.telegram.session_name)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.client is not None:
            await self.client.disconnect()
            log.info("Telegram client disconnected")

    # ── Failsafe cleanup helper ───────────────────────────────────────────────

    def _maybe_cleanup(self, platform: str, username: str, file_path: str) -> None:
        """
        SINGLE entry point for deleting a successfully-uploaded local file.

        Preconditions (caller responsibility):
          - Telethon send raised no exception
          - db.mark_sent(file_path) has been called and committed

        We re-read telegram_sent from the DB here as a paranoid defense:
        if a future refactor moves mark_sent AFTER cleanup, this check
        fires a loud ERROR log and refuses to delete — turning a silent
        data-loss bug into a "file didn't delete, why?" question.

        Cost: one indexed SELECT per file. Trivial; correctness is worth it.
        """
        if not self.delete_policy.should_delete(platform, username):
            return
        state = self.db.telegram_sent_state(file_path)
        if state != 1:
            log.error(
                "    refusing to delete %s — DB says telegram_sent=%s "
                "(expected 1). Possible regression in upload flow.",
                Path(file_path).name, state,
            )
            return
        _cleanup(file_path)

    # ── Public API ────────────────────────────────────────────────────────────

    async def upload_pending(self, platform: str, username: str) -> tuple[int, int]:       
        rows = self.db.pending_uploads(platform, username)
        if not rows:
            return 0, 0

        # Guard: if the first pending file's parent directory is inaccessible,
        # the storage is likely unmounted. Bail entirely rather than mark_failed
        # on every file — those rows should stay pending for when the drive returns.
        first_path = Path(rows[0]["file_path"])
        if not first_path.parent.exists():
            log.error(
                "  [%s] @%s: parent dir unreachable (%s) — "
                "storage may be unmounted. Skipping upload; rows stay pending.",
                platform, username, first_path.parent,
            )
            return 0, 0
        assert self.client is not None, "Use as `async with TelegramUploader(...):`"

        # Resolve destination ONCE per (platform, user) — cheap, but no
        # reason to re-walk env vars per file.
        peer = self.router.peer_for(platform, username)
        chat_explain = self.router.explain(platform, username)
        log.info("  Uploading: %d pending for @%s [%s] → %s",
                 len(rows), username, platform, chat_explain)

        buckets = _split_by_type(rows)
        for kind, items in buckets.items():
            if items:
                log.info("    %s: %d", kind, len(items))

        sent = failed = 0

        for label, items in (("📷", buckets["photos"]), ("🎬", buckets["videos"])):
            s, f = await self._upload_album_bucket(
                platform, username, peer, items, label,
            )
            sent += s; failed += f

        for row in buckets["gifs"] + buckets["other"]:
            ok = await self._upload_single(platform, username, peer, row)
            if ok:
                # Order matters: mark_sent FIRST, then maybe delete.
                self.db.mark_sent(row["file_path"])
                sent += 1
                self._maybe_cleanup(platform, username, row["file_path"])
            else:
                self.db.mark_failed(row["file_path"])
                failed += 1

        log.info("  @%s [%s]: ✓%d ✗%d", username, platform, sent, failed)
        return sent, failed

    # ── Album path ────────────────────────────────────────────────────────────

    async def _upload_album_bucket(
        self,
        platform: str,
        username: str,
        peer:     Any,
        rows:     list,
        label:    str,
    ) -> tuple[int, int]:
        if not rows:
            return 0, 0

        sent = failed = 0
        batches = list(_chunked(rows, ALBUM_MAX))

        for batch_idx, batch in enumerate(batches, start=1):
            existing = []
            for r in batch:
                if Path(r["file_path"]).exists():
                    existing.append(r)
                else:
                    log.warning("    missing on disk: %s",
                                Path(r["file_path"]).name)
                    self.db.mark_failed(r["file_path"]); failed += 1
            if not existing:
                continue

            files = [r["file_path"] for r in existing]
            header = f"{label} @{username} · {platform}"
            captions = _album_caption_list(header, batch_idx, len(batches), len(existing))

            log.info("    %s batch %d/%d (%d files)",
                     label, batch_idx, len(batches), len(existing))

            ok = await self._send_with_retries(
                peer        = peer,
                files       = files,
                captions    = captions,
                supports_streaming = True,
            )
            if ok:
                # Mark the entire batch sent BEFORE any cleanup.
                for r in existing:
                    self.db.mark_sent(r["file_path"])
                    sent += 1
                for r in existing:
                    self._maybe_cleanup(platform, username, r["file_path"])
            else:
                for r in existing:
                    self.db.mark_failed(r["file_path"]); failed += 1

            if batch_idx < len(batches):
                await asyncio.sleep(self.config.inter_album_sleep)

        return sent, failed

    # ── Single path ───────────────────────────────────────────────────────────

    async def _upload_single(self, platform: str, username: str,
                             peer: Any, row) -> bool:
        fp = row["file_path"]
        if not Path(fp).exists():
            log.warning("    missing on disk: %s", Path(fp).name)
            return False

        identifier = row["identifier"] or ""
        caption = (
            f"@{username} · {platform} · {identifier}"
            if identifier else
            f"@{username} · {platform}"
        )

        return await self._send_with_retries(
            peer               = peer,
            files              = [fp],
            captions           = caption,
            supports_streaming = True,
        )

    # ── Retry envelope (FloodWait-aware) ──────────────────────────────────────

    async def _send_with_retries(
        self,
        *,
        peer:               Any,
        files:              list[str],
        captions:           list[str | None] | str,
        supports_streaming: bool,
    ) -> bool:
        assert self.client is not None

        attempts = 0
        while attempts < self.config.max_retries:
            try:
                if len(files) == 1:
                    cap = captions[0] if isinstance(captions, list) else captions
                    await self.client.send_file(
                        peer, files[0],
                        caption=cap,
                        supports_streaming=supports_streaming,
                    )
                else:
                    await self.client.send_file(
                        peer, files,
                        caption=captions,
                        supports_streaming=supports_streaming,
                    )
                return True

            except FloodWaitError as e:
                if e.seconds > MAX_FLOOD_WAIT_S:
                    log.error(
                        "    FloodWait: server requested %ds (> cap %ds) — aborting send. "
                        "Account may be rate-limited; back off this platform.",
                        e.seconds, MAX_FLOOD_WAIT_S,
                    )
                    return False
                wait_s = e.seconds + 1
                log.warning("    FloodWait: server requested %ds — sleeping", wait_s)
                await asyncio.sleep(wait_s)
                continue

            except (ConnectionError, OSError) as e:
                attempts += 1
                delay = self.config.retry_base_delay * (2 ** (attempts - 1))
                log.warning("    network error (attempt %d/%d): %s — retry in %.1fs",
                            attempts, self.config.max_retries, e, delay)
                await asyncio.sleep(delay)

            except Exception as e:
                attempts += 1
                delay = self.config.retry_base_delay * (2 ** (attempts - 1))
                log.warning("    send error (attempt %d/%d): %s: %s — retry in %.1fs",
                            attempts, self.config.max_retries,
                            type(e).__name__, e, delay)
                if attempts < self.config.max_retries:
                    await asyncio.sleep(delay)

        log.error("    gave up after %d attempts", self.config.max_retries)
        return False


# ── Cleanup (module-level; orchestrator's disk-full path also calls this) ────

def _cleanup(file_path: str) -> None:
    """
    Raw delete: media file + its sidecar(s). NOT gated — all gating is
    in `_maybe_cleanup` and `_purge_sent_files`. Do not add new direct
    callers; if you need to delete an uploaded file, go through
    `TelegramUploader._maybe_cleanup`.
    """
    p = Path(file_path)
    try: p.unlink(missing_ok=True)
    except OSError as e:
        log.warning("    cleanup failed: %s", e); return
    for suffix in (".json", ".info.json"):
        try: p.with_suffix(suffix).unlink(missing_ok=True)
        except OSError: pass
    # Also try the gallery-dl-style sidecar `<full_name>.json`:
    try: (p.parent / (p.name + ".json")).unlink(missing_ok=True)
    except OSError: pass
