"""
dispatcher.send
───────────────
The send Strategy. SendStrategy ABC defines the contract; TelethonSend-
Strategy implements it using Telethon.

WHY STRATEGY:
  Today: Telethon (MTProto user-account uploads, native albums). The per-file
  upload ceiling is the account's MTProto limit (4GB on Premium); files over it
  are split into <=1GB parts up-front at ingest (core.media_prep), so the send
  path never has to chunk a file itself.
  Tomorrow: maybe Bot API for some channels, maybe a fake strategy in
  tests, maybe MTProxy in a different region. The drain loop should not
  care which one is mounted — it just calls .send().

ALBUM BATCHING is NOT in slice 1.
  The archiver's _upload_album_bucket logic batches up to 10 files into
  one Telegram album. In the dispatcher world, every row is a single
  send — albums would require either:
    (a) reading multiple rows at once and committing them atomically, or
    (b) a separate "album_id" column to group rows.
  Both are real features but they break the simple claim-one-send-one
  loop. Slice 1 ships single-file sends; album batching is a sub-slice
  for later.

FLOODWAIT semantics:
  Telethon raises FloodWaitError with .seconds. We treat any value
  > max_flood_wait_s as "give up this attempt, requeue without burning
  retry budget" — long flood waits indicate a more serious rate-limit
  problem that benefits from operator awareness. The drain loop can
  surface this via logs and status.

STALL WATCHDOG:
  A half-open TCP connection (sleep/wake, VPN exit dying, NAT timeout)
  makes Telethon's upload await forever WITHOUT raising — retries only
  fire on exceptions, so the serial drain loop would freeze for good
  (observed: a whole night of zero uploads with one row wedged in
  'sending'). Every send attempt therefore runs under asyncio.wait_for
  with a size-aware deadline: stall_base_timeout_s of fixed grace plus
  payload_bytes / stall_min_rate_kib_s. A slow-but-moving link easily
  beats the assumed floor rate; only a genuine stall hits the deadline.
  Timeout counts as a normal network attempt, and the client is force-
  reconnected first — retrying on the same wedged socket cannot succeed.

ERROR shape:
  SendResult.ok=False with flood_wait_s set → "wait then requeue, no
                                              attempt counted"
  SendResult.ok=False with error set       → "failed; count this attempt"
  SendResult.ok=True                       → done
"""

from __future__ import annotations

import abc
import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telethon import TelegramClient, helpers, utils as tg_utils
from telethon.errors import (
    AuthKeyError, FloodWaitError, ImageProcessFailedError, MediaEmptyError,
    MediaInvalidError, UnauthorizedError,
)
from telethon.tl import functions, types as tg_types

from core.files import media_bucket
from core import media_prep, Sanitizer

from . import fast_upload, image_fix
from .media_meta import make_thumbnail, probe_video
from .progress import ProgressReporter

log = logging.getLogger(__name__)


def _append_filetype_tag(caption: str | None, file_path: str) -> str | None:
    """Append a `#<ext>` hashtag (e.g. `#mkv`) for a file shipped as a document,
    so the archival original is taggable/filterable in the chat. The extension is
    lowercased and stripped of its dot; a file with no extension is left as-is.
    Returns the tag alone when there was no caption."""
    ext = Path(file_path).suffix.lstrip(".").lower()
    if not ext:
        return caption
    tag = f"#{ext}"
    return f"{caption}\n{tag}" if caption else tag


def _video_attributes(file_path: str):
    """Explicit [DocumentAttributeVideo] for a video, or None.

    Telethon can't infer video dimensions without the optional `hachoir`
    dependency (absent here), so it would otherwise attach a 1×1 / 0-duration
    placeholder and Telegram would render the clip at a bogus resolution. We
    probe the real display geometry with ffprobe and hand Telethon a correct
    attribute, which get_attributes() merges in (overriding the placeholder).
    None → not a video, or probe failed; caller uploads as-is."""
    meta = probe_video(file_path)
    if meta is None:
        return None
    return [tg_types.DocumentAttributeVideo(
        duration=meta.duration, w=meta.width, h=meta.height,
        supports_streaming=True,
    )]


class SessionUnauthorized(RuntimeError):
    """The Telegram session is no longer usable — never logged in, revoked,
    expired, or the account was deactivated/banned. This is FATAL and not
    retryable: every send would fail identically, so the daemon must stop
    rather than spin its retry/circuit-breaker machinery forever or block on an
    interactive login prompt. Subclasses RuntimeError so the CLI's existing
    top-level handler turns it into a clean message + non-zero exit (which
    launchd surfaces) instead of a traceback. The in-flight 'sending' rows are
    recovered by the next run's startup watchdog, so nothing is lost."""


# ── Result shape ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SendResult:
    """
    Three legal shapes:
      ok=True                  -> success
      ok=False, flood_wait_s=N -> server-side rate limit; requeue
      ok=False, error="..."    -> real failure; count an attempt

    image_process_failed flags the specific case where Telegram rejected the
    file(s) during photo processing (IMAGE_PROCESS_FAILED). It's deterministic,
    so the retry envelope returns immediately and the caller normalizes the
    image(s) with image_fix before a single re-send rather than burning retries.

    media_empty flags MediaEmptyError — Telegram rejected the uploaded media for
    this destination. Retrying the identical send within the budget is futile and,
    at the head of the queue, blocks everything behind it (a single poison album
    starves the whole drain). So the envelope returns immediately and the caller
    QUARANTINES the batch (terminal 'failed', one hit) to keep the queue moving.
    It is frequently transient (a server-side hiccup for that chat), so quarantined
    rows stay recoverable via `archiver reset failed` once the condition clears —
    no bytes or files are given up.
    """
    ok:                   bool
    error:                str | None = None
    flood_wait_s:         int | None = None
    image_process_failed: bool = False
    media_empty:          bool = False


# ── Strategy ABC ──────────────────────────────────────────────────────────

class SendStrategy(abc.ABC):
    """Pure abstract. Concrete impls own their own connection lifecycle."""

    @abc.abstractmethod
    async def __aenter__(self) -> "SendStrategy": ...

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc, tb) -> None: ...

    @abc.abstractmethod
    async def send(
        self,
        *,
        peer: Any,
        file_path: str,
        caption: str | None,
        ensure_streamable: bool = True,
        topic_id: int | None = None,
    ) -> SendResult: ...

    @abc.abstractmethod
    async def send_album(
        self,
        *,
        peer: Any,
        file_paths: list[str],
        caption: str | None,
        topic_id: int | None = None,
    ) -> SendResult: ...


# ── Telethon implementation ───────────────────────────────────────────────

class TelethonSendStrategy(SendStrategy):
    """
    Single Telegram client per drain run.

    Lifecycle:
      async with TelethonSendStrategy(creds, ...) as strategy:
          await strategy.send(peer=..., file_path=..., caption=...)
      # client disconnects on exit
    """

    def __init__(
        self,
        *,
        api_id:           int,
        api_hash:         str,
        phone:            str,
        session_name:     str,
        max_retries:      int   = 4,
        retry_base_delay: float = 2.0,
        max_flood_wait_s: int   = 600,
        stall_base_timeout_s: float = 600.0,
        stall_min_rate_kib_s: float = 64.0,
        upload_connections: int = 8,   # fast_upload.MAX_CONNECTIONS (see config)
        fast_album: bool = True,        # parallel video-album path (see config)
        progress: ProgressReporter | None = None,
        sanitizer: Sanitizer | None = None,
    ):
        self._api_id           = api_id
        self._api_hash         = api_hash
        self._phone            = phone
        self._session_name     = session_name
        self._max_retries      = max_retries
        self._retry_base_delay = retry_base_delay
        self._max_flood_wait_s = max_flood_wait_s
        self._stall_base_timeout_s = stall_base_timeout_s
        self._stall_min_rate_kib_s = stall_min_rate_kib_s
        self._upload_connections = upload_connections
        self._fast_album = fast_album
        self._progress = progress
        self._sanitizer = sanitizer or Sanitizer([])
        self._client: TelegramClient | None = None

    def _display_name(self, path: str) -> str:
        """The filename Telegram should show: strip the internal ".tgprep"
        marker (media_prep.clean_upload_name) AND any banned word
        (self._sanitizer), protecting the extension. Returns the basename
        unchanged when neither applies — callers only override the name attribute
        when it differs, so a no-op sanitizer keeps the status-quo path."""
        base = media_prep.clean_upload_name(path)
        return self._sanitizer.sanitize_stem(base)

    def _progress_cb(self, file_path: str, *,
                     batch_pos: int | None = None,
                     batch_total: int | None = None):
        """Heartbeat callback for one file upload, or None when reporting is
        off (tests, fakes). Telethon accepts progress_callback=None."""
        if self._progress is None:
            return None
        return self._progress.callback(
            file_path, batch_pos=batch_pos, batch_total=batch_total)

    def _progress_done(self) -> None:
        if self._progress is not None:
            self._progress.clear()

    def _stall_timeout(self, payload_bytes: int) -> float:
        """Per-attempt deadline: fixed grace + worst-tolerated transfer time.
        FloodWait sleeps happen OUTSIDE the attempt, so they never eat into it."""
        transfer_s = payload_bytes / (self._stall_min_rate_kib_s * 1024.0)
        return self._stall_base_timeout_s + transfer_s

    @staticmethod
    def _payload_bytes(file_paths: list[str]) -> int:
        """Total bytes about to go over the wire; a vanished file counts 0
        (the send itself will surface the real error)."""
        total = 0
        for fp in file_paths:
            try:
                total += Path(fp).stat().st_size
            except OSError:
                pass
        return total

    async def _force_reconnect(self) -> None:
        """Tear down and re-establish the MTProto connection after a stall.
        Both halves are themselves deadline-bound — a wedged socket can hang
        disconnect() too — and best-effort: the retry's send will surface any
        connection problem as a normal network error."""
        assert self._client is not None
        try:
            await asyncio.wait_for(self._client.disconnect(), timeout=30)
        except Exception as e:
            log.warning("telethon: disconnect after stall failed: %s", e)
        try:
            await asyncio.wait_for(self._client.connect(), timeout=30)
            log.info("telethon: reconnected after stall")
        except Exception as e:
            log.warning("telethon: reconnect after stall failed: %s", e)

    async def __aenter__(self) -> "TelethonSendStrategy":
        self._client = TelegramClient(
            self._session_name, self._api_id, self._api_hash,
        )
        # Connect and verify authorization WITHOUT Telethon's interactive login.
        # A headless daemon (launchd) has no terminal: client.start() would block
        # forever on the code prompt, or EOF into a restart crash-loop, on a dead
        # session. Fail fast with a clear, actionable message instead. Only when
        # a human is at a TTY (manual first-run) do we allow the interactive
        # login flow so the session can be created in the first place.
        await self._client.connect()
        if not await self._client.is_user_authorized():
            if sys.stdin.isatty():
                await self._client.start(phone=self._phone)
            else:
                await self._client.disconnect()
                raise SessionUnauthorized(
                    f"Telegram session {self._session_name!r} is not authorized "
                    "(never logged in, revoked, expired, or the account was "
                    "deactivated). Run `dispatcher start` once from an "
                    "interactive terminal to log in; refusing to block a "
                    "headless daemon on a login prompt."
                )
        log.info("telethon: connected (session=%s)", self._session_name)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._progress_done()
        if self._client is not None:
            await self._client.disconnect()
            log.info("telethon: disconnected")

    async def check_destination(
        self, *, peer: Any, topic_id: int | None = None,
    ) -> "tuple[bool, str]":
        """Verify a send destination exists on Telegram WITHOUT sending. Returns
        (ok, human detail). Resolves the chat entity; when topic_id is set, also
        confirms the forum topic by fetching its root message — a forum topic's
        thread id IS the id of that creation message, so a None result means the
        topic doesn't exist (or the chat isn't a forum). Powers `check-routes`."""
        assert self._client is not None, "use as async context manager"
        try:
            ent = await self._client.get_entity(peer)
        except Exception as e:
            return False, f"chat NOT found ({type(e).__name__})"
        name = (getattr(ent, "title", None) or getattr(ent, "username", None)
                or getattr(ent, "id", "?"))
        if topic_id is None:
            return True, str(name)
        try:
            msg = await self._client.get_messages(ent, ids=topic_id)
        except Exception as e:
            return False, f"chat OK ({name}) — topic {topic_id} check errored ({type(e).__name__})"
        if msg is None:
            return False, f"chat OK ({name}) — topic {topic_id} NOT found"
        return True, f"{name} · topic {topic_id}"

    async def send(
        self,
        *,
        peer: Any,
        file_path: str,
        caption: str | None,
        ensure_streamable: bool = True,
        filetype_tag: bool = False,
        topic_id: int | None = None,
    ) -> SendResult:
        """
        Single-file send with FloodWait + exponential-backoff retry.

        topic_id (a forum message_thread_id) is passed to Telethon as `reply_to`,
        which posts the message INTO that forum topic. None → the chat's General
        topic (no thread). Telethon's reply_to is how forum topics are targeted.

        filetype_tag appends a `#<ext>` hashtag (e.g. `#mkv`) to the caption when
        the file ships as a document. It is opt-in per source — only chat_id-folder
        (orphaned) items set it — so a recorder/archiver document send is untagged.

        ensure_streamable gates the send-time conversion net. It is the safety
        net for producers that DON'T prep at ingest (the recorder, whose remux
        is fail-soft). Items from sources that already ran media_prep.prepare()
        at ingest pass ensure_streamable=False, so an intentionally non-streamable
        file — e.g. a .mkv kept as a full-quality document alongside its .mp4
        preview — ships as-is instead of being re-converted here.

        Returns SendResult; never raises (caller logic is simpler if it
        can branch on .ok / .flood_wait_s instead of try/except).
        """
        assert self._client is not None, "use as async context manager"

        if not Path(file_path).exists():
            return SendResult(ok=False, error=f"file missing on disk: {file_path}")

        # Parent-dir-exists check catches an unmounted drive: every file
        # in the queue from that drive would otherwise hard-fail and burn
        # its entire retry budget within seconds.
        if not Path(file_path).parent.exists():
            return SendResult(
                ok=False,
                error=f"parent dir unreachable: {Path(file_path).parent}",
            )

        # Safety net for videos that bypassed ingest-time prep (chiefly recorder
        # recordings, whose remux is allowed to fall back to the raw .flv/.ts so
        # a recording is never lost). Convert a non-streamable container to a
        # temp .mp4 and send THAT; None → already streamable / not a video /
        # conversion failed → send the original unchanged. Off the event loop:
        # ffmpeg can take seconds to minutes.
        prepped = None
        as_document = False
        if ensure_streamable:
            prepped = await asyncio.to_thread(
                media_prep.streamable_temp, Path(file_path))
        else:
            # Shipping as-is (producer prepped at ingest). If this is a video
            # Telegram can't stream inline, it's a deliberately-kept full-quality
            # original — chiefly a .mkv kept alongside its .mp4 preview. Send it
            # as a downloadable DOCUMENT, not as a streaming video: otherwise
            # Telegram renders the .mkv as a second playable video and the chat
            # shows the same recording twice instead of one preview + one
            # archival download.
            as_document = await asyncio.to_thread(
                media_prep.is_nonstreamable_video, Path(file_path))
        send_path = str(prepped) if prepped is not None else file_path

        # Photo pre-flight (proactive compatibility, mirroring the album path).
        # A photo Telegram's pipeline would refuse is normalized to a safe JPEG
        # BEFORE the first send rather than only after an IMAGE_PROCESS_FAILED
        # bounce, so single sends and album sends are made compatible the same
        # way. One that can't become a clean photo (aspect ratio beyond the
        # photo limit) ships as a downloadable document instead. Photos never
        # take the video conversion path (prepped is always None for them), so
        # this and the streamable net are mutually exclusive.
        photo_temp: str | None = None
        if not as_document and media_bucket(send_path) == "photo":
            verdict = await asyncio.to_thread(image_fix.photo_needs_fix, send_path)
            if verdict is None:
                as_document = True                      # un-fixable AR → document
            elif verdict is True:
                photo_temp = await asyncio.to_thread(
                    image_fix.make_safe_photo, send_path)
                if photo_temp:
                    send_path = photo_temp
            # verdict False → already compatible; send as-is.

        if as_document:
            # A pure document send: no video attributes, no poster thumb, no
            # streaming flag. Telegram stores the bytes verbatim for download.
            # The parallel upload happens INSIDE _do_doc so a retry re-uploads.
            # Tag the caption with the container type (#mkv, #avi, …) so the
            # archival full-quality download is visibly distinguished from its
            # streamable .mp4 preview in the chat. Chat_id-folder items only.
            doc_caption = (_append_filetype_tag(caption, file_path)
                           if filetype_tag else caption)
            # Override the shown filename when ".tgprep"/a banned word changed it
            # — a document's name is the most visible text after the caption.
            doc_display = self._display_name(send_path)
            doc_attrs = ([tg_types.DocumentAttributeFilename(doc_display)]
                         if doc_display != Path(send_path).name else None)
            try:
                async def _do_doc():
                    media = await self._upload_document(
                        send_path, attributes=doc_attrs, thumb_path=None,
                        supports_streaming=False, force_document=True,
                        progress_cb=self._progress_cb(file_path))
                    await self._client.send_file(peer, media, caption=doc_caption,
                                                 reply_to=topic_id)
                return await self._send_with_retries(
                    _do_doc, what=f"{Path(file_path).name} (document)",
                    payload_bytes=self._payload_bytes([send_path]),
                )
            finally:
                self._progress_done()

        # Both probes shell out to ffprobe/ffmpeg (seconds, worst-case tens) —
        # off the event loop so signal handling and FloodWait timers stay live.
        attributes = await asyncio.to_thread(_video_attributes, send_path)
        # Telethon names the upload after the file on disk. Whenever that name
        # carries the internal ".tgprep" marker — a send-time conversion temp OR
        # an as-is file converted in place at ingest (an incompatible-codec .mp4
        # stored as "<stem>.tgprep.mp4") — override it with the clean name so the
        # tag never reaches Telegram. A user-supplied filename still wins in
        # get_attributes().
        display = self._display_name(send_path)
        if display != Path(send_path).name:
            attributes = (attributes or []) + [
                tg_types.DocumentAttributeFilename(display)]
        # Explicit poster frame so Telegram doesn't auto-grab a black/white
        # fade-in frame as the inline preview. None → not a video / probe
        # failed; Telethon falls back to server-side generation (status quo).
        thumb = await asyncio.to_thread(make_thumbnail, send_path)

        # Videos go up via the parallel multi-connection uploader (big-file
        # speedup); photos/gifs keep Telethon's path-based send so its photo
        # handling — and the image-reprocess retry below — stays intact. Both
        # build the upload INSIDE _do so a FloodWait/stall retry re-uploads.
        is_video = media_bucket(send_path) == "video"

        try:
            if is_video:
                async def _do():
                    media = await self._upload_document(
                        send_path, attributes=attributes, thumb_path=thumb,
                        supports_streaming=True, force_document=False,
                        progress_cb=self._progress_cb(file_path))
                    await self._client.send_file(peer, media, caption=caption,
                                                 reply_to=topic_id)
            else:
                async def _do():
                    await self._client.send_file(
                        peer, send_path, caption=caption, supports_streaming=True,
                        attributes=attributes, thumb=thumb, reply_to=topic_id,
                        progress_callback=self._progress_cb(file_path),
                    )
            result = await self._send_with_retries(
                _do, what=Path(file_path).name,
                payload_bytes=self._payload_bytes([send_path]),
            )
        finally:
            self._progress_done()
            for tmp in (prepped, photo_temp, thumb):
                if tmp:
                    try:
                        os.unlink(tmp)
                    except OSError:
                        pass
        if result.ok or not result.image_process_failed:
            return result

        # Telegram refused to process this image as a photo. Normalize it with
        # ffmpeg and re-send once; on conversion failure keep the original
        # result so the drain loop counts the attempt.
        safe = await asyncio.to_thread(image_fix.make_safe_photo, file_path)
        if not safe:
            return result
        try:
            async def _do_retry():
                await self._client.send_file(
                    peer, safe, caption=caption, supports_streaming=True,
                    reply_to=topic_id,
                    progress_callback=self._progress_cb(file_path),
                )
            return await self._send_with_retries(
                _do_retry, what=f"{Path(file_path).name} (converted)",
                payload_bytes=self._payload_bytes([safe]),
            )
        finally:
            self._progress_done()
            try:
                os.unlink(safe)
            except OSError:
                pass

    async def send_album(
        self,
        *,
        peer: Any,
        file_paths: list[str],
        caption: str | None,
        topic_id: int | None = None,
    ) -> SendResult:
        """Send up to 10 files as ONE Telegram album (SendMultiMedia).

        topic_id (forum message_thread_id) → Telethon `reply_to`, posting the
        whole album into that topic; None → General.

        Atomic at the API level: the single send_file([..]) call either
        returns (all items posted) or raises (none posted) — there is no
        partial album, which is what lets the drain loop mark the whole
        batch sent-or-failed together.

        A1 caption semantics: Telegram shows the caption only on the album's
        first item, so we pass [caption, None, None, ...]. Caller is
        responsible for pre-filtering missing files (drain does this so it
        can mark the missing ones failed individually).
        """
        assert self._client is not None, "use as async context manager"
        if not file_paths:
            return SendResult(ok=False, error="send_album: empty file list")

        # caption only on the first item; rest None.
        captions: list[str | None] = [caption] + [None] * (len(file_paths) - 1)

        # Photo albums go through their own normalize-and-send path; video albums
        # are handled below. Both ultimately use Telethon's native list send.
        # Albums are homogeneous (drain groups by media bucket), so the anchor's
        # bucket decides which path we take.
        is_video_album = media_bucket(file_paths[0]) == "video"
        if not is_video_album:
            return await self._send_photo_album(
                peer, file_paths, captions, topic_id=topic_id)

        # HOW a video album is built. Two paths, same delivery contract:
        #
        #  FAST (default) — _send_video_album_fast: upload each item over the
        #  multi-connection fan-out, materialize via messages.UploadMedia, group
        #  with SendMultiMedia. UploadMedia re-derives each item's geometry +
        #  poster SERVER-side, so the album doesn't lean on hachoir, and big
        #  items (split-original parts, large clips) ride the same parallel
        #  uploader single sends use instead of going up serially.
        #
        #  NATIVE — Telethon's list send: one upload at a time, geometry derived
        #  by Telethon (needs hachoir, a hard dep; without it albums render as
        #  1x1 images). The fallback when the fast path's Telethon internals are
        #  absent, or when FAST_ALBUM=0 pins it.
        #
        # An earlier note here claimed the fast path was "REJECTED by Telegram
        # (MediaEmptyError) — materialized or not". That bisection missed the
        # exact extraction: the album media must be get_input_media(r.document,
        # supports_streaming=True) — the Document, with the flag — NOT the
        # MessageMediaDocument wrapper. With that, H.264 and VP9 albums group
        # cleanly (verified live). Either path leaves the converter alone:
        # a per-item Telegram rejection surfaces as media_empty and the drain's
        # recover_media_empty re-sends that item with the streamable net ON.
        if self._fast_album and fast_upload._internals_present(self._client):
            return await self._send_video_album_fast(
                peer, file_paths, captions, topic_id=topic_id)

        async def _do():
            await self._client.send_file(
                peer, file_paths, caption=captions, supports_streaming=True,
                reply_to=topic_id,
                progress_callback=self._progress_cb(
                    file_paths[0], batch_total=len(file_paths)),
            )
        try:
            return await self._send_with_retries(
                _do, what=f"album[{len(file_paths)}] {Path(file_paths[0]).name}…",
                payload_bytes=self._payload_bytes(file_paths),
            )
        finally:
            self._progress_done()

    async def _send_video_album_fast(
        self,
        peer: Any,
        file_paths: list[str],
        captions: list[str | None],
        *,
        topic_id: int | None = None,
    ) -> SendResult:
        """Group a video album from pre-uploaded documents — the parallel
        equivalent of Telethon's serial native list send.

        Per item: fast_upload fan-out → InputMediaUploadedDocument → messages.
        UploadMedia materializes it server-side → the resulting InputMediaDocument
        is grouped with messages.SendMultiMedia.

        MINIMAL RECIPE — and it has to be. The uploaded document is built with
        ONLY get_attributes(path, supports_streaming=True) (the _upload_document
        defaults: attributes=None, thumb=None). Attaching an explicit
        DocumentAttributeVideo, a poster thumb, or a DocumentAttributeFilename to
        the to-be-grouped item makes Telegram reject the group with
        MediaInvalid/MediaEmpty (verified live — this is the real grain of truth
        behind the old "supplying them breaks the group" note). It costs nothing:
        the native list send didn't carry per-item thumbs/filenames either, and
        UploadMedia re-derives geometry AND a poster server-side (so VP9 renders
        correctly without hachoir leaning on us). Geometry is thus double-safe;
        the on-disk filename shows, exactly as the native path behaved.

        INTEGRITY CONTRACT. The whole sequence runs inside _send_with_retries so
        a MediaEmptyError raised at EITHER new call site — the per-item
        UploadMedia or the group SendMultiMedia — maps to SendResult.media_empty,
        exactly as the native send_file does today. The drain then runs its
        per-item recover_media_empty (re-encode + single send) untouched. Same
        for FloodWait, the stall watchdog, and a dead session. Items upload one
        at a time (each fanned across the connections) — never N×connections at
        once, which the home DC would throttle. A retry re-uploads from scratch
        (fresh file_ids): correct, if not free — efficiency is the lowest build
        priority and album retries are rare."""
        assert self._client is not None
        n = len(file_paths)
        client = self._client

        async def _parsed_caption(text: str | None):
            """Match the native list send's caption handling (markdown parse via
            the client's parse_mode). Defensive: any parse hiccup ships the raw
            text rather than failing the album."""
            try:
                return await client._parse_message_text(
                    text or "", client.parse_mode)
            except Exception:
                return (text or ""), None

        async def _do():
            media = []
            for i, fp in enumerate(file_paths):
                uploaded = await self._upload_document(
                    fp, attributes=None, thumb_path=None,
                    supports_streaming=True, force_document=False,
                    progress_cb=self._progress_cb(
                        fp, batch_pos=i + 1, batch_total=n))

                # Materialize: Telegram registers the uploaded bytes as a
                # server-side Document (re-deriving its own attributes + poster,
                # which is why VP9 groups fine). Extract the Document WITH the
                # streaming flag — the MessageMediaDocument wrapper form yields
                # invalid grouping media and would MediaEmpty.
                r = await client(functions.messages.UploadMediaRequest(
                    peer, media=uploaded))
                input_media = tg_utils.get_input_media(
                    r.document, supports_streaming=True)

                msg, entities = await _parsed_caption(captions[i])
                media.append(tg_types.InputSingleMedia(
                    media=input_media, message=msg, entities=entities,
                    random_id=helpers.generate_random_long()))

            await client(functions.messages.SendMultiMediaRequest(
                peer, multi_media=media,
                reply_to=(tg_types.InputReplyToMessage(topic_id)
                          if topic_id is not None else None)))

        try:
            return await self._send_with_retries(
                _do, what=f"album[{n}] {Path(file_paths[0]).name}… (fast)",
                payload_bytes=self._payload_bytes(file_paths),
            )
        finally:
            self._progress_done()

    async def _send_photo_album(
        self,
        peer: Any,
        file_paths: list[str],
        captions: list[str | None],
        *,
        topic_id: int | None = None,
    ) -> SendResult:
        """Send a photo album, normalizing any image Telegram would reject.

        Two-phase:
          1. Preflight — probe each file and isolate the ones that violate
             Telegram's photo limits, re-encoding only those to a safe JPEG
             (image_fix). Good files are sent untouched. The whole batch still
             goes out together as one album.
          2. Fallback — if the album is STILL rejected (an odd encoding that
             passed the dimension/size preflight), re-encode every remaining
             original and retry the album once. After that we give up and let
             the drain loop mark the batch failed.

        Temp files from any conversion are always cleaned up.
        """
        temps: list[str] = []
        try:
            prepared: list[str] = []
            for fp in file_paths:
                verdict = await asyncio.to_thread(image_fix.photo_needs_fix, fp)
                if verdict is True:
                    safe = await asyncio.to_thread(image_fix.make_safe_photo, fp)
                    if safe:
                        temps.append(safe)
                        prepared.append(safe)
                    else:
                        prepared.append(fp)  # conversion failed → best effort
                else:
                    # False (safe) or None (extreme aspect ratio we can't fix
                    # into a clean photo) → send the original as-is.
                    prepared.append(fp)

            what = f"album[{len(prepared)}] {Path(file_paths[0]).name}…"

            # One album-level callback (batch_pos=None): Telethon uploads the
            # list sequentially through this single callback, so per-file
            # attribution isn't knowable here — the heartbeat still shows
            # name, album size, and live byte counts.
            async def _do():
                await self._client.send_file(
                    peer, prepared, caption=captions, supports_streaming=True,
                    reply_to=topic_id,
                    progress_callback=self._progress_cb(
                        file_paths[0], batch_total=len(prepared)),
                )
            result = await self._send_with_retries(
                _do, what=what, payload_bytes=self._payload_bytes(prepared),
            )
            if result.ok or not result.image_process_failed:
                return result

            # Preflight passed but Telegram still rejected something — convert
            # every not-yet-converted original and retry the album once.
            log.warning(
                "image_fix: %s still rejected after preflight — converting "
                "remaining originals and retrying", what,
            )
            retry_paths: list[str] = []
            for orig, prep in zip(file_paths, prepared):
                if prep in temps:        # already a converted temp
                    retry_paths.append(prep)
                    continue
                safe = await asyncio.to_thread(image_fix.make_safe_photo, orig)
                if safe:
                    temps.append(safe)
                    retry_paths.append(safe)
                else:
                    retry_paths.append(orig)

            async def _do_retry():
                await self._client.send_file(
                    peer, retry_paths, caption=captions, supports_streaming=True,
                    reply_to=topic_id,
                    progress_callback=self._progress_cb(
                        file_paths[0], batch_total=len(retry_paths)),
                )
            return await self._send_with_retries(
                _do_retry, what=f"{what} (converted)",
                payload_bytes=self._payload_bytes(retry_paths),
            )
        finally:
            self._progress_done()
            for t in temps:
                try:
                    os.unlink(t)
                except OSError:
                    pass

    async def _upload_document(
        self, send_path: str, *, attributes, thumb_path: str | None,
        supports_streaming: bool, force_document: bool, progress_cb,
    ):
        """Upload one file via the parallel multi-connection uploader and wrap
        it as an InputMediaUploadedDocument ready for send_file.

        The single choke point for every big-file send — single videos, kept
        originals (force_document), and album items all funnel through here, so
        the FastTelethon fan-out and the InputMedia construction live in ONE
        place. `attributes` is passed straight to get_attributes (an explicit
        DocumentAttributeFilename there wins over the derived basename); the
        thumb is uploaded and baked in when present."""
        assert self._client is not None
        handle = await fast_upload.upload_file(
            self._client, send_path,
            connections=self._upload_connections, progress_callback=progress_cb,
        )
        thumb_handle = (
            await self._client.upload_file(thumb_path) if thumb_path else None
        )
        attrs, mime = tg_utils.get_attributes(
            send_path,
            attributes=attributes,
            supports_streaming=supports_streaming,
            force_document=force_document,
        )
        return tg_types.InputMediaUploadedDocument(
            file=handle, mime_type=mime, attributes=attrs, thumb=thumb_handle,
        )

    async def _send_with_retries(
        self, send_fn, *, what: str, payload_bytes: int = 0,
    ) -> SendResult:
        """Shared FloodWait + exponential-backoff envelope for both single
        and album sends. `send_fn` is an async no-arg callable performing
        the actual Telethon send_file; the only thing that differs between
        single and album is that call, so the retry/flood logic lives here
        once rather than being duplicated (and able to drift).

        Every attempt runs under the stall watchdog (see module docstring):
        a deadline sized to payload_bytes converts a silent network freeze
        into a countable, retryable failure instead of an eternal await."""
        timeout_s = self._stall_timeout(payload_bytes)
        attempts = 0
        last_error: str | None = None
        while attempts < self._max_retries:
            try:
                await asyncio.wait_for(send_fn(), timeout=timeout_s)
                return SendResult(ok=True)

            except (UnauthorizedError, AuthKeyError) as e:
                # The session died under us (revoked/expired/deactivated). This
                # is permanent: every subsequent send fails identically, so
                # retrying or letting the circuit breaker spin a cooldown loop
                # only hammers Telegram with doomed uploads. Surface it as fatal
                # so the daemon stops cleanly — the startup auth check then keeps
                # it down with an actionable message until the session is fixed.
                raise SessionUnauthorized(
                    f"Telegram session rejected mid-send ({type(e).__name__}: "
                    f"{e}) while sending {what}"
                ) from e

            except FloodWaitError as e:
                if e.seconds > self._max_flood_wait_s:
                    log.error(
                        "telethon: FloodWait %ds > cap %ds — surfacing to dispatcher",
                        e.seconds, self._max_flood_wait_s,
                    )
                    return SendResult(ok=False, flood_wait_s=int(e.seconds))
                wait_s = int(e.seconds) + 1
                log.warning("telethon: FloodWait %ds (%s) — sleeping", wait_s, what)
                await asyncio.sleep(wait_s)
                continue   # do NOT count as an attempt

            except ImageProcessFailedError as e:
                # Deterministic: the server can't process this image as a
                # photo, so retrying the identical send is pointless. Surface
                # it so the caller can normalize the file and re-send once.
                log.warning(
                    "telethon: image rejected (%s): %s — normalizing & retrying",
                    what, e,
                )
                return SendResult(
                    ok=False,
                    error=f"{type(e).__name__}: {e}",
                    image_process_failed=True,
                )

            except (MediaEmptyError, MediaInvalidError) as e:
                # Telegram rejected the uploaded media for this destination.
                # MediaInvalid is its sibling — an album whose grouped items the
                # server won't accept together (e.g. a mixed-codec batch, which
                # BOTH the fast and native list sends hit). Treat them the same:
                # retrying the identical send within the budget cannot help and,
                # because the failing rows hold the oldest discovered_at, they sit
                # at the head of the queue and block everything behind them. Bail
                # out on the FIRST hit (no 4x backoff storm) and let the caller
                # quarantine the batch so the drain moves on — for an album that
                # is recover_media_empty, which re-sends each item individually
                # (single-codec-of-one, so it groups trivially) with the
                # streamable net ON. Often transient, so `reset failed` recovers
                # it later — see SendResult.media_empty.
                log.warning(
                    "telethon: media rejected by destination (%s): %s — "
                    "quarantining (recover with `reset failed`)", what, e,
                )
                return SendResult(
                    ok=False,
                    error=f"{type(e).__name__}: {e}",
                    media_empty=True,
                )

            except (TimeoutError, asyncio.TimeoutError):
                # Stall watchdog fired: no exception from the socket, just no
                # progress within the deadline. Must precede the OSError arm —
                # builtin TimeoutError IS an OSError subclass. The connection
                # is presumed wedged; recycle it before the next attempt.
                attempts += 1
                last_error = (
                    f"stalled: send incomplete after {timeout_s:.0f}s "
                    f"({payload_bytes} bytes)"
                )
                log.warning(
                    "telethon: stall attempt %d/%d (%s): no completion in "
                    "%.0fs — reconnecting",
                    attempts, self._max_retries, what, timeout_s,
                )
                await self._force_reconnect()
                continue

            except (ConnectionError, OSError) as e:
                attempts += 1
                last_error = f"{type(e).__name__}: {e}"
                delay = self._retry_base_delay * (2 ** (attempts - 1))
                log.warning(
                    "telethon: network err attempt %d/%d (%s): %s — retry in %.1fs",
                    attempts, self._max_retries, what, e, delay,
                )
                if attempts < self._max_retries:
                    await asyncio.sleep(delay)

            except Exception as e:
                attempts += 1
                last_error = f"{type(e).__name__}: {e}"
                delay = self._retry_base_delay * (2 ** (attempts - 1))
                log.warning(
                    "telethon: send err attempt %d/%d (%s): %s: %s — retry in %.1fs",
                    attempts, self._max_retries, what,
                    type(e).__name__, e, delay,
                )
                if attempts < self._max_retries:
                    await asyncio.sleep(delay)

        return SendResult(
            ok=False,
            error=last_error or "send failed (no exception captured)",
        )
