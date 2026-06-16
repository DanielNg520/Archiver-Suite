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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telethon import TelegramClient, utils as tg_utils
from telethon.errors import FloodWaitError, ImageProcessFailedError
from telethon.tl import types as tg_types

from core.files import media_bucket
from core import media_prep

from . import fast_upload, image_fix
from .media_meta import make_thumbnail, probe_video
from .progress import ProgressReporter

log = logging.getLogger(__name__)


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
    """
    ok:                   bool
    error:                str | None = None
    flood_wait_s:         int | None = None
    image_process_failed: bool = False


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
    ) -> SendResult: ...

    @abc.abstractmethod
    async def send_album(
        self,
        *,
        peer: Any,
        file_paths: list[str],
        caption: str | None,
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
        upload_connections: int = 4,
        progress: ProgressReporter | None = None,
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
        self._progress = progress
        self._client: TelegramClient | None = None

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
        await self._client.start(phone=self._phone)
        log.info("telethon: connected (session=%s)", self._session_name)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._progress_done()
        if self._client is not None:
            await self._client.disconnect()
            log.info("telethon: disconnected")

    async def send(
        self,
        *,
        peer: Any,
        file_path: str,
        caption: str | None,
        ensure_streamable: bool = True,
    ) -> SendResult:
        """
        Single-file send with FloodWait + exponential-backoff retry.

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

        if as_document:
            # A pure document send: no video attributes, no poster thumb, no
            # streaming flag. Telegram stores the bytes verbatim for download.
            # The parallel upload happens INSIDE _do_doc so a retry re-uploads.
            try:
                async def _do_doc():
                    media = await self._upload_document(
                        send_path, attributes=None, thumb_path=None,
                        supports_streaming=False, force_document=True,
                        progress_cb=self._progress_cb(file_path))
                    await self._client.send_file(peer, media, caption=caption)
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
        display = media_prep.clean_upload_name(send_path)
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
                    await self._client.send_file(peer, media, caption=caption)
            else:
                async def _do():
                    await self._client.send_file(
                        peer, send_path, caption=caption, supports_streaming=True,
                        attributes=attributes, thumb=thumb,
                        progress_callback=self._progress_cb(file_path),
                    )
            result = await self._send_with_retries(
                _do, what=Path(file_path).name,
                payload_bytes=self._payload_bytes([send_path]),
            )
        finally:
            self._progress_done()
            if prepped is not None:
                try:
                    os.unlink(prepped)
                except OSError:
                    pass
            if thumb:
                try:
                    os.unlink(thumb)
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
    ) -> SendResult:
        """Send up to 10 files as ONE Telegram album (SendMultiMedia).

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

        # Photo albums are sent the original way (paths): Telethon resizes
        # photos and the 1×1 placeholder bug is video-only, so there's nothing
        # to fix and no reason to bypass its photo handling. Video albums need
        # the custom path — see _build_album_item. Albums are homogeneous
        # (drain groups by media bucket), so the anchor's bucket decides.
        is_video_album = media_bucket(file_paths[0]) == "video"
        if not is_video_album:
            return await self._send_photo_album(peer, file_paths, captions)

        # Poster frames for each clip, built once so they survive retries; same
        # black/white-fade-in fix as the single-send path. fp → thumb-path|None.
        thumbs = {
            fp: await asyncio.to_thread(make_thumbnail, fp) for fp in file_paths
        }
        try:
            async def _do():
                # Telethon's album path (_send_album) doesn't forward an
                # `attributes=` argument, so bare paths would give every video
                # the same 1×1 placeholder. Pre-build each item as InputMedia
                # with explicit attributes — the only way to get correct
                # per-video geometry in a multi-item album.
                payload = [
                    await self._build_album_item(
                        fp, thumbs.get(fp),
                        batch_pos=i + 1, batch_total=len(file_paths),
                    )
                    for i, fp in enumerate(file_paths)
                ]
                await self._client.send_file(
                    peer, payload, caption=captions, supports_streaming=True,
                )
            return await self._send_with_retries(
                _do, what=f"album[{len(file_paths)}] {Path(file_paths[0]).name}…",
                payload_bytes=self._payload_bytes(file_paths),
            )
        finally:
            self._progress_done()
            for t in thumbs.values():
                if t:
                    try:
                        os.unlink(t)
                    except OSError:
                        pass

    async def _send_photo_album(
        self,
        peer: Any,
        file_paths: list[str],
        captions: list[str | None],
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

    async def _build_album_item(self, file_path: str, thumb: str | None = None,
                                *, batch_pos: int | None = None,
                                batch_total: int | None = None):
        """Upload one video and wrap it as an InputMediaUploadedDocument for an
        album send, injecting explicit display geometry when we have it.

        Only called for video albums (see send_album). Telethon's _send_album
        accepts pre-built InputMedia and preserves the baked-in attributes,
        which is how we get per-item dimensions the path-list album API can't
        express. If the probe failed for this file, we still upload it as a
        video document — just without the explicit attribute (status quo for
        that one file), never as a photo.

        `thumb` is a local JPEG poster path (or None); when present it is
        uploaded and baked in so the album item gets a real preview frame
        instead of a server-picked black/white fade-in frame."""
        video_attrs = await asyncio.to_thread(_video_attributes, file_path)
        # Strip any ".tgprep" marker from the album item's filename too (same
        # reason as the single path) — an explicit DocumentAttributeFilename
        # overrides the basename get_attributes would otherwise derive.
        display = media_prep.clean_upload_name(file_path)
        name_attr = ([tg_types.DocumentAttributeFilename(display)]
                     if display != Path(file_path).name else [])
        return await self._upload_document(
            file_path,
            attributes=(video_attrs or []) + name_attr,  # [] → no override
            thumb_path=thumb, supports_streaming=True, force_document=False,
            progress_cb=self._progress_cb(
                file_path, batch_pos=batch_pos, batch_total=batch_total),
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
