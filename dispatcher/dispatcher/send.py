"""
dispatcher.send
───────────────
The send Strategy. SendStrategy ABC defines the contract; TelethonSend-
Strategy implements it using Telethon.

WHY STRATEGY:
  Today: Telethon (MTProto user-account uploads, 2GB limit, native albums).
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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telethon import TelegramClient, utils as tg_utils
from telethon.errors import FloodWaitError
from telethon.tl import types as tg_types

from core.files import media_bucket

from .media_meta import probe_video

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
    """
    ok:            bool
    error:         str | None = None
    flood_wait_s:  int | None = None


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
    ):
        self._api_id           = api_id
        self._api_hash         = api_hash
        self._phone            = phone
        self._session_name     = session_name
        self._max_retries      = max_retries
        self._retry_base_delay = retry_base_delay
        self._max_flood_wait_s = max_flood_wait_s
        self._client: TelegramClient | None = None

    async def __aenter__(self) -> "TelethonSendStrategy":
        self._client = TelegramClient(
            self._session_name, self._api_id, self._api_hash,
        )
        await self._client.start(phone=self._phone)
        log.info("telethon: connected (session=%s)", self._session_name)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._client is not None:
            await self._client.disconnect()
            log.info("telethon: disconnected")

    async def send(
        self,
        *,
        peer: Any,
        file_path: str,
        caption: str | None,
    ) -> SendResult:
        """
        Single-file send with FloodWait + exponential-backoff retry.

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

        attributes = _video_attributes(file_path)

        async def _do():
            await self._client.send_file(
                peer, file_path, caption=caption, supports_streaming=True,
                attributes=attributes,
            )
        return await self._send_with_retries(_do, what=Path(file_path).name)

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

        async def _do():
            if is_video_album:
                # Telethon's album path (_send_album) doesn't forward an
                # `attributes=` argument, so bare paths would give every video
                # the same 1×1 placeholder. Pre-build each item as InputMedia
                # with explicit attributes — the only way to get correct
                # per-video geometry in a multi-item album.
                payload = [await self._build_album_item(fp) for fp in file_paths]
            else:
                payload = file_paths
            await self._client.send_file(
                peer, payload, caption=captions, supports_streaming=True,
            )
        return await self._send_with_retries(
            _do, what=f"album[{len(file_paths)}] {Path(file_paths[0]).name}…",
        )

    async def _build_album_item(self, file_path: str):
        """Upload one video and wrap it as an InputMediaUploadedDocument for an
        album send, injecting explicit display geometry when we have it.

        Only called for video albums (see send_album). Telethon's _send_album
        accepts pre-built InputMedia and preserves the baked-in attributes,
        which is how we get per-item dimensions the path-list album API can't
        express. If the probe failed for this file, we still upload it as a
        video document — just without the explicit attribute (status quo for
        that one file), never as a photo."""
        assert self._client is not None
        handle = await self._client.upload_file(file_path)
        attrs, mime = tg_utils.get_attributes(
            file_path,
            attributes=_video_attributes(file_path),  # None → no override
            supports_streaming=True,
        )
        return tg_types.InputMediaUploadedDocument(
            file=handle, mime_type=mime, attributes=attrs,
        )

    async def _send_with_retries(self, send_fn, *, what: str) -> SendResult:
        """Shared FloodWait + exponential-backoff envelope for both single
        and album sends. `send_fn` is an async no-arg callable performing
        the actual Telethon send_file; the only thing that differs between
        single and album is that call, so the retry/flood logic lives here
        once rather than being duplicated (and able to drift)."""
        attempts = 0
        last_error: str | None = None
        while attempts < self._max_retries:
            try:
                await send_fn()
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
