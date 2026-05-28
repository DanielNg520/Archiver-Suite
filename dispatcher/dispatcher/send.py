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

from telethon import TelegramClient
from telethon.errors import FloodWaitError

log = logging.getLogger(__name__)


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

        attempts = 0
        last_error: str | None = None
        while attempts < self._max_retries:
            try:
                await self._client.send_file(
                    peer, file_path,
                    caption=caption,
                    supports_streaming=True,
                )
                return SendResult(ok=True)

            except FloodWaitError as e:
                if e.seconds > self._max_flood_wait_s:
                    log.error(
                        "telethon: FloodWait %ds > cap %ds — surfacing to dispatcher",
                        e.seconds, self._max_flood_wait_s,
                    )
                    return SendResult(
                        ok=False, flood_wait_s=int(e.seconds),
                    )
                # Sub-cap waits: handle inline within this attempt.
                wait_s = int(e.seconds) + 1
                log.warning("telethon: FloodWait %ds — sleeping", wait_s)
                await asyncio.sleep(wait_s)
                continue   # do NOT count as an attempt

            except (ConnectionError, OSError) as e:
                attempts += 1
                last_error = f"{type(e).__name__}: {e}"
                delay = self._retry_base_delay * (2 ** (attempts - 1))
                log.warning(
                    "telethon: network err attempt %d/%d: %s — retry in %.1fs",
                    attempts, self._max_retries, e, delay,
                )
                if attempts < self._max_retries:
                    await asyncio.sleep(delay)

            except Exception as e:
                attempts += 1
                last_error = f"{type(e).__name__}: {e}"
                delay = self._retry_base_delay * (2 ** (attempts - 1))
                log.warning(
                    "telethon: send err attempt %d/%d: %s: %s — retry in %.1fs",
                    attempts, self._max_retries,
                    type(e).__name__, e, delay,
                )
                if attempts < self._max_retries:
                    await asyncio.sleep(delay)

        return SendResult(
            ok=False,
            error=last_error or "send failed (no exception captured)",
        )
