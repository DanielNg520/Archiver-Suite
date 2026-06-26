"""
dispatcher.fast_upload
──────────────────────
Multi-connection file upload for Telethon — the "FastTelethon" pattern.

Telethon's stock ``upload_file`` streams 512 KiB parts SEQUENTIALLY over a
single MTProto sender, so throughput is capped at ``part_size / round-trip``.
The official clients open several connections to the file's data-centre and
push parts concurrently. This module does the same: it borrows N exported
senders and drives them from a bounded producer/consumer pipeline, then returns
the ``InputFileBig`` handle that ``send_file`` accepts in place of a path.

DESIGN
  • Bounded ``asyncio.Queue`` of ``(index, bytes)`` → constant memory regardless
    of file size (a 4 GB upload never buffers more than a few parts).
  • One producer reads parts off the event loop (``asyncio.to_thread``) so disk
    I/O never stalls the FloodWait/stall-watchdog timers.
  • N consumers each own ONE borrowed sender; senders are acquired/released
    through an ``AsyncExitStack`` so they are always returned, even on cancel.
  • Progress is reported through the same ``(sent, total)`` callback contract
    Telethon uses, so the dispatcher's heartbeat works unchanged.

SAFETY (this is the universal send choke point — it must never lose a file)
  This is purely an optimization of the UPLOAD step. Every non-trivial failure
  — a rejected part, a Telethon-internal that moved on an upgrade, or simply a
  file too small to benefit — falls back to ``client.upload_file``. A partially
  uploaded ``file_id`` left on Telegram's side is harmless; the fallback starts
  a fresh upload. Callers therefore get stock behaviour whenever the fast path
  cannot run, never an error from this layer.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any, Awaitable, Callable

from telethon import helpers
from telethon.network import MTProtoSender
from telethon.tl import functions, types

log = logging.getLogger(__name__)

PART_SIZE = 512 * 1024                 # Telegram's hard maximum per part
_BIG_THRESHOLD = 10 * 1024 * 1024      # >10 MiB ⇒ SaveBigFilePart (no md5)
MAX_CONNECTIONS = 8                    # diminishing returns / politeness past this

ProgressCb = Callable[[int, int], Any]


def _internals_present(client: Any) -> bool:
    """The fast path reaches into Telethon internals. If a library upgrade
    moved any of them, degrade to the public serial uploader rather than crash
    the send."""
    session = getattr(client, "session", None)
    return (
        hasattr(client, "_get_dc")
        and hasattr(client, "_connection")
        and getattr(session, "dc_id", None) is not None
        and getattr(session, "auth_key", None) is not None
    )


async def _connect_sender(client: Any) -> MTProtoSender:
    """A fresh MTProtoSender connected to the HOME data-centre, reusing the
    session's existing auth key.

    Uploads always target the home DC, and Telegram REJECTS exporting auth for
    the DC you're already connected to — so unlike Telethon's
    _borrow_exported_sender (which exports auth and fails here), we hand the
    sender the session auth_key directly and skip the export entirely. Telegram
    permits many concurrent connections sharing one auth key; that is exactly
    what makes the parallel fan-out possible. Caller disconnects it."""
    dc = await client._get_dc(client.session.dc_id)
    # auto_reconnect=False: a borrowed sender must NOT run Telethon's background
    # reconnect. On a drop we WANT the part-send to raise so the parallel path
    # fails fast and upload_file falls back to serial — and it removes the
    # reconnect-vs-disconnect race (see send.py __aenter__) for these senders too.
    sender = MTProtoSender(client.session.auth_key, loggers=client._log,
                           auto_reconnect=False)
    await sender.connect(client._connection(
        dc.ip_address, dc.port, dc.id,
        loggers=client._log, proxy=client._proxy,
        local_addr=getattr(client, "_local_addr", None),
    ))
    return sender


async def upload_file(
    client: Any,
    path: str | Path,
    *,
    connections: int = 4,
    part_size: int = PART_SIZE,
    file_name: str | None = None,
    progress_callback: ProgressCb | None = None,
) -> Any:
    """Upload ``path`` and return an ``InputFile``/``InputFileBig`` handle.

    Uses the parallel fast path for big files when ``connections > 1`` and the
    Telethon internals are present; otherwise delegates to the stock serial
    uploader (which also owns the small-file md5 path). Never raises for a
    fast-path-specific reason — it falls back instead."""
    path = Path(path)
    size = path.stat().st_size
    serial = lambda: client.upload_file(                       # noqa: E731
        str(path), file_name=file_name or path.name,
        progress_callback=progress_callback)

    if size <= _BIG_THRESHOLD or connections <= 1 or not _internals_present(client):
        return await serial()

    try:
        return await _parallel_upload(
            client, path, size, connections, part_size,
            file_name or path.name, progress_callback)
    except asyncio.CancelledError:
        raise
    except Exception as e:                                     # pragma: no cover
        log.warning("fast_upload: parallel path failed (%s) — serial fallback "
                    "for %s", e, path.name)
        return await serial()


async def _parallel_upload(
    client: Any, path: Path, size: int, connections: int, part_size: int,
    file_name: str, progress_callback: ProgressCb | None,
) -> types.InputFileBig:
    file_id = helpers.generate_random_long()
    part_count = (size + part_size - 1) // part_size
    workers = max(1, min(connections, MAX_CONNECTIONS, part_count))

    # maxsize gives backpressure: the producer blocks once a couple of parts per
    # worker are queued, so memory stays bounded no matter the file size.
    queue: asyncio.Queue = asyncio.Queue(maxsize=workers * 2)
    sent_bytes = 0  # single event loop ⇒ no lock needed for this counter

    async def produce() -> None:
        with open(path, "rb") as fh:
            for index in range(part_count):
                chunk = await asyncio.to_thread(fh.read, part_size)
                if not chunk:
                    break
                await queue.put((index, chunk))
        for _ in range(workers):
            await queue.put(None)              # one poison pill per consumer

    async def consume(sender: Any) -> None:
        nonlocal sent_bytes
        while True:
            item = await queue.get()
            if item is None:
                return
            index, chunk = item
            ok = await sender.send(functions.upload.SaveBigFilePartRequest(
                file_id, index, part_count, chunk))
            if not ok:
                raise IOError(f"part {index}/{part_count} rejected by Telegram")
            sent_bytes += len(chunk)
            if progress_callback is not None:
                res = progress_callback(sent_bytes, size)
                if inspect.isawaitable(res):
                    await res

    async with AsyncExitStack() as stack:
        senders = []
        for _ in range(workers):
            sender = await _connect_sender(client)
            stack.push_async_callback(sender.disconnect)
            senders.append(sender)

        tasks = [asyncio.create_task(produce())]
        tasks += [asyncio.create_task(consume(s)) for s in senders]
        try:
            await asyncio.gather(*tasks)
        except BaseException:
            for t in tasks:
                t.cancel()
            # Drain cancellations so no task is left pending when senders return.
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    log.debug("fast_upload: %s uploaded in %d parts over %d connections",
              file_name, part_count, workers)
    return types.InputFileBig(file_id, part_count, file_name)
