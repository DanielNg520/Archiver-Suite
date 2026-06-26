"""
dispatcher.keepalive
─────────────────────
TCP keepalive for the Telegram (MTProto) connection.

THE FAILURE IT FIXES. A half-open TCP socket — the peer silently gone after a
VPN/Tailscale exit drop, a laptop sleep/wake, or a NAT timeout — stays
ESTABLISHED in the kernel forever. Any await on it (the sender's recv loop, the
response to a peer-resolve, a send) then hangs with NO error and NO log: the
drain wedges silently, no row fails, nothing reconnects. macOS's default
keepalive idle is 7200 s (2 h), so the kernel won't notice for hours.

THE FIX. Enable aggressive TCP keepalive on every socket Telethon opens, so the
kernel probes a silent peer and RESETS the connection in ~idle+intvl*cnt seconds
(~35 s by default). The reset surfaces as an ordinary ConnectionError, which the
dispatcher's reconnect path (send.py's _force_reconnect — the SOLE reconnect
authority, since Telethon's auto_reconnect is off) then self-heals: fast
detection + clean recovery, no race.

COVERAGE. Applies to EVERY connection the client makes: the main client, the
dispatcher's _force_reconnect-rebuilt connections, AND fast_upload's borrowed
senders (they build their connection from the client's connection class, so they
inherit it for free). The one case keepalive does NOT cover is a send already in
progress with bytes stuck unacked (governed by TCP retransmission, not
keepalive) — that stays covered by send.py's stall watchdog.

Tunables (env, safe defaults):
  TCP_KEEPALIVE_IDLE_S   (20)  idle seconds before the first probe
  TCP_KEEPALIVE_INTVL_S  (5)   seconds between probes
  TCP_KEEPALIVE_CNT      (3)   unanswered probes before the kernel drops it
"""

from __future__ import annotations

import logging
import socket

from telethon.network.connection.tcpfull import ConnectionTcpFull

from core import env

log = logging.getLogger(__name__)

IDLE_S  = env.opt_int("TCP_KEEPALIVE_IDLE_S", 20, min_value=1)
INTVL_S = env.opt_int("TCP_KEEPALIVE_INTVL_S", 5, min_value=1)
CNT     = env.opt_int("TCP_KEEPALIVE_CNT", 3, min_value=1)


def apply_keepalive(sock: socket.socket, *, idle: int = IDLE_S,
                    intvl: int = INTVL_S, cnt: int = CNT) -> None:
    """Enable TCP keepalive and tune its timers on a connected socket.

    Best-effort per option — platforms expose different names (Linux
    TCP_KEEPIDLE vs macOS TCP_KEEPALIVE) — and never raises: a missing knob just
    means that timer keeps its OS default."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    # idle-before-first-probe: TCP_KEEPIDLE on Linux, TCP_KEEPALIVE on macOS.
    for name in ("TCP_KEEPIDLE", "TCP_KEEPALIVE"):
        opt = getattr(socket, name, None)
        if opt is not None:
            sock.setsockopt(socket.IPPROTO_TCP, opt, idle)
            break
    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, intvl)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, cnt)


class KeepAliveConnectionTcpFull(ConnectionTcpFull):
    """Telethon's default TCP transport, with keepalive armed on every socket.

    Pass as ``connection=`` to TelegramClient. The override runs AFTER the base
    connect succeeds and is fully best-effort — a socket-option failure logs and
    leaves the (working) connection untouched, never blocking a connect."""

    async def _connect(self, timeout=None, ssl=None):
        await super()._connect(timeout=timeout, ssl=ssl)
        try:
            sock = self._writer.get_extra_info("socket")
            if sock is not None:
                apply_keepalive(sock)
                log.debug("keepalive: armed (idle=%ds intvl=%ds cnt=%d)",
                          IDLE_S, INTVL_S, CNT)
        except Exception as e:                       # never break a connection
            log.warning("keepalive: could not arm socket options: %s", e)
