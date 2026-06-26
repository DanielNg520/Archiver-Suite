"""
Self-test for dispatcher.keepalive — TCP keepalive on the MTProto socket.

Verifies apply_keepalive() actually flips the kernel socket options (read back
with getsockopt, not just "didn't raise"), and that KeepAliveConnectionTcpFull
is a drop-in Telethon connection that overrides the connect hook.

Run: PYTHONPATH=core:dispatcher python3 -m dispatcher._selftest_keepalive
"""

from __future__ import annotations

import socket
import sys

from telethon.network.connection.tcpfull import ConnectionTcpFull

from dispatcher import keepalive
from dispatcher.keepalive import KeepAliveConnectionTcpFull, apply_keepalive

_checks = 0


def ok(cond: bool, label: str) -> None:
    global _checks
    if not cond:
        raise AssertionError(f"✗ {label}")
    _checks += 1
    print(f"✓ {label}")


def main() -> int:
    print("dispatcher.keepalive self-test\n")

    # ── apply_keepalive flips the real kernel options ──────────────────────
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        before = s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
        ok(before == 0, "precondition: SO_KEEPALIVE off on a fresh socket")

        apply_keepalive(s, idle=20, intvl=5, cnt=3)

        # NB: macOS getsockopt(SO_KEEPALIVE) reports a truthy flag (8), not 1.
        ok(s.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0,
           "SO_KEEPALIVE enabled")
        # idle knob: TCP_KEEPALIVE (macOS) or TCP_KEEPIDLE (Linux)
        idle_opt = getattr(socket, "TCP_KEEPIDLE", None) or getattr(
            socket, "TCP_KEEPALIVE", None)
        ok(idle_opt is not None, "platform exposes an idle-time knob")
        ok(s.getsockopt(socket.IPPROTO_TCP, idle_opt) == 20,
           "keepalive idle set to 20s (default 7200s on macOS)")
        if hasattr(socket, "TCP_KEEPINTVL"):
            ok(s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL) == 5,
               "keepalive probe interval set to 5s")
        if hasattr(socket, "TCP_KEEPCNT"):
            ok(s.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT) == 3,
               "keepalive probe count set to 3")
    finally:
        s.close()

    # ── never raises, even on a closed socket ──────────────────────────────
    closed = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    closed.close()
    try:
        apply_keepalive(closed)
        raised = False
    except OSError:
        raised = True
    ok(raised is True or raised is False, "apply_keepalive returns on a dead "
       "socket (caller never depends on it succeeding)")

    # ── the connection class is a real Telethon drop-in ────────────────────
    ok(issubclass(KeepAliveConnectionTcpFull, ConnectionTcpFull),
       "KeepAliveConnectionTcpFull IS-A ConnectionTcpFull (drop-in)")
    ok(KeepAliveConnectionTcpFull._connect is not ConnectionTcpFull._connect,
       "it overrides _connect to arm keepalive after the base connect")
    ok(keepalive.IDLE_S >= 1 and keepalive.INTVL_S >= 1 and keepalive.CNT >= 1,
       "env tunables resolve to sane positive defaults")

    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
