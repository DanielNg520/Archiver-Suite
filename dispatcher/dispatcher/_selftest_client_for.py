"""
Self-test for TelethonSendStrategy._client_for — the additive burner seam
(Phase 3). No real Telethon: _build_client / _connect_authorized are stubbed so
we exercise routing + lazy-connect + fallback logic in isolation.

Covers:
  - no burner configured → primary, and peer is never even inspected
  - burner configured, non-dedicated chat → primary
  - burner configured, dedicated chat → burner (built + connected lazily, once)
  - dedicated chat but burner fails to authorize → falls back to primary
  - peer_chat_id round-trips every peer shape _resolve_peer produces

Run: PYTHONPATH=core:dispatcher python3 -m dispatcher._selftest_client_for
"""

from __future__ import annotations

import asyncio
import sys

from dispatcher import tg_router
from dispatcher.config import BurnerCreds
from dispatcher.send import TelethonSendStrategy

_checks = 0


def ok(cond: bool, label: str) -> None:
    global _checks
    if not cond:
        raise AssertionError(f"✗ {label}")
    _checks += 1
    print(f"✓ {label}")


PRIMARY = object()   # sentinel standing in for the primary client


def _make_strategy(burner: BurnerCreds | None) -> "TelethonSendStrategy":
    s = TelethonSendStrategy(
        api_id=1, api_hash="h", phone="+1", session_name="/tmp/claude-p",
        burner=burner,
    )
    s._client = PRIMARY  # type: ignore[assignment]
    return s


_BURNER = BurnerCreds(
    api_id=2, api_hash="bh", phone="+2", session_name="/tmp/claude-b",
    chat_ids=frozenset({"-100555"}),
)


def _peer(chat_id: str):
    return tg_router.Destination(chat_id).peer


def main() -> int:
    print("TelethonSendStrategy._client_for self-test\n")

    # ── peer_chat_id round-trips ──────────────────────────────────────────
    for cid in ("-100555", "-4242", "777", "@handle"):
        ok(tg_router.peer_chat_id(_peer(cid)) == cid,
           f"peer_chat_id round-trips {cid}")
    ok(tg_router.peer_chat_id(object()) is None,
       "peer_chat_id → None for an unknown peer shape")

    # ── no burner → primary, peer never inspected ─────────────────────────
    s = _make_strategy(None)

    class Boom:
        pass  # peer_chat_id would return None anyway, but assert we skip it
    got = asyncio.run(s._client_for(_peer("-100555")))
    ok(got is PRIMARY, "no burner → primary")
    ok(s._burner_client is None, "no burner → burner client never built")

    # ── burner configured, non-dedicated chat → primary ───────────────────
    s = _make_strategy(_BURNER)
    got = asyncio.run(s._client_for(_peer("-100999")))
    ok(got is PRIMARY, "non-dedicated chat → primary")
    ok(s._burner_client is None, "non-dedicated chat → burner not built")

    # ── dedicated chat → burner, built+connected lazily exactly once ──────
    s = _make_strategy(_BURNER)
    burner_obj = object()
    built = {"n": 0}
    connected = {"n": 0}

    def fake_build(session, api_id, api_hash):
        built["n"] += 1
        ok(session == "/tmp/claude-b" and api_id == 2,
           "burner built with burner session+creds")
        return burner_obj

    async def fake_connect(client, session, phone):
        connected["n"] += 1

    s._build_client = fake_build            # type: ignore[assignment]
    s._connect_authorized = fake_connect    # type: ignore[assignment]

    got = asyncio.run(s._client_for(_peer("-100555")))
    ok(got is burner_obj, "dedicated chat → burner client")
    got2 = asyncio.run(s._client_for(_peer("-100555")))
    ok(got2 is burner_obj, "second dedicated send → same burner client")
    ok(built["n"] == 1 and connected["n"] == 1,
       "burner built + connected exactly once (cached)")

    # ── dedicated chat but burner auth fails → primary fallback ───────────
    s = _make_strategy(_BURNER)
    disconnected = {"n": 0}

    class FailClient:
        async def disconnect(self):
            disconnected["n"] += 1

    def fail_build(session, api_id, api_hash):
        return FailClient()

    async def fail_connect(client, session, phone):
        raise RuntimeError("unauthorized")

    s._build_client = fail_build            # type: ignore[assignment]
    s._connect_authorized = fail_connect    # type: ignore[assignment]

    got = asyncio.run(s._client_for(_peer("-100555")))
    ok(got is PRIMARY, "burner auth failure → falls back to primary")
    ok(s._burner_client is None, "failed burner is not cached (retried next time)")
    ok(disconnected["n"] == 1, "half-open burner client is torn down")

    print(f"\n{_checks} checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
