"""
Self-test for dispatcher.config — optional burner account (Phase 1).

Exercises BurnerCreds.from_env in isolation (no PolicyStore / filesystem creds):
  - unset / empty → None (feature inert)
  - chat_ids present but no distinct login → None
  - login present but no chat_ids → None
  - configured → normalized chat_ids, api_id/api_hash inherit the primary
  - dash-free numeric + @handle route tokens normalize like parse_route

Run: PYTHONPATH=core:dispatcher python3 -m dispatcher._selftest_config
"""

from __future__ import annotations

import os
import sys

from dispatcher.config import BurnerCreds, TelegramCreds

_checks = 0


def ok(cond: bool, label: str) -> None:
    global _checks
    if not cond:
        raise AssertionError(f"✗ {label}")
    _checks += 1
    print(f"✓ {label}")


# Burner-related keys we set/clear per case so cases don't leak into each other.
_KEYS = (
    "BURNER_CHAT_IDS", "TELEGRAM_BURNER_SESSION", "TELEGRAM_BURNER_PHONE",
    "TELEGRAM_BURNER_API_ID", "TELEGRAM_BURNER_API_HASH",
)


def _env(**vals: str) -> None:
    for k in _KEYS:
        os.environ.pop(k, None)
    for k, v in vals.items():
        os.environ[k] = v


PRIMARY = TelegramCreds(
    api_id=111, api_hash="primhash", phone="+1000",
    session_name="/tmp/claude-primary-session",
)


def main() -> int:
    print("dispatcher.config burner self-test\n")

    # ── inert cases → None ────────────────────────────────────────────────
    _env()
    ok(BurnerCreds.from_env(PRIMARY) is None, "unset → None (feature off)")

    _env(BURNER_CHAT_IDS="-100123")
    ok(BurnerCreds.from_env(PRIMARY) is None,
       "chat_ids but no session/phone → None")

    _env(TELEGRAM_BURNER_PHONE="+1999")
    ok(BurnerCreds.from_env(PRIMARY) is None,
       "login but no chat_ids → None")

    # ── configured (session-based, inherits primary api creds) ────────────
    _env(BURNER_CHAT_IDS="-100123, 456, @somechannel",
         TELEGRAM_BURNER_SESSION="/tmp/claude-burner-session")
    b = BurnerCreds.from_env(PRIMARY)
    ok(b is not None, "chat_ids + session → configured")
    assert b is not None
    ok(b.api_id == 111 and b.api_hash == "primhash",
       "api_id/api_hash inherit the primary when unset")
    ok(b.phone == "+1000", "phone inherits the primary when unset")
    ok(b.session_name == "/tmp/claude-burner-session", "session honored")
    # 456 (dash-free numeric) → -456; @handle kept; -100123 kept.
    ok(b.chat_ids == frozenset({"-100123", "-456", "@somechannel"}),
       "chat_ids normalized via parse_route")
    ok(b.routes("-456") and not b.routes("-999"),
       "routes() matches the normalized set")

    # ── phone-only login + own api creds, default session name ────────────
    _env(BURNER_CHAT_IDS="-100777",
         TELEGRAM_BURNER_PHONE="+1999",
         TELEGRAM_BURNER_API_ID="222",
         TELEGRAM_BURNER_API_HASH="burnhash")
    b = BurnerCreds.from_env(PRIMARY)
    assert b is not None
    ok(b.api_id == 222 and b.api_hash == "burnhash", "own api creds honored")
    ok(b.phone == "+1999", "own phone honored")
    ok(b.session_name == PRIMARY.session_name + "-burner",
       "session defaults to <primary>-burner when only phone is set")

    print(f"\n{_checks} checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
