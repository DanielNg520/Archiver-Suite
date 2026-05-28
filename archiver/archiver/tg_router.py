"""
archiver.tg_router
──────────────────
Resolve the Telegram destination (chat peer) for a given (platform, user).

Why this is a separate module:
  Yesterday's uploader carried a single `chat_id` and used it for every
  send. The new requirement — per-platform channels — would otherwise
  scatter `if platform == "x":` branches through the low-level send code.
  Encapsulating the routing decision in one resolver keeps `send_file`
  ignorant of platforms; it just gets handed a peer.

Resolution chain (most specific wins):
  1. Per-user override   — env: TELEGRAM_CHAT_ID_<PLATFORM>_<USER>
  2. Per-platform override — env: TELEGRAM_CHAT_ID_<PLATFORM>
  3. Global default       — env: TELEGRAM_CHAT_ID (always required)

Why per-user override exists even though it's overkill for most setups:
  Some users archive a single "personal favorites" account separately
  from the rest. Costs nothing to support and matches the DeletePolicy
  shape, so the user experience stays consistent across both knobs.

Peer types we produce:
  - PeerChannel for `-100<id>` (supergroups / channels)
  - PeerChat    for `-<id>`    (legacy small groups)
  - PeerUser    for `<id>`     (DMs)
  - raw string  for `@username` (Telethon resolves on-the-fly)
This mirrors the original telegram._resolve_chat_id logic; we just call
into it from one place now.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

from telethon.tl.types import PeerChannel, PeerChat, PeerUser

log = logging.getLogger(__name__)


def _resolve_peer(chat_id: str) -> Any:
    """
    String chat ID → Telethon peer object (or raw string for @usernames).

    Telethon's entity resolution requires the chat to be in the session's
    entity cache, which on a fresh session it won't be. Using PeerChannel/
    PeerChat/PeerUser directly bypasses that lookup — sends work on the
    first try without a warm-up dialogs call.
    """
    s = str(chat_id).strip()
    if s.startswith("@"):
        return s
    try:
        n = int(s)
    except ValueError:
        return s  # let Telethon try to resolve it as-is
    if s.startswith("-100"):
        return PeerChannel(int(s[4:]))
    if n < 0:
        return PeerChat(-n)
    return PeerUser(n)


def _user_key(platform: str, username: str) -> str:
    return f"TELEGRAM_CHAT_ID_{platform.upper()}_{username.upper()}"


def _platform_key(platform: str) -> str:
    return f"TELEGRAM_CHAT_ID_{platform.upper()}"


@dataclass(frozen=True)
class TelegramRouter:
    """
    Immutable resolver. Built once at TelegramUploader construction.

    `default_chat_id` is the global TELEGRAM_CHAT_ID — guaranteed
    non-empty by Config validation (always required).
    """
    default_chat_id: str

    def chat_id_for(self, platform: str, username: str) -> str:
        """Return the resolved chat ID string (still env-var-style)."""
        v = os.environ.get(_user_key(platform, username), "").strip()
        if v:
            return v
        v = os.environ.get(_platform_key(platform), "").strip()
        if v:
            return v
        return self.default_chat_id

    def peer_for(self, platform: str, username: str):
        """Return the Telethon peer object ready for send_file."""
        return _resolve_peer(self.chat_id_for(platform, username))

    def explain(self, platform: str, username: str) -> str:
        uk = _user_key(platform, username)
        if os.environ.get(uk, "").strip():
            return f"{os.environ[uk]} (via {uk})"
        pk = _platform_key(platform)
        if os.environ.get(pk, "").strip():
            return f"{os.environ[pk]} (via {pk})"
        return f"{self.default_chat_id} (global TELEGRAM_CHAT_ID)"


def validate_overrides(
    known_users: dict[str, tuple[str, ...]],
) -> list[str]:
    """
    Mirror of policies.validate_overrides — catches typo'd env var
    names that would silently fall through to the global chat.
    Returns warnings; does not raise.
    """
    valid_keys: set[str] = set()
    valid_platform_keys: set[str] = set()
    for platform, users in known_users.items():
        valid_platform_keys.add(_platform_key(platform))
        for u in users:
            valid_keys.add(_user_key(platform, u))

    warnings: list[str] = []
    for env_key in os.environ:
        if not env_key.startswith("TELEGRAM_CHAT_ID_"):
            continue
        if env_key in valid_platform_keys or env_key in valid_keys:
            continue
        warnings.append(
            f"tg_router: env var {env_key!r} doesn't match any "
            "configured (platform, user). Will be ignored. "
            "Check spelling / case."
        )
    return warnings
