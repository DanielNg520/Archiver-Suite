"""
dispatcher.tg_router
────────────────────
Ported verbatim from archiver.tg_router. Resolves the Telegram destination
(chat peer) for a given (platform, user).

Resolution chain (most specific wins):
  1. TELEGRAM_CHAT_ID_TIKTOK_LIVE_<USER>  (TikTok recorder/live override)
  2. TELEGRAM_CHAT_ID_TIKTOK_LIVE         (all TikTok recorder/live uploads)
  3. TELEGRAM_CHAT_ID_<PLATFORM>_<USER>   (per-user override)
  4. TELEGRAM_CHAT_ID_<PLATFORM>          (per-platform override)
  5. TELEGRAM_CHAT_ID                     (global default; required)

These env vars are read from dispatcher's own .env at
~/.config/dispatcher/.env. The dispatcher process loads its own
environment — no collision with archiver's .env even though the
variable names happen to match.
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

    Constructing PeerChannel/PeerChat/PeerUser directly bypasses Telethon's
    entity-cache lookup — sends work on the first try without a warm-up
    dialogs() call.
    """
    s = str(chat_id).strip()
    if s.startswith("@"):
        return s
    try:
        n = int(s)
    except ValueError:
        return s
    if s.startswith("-100"):
        return PeerChannel(int(s[4:]))
    if n < 0:
        return PeerChat(-n)
    return PeerUser(n)


def _user_key(platform: str, username: str) -> str:
    return f"TELEGRAM_CHAT_ID_{platform.upper()}_{username.upper()}"


def _platform_key(platform: str) -> str:
    return f"TELEGRAM_CHAT_ID_{platform.upper()}"


def _is_tiktok_live(platform: str, source: str | None) -> bool:
    return platform.lower() == "tiktok" and (source or "").lower() == "recorder"


@dataclass(frozen=True)
class TelegramRouter:
    """Immutable resolver. Built once at dispatcher startup."""
    default_chat_id: str

    def chat_id_for(
        self,
        platform: str,
        username: str,
        *,
        source: str | None = None,
    ) -> str:
        if _is_tiktok_live(platform, source):
            v = os.environ.get(_user_key("tiktok_live", username), "").strip()
            if v:
                return v
            v = os.environ.get(_platform_key("tiktok_live"), "").strip()
            if v:
                return v
        v = os.environ.get(_user_key(platform, username), "").strip()
        if v:
            return v
        v = os.environ.get(_platform_key(platform), "").strip()
        if v:
            return v
        return self.default_chat_id

    def peer_for(self, platform: str, username: str, *, source: str | None = None):
        return _resolve_peer(
            self.chat_id_for(platform, username, source=source)
        )

    def explain(
        self,
        platform: str,
        username: str,
        *,
        source: str | None = None,
    ) -> str:
        if _is_tiktok_live(platform, source):
            uk = _user_key("tiktok_live", username)
            if os.environ.get(uk, "").strip():
                return f"{os.environ[uk]} (via {uk})"
            pk = _platform_key("tiktok_live")
            if os.environ.get(pk, "").strip():
                return f"{os.environ[pk]} (via {pk})"
        uk = _user_key(platform, username)
        if os.environ.get(uk, "").strip():
            return f"{os.environ[uk]} (via {uk})"
        pk = _platform_key(platform)
        if os.environ.get(pk, "").strip():
            return f"{os.environ[pk]} (via {pk})"
        return f"{self.default_chat_id} (global TELEGRAM_CHAT_ID)"
