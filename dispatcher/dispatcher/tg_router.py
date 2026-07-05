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

FORUM TOPICS — each layer has an optional sibling TELEGRAM_TOPIC_<…> var naming
a forum topic (message_thread_id) within that chat. The chat AND its topic are
ALWAYS resolved from the SAME layer (see _destination_from_env): you can never
end up posting to a chat from layer 3 inside a topic from layer 4. Orphaned rows
carry an explicit chat_id+topic_id pair (parsed together from the `.t<id>`
folder name), which overrides the env chain entirely — same atomicity.

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

from core import parse_route

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


def peer_chat_id(peer: Any) -> str | None:
    """Inverse of _resolve_peer: a Telethon peer (or raw @username string) → the
    canonical chat_id string that produced it, so a caller holding only the peer
    can compare it against parse_route-normalized chat_ids (e.g. the burner set).
    Returns None for a peer shape we don't recognize — the caller treats that as
    'not a burner chat' and stays on the primary."""
    if isinstance(peer, str):
        return peer.strip()
    if isinstance(peer, PeerChannel):
        return f"-100{peer.channel_id}"
    if isinstance(peer, PeerChat):
        return f"-{peer.chat_id}"
    if isinstance(peer, PeerUser):
        return str(peer.user_id)
    return None


def _user_key(platform: str, username: str) -> str:
    return f"TELEGRAM_CHAT_ID_{platform.upper()}_{username.upper()}"


def _platform_key(platform: str) -> str:
    return f"TELEGRAM_CHAT_ID_{platform.upper()}"


def _topic_for_chat_key(chat_key: str) -> str:
    """The sibling TELEGRAM_TOPIC_<…> var for a TELEGRAM_CHAT_ID_<…> var, so a
    layer's chat and topic are always named in lockstep."""
    return chat_key.replace("TELEGRAM_CHAT_ID", "TELEGRAM_TOPIC", 1)


def _topic_env(chat_key: str) -> int | None:
    """Read+parse the forum topic id siblng to `chat_key`. A non-integer value
    is a config typo: warn loudly and fall back to General (None) rather than
    crash the daemon or silently misroute to a numeric-looking thread."""
    raw = os.environ.get(_topic_for_chat_key(chat_key), "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        log.warning("router: %s=%r is not an integer topic id — ignoring "
                    "(posting to General)", _topic_for_chat_key(chat_key), raw)
        return None


def _is_tiktok_live(platform: str, source: str | None) -> bool:
    return platform.lower() == "tiktok" and (source or "").lower() == "recorder"


class RouteError(ValueError):
    """The item carries a chat_id that isn't a valid Telegram destination.
    Raised so the drain loop can fail the batch cleanly instead of throwing
    deep inside the send."""


@dataclass(frozen=True)
class Destination:
    """A resolved send target: a chat and (optionally) a forum topic within it.
    Resolved as ONE unit so chat and topic can never come from different env
    layers. topic_id is None for the chat's General topic."""
    chat_id:  str
    topic_id: int | None = None

    @property
    def peer(self) -> Any:
        return _resolve_peer(self.chat_id)


@dataclass(frozen=True)
class TelegramRouter:
    """Immutable resolver. Built once at dispatcher startup."""
    default_chat_id: str

    # ── Item-aware entry point (explicit chat_id wins) ────────────────────
    def destination_for_item(self, item) -> Destination:
        """Resolve the full destination (chat + topic) for one item. An explicit
        chat_id on the row (orphaned files, whose folder name IS the destination)
        overrides the env resolution entirely; its topic_id travels with it as a
        pair. Normalized through parse_route — the SAME canonical normalizer the
        ingester uses — so a dash-free numeric id stored on the row (a legacy row,
        or one enqueued via `archiver ingest --chat 100…`) is re-signed to its
        `-100…` form here and resolves as the channel it is, never a PeerUser.
        A fat-fingered id fails fast and loud here, not mid-send."""
        if item.chat_id:
            route = parse_route(item.chat_id.strip())
            if route is None:
                raise RouteError(
                    f"item id={item.id}: chat_id {item.chat_id!r} is not a "
                    f"valid Telegram destination"
                )
            return Destination(route.chat_id, item.topic_id)
        return self._destination_from_env(
            item.platform, item.username, source=item.source)

    def chat_id_for_item(self, item) -> str:
        """Bare chat_id for one item (back-compat / tests). See
        destination_for_item for the chat+topic pair the sender uses."""
        return self.destination_for_item(item).chat_id

    def peer_for_item(self, item):
        return self.destination_for_item(item).peer

    def _destination_from_env(
        self,
        platform: str,
        username: str,
        *,
        source: str | None = None,
    ) -> Destination:
        """Walk the precedence chain; the FIRST chat var that is set decides the
        destination, and the topic is read from that SAME layer's sibling var."""
        keys: list[str] = []
        if _is_tiktok_live(platform, source):
            keys += [_user_key("tiktok_live", username),
                     _platform_key("tiktok_live")]
        keys += [_user_key(platform, username), _platform_key(platform)]
        for k in keys:
            v = os.environ.get(k, "").strip()
            if v:
                return Destination(v, _topic_env(k))
        # Global default layer: TELEGRAM_CHAT_ID / TELEGRAM_TOPIC.
        return Destination(self.default_chat_id, _topic_env("TELEGRAM_CHAT_ID"))
