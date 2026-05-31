"""
core.routing
────────────
Telegram chat_id grammar — the ONE place that decides "does this string name a
Telegram destination?". Shared by ingest (a top-level folder named like a
chat_id is a route dir) and the dispatcher (an item's chat_id column is a valid
send target). Keeping it here means the two sides can never disagree on what a
chat_id looks like.

Accepted forms:
  -100xxxxxxxxxx   supergroup/channel (the common case)
  -xxxxxxxxx       legacy group
  xxxxxxxxx        user/bot numeric id
  @name            public @username (>=5 chars, Telegram's minimum)

These cover every value _resolve_peer in the dispatcher router can turn into a
Telethon peer. A numeric id is matched by the signed-integer branch; an @handle
by the username branch.
"""

from __future__ import annotations

import re

# Signed integer (covers -100…, legacy -…, and positive ids) OR an @username.
CHAT_ID_RE = re.compile(r"^(?:-?\d+|@\w{5,})$")


def is_chat_id(name: str) -> bool:
    """True iff `name` is a syntactically valid Telegram chat_id / @handle.

    Deliberately strict: a top-level folder that is neither a known platform
    NOR a valid chat_id is skipped, never guessed at — misrouting would send
    private media to the wrong channel."""
    return bool(CHAT_ID_RE.match(name.strip()))
