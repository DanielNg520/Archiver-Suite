r"""
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

FORUM TOPICS — a route may target a specific forum topic by suffixing the
chat_id with `.t<topic_id>` (a forum's message_thread_id), e.g.

    -1001234567890.t42        →  chat -100…, topic 42
    @mychannel.t42            →  @mychannel, topic 42
    -1001234567890            →  chat -100…, General (no thread)

The `.` delimiter is chosen because it is the ONLY separator that is both
filesystem-safe everywhere (legal on exFAT/FAT/APFS/ext4) and grammar-disjoint:
a numeric id (`-?\d+`) and an @handle (`[A-Za-z0-9_]`) can never contain a dot,
so a dot in a route token is UNAMBIGUOUSLY the topic delimiter — never a
coincidence. It also needs no quoting in any shell, URL, or sync tool (unlike
`#`, which is a shell glob/comment and a URL fragment). The literal `t` marker
keeps `…​.t42` from reading as a malformed float and self-documents as "topic".

`is_chat_id` validates a BARE chat_id (the value stored in the items.chat_id
column, topic split off into items.topic_id). `parse_route` is the folder-name
discriminator that understands the optional topic suffix; the two never disagree
because parse_route reuses CHAT_ID_RE's grammar for its chat part.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Signed integer (covers -100…, legacy -…, and positive ids) OR an @username.
_CHAT_ID = r"(?:-?\d+|@\w{5,})"
CHAT_ID_RE = re.compile(rf"^{_CHAT_ID}$")
# Same chat grammar plus an optional `.t<digits>` forum-topic suffix.
ROUTE_RE = re.compile(rf"^(?P<chat>{_CHAT_ID})(?:\.t(?P<topic>\d+))?$")


def is_chat_id(name: str) -> bool:
    """True iff `name` is a syntactically valid BARE Telegram chat_id / @handle
    (no topic suffix). Used to validate the stored items.chat_id column.

    Deliberately strict: a top-level folder that is neither a known platform
    NOR a valid chat_id is skipped, never guessed at — misrouting would send
    private media to the wrong channel."""
    return bool(CHAT_ID_RE.match(name.strip()))


@dataclass(frozen=True)
class Route:
    """A parsed destination: a chat_id and an optional forum topic. topic_id is
    None for the chat's General topic (no message thread)."""
    chat_id:  str
    topic_id: int | None = None


def parse_route(name: str) -> Route | None:
    """Parse a route token `<chat_id>[.t<topic_id>]` into a Route, or None if it
    is not a valid destination. The folder-name discriminator for the orphaned
    ingester: a top-level folder routes iff this returns non-None.

    DASH-FREE NUMERIC FOLDERS — a leading `-` makes a folder hostile to shell
    tools (`rm -100…` reads the name as flags), so a route folder may be named
    with the BARE digits and we re-add the `-` here: `1001234567890` is
    normalized to `-1001234567890`. The canonical (`-`-prefixed) form is what
    lands in the items.chat_id column, so the dispatcher always routes on one
    consistent value. The explicit `-` form is still accepted unchanged.

    Consequence: an all-digits folder ALWAYS means the negative chat id `-<n>`;
    a positive user-id DM cannot be expressed dash-free — use the `@handle`. And
    because output_dir holds only platform folders and chat-route folders, any
    bare-numeric top-level folder is taken as a chat route. A precheck that the
    chat actually exists (a `just-in-case` net) belongs in the dispatcher, which
    owns the Telegram client — see the routing notes."""
    m = ROUTE_RE.match(name.strip())
    if not m:
        return None
    chat = m.group("chat")
    # Bare digits (no leading '-', not an @handle) → the negative chat id.
    if chat.isdigit():
        chat = f"-{chat}"
    topic = m.group("topic")
    return Route(chat_id=chat,
                 topic_id=int(topic) if topic is not None else None)
