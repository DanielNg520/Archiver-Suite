"""
core.models
───────────
The single typed representation of an item and its delivery lifecycle.

ONE SOURCE OF TRUTH:
  Before this package the delivery state of a file was written in two
  databases — archive.db.media.telegram_sent (NULL/0/1/2) and
  dispatcher.db.upload_queue.status — and a polling bridge copied one into
  the other. That mirror is gone. An item's status lives in exactly one
  row in exactly one table, and every process reads/writes that row.

STATE MACHINE (status column):

    pending ──claim──▶ sending ──ok────▶ sent
       ▲                  │
       │                  ├──retry left──▶ pending   (mark_failed, attempts<max)
       │                  ├──no retry────▶ failed    (mark_failed, attempts>=max)
       │                  └──floodwait───▶ pending   (requeue, no attempt counted)
       │                  └──watchdog────▶ pending   (crashed mid-send)
       └──reset───────────(failed|sent)

  'pending'  producer wrote it; claimable by the dispatcher.
  'sending'  dispatcher claimed it; a send is in flight.
  'sent'     Telegram confirmed delivery (terminal until reset).
  'failed'   retries exhausted (terminal until reset).

  There is no 'queued' state. Under two databases 'queued' meant "handed
  off to the other DB, outcome unknown" — pure handoff bookkeeping. With
  one table, writing the row IS the handoff, so 'pending' covers it.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from enum import Enum


class Status(str, Enum):
    """str-Enum so values compare/serialize as plain SQLite text."""
    PENDING = "pending"
    SENDING = "sending"
    SENT    = "sent"
    FAILED  = "failed"


# Terminal states are not claimable and are only left via an explicit reset.
TERMINAL = frozenset({Status.SENT, Status.FAILED})


@dataclass(frozen=True)
class Item:
    """
    Immutable snapshot of one items-table row. Returned by the store so
    callers can't mutate a live cursor row. Mirrors the column order of
    core.schema.ITEMS_DDL.
    """
    id:              int
    source:          str            # 'archiver' | 'recorder'
    platform:        str
    username:        str
    identifier:      str            # platform-side id; 'manual_*'/'recorder_*' synthesized
    file_path:       str
    upload_date:     str | None     # YYYYMMDD post date — drives the download floor
    file_size_bytes: int | None
    title:           str | None
    discovered_at:   str            # when the row was first written
    status:          str
    priority:        int
    caption:         str | None     # producer-set; dispatcher falls back to a formatter
    attempts:        int
    claimed_at:      str | None
    sent_at:         str | None
    last_error:      str | None
    tg_message_id:   int | None
    # Redesign columns (schema v1). Defaulted so callers that build an Item
    # without them (and pre-backfill rows) stay valid.
    content_hash:    str | None = None   # full SHA-256; global dedup key
    chat_id:         str | None = None   # explicit Telegram dest (chat_id folders)
    group_key:       str | None = None   # explicit album batch identity
    # Forum-topic destination (schema v4). Twin of chat_id: set only when the
    # destination is explicit (orphaned `.t<id>` folders); NULL → General topic.
    topic_id:        int | None = None

    @classmethod
    def from_row(cls, r: sqlite3.Row) -> "Item":
        return cls(**{k: r[k] for k in r.keys()})
