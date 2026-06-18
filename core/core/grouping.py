"""
core.grouping
─────────────
Send-grouping identity for media_prep SPLIT PARTS.

When a file is too big for Telegram, media_prep splits it into <=1 GiB parts
(PrepResult.individual=True). We want all parts of ONE original to ship as a
single album (ordered, grouped) rather than as separate messages. The dispatcher
albums any rows that share COALESCE(group_key, caption, ''), so the producers
stamp every part of one original with the SAME synthetic group_key built here.

The key is namespaced with a `split:` sentinel so the dispatcher can recognise a
split album (to caption it with every part name, and to exempt it from the
min-batch gate — a split set is already a complete unit and must flush at once).

A part set larger than Telegram's 10-item media-group cap is claimed in
consecutive albums (claim_batch caps at ALBUM_MAX); ordering across those albums
is preserved because parts register, hence sort, in sequence.
"""

from __future__ import annotations

SPLIT_GROUP_PREFIX = "split:"


def split_group_key(platform: str, username: str, original_stem: str) -> str:
    """Synthetic album key shared by every part of one split original. Unique
    per (platform, username, original stem) — two distinct originals never merge
    unless they share all three, which would also make them indistinguishable on
    disk."""
    return f"{SPLIT_GROUP_PREFIX}{platform}:{username}:{original_stem}"


def is_split_group(group_key: str | None) -> bool:
    """True iff group_key was minted by split_group_key (a split-part album)."""
    return bool(group_key) and group_key.startswith(SPLIT_GROUP_PREFIX)
