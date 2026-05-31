"""
core.orphaned
─────────────
Ingest "loose" files that belong to no platform. The user drops them into
top-level folders under output_dir whose NAME is the Telegram chat_id they
should be sent to:

    output_dir/<chat_id>/<subpath…>/<file>

The folder name IS the routing authority — no env var, no config lookup. Each
file becomes a normal pending item (source='orphaned') and flows through the
same dedup, batching, send, and delete paths as everything else.

GROUPING / CAPTION
  - file in a SUBFOLDER  → album per subfolder. group_key='<chat_id>/<sub>',
    no per-file caption; the dispatcher builds 'sub\\nfile1\\nfile2' at send.
  - file DIRECTLY in the chat_id folder → sent INDIVIDUALLY, one message per
    file with its own filename as caption. Forced by a unique batch key
    (group_key=NULL, caption=the filename) so claim_batch never groups two of
    them; the displayed caption is still the stem.

DISCRIMINATOR (the safety-critical bit)
  A top-level folder is a route dir iff its name is NOT a known platform AND
  is a syntactically valid chat_id (core.routing.is_chat_id). Anything else is
  skipped with a warning — we never guess, because a wrong guess uploads
  private content to the wrong place.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from .dedup import MEDIA_EXTENSIONS
from .ingest import register_file, IngestOutcome
from .routing import is_chat_id
from .store import ItemStore

log = logging.getLogger(__name__)

ORPHANED_SOURCE = "orphaned"
# Synthetic platform value for orphaned rows. Keeps them out of every
# platform/recorder album (source + platform are both in the claim key) and
# out of the archiver's per-platform reconcile loop.
ORPHANED_PLATFORM = "orphaned"


@dataclass
class OrphanedReport:
    """Per chat_id-folder result, str()-able into a log line."""
    chat_id:   str
    scanned:   int  = 0
    inserted:  int  = 0
    deduped:   int  = 0   # DEDUP_DROPPED + DEDUP_ADOPTED
    known:     int  = 0   # ALREADY_KNOWN
    unstable:  int  = 0
    failed:    int  = 0   # HASH_FAILED
    skipped_dir: bool = False   # set when the top-level name wasn't a chat_id
    errors:    list[str] = field(default_factory=list)

    def __str__(self) -> str:
        if self.skipped_dir:
            return f"[orphaned] {self.chat_id}: SKIPPED (not a platform or chat_id)"
        return (
            f"[orphaned] {self.chat_id}: scanned={self.scanned}, "
            f"+{self.inserted}, deduped={self.deduped}, known={self.known}, "
            f"unstable={self.unstable}, failed={self.failed}"
        )


def ingest_chat_id_dirs(
    store:           ItemStore,
    output_dir:      str | Path,
    *,
    known_platforms: list[str] | set[str],
    priority:        int = 100,
) -> list[OrphanedReport]:
    """Scan output_dir's top-level folders; ingest every chat_id-named one.
    Returns one report per top-level folder considered."""
    base = Path(output_dir)
    reports: list[OrphanedReport] = []
    if not base.exists():
        return reports

    known = {p.lower() for p in known_platforms}
    for entry in sorted(base.iterdir()):
        try:
            if not entry.is_dir():
                continue
        except OSError:
            continue
        name = entry.name
        if name.lower() in known:
            continue   # a platform dir — the archiver's reconcile pass owns it
        if not is_chat_id(name):
            log.warning(
                "orphaned: top-level dir %r is neither a known platform nor a "
                "valid chat_id — skipping (rename it to the destination chat_id "
                "to route it)", name,
            )
            reports.append(OrphanedReport(chat_id=name, skipped_dir=True))
            continue
        reports.append(ingest_folder(store, entry, chat_id=name, priority=priority))
    return reports


_OUTCOME_TALLY = {
    IngestOutcome.INSERTED:      "inserted",
    IngestOutcome.DEDUP_DROPPED: "deduped",
    IngestOutcome.DEDUP_ADOPTED: "deduped",
    IngestOutcome.ALREADY_KNOWN: "known",
    IngestOutcome.UNSTABLE:      "unstable",
    IngestOutcome.HASH_FAILED:   "failed",
}


def ingest_folder(
    store: ItemStore, folder: Path, *, chat_id: str, priority: int = 100,
) -> OrphanedReport:
    """Ingest every media file under `folder`, routed to `chat_id`. Two shapes:

      - file in a SUBFOLDER  → album per subfolder. group_key='<chat_id>/<sub>',
        no per-file caption (the dispatcher builds 'sub\\nfile1\\nfile2' at send).
      - file DIRECTLY in folder → sent INDIVIDUALLY, one message per file with
        its own filename as caption. We force that by giving each such file a
        unique batch key (group_key=NULL, caption=the filename), so claim_batch
        never groups two of them; the displayed caption is still the stem.

    Reusable for both the chat_id-folder sweep and `archiver ingest --path`,
    where `folder` is arbitrary and `chat_id` is supplied explicitly."""
    rep = OrphanedReport(chat_id=chat_id)
    for f in sorted(folder.rglob("*")):
        try:
            if not f.is_file():
                continue
        except OSError:
            continue
        if f.suffix.lower() not in MEDIA_EXTENSIONS:
            continue
        rep.scanned += 1

        rel = f.relative_to(folder)
        subpath = rel.parent.as_posix()
        subpath = "" if subpath == "." else subpath
        if subpath:
            group_key, caption = f"{chat_id}/{subpath}", None
        else:
            # Top-level loose file → its own message. NULL group_key means
            # claim_batch groups by caption instead; a per-file filename makes
            # that key unique, so each loose file sends alone. Display still
            # uses the stem (see drain.orphaned_caption).
            group_key, caption = None, f.name

        try:
            res = register_file(
                store, f,
                source    = ORPHANED_SOURCE,
                platform  = ORPHANED_PLATFORM,
                username  = chat_id,
                chat_id   = chat_id,
                group_key = group_key,
                caption   = caption,
                priority  = priority,
            )
        except Exception as e:               # pragma: no cover — defensive
            rep.errors.append(f"{f.name}: {e}")
            log.exception("orphaned: register_file raised on %s", f)
            continue

        setattr(rep, _OUTCOME_TALLY[res.outcome],
                getattr(rep, _OUTCOME_TALLY[res.outcome]) + 1)
    return rep


def subfolder_of(chat_id: str, group_key: str | None) -> str:
    """Display subfolder for an orphaned row: group_key with the leading
    '<chat_id>/' stripped. Empty when the file sat directly under the chat_id
    folder. Used by the dispatcher to build the album caption header."""
    if not group_key:
        return ""
    prefix = f"{chat_id}/"
    return group_key[len(prefix):] if group_key.startswith(prefix) else ""
