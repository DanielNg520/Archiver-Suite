"""
core.dedup
──────────
Content-hash duplicate detection for one user's media directory.

Lives in `core` (not archiver) because loose/orphaned files — dropped into
chat_id-named folders that belong to no platform — are never visited by the
archiver's per-(platform, user) loop. Dedup is a property of the SHARED store,
so every producer's output runs through the same funnel.

ALGORITHM: three-stage funnel.
  Stage 1: group all media files by exact byte size. Discard singletons.
  Stage 2: SHA-256 the first 64 KB of each survivor. Group by partial
           hash; discard singletons.
  Stage 3: full SHA-256 of survivors. Files sharing a full hash are
           confirmed bit-identical duplicates.

Each stage is strictly more selective AND strictly more expensive than
the previous, so the funnel shape minimizes total I/O for the common
case (most files are unique). The full hash only runs on the small
set that survived both prefilters.

  Theoretical worst case: every file is a duplicate of every other.
  Then full hash runs on every file. This is fine — that's also the
  case where dedup work has maximum payoff.

WINNER SELECTION (per confirmed duplicate group):
  Score tuple (higher wins lexicographically):
    1. canonical filename format (YYYYMMDD_<id>[_<num>])  → strongest signal
    2. has a sidecar JSON                                 → richer metadata
    3. has a DB row (i.e. tracked by the system)
  Tiebreaker: earliest `downloaded_at` (ISO 8601 strings sort correctly
              lexicographically; None sinks to last).

  Rationale: filename and sidecar both point at "produced by our
  downloader, not a manual copy". The original copy (earliest
  downloaded_at) wins over a redownload.

DB RECONCILIATION (per group):
  Four cases for (winner_row, loser_row) presence in DB:
    (row, row)  → DELETE loser's row.
    (—,   row)  → ADOPT: UPDATE loser's row.file_path = winner's path,
                  preserving telegram_sent/sent_at/etc.
    (row, —  )  → No-op in DB; just unlink file.
    (—,   —  )  → No-op in DB; next reconcile picks up winner.

SIDECAR CLEANUP:
  When a file is deleted, its .json / .info.json / .name.json sidecars
  are deleted too. Otherwise reconcile would later resolve them as
  orphaned identities and create phantom DB rows.

IDEMPOTENCY:
  A second run finds nothing — duplicates are gone after the first run.
  Safe to call after every download, safe to call repeatedly by hand.

DISK-DELETE SAFETY:
  If unlink fails (permissions, EIO, etc.) the corresponding DB row is
  NOT touched. Keeps state coherent at the cost of leaving a duplicate
  in place — log loudly so the operator notices.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator

from .hashing import partial_hash, full_hash, PARTIAL_HASH_BYTES

if TYPE_CHECKING:
    from .store import ItemStore

log = logging.getLogger(__name__)

# Re-exported for existing importers; the one definition lives in core.files.
from .files import MEDIA_EXTENSIONS  # noqa: E402  (after logger on purpose)

# Canonical filename pattern, identical in shape to identity._FILENAME_RE.
# Duplicated rather than imported because the convention is a public
# property of the system, not an internal detail of identity.py.
_CANONICAL_RE = re.compile(
    r"^(?P<date>\d{8})_(?P<ident>.+?)(?:_(?P<num>\d+))?$"
)

# Hash window/chunk constants and the hash primitives now live in core.hashing
# (one definition, shared with ingest's content_hash stamping). This module
# owns only the FUNNEL that arranges those primitives into a cheap-first scan.


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class DupGroup:
    """A set of files confirmed bit-identical by full hash."""
    digest: str
    paths:  tuple[Path, ...]


@dataclass
class DedupReport:
    """
    Per-user result. Aggregated into bootstrap-style summary output.

    In dry-run mode, ALL counters represent PLANNED actions (what WOULD
    happen if dry_run=False). The on-disk state and DB state are
    unchanged. This is the standard dry-run pattern — the report
    predicts impact.
    """
    platform:         str
    username:         str
    scanned:          int       = 0
    size_groups:      int       = 0   # # groups with size collisions
    confirmed_groups: int       = 0   # # groups after full-hash confirmation
    kept:             int       = 0   # 1 per confirmed group
    deleted:          int       = 0   # files physically unlinked
    db_rows_removed:  int       = 0
    db_rows_adopted:  int       = 0
    sidecars_removed: int       = 0
    bytes_freed:      int       = 0
    errors:           list[str] = field(default_factory=list)
    dry_run:          bool      = True

    def __str__(self) -> str:
        mode = "DRY RUN" if self.dry_run else "LIVE"
        verb = "would_delete" if self.dry_run else "deleted"
        mb   = self.bytes_freed / (1024 * 1024)
        return (
            f"[{self.platform}] @{self.username} [{mode}]: "
            f"scanned={self.scanned}, dup_groups={self.confirmed_groups}, "
            f"{verb}={self.deleted} ({mb:.1f} MB), "
            f"db_removed={self.db_rows_removed}, db_adopted={self.db_rows_adopted}, "
            f"sidecars={self.sidecars_removed}, errors={len(self.errors)}"
        )


# ── Stage 1: enumerate + size group ───────────────────────────────────────────

def _iter_media_files(user_dir: Path) -> Iterator[Path]:
    """
    Recursive media enumeration. Mirrors reconcile's rglob walk and
    stability.py's hidden/incomplete filters at the cheap level (no
    stat-sleep-stat probe — dedup runs post-download, the dust has
    already settled).
    """
    for p in user_dir.rglob("*"):
        try:
            if not p.is_file():
                continue
        except OSError:
            continue
        if p.suffix.lower() not in MEDIA_EXTENSIONS:
            continue
        if p.name.startswith("."):
            continue
        # Mirror stability._INCOMPLETE_SUFFIXES — these would be unstable
        # downloads we should not touch.
        if p.suffix.lower() in {".part", ".tmp", ".crdownload",
                                 ".partial", ".ytdl"}:
            continue
        yield p


def _group_by_size(paths: Iterable[Path]) -> dict[int, list[Path]]:
    groups: dict[int, list[Path]] = defaultdict(list)
    for p in paths:
        try:
            groups[p.stat().st_size].append(p)
        except OSError as e:
            log.warning("dedup: stat failed on %s: %s", p, e)
    return {sz: ps for sz, ps in groups.items() if len(ps) > 1}


# ── Stage 2 & 3: hashing ──────────────────────────────────────────────────────
# Hash primitives are core.hashing.{partial_hash, full_hash}; this funnel just
# arranges them cheap-first.

def _funnel(paths: list[Path]) -> dict[str, list[Path]]:
    """
    Partial-then-full hash filter on one size-group. Returns
    {full_digest: [paths]} for groups with 2+ confirmed-identical files.
    """
    partial: dict[str, list[Path]] = defaultdict(list)
    for p in paths:
        digest = partial_hash(p, PARTIAL_HASH_BYTES)
        if digest is not None:
            partial[digest].append(p)

    full: dict[str, list[Path]] = defaultdict(list)
    for survivors in partial.values():
        if len(survivors) < 2:
            continue
        for p in survivors:
            digest = full_hash(p)
            if digest is not None:
                full[digest].append(p)

    return {d: ps for d, ps in full.items() if len(ps) > 1}


# ── Winner selection ──────────────────────────────────────────────────────────

def _is_canonical(path: Path) -> bool:
    return bool(_CANONICAL_RE.match(path.stem))


def _has_sidecar(path: Path) -> bool:
    """Mirrors identity._read_sidecar's three probed patterns."""
    return any(
        sc.exists() for sc in (
            path.with_suffix(".json"),
            path.with_suffix(".info.json"),
            path.parent / (path.name + ".json"),
        )
    )


def _pick_winner(
    paths:    list[Path],
    db_meta:  dict[Path, str | None],
) -> tuple[Path, list[Path]]:
    """
    Sort by (canonical desc, sidecar desc, has-db-row desc, downloaded_at asc).
    Earliest downloaded_at = "kept it longer" = more likely the original.

    Implementation note: we sort ASCENDING by a key in which the desc
    fields are negated. ISO 8601 strings sort lexicographically in time
    order, so we leave downloaded_at as-is. None timestamps sink to
    last via the '\uffff' sentinel — a missing timestamp shouldn't win
    a tiebreak over a known-early one.
    """
    def sort_key(p: Path) -> tuple:
        canon       = _is_canonical(p)
        sidecar     = _has_sidecar(p)
        ts          = db_meta.get(p)
        has_row     = ts is not None
        ts_for_sort = ts if ts is not None else "\uffff"
        # Negate the "higher is better" fields to put them first in asc sort.
        # str(p) is the FINAL, total-order tiebreak: the orphaned/loose case
        # routinely has TWO random-named copies with no sidecar, no row, and
        # no timestamp (every prior field ties). Without it the survivor would
        # depend on filesystem iteration order — non-deterministic across runs,
        # so a re-scan could keep a DIFFERENT file and re-create the duplicate
        # we just deleted. The absolute path is unique and stable.
        return (
            0 if canon else 1,
            0 if sidecar else 1,
            0 if has_row else 1,
            ts_for_sort,
            str(p),
        )

    ranked = sorted(paths, key=sort_key)
    return ranked[0], ranked[1:]


# ── DB reconciliation ─────────────────────────────────────────────────────────

def _fetch_db_meta(
    db:    "ItemStore",
    paths: list[Path],
) -> dict[Path, dict | None]:
    """
    One small query per path in this group. N is tiny (a duplicate
    group is typically 2-3 files); not worth batching.
    """
    out: dict[Path, dict | None] = {}
    for p in paths:
        row = db.conn.execute(
            "SELECT id, identifier, discovered_at AS downloaded_at, status "
            "FROM items WHERE file_path = ?",
            (str(p),),
        ).fetchone()
        out[p] = dict(row) if row else None
    return out


def _delete_sidecars(media_path: Path, *, dry_run: bool) -> int:
    """
    Delete .json / .info.json / .name.json sidecars accompanying
    media_path. Returns count of sidecars that existed (and would be
    deleted in live mode). Failures are logged but don't abort —
    a stranded sidecar is far less bad than a stranded media file.
    """
    count = 0
    for sc in (
        media_path.with_suffix(".json"),
        media_path.with_suffix(".info.json"),
        media_path.parent / (media_path.name + ".json"),
    ):
        if sc.exists():
            count += 1
            if not dry_run:
                try:
                    sc.unlink()
                except OSError as e:
                    log.warning("dedup: sidecar unlink failed %s: %s", sc, e)
    return count


# ── Main entry ────────────────────────────────────────────────────────────────

def dedup_user(
    platform_name: str,
    username:      str,
    user_dir:      Path,
    db:            "ItemStore",
    *,
    dry_run:       bool = True,
) -> DedupReport:
    """
    Holistic dedup for one user's directory.

    Args:
      platform_name: 'x', 'tiktok', 'instagram' — for reporting only.
      username:      ditto.
      user_dir:      typically {output_dir}/{platform}/{username}/.
      db:            ItemStore — used for both metadata lookup and
                     post-delete row maintenance.
      dry_run:       if True, plan but do not modify disk or DB.

    Returns a DedupReport you can str() into a log line.
    """
    report = DedupReport(platform=platform_name, username=username, dry_run=dry_run)

    if not user_dir.exists():
        return report

    all_paths = list(_iter_media_files(user_dir))
    report.scanned = len(all_paths)
    if len(all_paths) < 2:
        return report

    size_grouped = _group_by_size(all_paths)
    report.size_groups = len(size_grouped)
    if not size_grouped:
        return report

    # Run the partial+full funnel PER size-group. Cross-size partial
    # collisions are impossible by definition (different sizes can't
    # hash equal at any prefix length AND be confirmed-equal at full
    # length), so we avoid wasted full hashes from cross-size partial
    # collisions.
    confirmed: list[DupGroup] = []
    for candidates in size_grouped.values():
        full_groups = _funnel(candidates)
        for digest, paths in full_groups.items():
            confirmed.append(DupGroup(digest=digest, paths=tuple(paths)))

    report.confirmed_groups = len(confirmed)
    if not confirmed:
        return report

    for group in confirmed:
        db_rows = _fetch_db_meta(db, list(group.paths))
        meta_for_score = {
            p: (r["downloaded_at"] if r else None)
            for p, r in db_rows.items()
        }
        winner, losers = _pick_winner(list(group.paths), meta_for_score)
        report.kept += 1

        winner_row = db_rows[winner]
        log.info(
            "dedup [%s/%s] keep %s (%d duplicate(s), digest=%s)",
            platform_name, username, winner.name, len(losers), group.digest[:12],
        )

        for loser in losers:
            loser_row = db_rows[loser]

            # Measure before delete so we can report bytes_freed accurately.
            try:
                size = loser.stat().st_size
            except OSError:
                size = 0

            sidecar_count = _delete_sidecars(loser, dry_run=dry_run)

            if not dry_run:
                try:
                    loser.unlink()
                except OSError as e:
                    msg = f"unlink {loser}: {e}"
                    report.errors.append(msg)
                    log.error("dedup: %s", msg)
                    # Disk delete failed → leave DB row alone. Reporting
                    # bytes_freed and sidecars_removed would be misleading
                    # since the loser file is still on disk.
                    continue

            report.deleted          += 1
            report.bytes_freed      += size
            report.sidecars_removed += sidecar_count
            log.info("  → removed %s (%d bytes)", loser.name, size)

            # DB reconciliation — four cases. The ADOPT case is the
            # subtle one: when the winner has no DB row but the loser
            # does, we re-point the loser's row at the winner's path
            # to preserve delivery-status history.
            if winner_row is not None and loser_row is not None:
                if not dry_run:
                    db.conn.execute(
                        "DELETE FROM items WHERE id = ?", (loser_row["id"],),
                    )
                    db.conn.commit()
                report.db_rows_removed += 1

            elif winner_row is None and loser_row is not None:
                # ADOPT
                if not dry_run:
                    db.conn.execute(
                        "UPDATE items SET file_path = ? WHERE id = ?",
                        (str(winner), loser_row["id"]),
                    )
                    db.conn.commit()
                report.db_rows_adopted += 1
                # Subsequent losers in this group should treat the
                # winner as now-rowed.
                winner_row = loser_row

            # The other two cases (winner has row + loser doesn't, or
            # neither has a row) need no DB change. The next reconcile
            # pass picks up any orphan winner.

    return report
