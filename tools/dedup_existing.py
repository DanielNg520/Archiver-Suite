"""
tools.dedup_existing
────────────────────
One-time (re-runnable) maintenance: collapse duplicate posts that were already
ENQUEUED with distinct identifiers before identity.resolve learned to extract a
stable per-asset post id. Those rows have different file_path, different
content_hash (re-encode), and different identifiers, so NONE of the live dedup
layers catch them — they upload the same post twice.

This detects them purely from the DATABASE (no folder-vs-folder compare, per the
operator's request) using a per-asset FINGERPRINT parsed from the filename:

  instagram : the media PK            → ('instagram', <pk>)
  tiktok    : video id + asset kind   → ('tiktok', <id>, 'v')            [videos]
              keeps carousel images distinct → ('tiktok', <id>, 'p', <n>) [photos]

Within each fingerprint group:
  • if a copy was already really sent → suppress EVERY pending/failed copy.
  • else keep the lowest-id pending copy → suppress the rest.

"Suppress" mirrors the dispatcher's own dedup: the row is marked sent-by-twin
(status='sent', last_error='deduped …', no tg_message_id) so it never uploads,
and its redundant on-disk file is deleted through the DeletionGuard (so a
safebraked scope is still shielded).

SAFETY / concurrency: the suppression UPDATE is guarded on status IN
('pending','failed'), so a row the dispatcher claims mid-run (→ 'sending') is
skipped, never overwritten. Files are deleted ONLY for rows this run actually
suppressed (now un-claimable), so the dispatcher can't be sending them. Safe to
run with the dispatcher live, though stopping it makes the pass fully clean.

Usage (from repo root, with the suite path):
    PYTHONPATH="core" python3 -m tools.dedup_existing            # dry-run
    PYTHONPATH="core" python3 -m tools.dedup_existing --apply    # mutate + delete
    PYTHONPATH="core" python3 -m tools.dedup_existing --apply --keep-files
"""

from __future__ import annotations

import argparse
import os
from collections import defaultdict
from pathlib import Path

from core import ItemStore, PolicyStore, DeletionGuard
from core.store import now_iso

VIDEO_EXT = {".mp4", ".mov", ".webm", ".mkv", ".ts", ".m4v"}


def fingerprint(platform: str, file_path: str):
    """Per-asset post fingerprint, or None if the filename has no parseable
    post id (then we leave the row alone — content_hash dedup still applies)."""
    stem, ext = os.path.splitext(os.path.basename(file_path))
    ext = ext.lower()
    parts = stem.split("_")
    longids = [p for p in parts if p.isdigit() and len(p) >= 16]
    if not longids:
        return None
    pid = longids[0]
    if platform == "instagram":
        # The media PK is per-asset (carousel images each have their own),
        # so the PK alone is the correct one-row-per-asset key.
        return ("instagram", pid)
    if platform == "tiktok":
        if ext in VIDEO_EXT:
            # A video is a single asset: {id}.mp4 (yt-dlp) and {id}_0.mp4
            # (gallery-dl) are the SAME video → one key, no _num.
            return ("tiktok", pid, "v")
        # Photo carousel: keep the trailing numeric asset index distinct so
        # {id}_1.jpg and {id}_2.jpg are NOT collapsed.
        tail = parts[-1] if (parts[-1].isdigit() and len(parts[-1]) < 10) else "0"
        return ("tiktok", pid, "p", tail)
    return None


def _real_sent(r) -> bool:
    return r["status"] == "sent" and not str(r["last_error"] or "").startswith("deduped")


def main() -> int:
    ap = argparse.ArgumentParser(description="Collapse already-enqueued duplicate posts.")
    ap.add_argument("--apply", action="store_true",
                    help="Actually suppress duplicates (default: dry-run).")
    ap.add_argument("--keep-files", action="store_true",
                    help="Suppress the row but do NOT delete its on-disk file.")
    ap.add_argument("--db", default=None, help="Override suite DB path.")
    args = ap.parse_args()

    db = ItemStore.open(args.db)
    guard = DeletionGuard(PolicyStore())

    rows = db.conn.execute(
        "SELECT id, platform, username, status, file_path, content_hash, last_error "
        "FROM items WHERE platform IN ('instagram','tiktok')"
    ).fetchall()

    groups: dict[tuple, list] = defaultdict(list)
    for r in rows:
        fp = fingerprint(r["platform"], r["file_path"])
        if fp is not None:
            groups[fp].append(r)

    # Decide which rows to suppress and onto which keeper.
    plan: list[tuple] = []   # (row, keeper_id, why)
    for fp, rs in groups.items():
        sent = [r for r in rs if _real_sent(r)]
        pend = [r for r in rs if r["status"] in ("pending", "failed")]
        if not pend:
            continue
        if sent:
            keeper = sent[0]["id"]
            for r in pend:
                plan.append((r, keeper, "already-sent"))
        elif len(pend) > 1:
            ordered = sorted(pend, key=lambda r: r["id"])
            keeper = ordered[0]["id"]
            for r in ordered[1:]:
                plan.append((r, keeper, "intra-queue"))

    by_plat = defaultdict(lambda: [0, 0])
    for r, _k, why in plan:
        by_plat[r["platform"]][0 if why == "already-sent" else 1] += 1

    print(f"{'APPLY' if args.apply else 'DRY-RUN'}: "
          f"{len(plan)} duplicate row(s) to suppress across "
          f"{len(groups)} fingerprint group(s)")
    for plat, (a, b) in sorted(by_plat.items()):
        print(f"  [{plat}] {a} already-sent twin, {b} intra-queue extra")
    if not args.apply:
        print("\nSample (first 12):")
        for r, k, why in plan[:12]:
            print(f"  id={r['id']:>6} {r['platform']}/@{r['username']} {why} "
                  f"→ keep id={k}  {os.path.basename(r['file_path'])}")
        print("\nRe-run with --apply to suppress + delete redundant files "
              "(--keep-files to keep files).")
        db.close()
        return 0

    suppressed = deleted = skipped = 0
    for r, keeper, _why in plan:
        # Guarded: only collapse a row that is STILL pending/failed (a row the
        # dispatcher claimed mid-run is now 'sending' → skipped, never clobbered).
        cur = db.conn.execute(
            "UPDATE items SET status='sent', sent_at=?, claimed_at=NULL, "
            "tg_message_id=NULL, last_error=? "
            "WHERE id=? AND status IN ('pending','failed')",
            (now_iso(), f"deduped (maintenance): same post already queued/sent as id={keeper}",
             r["id"]),
        )
        db.conn.commit()
        if cur.rowcount != 1:
            skipped += 1
            continue
        suppressed += 1
        if not args.keep_files:
            try:
                if guard.delete(r["platform"], r["username"], r["file_path"],
                                reason="dedup-existing-maintenance"):
                    deleted += 1
            except Exception as e:
                print(f"  warn: could not delete {r['file_path']}: {e}")

    print(f"\nDone: suppressed {suppressed}, files deleted {deleted}, "
          f"skipped {skipped} (claimed mid-run / already terminal).")
    db.close()
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
