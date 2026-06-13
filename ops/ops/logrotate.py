"""
ops.logrotate
─────────────
Copytruncate rotation for the launchd-captured worker logs in ~/.local/log.

WHY NOT newsyslog/rename rotation: launchd holds an O_APPEND descriptor to
StandardOutPath/StandardErrorPath for as long as the worker runs. Renaming
the file would leave launchd writing into the renamed inode forever (the
fresh file stays empty), and launchd cannot be signalled to reopen. Copying
the bytes out and truncating the SAME inode in place keeps the writer's
descriptor valid — with O_APPEND every subsequent write lands at the new EOF.

Trade-off: lines written between the copy and the truncate are lost. For
worker logs rotated at a quiet hour this is acceptable; correctness of the
pipeline never depends on log contents.

WHY AT ALL: the 2026-06-12 overnight-stall incident was undiagnosable from
logs — the live files held only post-boot lines. Rotated, compressed
generations give worker history that survives reboots and truncation.
"""

from __future__ import annotations

import gzip
import shutil
from datetime import datetime
from pathlib import Path

DEFAULT_LOG_DIR   = Path("~/.local/log").expanduser()
DEFAULT_MAX_BYTES = 1 * 1024 * 1024   # rotate once a live log exceeds 1 MiB
DEFAULT_KEEP      = 7                 # compressed generations per log


def rotate_logs(
    log_dir:   Path = DEFAULT_LOG_DIR,
    *,
    max_bytes: int = DEFAULT_MAX_BYTES,
    keep:      int = DEFAULT_KEEP,
) -> list[str]:
    """Rotate every oversized *.log in log_dir; return one line per action.

    Idempotent and crash-safe: a rotation is copy → fsync-free gzip → truncate,
    so an interruption can at worst leave one extra .gz generation (pruned on
    the next run) or an un-truncated live log (rotated again next run).
    """
    actions: list[str] = []
    for live in sorted(log_dir.glob("*.log")):
        try:
            size = live.stat().st_size
        except OSError:
            continue
        if size < max_bytes:
            continue

        # Microseconds keep same-second rotations (tests, manual re-runs) from
        # colliding into one generation; still sorts lexicographically by age.
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S.%f")
        dest = live.with_name(f"{live.name}.{stamp}.gz")
        try:
            with live.open("rb") as src, gzip.open(dest, "wb") as gz:
                shutil.copyfileobj(src, gz)
            with live.open("r+b") as f:
                f.truncate(0)
        except OSError as e:
            actions.append(f"ERROR rotating {live.name}: {e}")
            continue
        actions.append(f"rotated {live.name} ({size} bytes → {dest.name})")

        # Prune oldest generations beyond `keep`. The timestamp suffix sorts
        # lexicographically == chronologically.
        gens = sorted(log_dir.glob(f"{live.name}.*.gz"))
        for old in gens[:-keep] if keep > 0 else gens:
            try:
                old.unlink()
                actions.append(f"pruned {old.name}")
            except OSError:
                pass
    return actions
