"""
log_setup  (VENDOR THIS FILE)
─────────────────────────────
Drop a copy of this file into each service package (dispatcher/,
recorder/, archiver/) and call setup_file_logging(name) early in that
package's cli.main(). It is intentionally standalone and dependency-free
so it can be copied verbatim — same "port, don't import" rule used for
policy_store across packages.

Why a RotatingFileHandler and not the launchd .out/.err logs:
  launchd's StandardOutPath/StandardErrorPath capture raw stdout/stderr
  and NEVER rotate — they grow unbounded. This handler gives each service
  a real, size-capped application log (50 MB x 5 = 250 MB ceiling) at a
  predictable path, while the launchd logs remain a thin crash-catcher
  for anything that dies before logging is configured.

Usage (in each service's cli.main, replacing logging.basicConfig):

    from .log_setup import setup_file_logging
    setup_file_logging("dispatcher", verbose=args.verbose)
"""

from __future__ import annotations

import logging
import logging.handlers
import sys
from pathlib import Path

LOG_DIR = Path.home() / ".local" / "log"
MAX_BYTES = 50 * 1024 * 1024   # 50 MB per file
BACKUP_COUNT = 5               # keep 5 rotated files → ~250 MB ceiling


def setup_file_logging(name: str, *, verbose: bool = False,
                       also_stderr: bool = True) -> None:
    """Configure root logging: a rotating file at ~/.local/log/<name>.log
    plus (optionally) stderr so launchd's .err log still sees lines.

    Idempotent-ish: clears existing root handlers first so a re-call
    (or a stray basicConfig elsewhere) doesn't double-log."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    level = logging.DEBUG if verbose else logging.INFO

    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.handlers.RotatingFileHandler(
        LOG_DIR / f"{name}.log",
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)
    root.addHandler(file_handler)

    if also_stderr:
        stream = logging.StreamHandler(sys.stderr)
        stream.setFormatter(fmt)
        stream.setLevel(level)
        root.addHandler(stream)
