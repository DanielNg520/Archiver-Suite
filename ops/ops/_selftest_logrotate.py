"""
ops._selftest_logrotate
───────────────────────
Proves the copytruncate rotation contract:
  - under-threshold logs are untouched
  - an oversized log is gzipped to a timestamped generation and truncated
    IN PLACE (same inode — launchd's O_APPEND descriptor must stay valid)
  - generations beyond `keep` are pruned, oldest first
  - a non-.log file is never touched

Run:  python3 -m ops._selftest_logrotate
Style matches core's _selftest scripts: plain asserts, checkmark per
assertion, nonzero exit on first failure. Temp dir only.
"""

from __future__ import annotations

import gzip
import os
import tempfile
from pathlib import Path

from .logrotate import rotate_logs

_checks = 0


def ok(cond: bool, label: str) -> None:
    global _checks
    if not cond:
        raise AssertionError(f"✗ {label}")
    _checks += 1
    print(f"✓ {label}")


def main() -> int:
    print("ops logrotate selftest")
    with tempfile.TemporaryDirectory() as d:
        logs = Path(d)

        small = logs / "small.log"
        small.write_bytes(b"tiny\n")
        big = logs / "big.log"
        big.write_bytes(b"X" * 4096)
        other = logs / "session.db"          # non-.log must never be touched
        other.write_bytes(b"Y" * 4096)
        inode_before = big.stat().st_ino

        actions = rotate_logs(logs, max_bytes=1024, keep=2)
        ok(any("rotated big.log" in a for a in actions),
           "oversized log was rotated")
        ok(not any("small.log" in a for a in actions),
           "under-threshold log untouched")
        ok(small.read_bytes() == b"tiny\n", "small log contents intact")
        ok(other.stat().st_size == 4096, "non-.log file never touched")

        ok(big.stat().st_size == 0, "live log truncated to empty")
        ok(big.stat().st_ino == inode_before,
           "truncate kept the SAME inode (launchd's fd stays valid)")

        gens = sorted(logs.glob("big.log.*.gz"))
        ok(len(gens) == 1, "exactly one compressed generation created")
        with gzip.open(gens[0], "rb") as gz:
            ok(gz.read() == b"X" * 4096,
               "generation holds the original bytes, gzip-intact")

        # writer-keeps-appending semantics: an O_APPEND fd opened before the
        # rotation must land bytes at the new (empty) EOF afterwards
        fd = os.open(big, os.O_WRONLY | os.O_APPEND)
        try:
            big.write_bytes(b"Z" * 2048)     # refill over threshold
            rotate_logs(logs, max_bytes=1024, keep=2)
            os.write(fd, b"after-rotate\n")
        finally:
            os.close(fd)
        ok(big.read_bytes() == b"after-rotate\n",
           "pre-existing O_APPEND writer lands at the truncated file's EOF")

        # keep=2: a third rotation must prune the oldest generation
        big.write_bytes(big.read_bytes() + b"W" * 2048)
        rotate_logs(logs, max_bytes=1024, keep=2)
        gens = sorted(logs.glob("big.log.*.gz"))
        ok(len(gens) == 2, "old generations pruned down to keep=2")

    print(f"\nALL PASS ({_checks} checks)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
