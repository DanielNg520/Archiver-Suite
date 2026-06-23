"""
core.heartbeat
──────────────
The cross-process status-file primitive. Several workers publish a tiny JSON
"I'm alive and here's what I'm doing" file that another process reads for a
status line — the dispatcher's upload progress, the archiver loop's phase. They
all want the SAME three guarantees, which used to be copy-pasted into each
writer AND each reader (dispatcher.progress, archiver.loop_state, and ops' own
copies):

  1. ATOMIC write   — tmp + os.replace, so a reader sees the old file or the new
                      one, never a half-written one.
  2. LIVENESS gate  — a read returns None if the writer pid is gone, so a
                      crashed worker can never leave a lying status behind.
  3. STALENESS gate — optionally, a read returns None if the heartbeat is older
                      than a deadline (a writer that's wedged but not dead).

Centralizing them means a fix to the liveness/atomicity logic lands everywhere
at once instead of in one of five places. Each caller still owns its own
payload shape (and any domain validation) via the `validate` predicate.

CONTRACT: never raises for an I/O problem — a status file must never break the
work it describes. A write failure is swallowed; a read failure returns None.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Callable


def pid_alive(pid: int) -> bool:
    """Is `pid` a live process? The suite's one liveness primitive (used by
    read_live and by the recorder's pid-file checks). signal 0 is an existence
    probe: ProcessLookupError ⇒ gone (dead); PermissionError ⇒ the process
    EXISTS but is owned by another user (alive — matters on a multi-user host,
    never fires on the suite's single-user setup); any other OSError ⇒ treat as
    dead. A non-int pid is dead."""
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, ValueError, TypeError):
        return False
    return True


def write_atomic(path: Path, state: dict) -> None:
    """Atomically publish `state` as JSON at `path` (creating parent dirs).

    The caller owns the payload — including the `pid` and `updated_at` fields
    that read_live() needs for its liveness/staleness gates. Swallows OSError:
    a status write must never break the operation it reports on."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(state), encoding="utf-8")
        os.replace(tmp, path)   # atomic: readers see old or new, never torn
    except OSError:
        pass


def read_live(
    path: Path,
    *,
    stale_after_s: float | None = None,
    validate: Callable[[dict], bool] | None = None,
) -> dict | None:
    """Read a heartbeat, or None if it is absent, malformed, failing `validate`,
    stale (older than `stale_after_s`), or written by a process that is gone.

    `validate(data)` is the caller's domain check (e.g. "has sent+total",
    "phase in {...}"). Liveness uses the payload's `pid`; staleness uses its
    `updated_at` (epoch seconds). A missing pid/updated_at fails the respective
    gate — i.e. an unparseable heartbeat is treated as dead, never trusted."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    if validate is not None and not validate(data):
        return None
    if stale_after_s is not None:
        if time.time() - data.get("updated_at", 0) > stale_after_s:
            return None
    try:
        pid = int(data["pid"])
    except (KeyError, ValueError, TypeError):
        return None       # no usable pid → can't prove liveness → treat as dead
    return data if pid_alive(pid) else None


def clear(path: Path) -> None:
    """Remove a heartbeat (call on clean exit so a stopped worker doesn't read
    back as forever-busy — belt-and-suspenders with the liveness gate)."""
    try:
        path.unlink()
    except OSError:
        pass
