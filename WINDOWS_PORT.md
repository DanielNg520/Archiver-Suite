# Windows Port Plan — Archiver Suite

> Execution plan for porting the suite (macOS/POSIX → Windows). Read
> `PROJECT_MAP.md` first. The suite is POSIX-bound in exactly four places; the
> DB spine, media pipeline, and Telethon path are already portable. Work is
> concentrated, not scattered.

## Strategy: one platform seam, three adapters

Do **not** scatter `if os.name == "nt"` checks. Add `core/core/platform/` with
three adapters, each with a POSIX and a Windows implementation. This contains
100% of the non-negotiable work in ~3 files and keeps `store`/`ingest`/`send`/
`media_prep` platform-blind.

| Adapter | Replaces | POSIX impl | Windows impl |
|---|---|---|---|
| `filelock` | `fcntl.flock` | `fcntl` | `msvcrt.locking` |
| `procgroup` | `os.killpg`/`setsid` | process group | Job Object / `taskkill /T /F` |
| `paths` + `service` | `~/.config`, launchd | XDG + launchd | `%APPDATA%` + Windows Service/Task Scheduler |

Add `platformdirs` and (Windows-only) `pywin32` to deps.

---

## Phase 0 — Pre-port hygiene (do first, low risk)

- [ ] **Delete checked-in `build/` trees**: `core/build/`, `dispatcher/build/`,
  `recorder/build/`. They are stale source duplicates that will double every
  grep and let you patch a file while missing its `build/` twin. Add
  `**/build/` to `.gitignore`.
- [ ] Confirm `ffmpeg`, `ffprobe`, `yt-dlp`, `gallery-dl` install on PATH on
  Windows (all already discovered via PATH — no hardcoded prefixes to fix).

---

## Phase 1 — Path centralization (must; blocks everything else)

All config paths funnel through `Path.expanduser()` on hardcoded
`~/.config/archiver-suite/...` and `~/.recorder/pid`. Wrong on Windows.

- [ ] Add `core/core/platform/paths.py` with `config_root()` using
  `platformdirs.user_config_dir("archiver-suite")` (→ `%APPDATA%\archiver-suite`
  on Windows, `~/.config/archiver-suite` on POSIX). Add `state_dir()`,
  `locks_dir()`.
- [ ] Route every literal through it. Sites to change:
  - `core/core/paths.py:31,43,48,54` (locks, progress.json, loop.json, recorder pid)
  - `core/core/policy_store.py:42` (config.toml)
  - `core/core/schema.py:38,42` (`DEFAULT_DB_PATH` / `ARCHIVER_DB`)
  - `core/core/migrate.py:209,210` (CLI defaults)
  - `core/core/sanitize.py:111,141` (banned-words file)
- [ ] Keep `ARCHIVER_DB` env override working (it already wins in `schema.py:42`).

**Verify:** `core.paths` resolves under `%APPDATA%` on Windows; `_selftest` DB
opens; existing macOS paths unchanged.

---

## Phase 2 — Instance lock (nonnegotiable; the load-bearing one)

`core/core/instance_lock.py`, `dispatcher/dispatcher/instance_lock.py`,
`recorder/recorder/lock.py` all rest on `fcntl.flock` with the design promise
that the **kernel auto-releases the lock on SIGKILL/power loss** — no stale-lock
dance. `fcntl` does not exist on Windows.

- [ ] Add `core/core/platform/filelock.py`:
  - POSIX: current `fcntl.flock(LOCK_EX|LOCK_NB)` / `LOCK_SH` probe / `LOCK_UN`.
  - Windows: `msvcrt.locking(fd, LK_NBLCK, 1)` — same kernel-auto-release
    semantics on process exit/crash.
- [ ] Rewrite `InstanceLock` to call the adapter. Preserve the shared-lock
  probe (`instance_lock.py:75`) used to detect a live holder.
- [ ] **Do NOT fall back to PID files.** That reintroduces the exact stale-lock
  race this module was built to avoid.

**Verify:** second instance fails to acquire while first holds; killing the
holder (Task Manager) frees the lock immediately with no manual cleanup.

---

## Phase 3 — Recorder process-group kill (nonnegotiable; data-loss guard)

`recorder/recorder/capture.py` uses `start_new_session=True` +
`os.killpg`/`os.getpgid` to group-kill yt-dlp **and its orphaned child ffmpeg**.
This is the fix for the orphaned-ffmpeg data-loss bug — not incidental. The
existing `hasattr(os, "killpg")` bare-pid fallback (`capture.py:188`) is **not**
sufficient on Windows; it orphans ffmpeg and loses footage.

- [ ] Add `core/core/platform/procgroup.py`:
  - POSIX: current `start_new_session` spawn + `killpg(getpgid(pid), sig)`.
  - Windows: spawn with `CREATE_NEW_PROCESS_GROUP`; on stop send
    `CTRL_BREAK_EVENT`, then escalate to a Job Object kill or
    `taskkill /PID <pid> /T /F` (`/T` kills the whole child tree — this is the
    invariant: parent death must guarantee child ffmpeg death).
- [ ] Route `capture.py` spawn + `_signal_group` through the adapter.

**Verify:** kill the recorder mid-capture; confirm **no** stray `ffmpeg.exe`
survives in Task Manager and the segment isn't left half-written.

---

## Phase 4 — Signals & shutdown (nonnegotiable)

`SIGTERM` doesn't exist on Windows; `signal.signal(SIGTERM, …)` raises.

- [ ] `dispatcher/dispatcher/cli.py:99` and `recorder/recorder/cli.py:136,137,216,217`:
  guard `SIGTERM` registration to POSIX; on Windows register `SIGINT` +
  `CTRL_CLOSE_EVENT` (or the service stop handler in Phase 5) → same
  `stop_event` → drain exits cleanly.
- [ ] `recorder/cli.py:243` `os.kill(pid, SIGTERM)` (stop command): on Windows
  send `CTRL_BREAK_EVENT`/`taskkill` via the procgroup adapter.

---

## Phase 5 — Delete daemonize + service management

- [ ] **Delete** `_daemonize()` (`recorder/recorder/cli.py:145–160`,
  double-fork `os.fork`/`os.setsid`). It won't import on Windows and the code
  comment already says the service manager is the real backgrounding
  mechanism. Remove the `--daemon` flag or make it a no-op that points at the
  service.
- [ ] Replace launchd (`ops/launchd/*.plist`, `launchctl` wiring in `ops`) with
  a Windows adapter. Pick one:
  - **Task Scheduler** (simplest): run-at-startup + restart-on-failure. Good
    enough given `ops` just shells to the service manager.
  - **Windows Service** via `pywin32` `win32serviceutil` (cleaner, survives
    logoff).
  - **NSSM** (wrap the existing CLIs as services with zero code).
- [ ] Update `ops/health.py` and `ops/cli.py` to query the chosen mechanism
  instead of `launchctl list`.

---

## Already portable — do NOT touch

- **SQLite WAL + one-file coordination** — identical on Windows. This is *why*
  the port is tractable.
- **Atomic writes** (`os.replace` + tmp + `fsync` in `heartbeat.py`,
  `policy_store.py`, `sorter.py`) — `os.replace` is atomic on Windows too.
  **Caveat:** Windows cannot replace/delete a file with an open handle — audit
  that nothing holds the target open across the replace.
- **Telethon / FastTelethon / hachoir / media_prep / ffmpeg subprocesses** —
  cross-platform.

---

## Test order on Windows

1. `core` selftests (DB, paths, policy_store, media_prep) — needs ffmpeg on PATH.
2. `filelock` two-instance race + kill-frees-lock.
3. Recorder capture + kill → no orphan ffmpeg.
4. Full seam test: `tests/test_seams.py` (set `PYTHONPATH=core;archiver;recorder;dispatcher;ops`
   — note Windows uses `;` not `:` as the PYTHONPATH separator).
5. Service install + reboot survival.

## Definition of done

- No `fcntl`, `os.fork`, `os.setsid`, `os.killpg`, or bare `SIGTERM` reachable
  on the Windows code path.
- All config under `%APPDATA%\archiver-suite`.
- Kill-tests leave no orphan ffmpeg and no stale locks.
- Workers auto-start on boot and restart on crash via the chosen service manager.
