# Single-Source Migration & Install

This redesign collapses the two databases (`archive.db` + `dispatcher.db`)
into **one** SQLite file with **one** `items` table, owned by a shared
`core` library that all four processes import. The `reconcile_dispatch_
outcomes` bridge, the `telegram_sent` mirror column, and the `seen_by_
archiver` flag are gone — there is nothing left to reconcile because there
is no longer a second copy of delivery state.

```
~/.config/archiver-suite/suite.db        ← the ONE database (items table)
```

`items.status` is an explicit text state machine:

```
pending ──claim──▶ sending ──ok──▶ sent
                      └──fail (attempts<max)──▶ pending
                      └──fail (attempts≥max)──▶ failed
```

---

## 1. What `core` is, and why install is different now

`core` is a local, **unpublished** package (`archiver-suite-core`). It is
not on PyPI, so the four apps can't resolve it the normal way. With pipx,
every app lives in its own isolated venv, so `core` must be installed
*into each app's venv*. The mechanism for that is `pipx inject`.

Installing `core` **editable** (`--editable`) means every app imports it
from the same `./core` source tree — so when you edit `core`, all four
apps see the change immediately, with no reinstall. This is the correct
shape for a shared contract: one definition, one version, zero drift.

---

## 2. Install (from the repo root)

Install order **no longer matters** — `core` creates the schema
idempotently the first time any process connects (`CREATE TABLE IF NOT
EXISTS`). The old "install the dispatcher first because it owns the
schema" rule is obsolete.

```
pipx install ./dispatcher --python 3.13
pipx install ./archiver   --python 3.13
pipx install ./recorder   --python 3.13
pipx install ./ops        --python 3.13
```

Then inject `core` (editable) into each app's venv. The target is the
**distribution name**, not the directory:

```
pipx inject --editable dispatcher     ./core
pipx inject --editable media-archiver ./core
pipx inject --editable recorder       ./core
pipx inject --editable ops            ./core
```

Verify every entry point resolves and imports:

```
dispatcher --help >/dev/null && echo dispatcher OK
archiver   --help >/dev/null && echo archiver OK
recorder   --help >/dev/null && echo recorder OK
ops health        >/dev/null && echo ops OK
```

### Reinstalling after edits

- Edited **core** → nothing to do. The editable inject makes the change
  live in all four venvs at once.
- Edited a **service** (archiver/dispatcher/recorder/ops) →
  `pipx reinstall <name> --python 3.13`.

`pipx reinstall` is documented to preserve injected packages. If a
reinstall ever drops the editable `core` (verify with
`pipx list --include-injected`), just re-run the matching
`pipx inject --editable <name> ./core`.

---

## 3. One-time data migration (run ONCE, with services stopped)

Fold the two legacy databases into the new `suite.db`. **Stop all three
services first** so you migrate a consistent snapshot:

```
ops unload
```

Run the migrator. It needs `core` importable; the simplest way from the
repo root is to put `core` on the path for this one command:

```
PYTHONPATH=core python3.13 -m core.migrate --archive-db ~/.config/archiver/archive.db --dispatcher-db ~/.config/dispatcher/dispatcher.db --out ~/.config/archiver-suite/suite.db
```

What it does, and why it's safe:

- Joins `media` (archiver) to `upload_queue` (dispatcher) on `file_path`.
- Resolves status with **queue-as-truth**: a queue row that says `done`
  becomes `sent` even if the archiver's mirror disagreed — the dispatcher
  is the process that actually performed the send, so its record wins.
  `claimed` rows reset to `pending` (re-claimable); `failed` stays
  `failed`; rows with no queue counterpart fall back to the archiver's
  `telegram_sent` mirror.
- Recorder-only rows (in the queue, never in `media`) become standalone
  items with a synthesized `recorder_<stem>` identifier — the same scheme
  the live recorder now uses, so a migrated recording and a re-enqueued
  one collide instead of duplicating.
- Copies `checkpoints`, `circuit`, and `metadata` verbatim.
- Uses `INSERT OR IGNORE`, so it is **idempotent** — re-running it changes
  nothing. The two source DBs are opened read-only and never modified;
  keep them around until you've confirmed the new system works.

Sanity-check the result before starting services:

```
sqlite3 ~/.config/archiver-suite/suite.db "SELECT status, COUNT(*) FROM items GROUP BY status;"
```

Then bring the system back up (staged, per AUTOMATION.md):

```
ops load
ops health
```

---

## 4. Per-user delete policy moved

Delete-after-upload is enforced by the **dispatcher** (the process that
sends and therefore the one that may delete). The policy lives in
`~/.config/archiver-suite/config.toml` (the shared policy store), e.g.:

```toml
[platform.tiktok.user.cooluser]
delete_after_upload = true
```

The dispatcher reads this once at start, so restart it after editing.

---

## 5. What was removed (and where it went)

| Removed | Why | Replacement |
|---|---|---|
| `archive.db` + `dispatcher.db` (two DBs) | two sources of truth | one `suite.db` / `items` |
| `reconcile_dispatch_outcomes()` | only existed to copy truth→mirror | nothing — no mirror to sync |
| `telegram_sent` int (NULL/0/1/2), `seen_by_archiver` | magic-int mirror | `items.status` text enum |
| archiver Telegram creds + session + `tg_router` + `dispatch_client` | archiver sends nothing post-cutover | dispatcher owns the session + routing |
| archiver `chats` command | routing is the dispatcher's job | manage routing on the dispatcher side |
| `policy_store.py`/`policies.py` copied per package | "intentional duplication" that drifts | one `core.PolicyStore` / `core.policies` |
| install-order requirement | schema was dispatcher-owned | `core` creates schema idempotently |
| separate disk-`reconcile` vs DB-reconcile name clash | confusing | only disk reconcile remains (`reconcile_user`) |

---

## 6. Still open

- **`log_setup.py`**: AUTOMATION.md still tells you to `cp` it into each
  package. It can now live in `core` and be imported (`from core import
  setup_file_logging`) instead of copied. Not yet moved.
- Verify `pipx reinstall` preserves the editable `core` inject on your
  machine (see §2).
