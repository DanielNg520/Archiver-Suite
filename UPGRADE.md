# Upgrading to the new version

This release adds **content-hash dedup**, **chat_id "orphaned" folders**, a
**minimum-batch upload policy**, **local (download-free) platforms**, a
**per-platform download toggle**, and a **harmonized CLI** — all on top of the
existing single-`suite.db` design.

> If you have **not** yet done the original two-DB → `suite.db` migration, do
> that first (see **MIGRATION-AND-INSTALL.md** §3), then come back here.

---

## TL;DR

```bash
# 1. The new code is already in your local working tree (developed in place,
#    not pulled from a remote). Reinstall the service CLIs so they pick up the
#    new subcommands. core is injected --editable, so it's already live.
pipx reinstall dispatcher     --python 3.13
pipx reinstall media-archiver --python 3.13
pipx reinstall recorder       --python 3.13

# 2. schema migrates itself on first open — nothing to run.
# 3. one-time: hash existing files so dedup/cleanup work retroactively
archiver backfill

# 4. review the new defaults below, then start normally
```

(If `pipx reinstall` doesn't pick up working-tree changes on your setup, force
from the path: `pipx install ./dispatcher --force --python 3.13`, etc.)

---

## 1. Schema auto-migrates

`core` uses versioned migrations (`PRAGMA user_version`). The first time any
process opens `suite.db`, it adds `content_hash`, `chat_id`, `group_key` + two
indexes in one transaction. Idempotent, concurrency-safe, additive — **no
command to run, nothing destructive.** Current version: `SCHEMA_VERSION = 2`.

## 2. Reinstall the service CLIs

`core` is injected **editable** (live with no reinstall), but the new
subcommands ship in the service packages, so reinstall those once.

## 3. One-time backfill

`archiver backfill` reads every file lacking a hash and stamps it, extending the
move/rename-proof dedup and the "delete a re-introduced already-uploaded file"
cleanup to your existing rows. Resumable (Ctrl-C and re-run).

---

## Behavior changes to know about

### ⚠️ Platform uploads now batch

By default, **platform albums are held until 10 items accumulate** in the same
user+media-type group, then sent together. A partial flushes after **7 days**.
Recorder (live) and orphaned (chat_id) are exempt.

```bash
dispatcher config set min_batch_size 1     # disable → send whatever's pending
dispatcher config set min_batch_size 10 --platform x
dispatcher config set min_batch_max_wait_h 168
```
Restart the dispatcher after changing these.

### Global content dedup

The same bytes never upload twice. A re-introduced already-sent file is
recognized by content and deleted, not re-sent (the redundant copy is removed
unconditionally).

### New, optional capabilities (off/manual by default)

- **chat_id folders** — `output_dir/<chat_id>/…`. Manual `archiver ingest`, or
  `archiver auto-ingest set --enabled true`.
- **local platforms** — `archiver local add <name>` (hand-managed folder,
  reconcile + upload, no download).
- **per-platform download toggle** — `archiver download set --platform
  instagram --enabled false` (keep reconcile/upload, stop fetching, no cookies
  needed).

See **USER-GUIDE.md** for usage.

---

## Rollback

Nothing here is destructive to data: the added columns are nullable and ignored
by older code paths, and `backfill` only fills a column. Reverting the service
packages leaves `suite.db` usable.
