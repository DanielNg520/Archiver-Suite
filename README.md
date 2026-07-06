# Media Archiver Suite

A four-process system that archives social media (X, Instagram, TikTok) and
TikTok live streams to Telegram, losslessly and unattended.

```
┌─────────────┐       ┌──────────────┐
│  archiver   │──┐    │   recorder   │
│ (VOD pull)  │  │    │ (live record)│
└─────────────┘  │    └──────┬───────┘
                 │           │
                 ▼           ▼
            ┌────────────────────┐
            │     suite.db       │   ← the ONE database (shared state)
            │    (items table)   │
            └─────────┬──────────┘
                      │
                      ▼
              ┌───────────────┐
              │  dispatcher   │──→ Telegram
              │ (owns session)│
              └───────────────┘

         ops  ──→ health checks + launchd control (reads everything, owns nothing)
```

## Why four processes instead of one

The original archiver did everything in one process: download, then upload
to Telegram inline. That coupling caused three problems this redesign fixes:

1. **One Telegram sender, many producers.** Telegram allows only sane use
   of one user session at a time. With a single archiver that was fine, but
   adding a live recorder meant two things wanting to send at once. The
   **dispatcher** is now the *sole* process that talks to Telegram; everything
   else writes jobs to a queue and the dispatcher drains it serially. No
   session contention, ever. (The dispatcher may hold a second, optional
   *burner* account for a dedicated set of chats — still one serial sender,
   still the only talker; see **dispatcher/README.md**.)

2. **Downloads shouldn't block on uploads.** A slow Telegram upload (or a
   FloodWait) used to stall the whole archive cycle. Now the archiver
   enqueues and moves on; uploading happens asynchronously in the dispatcher.

3. **Live recording is real-time; archiving is batch.** They have opposite
   scheduling needs. Splitting them lets the recorder react in seconds while
   the archiver runs every few hours.

## The one rule that holds it together

**There is exactly one source of truth: the `items` table in `suite.db`.**
A file's entire life — discovered, downloaded, queued, sending, sent or
failed — is one row with one `status` column. Every process reads and
writes that one table through a shared `core` library; none of them keeps
its own private copy of delivery state, so there is nothing to reconcile.

The processes stay separate (separate crashable, separately-scheduled
units — see below), but they share *code*, not duplicate it. `core` holds
the schema, the state-machine transitions, the policy store, and the
filesystem helpers. One definition, imported four times. (The earlier
design copied `policy_store`/`tg_router` into each package "to preserve
independence"; in practice that just meant the copies could drift against
the one schema they all depend on. Process isolation comes from running
separate processes — not from giving them disjoint code.)

Install: the **Install** section below. Day-to-day usage of every feature:
**USER-GUIDE.md**. Dense architecture map for revising the code: **DESIGN.md**.

---

## The four modules

### dispatcher — the only thing that talks to Telegram

Owns the Telegram session(s). Polls the shared `items` table for `pending`
rows, claims one atomically, sends it, marks it `sent` (or `failed` after
retries). Optionally routes a dedicated set of chats through a second *burner*
account, with the primary as the fallback (**dispatcher/README.md**).
Handles FloodWait, retries with backoff, and an optional delete-after-upload
policy. A startup watchdog reverts rows stuck in `sending` (from a previous
crash) back to `pending`.

Priority order: lower number drains first. Archiver enqueues at **10**,
recorder at **20** — so VOD backlog sends ahead of (less time-sensitive)
finished recordings.

Detailed docs: `dispatcher/README.md`.

### archiver — pulls VODs from X / Instagram / TikTok

Your existing multi-platform archiver, now writing pending rows directly
into the shared `items` table instead of sending to Telegram itself. There
is no feature flag and no Telegram session in the archiver anymore — it
holds zero Telegram credentials. Writing the row *is* the handoff; the
dispatcher claims it on its next poll.

The download cutoff (`date_floor`) reads `MAX(upload_date WHERE
status='sent')` straight from the one table, so it only advances past
posts the dispatcher has actually confirmed delivered. A crash or a slow
queue never loses ground, even though sending is asynchronous — and there
is no mirror column or reconcile step to keep in sync.

While the recorder is actively recording TikTok, the archiver skips the
TikTok *download* step (it reads the recorder's lockfile). Uploads of existing
TikTok backlog still proceed.

### recorder — captures TikTok live streams

Watches a priority-ordered list of TikTok usernames. When one goes live, it
records the stream with yt-dlp (ffmpeg backend), and when the stream ends it
registers the file in `suite.db` as a pending dispatcher item. Records one
stream at a time; between recordings it re-scans the list so a higher-priority
user who just went live gets picked up.

Holds a lockfile (`~/.config/archiver-suite/locks/tiktok.lock`) only while actively
recording, so the archiver knows to skip TikTok downloads during that window.

TikTok live detection uses the `TikTokLive` library (not fragile manual
scraping). ffmpeg must be installed.

Detailed docs: `recorder/` source headers.

### ops — health checks and launchd management

Reads the other three (via `launchctl`, the SQLite DBs, and the lockfile) but
imports none of them. Provides:

```
ops health      one-shot system status
ops watch       auto-refreshing status
ops load        launchctl load all three services
ops unload      stop all three
ops restart <s> restart one service
```

Also ships the three launchd plists and `RUNBOOK.md` (failure recovery).

---

## On-disk layout

```
~/.config/archiver-suite/
    suite.db                THE ONE DATABASE: items + checkpoints + circuit
                            + metadata (+ -wal, -shm while running)
    config.toml             shared policy store (user lists + per-user
                            delete-after-upload / dedup policies)

~/.config/dispatcher/
    .env                    Telegram credentials + chat routing
    session.session         dispatcher's Telegram session

~/.config/recorder/
    .env                    TIKTOK_COOKIES_FILE, paths
    config.toml             priority-ordered TikTok user list + output_dir;
                            optional split mode (split_at_chunk_size/_chunk_gib)

~/.local/log/
    {dispatcher,recorder,archiver}.log          rotating app logs (if wired)
    {dispatcher,recorder,archiver}.{out,err}.log  launchd capture (unbounded)

~/.recorder/
    pid                     recorder pid file
    room_id_cache.json      TikTok room id cache

/Volumes/StorEDGE/archiver_downloads/           media output (external drive)
~/recorder-output/                              recorder output
```

---

## Install

Install order **no longer matters** — `core` creates the schema
idempotently the first time any process connects. Each app is installed
with pipx, then the shared `core` library is injected (editable) into each
app's venv:

```
pipx install ./dispatcher --python 3.13
pipx install ./archiver   --python 3.13
pipx install ./recorder   --python 3.13
pipx install ./ops        --python 3.13

pipx inject --editable dispatcher     ./core
pipx inject --editable media-archiver ./core
pipx inject --editable recorder       ./core
pipx inject --editable ops            ./core
```

The dispatcher needs `hachoir` (Telethon's video-metadata backend) — it ships
as a declared dependency, so a clean `pipx install ./dispatcher` pulls it in.
Without it, album videos upload as 1×1 static images and the dispatcher
refuses to start.

Editing `core` needs no reinstall (editable). After editing a service:
`pipx reinstall <package> --python 3.13` (packages: `dispatcher`,
`media-archiver`, `recorder`, `ops`).

Schema is created and migrated idempotently on first open (`PRAGMA
user_version`, current `core.schema.SCHEMA_VERSION`) — install order doesn't
matter and there is nothing to run by hand. First run requires interactive
Telegram auth once (launchd can't answer the SMS prompt). See AUTOMATION.md
step 1.

---

## Daily operation, once automated

You don't run anything by hand. launchd keeps all three alive. You check:

```
ops health
```

and read `RUNBOOK.md` when something's wrong. Full automation setup and the
recommended *staged* rollout (given prior kernel-panic history) are in
**AUTOMATION.md**.

---

## Module doc index

| Doc | Covers |
|-----|--------|
| `README.md` (this file) | architecture, install, layout |
| `DESIGN.md` | dense architecture map — modules, seams, choke points, invariants |
| `USER-GUIDE.md` | task-oriented daily use — every upload path + commands |
| `AUTOMATION.md` | launchd setup, staged rollout, every automated piece |
| `archiver/README.md` | archiver CLI, env vars, platforms |
| `dispatcher/README.md` | dispatcher CLI, env vars, queue smoke test |
| `ops/RUNBOOK.md` | failure recovery procedures |

**Capabilities** (see USER-GUIDE.md): global content-hash dedup (the same bytes
never upload twice; re-introduced uploads are cleaned up), a min-batch policy
(platform albums wait for 10 items / 7 days), chat_id "orphaned" folders for
loose files (with forum-topic routing and split-album grouping for oversize
files), local (download-free) platforms, a per-platform download toggle,
banned-word sanitizing of filenames/captions, auto-retirement of
suspended/banned accounts, and a harmonized CLI (`start`/`--once`, shared
`stats`/`queue`/`config`). Self-healing throughout: stuck-row watchdog, transient
failed-row auto-recovery, FloodWait/stall recovery, and a circuit breaker — see
DESIGN.md's self-healing map.
