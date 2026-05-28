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
            │   dispatcher.db    │   ← SQLite queue (the only shared state)
            │   (upload_queue)   │
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

1. **One Telegram session, many producers.** Telegram allows only sane use
   of one user session at a time. With a single archiver that was fine, but
   adding a live recorder meant two things wanting to send at once. The
   **dispatcher** is now the *sole* owner of the Telegram session; everything
   else writes jobs to a queue and the dispatcher drains it serially. No
   session contention, ever.

2. **Downloads shouldn't block on uploads.** A slow Telegram upload (or a
   FloodWait) used to stall the whole archive cycle. Now the archiver
   enqueues and moves on; uploading happens asynchronously in the dispatcher.

3. **Live recording is real-time; archiving is batch.** They have opposite
   scheduling needs. Splitting them lets the recorder react in seconds while
   the archiver runs every few hours.

## The one rule that holds it together

**Processes communicate ONLY through `dispatcher.db`.** No process imports
another's Python code. The archiver and recorder write rows; the dispatcher
reads them. This is why each can be installed, upgraded, crashed, or rolled
back independently. Shared logic (the `policy_store`, `tg_router`) was
*copied* into each package rather than imported — intentional duplication to
preserve independence.

---

## The four modules

### dispatcher — the only thing that talks to Telegram

Owns the Telegram session. Polls `upload_queue` for `pending` rows, claims
one atomically, sends it, marks it `done` (or `failed` after retries).
Handles FloodWait, retries with backoff, and an optional delete-after-upload
policy. A startup watchdog reverts rows stuck in `claimed` (from a previous
crash) back to `pending`.

Priority order: lower number drains first. Archiver enqueues at **10**,
recorder at **20** — so VOD backlog sends ahead of (less time-sensitive)
finished recordings.

Detailed docs: `dispatcher/README.md`.

### archiver — pulls VODs from X / Instagram / TikTok

Your existing multi-platform archiver, now modified to enqueue into
`dispatcher.db` instead of sending directly. Controlled by one feature flag:

- `ARCHIVER_USE_DISPATCHER=true` → enqueue (new behavior)
- `ARCHIVER_USE_DISPATCHER=false` → send directly via its own session (legacy
  fallback, your rollback path)

When enqueuing, files are marked `telegram_sent=2` (queued) locally. On the
next run, `reconcile_dispatch_outcomes()` reads the dispatcher's results and
flips them to `1` (sent) or `0` (failed). The download cutoff (`date_floor`)
only advances past files Telegram has *confirmed*, so a crash never loses
ground.

While the recorder is actively recording TikTok, the archiver skips the
TikTok *download* step (it reads the recorder's lockfile). Uploads of existing
TikTok backlog still proceed.

### recorder — captures TikTok live streams

Watches a priority-ordered list of TikTok usernames. When one goes live, it
records the stream with yt-dlp (ffmpeg backend), and when the stream ends it
enqueues the file into `dispatcher.db`. Records one stream at a time; between
recordings it re-scans the list so a higher-priority user who just went live
gets picked up.

Holds a lockfile (`~/.config/archiver/locks/tiktok.lock`) only while actively
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
~/.config/dispatcher/
    .env                    Telegram credentials + chat routing
    config.toml             delete-after-upload policy (machine-managed)
    dispatcher.db           THE QUEUE (+ -wal, -shm while running)
    session.session         dispatcher's Telegram session

~/.config/archiver/
    .env                    archiver creds, paths, ARCHIVER_USE_DISPATCHER
    config.toml             user lists + per-user policies
    archive.db              archiver's own record of downloaded media
    session                 archiver's Telegram session (legacy path only)
    locks/tiktok.lock       written by recorder while recording
    cookies/                Instagram/TikTok cookies

~/.config/recorder/
    .env                    TIKTOK_COOKIES_FILE, paths
    config.toml             priority-ordered TikTok user list

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

## Install order (matters)

The dispatcher owns the DB schema the others write into, so install it first.

```
cd dispatcher  && pipx install . --python 3.13
cd ../archiver && pipx install . --python 3.13
cd ../recorder && pipx install . --python 3.13
cd ../ops      && pipx install . --python 3.13
```

pipx has no editable mode. After any source edit:

```
pipx reinstall <package> --python 3.13
```

(packages: `dispatcher`, `media-archiver`, `recorder`, `ops`)

First run requires interactive Telegram auth (launchd can't answer the SMS
prompt). See AUTOMATION.md step 2.

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
| `README.md` (this file) | architecture, install order, layout |
| `AUTOMATION.md` | launchd setup, staged rollout, every automated piece |
| `dispatcher/README.md` | dispatcher CLI, env vars, queue smoke test |
| `ops/RUNBOOK.md` | failure recovery procedures |
