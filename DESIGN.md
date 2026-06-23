# Architecture & Design Note

A map of the Media Archiver Suite: what each worker owns, how they talk, the
shared data model, and the integrity / self-healing machinery that holds it
together. This is the "why it's shaped this way" companion to `README.md`
(what it does) and `USER-GUIDE.md` (how to drive it).

> Scope: reflects the codebase as of branch `dispatcher-compat-hardening`
> (June 2026). Cite modules by path; verify against code before treating any
> line:number as current.

---

## 1. System shape

Four long-running processes + one ops toolkit, all pivoting on **one SQLite
file**. No process talks to another over a socket; they coordinate through the
shared DB and a handful of on-disk artifacts (locks, JSON heartbeats).

```
            ┌──────────────┐      ┌──────────────┐
  social →  │   archiver   │      │   recorder   │  ← TikTok live
  media     │ (download +  │      │ (capture +   │
            │  reconcile)  │      │  enqueue)    │
            └──────┬───────┘      └──────┬───────┘
                   │ writes pending rows │
                   ▼                     ▼
              ╔══════════════════════════════════╗
              ║   suite.db  (one SQLite file)     ║
              ║   items · checkpoints · circuit · ║
              ║   metadata        [WAL mode]      ║
              ╚══════════════════╤═══════════════╝
                                 │ claims pending rows
                                 ▼
                         ┌───────────────┐
                         │  dispatcher   │ ──► Telegram (Telethon/MTProto)
                         │ (drain queue) │
                         └───────────────┘

   ops ── reads on-disk artifacts + launchd only (imports no worker) ──► health/watch/install/logrotate
   core ── the shared library every worker imports (DB, policies, media prep, dedup, ingest) ──
```

Key rule: **producers (archiver, recorder) share `core`; they never import each
other or the dispatcher.** The dispatcher shares `core` too. `ops` imports
*nothing* from the workers — it reads only files + launchd state, so it still
works when a worker is broken or uninstalled.

Three installed binaries (`archiver`, `recorder`, `dispatcher`) plus `ops`, each
a pipx venv with an **editable** `core`. (See the `pipx-core-editable-asymmetry`
note: a frozen non-editable core in any venv will crash on a schema bump —
verify after schema changes.)

---

## 2. The shared spine: `core`

`core/core/` is the single source of truth for everything cross-cutting. The
public surface is re-exported from `core/__init__.py` (`__all__`).

### 2.1 One database, one row per file (`schema.py`, `models.py`)

The `items` table holds **one row per media file, cradle to grave** — it merges
what used to be two databases (a catalog + an upload queue). Identity is pinned
by two UNIQUE constraints:

- `file_path` UNIQUE — one row per physical file
- `(platform, identifier)` UNIQUE — one row per platform post

Other tables: `checkpoints` (per platform/user `last_run_utc` + `date_floor`),
`circuit` (per-platform breaker state), `metadata` (k/v: cookie-refresh
timestamps, etc.).

**Status state machine** (`core/models.py`) — the heart of the handoff:

```
pending ──claim──▶ sending ──ok──▶ sent
   ▲                  │
   │                  ├─ retry left ─▶ pending   (mark_failed, attempts<max)
   │                  ├─ no retry ───▶ failed    (attempts>=max)
   │                  ├─ floodwait ──▶ pending   (requeue, no attempt counted)
   │                  └─ watchdog ───▶ pending   (crashed mid-send)
   └── reset ─────────(failed|sent)
```

There is **no `queued` state**: writing the row *is* the handoff. A producer
inserting a `pending` row and the dispatcher claiming it from the same table is
the entire coordination protocol — no mirror, no bridge, no second DB to drift.

Concurrency: WAL mode + `busy_timeout` so archiver/recorder/dispatcher/ops can
open the file at once; brief lock contention blocks-and-retries instead of
raising.

Schema evolves via `SCHEMA_VERSION` + keyed migrations recorded in
`PRAGMA user_version` (`schema.py`). v4 added `content_hash`, `chat_id`,
`topic_id`, `group_key` (global dedup, orphaned-folder routing, forum topics,
album grouping). A DB from the future (user_version > SCHEMA_VERSION) fails loud
at connect.

### 2.2 Store layering (`store.py`, `stores.py`)

`ItemStore` is the concrete store; `ProducerStore` / `QueueStore` / `AdminStore`
are role-narrowed protocol views over it, so each worker depends only on the
verbs it needs. The integrity-critical method is **`claim_batch`** — it atomically
claims a homogeneous album (same chat_id + topic_id + media-bucket + group/caption),
honors the per-platform min-batch gate, and clusters sends in priority order.

### 2.3 Policies (`policy_store.py`, `policies.py`)

`config.toml` is read through `PolicyStore`, which resolves a key by scope
(`user` > `platform` > global default) and can `explain()` where a value came
from. Typed wrappers: `DeletePolicy`, `RecorderDeletePolicy`, `DedupPolicy`,
`BatchPolicy`, `AutoIngestPolicy`, `DownloadPolicy`, `ProtectionPolicy`,
`SortPolicy`, `FailedRetryPolicy`. Frozen-dataclass configs hold a *reference*
to the mutable store, so the CLI can write settings without rebuilding config.

### 2.4 Other core services

- `identity.py` — resolves a file's (identifier, date, title) from sidecar JSON,
  filename pattern, or a stable content-hash fallback for manual files.
- `hashing.py` / `dedup.py` / `backfill.py` — content-hash dedup: never upload
  bytes already shipped, even across paths/platforms.
- `stability.py` — `is_stable()` skips half-written files (size settled).
- `ingest.py` — `register_file()`: the one enqueue path (content-hash dedup +
  requeue logic), reused by every producer rather than re-derived.
- `orphaned.py` — `chat_id`-named folders ("orphaned" media not tied to a
  platform user) ingest + subfolder→album routing → explicit Telegram chat.
- `routing.py` — `parse_route()` canonicalizes a chat_id / `.t<topic>` token
  (the dash-free→`-100…` normalizer shared by ingest and the router).
- `grouping.py` — split-part album group keys (oversize video split into parts
  that ship as one album).
- `media_prep.py` — make a file Telegram-compatible BEFORE enqueue (see §6).
- `ffprobe.py` / `ffmpeg.py` — the shared probe and run wrappers (subprocess +
  timeout + error handling in one place; callers keep their own parsing/output
  checks). Every ffprobe/ffmpeg call in the suite funnels through these.
- `heartbeat.py` — the cross-process status-file primitive: atomic write +
  pid-liveness + staleness-gated read. Backs dispatcher upload progress and the
  archiver loop phase; ops reads through it too (same liveness rules as the
  writers, so the monitor can't drift from the workers).
- `instance_lock.py` — generic single-instance flock; the dispatcher's
  session-keyed lock is a thin subclass (Template-Method error hook).
- `env.py` — env-var parsing (`req`/`opt`/`opt_int`/`opt_float`/`opt_bool`).
  Required values fail loud; optional tunables warn-and-default (a typo can't
  crash a daemon). Every worker config reads through it.
- `paths.py` — the single source of truth for the suite's cross-process file
  layout (tiktok lock, dispatcher progress, archiver loop, recorder pid). The
  seam contracts can't drift between writer and reader.
- `deletion.py` — `DeletionGuard`: the safebrake. Every disk-deletion path runs
  through it; a protected scope is never deleted even with delete-after-upload on.
- `sanitize.py` — banned-word stripping from filenames + captions at send.
- `sorter.py` — move loose `unsorted/` files into `platform/username/` homes.
- `instance_lock.py` — generic advisory single-instance lock (fcntl).
- `termui.py` — the shared terminal UI engine (banners, fields, colorized logs).

---

## 3. Worker: **archiver** (download + reconcile)

`archiver/archiver/` — pulls VOD/media from X, Instagram, TikTok and registers
it as `pending` rows. **Does not upload** (that's the dispatcher).

- **`orchestrator.py`** — Template Method driving one archive cycle: circuit
  check → health check → (recover if unhealthy) → per user {reconcile disk→DB →
  download new → advance checkpoints}. Post-run it reconciles every enabled
  platform, then runs the **orphaned ingest**, **auto-sort**, and **backfill**
  passes. Checkpoint = `date_floor = MAX(upload_date WHERE status='sent')` so
  incremental work stays correct under `delete_after_upload=true` and across long
  gaps (falls back to `last_run_utc` on first run).
- **`platforms.py`** — Strategy pattern: uniform `Platform` interface
  (`health_check`, `attempt_recovery`, `download`, `seed_archive`,
  `archive_path`). The orchestrator knows only `Platform`, not gallery-dl vs
  yt-dlp. New downloads detected by before/after dir diff (robust against
  extractor-set mtimes). `LocalPlatform` = a user-managed folder with no
  download step (`archiver local add`, or auto-discovered top-level dirs that
  aren't platforms or chat_id route dirs).
- **`reconcile.py`** — walks the on-disk archive, registers stable files (skips
  half-written ones), resolves identity per file, and seeds the extractor
  archives so a pre-existing 5,000-post account doesn't re-walk its timeline.
- **`cookies.py`** — Firefox `cookies.sqlite` → Netscape `cookies.txt`
  (copy-first to dodge Firefox's exclusive lock). Also the self-healer's
  cookie-refresh entry point on auth failure.
- **`lock_reader.py`** — reads (never writes) the recorder's TikTok soft-lock;
  archiver skips the TikTok *download* step while a capture is in flight
  (backlog uploads still proceed).
- **`loop_state.py`** — JSON phase heartbeat (`running`/`sleeping`) for `ops`.
- CLI (`cli.py`): `start`/`run`/`loop`, `ingest`, `sort`, `backfill`,
  `bootstrap`, `reset {failed,uploads,user,all}`, `local`, `cookies`, `config`,
  plus auto-* toggles (`auto-ingest`, `auto-sort`, `auto-retry`, `download`).

## 4. Worker: **recorder** (TikTok live capture)

`recorder/recorder/` — records live streams and enqueues finished files.

- **`state.py`** — explicit state machine (LISTENING → RECORDING → HANDOFF →
  STOPPED) + a producer/consumer uploader thread (recording produces files onto
  a `queue.Queue`; a daemon thread consumes → `core.ingest`), so a slow DB write
  can't make it miss the next stream. **Lock held only around an active
  recording**, not while merely LISTENING (so archiver can drain TikTok backlog
  while the recorder idles).
- **`capture.py`** — yt-dlp subprocess wrapper (ffmpeg downloader for HLS,
  MPEG-TS for disconnect survival, `--no-part`, infinite fragment retries).
  Process-group kill on terminate (fixes orphaned-ffmpeg data loss). Reconnects
  on a premature still-live exit instead of finalizing a truncated recording.
- **`platforms/`** — `base.py` `LivePlatform` Protocol (structural typing: a
  future TwitchLive needn't import recorder); `tiktok.py` uses the maintained
  `TikTokLive` lib (bridged sync↔async per-call) rather than re-scraping
  SIGI_STATE each time TikTok changes.
- **`enqueue.py`** — writes the item row via `core.ItemStore` at `priority=5`
  (drains before archiver's `priority=10` backlog) and exempt from the min-batch
  gate (each stream uploads immediately).
- **`startup_sweep.py`** — one reconciliation pass at `recorder start`: prune
  stale logs, reconcile every recording with the queue (sent→delete,
  pending/sending→leave, failed→re-arm, unknown→ingest), drop empty dirs.
- **`lock.py`** — the TikTok soft-lock context manager (the write side of the
  archiver contract). **`watch.py`** — live dashboard (`snapshot` I/O +
  pure `render`).

## 5. Worker: **dispatcher** (drain queue → Telegram)

`dispatcher/dispatcher/` — the only uploader. Claims `pending` rows, makes each
item Telegram-compatible, sends via Telethon, gates deletion.

- **`drain.py`** — `drain_forever`: serial claim→send→mark loop. Owns the
  in-memory **circuit breaker** (N consecutive systemic failures → cooldown),
  periodic **housekeeping** (failed-queue GC + auto-retry + stuck-`sending`
  watchdog), the missing-file/dedup pre-filter, and **`recover_media_empty`**
  (an atomic album rejected for one bad item → re-send each item individually so
  the good ones still deliver and only dead media quarantines).
- **`send.py`** — `TelethonSendStrategy`: the send envelope. FloodWait +
  exponential backoff + **stall watchdog** (a per-attempt deadline sized to
  payload bytes turns a silent TCP freeze into a retryable failure + reconnect).
  Single video / kept-original / album paths. **Fail-fast on auth loss** (§7).
- **`fast_upload.py`** — the FastTelethon parallel multi-connection uploader (N
  senders on the home DC sharing one auth key); always falls back to the serial
  uploader, never raises a fast-path-specific error.
- **`tg_router.py`** — resolves (platform, user) → `Destination(chat_id,
  topic_id)` via an env precedence chain (live > per-user > per-platform >
  global); an explicit row `chat_id` (orphaned folders) overrides entirely, with
  its topic resolved from the *same* layer.
- **`media_meta.py`** — ffprobe display geometry + duration → explicit
  `DocumentAttributeVideo` (Telethon can't infer without hachoir) + a
  representative poster thumbnail (skips black fade-in frames).
- **`image_fix.py`** — normalize photos Telegram's pipeline would reject
  (oversize, extreme aspect, odd encoding) to a safe baseline JPEG.
- **`delete.py`** — `maybe_delete`: the delete-after-upload gate, re-reads the
  row and refuses unless `status='sent'`, then defers to the `DeletionGuard`
  safebrake.
- **`progress.py`** — atomic JSON upload heartbeat for `dispatcher status` / ops.
- **`instance_lock.py`** — single-owner lock for the Telegram session.

## 6. The media pipeline (compatibility)

Every outgoing item is made Telegram-compatible. The intent: **nothing ships
broken**, and high-definition originals are preserved.

```
 VIDEO   non-streamable container/codec → convert to streamable mp4 (remux if
         codecs ok, else x264/aac re-encode); HD source containers (mkv/avi/ts)
         ALSO ship the original as a downloadable DOCUMENT beside the preview.
         Oversize (>4 GiB) → AutoSplitter into <=1 GiB parts shipped as one
         album (shared group_key). [core.media_prep at ingest; streamable_temp
         is the send-time net for un-prepped producers, chiefly the recorder.]

 PHOTO   incompatible (oversize / extreme aspect / odd encoding) → re-encode to
         a safe baseline JPEG; un-fixable aspect ratio → ship as a document.
         Now applied PROACTIVELY on both single AND album send paths.
         [dispatcher.image_fix]

 GUARD   still images are NOT videos: media_prep's send-time net and the
         document-decision are gated to PREP_VIDEO_EXTS so a .jpg/.webp is never
         "re-encoded" into a 0-second mp4 (see photos-reencoded-as-broken-video).
```

Album grouping is homogeneous by media bucket (`claim_batch` keys on it); video
albums use Telethon's native list-send (pre-built InputMedia groups are rejected
by Telegram); photo albums normalize-then-send with a convert-and-retry fallback.

## 7. Integrity & self-healing inventory

The standing priority order is **integrity > self-healing > seam robustness >
efficiency**. Mechanisms, by layer:

| Concern | Mechanism | Where |
|---|---|---|
| Never lose a file to a crash mid-send | `sending`→`pending` startup + 15-min watchdog | `store.reset_stuck_sending`, `drain.run_housekeeping` |
| Never upload duplicate bytes | global content-hash dedup (sent-twin + in-batch), suppression ordered AFTER delivery | `drain`, `dedup`, `ingest` |
| Never delete an undelivered file | delete gate re-reads `status='sent'`; `DeletionGuard` safebrake hard-override | `delete.py`, `deletion.py` |
| Atomic album, one bad item | per-item `recover_media_empty` fallback + quarantine | `drain.py` |
| Telegram rate limits | FloodWait sleep outside the attempt budget | `send._send_with_retries` |
| Silent network freeze | stall watchdog (byte-sized deadline) → reconnect | `send.py` |
| Systemic outage | in-memory circuit breaker (cooldown, not hammer) | `drain.py` |
| **Dead/revoked session** | **fail-fast: startup `is_user_authorized` check (no interactive prompt) + mid-send `UnauthorizedError`/`AuthKeyError` → fatal `SessionUnauthorized`, in-flight batch reverted** | `send.py`, `cli.py` |
| Half-written files | `stability.is_stable()` skip | `reconcile`, sweeps |
| Orphaned ffmpeg children | process-group kill | `recorder.capture` |
| Truncated live recording | reconnect on premature still-live exit | `recorder.capture` |
| **Stale TikTok soft-lock** (recorder crashed holding it) | **`held` gated on writer-pid liveness — a dead-writer lock self-heals to not-held so TikTok archiving resumes** | `archiver.lock_reader`, `ops.health` via `core.heartbeat` |
| Per-platform download failure | circuit breaker + cookie-refresh self-heal | `archiver.orchestrator`, `cookies.py` |
| Banned/gone accounts | auto-retire to a banned roster | `archiver` (see auto-ban note) |
| Failed-queue growth | retention prune (before auto-retry, to avoid retry storms) | `drain.run_housekeeping` |
| Lost log history | copytruncate rotation | `ops.logrotate` |

## 8. Cross-process seams (the contracts)

These are the only places workers couple. They are covered by
`tests/test_seams.py` (a cross-worker integration suite, 26 numbered seams).

1. **The DB handoff** — producer writes `pending`, dispatcher claims it. One
   table, one truth (`models.py`).
2. **TikTok soft-lock** — recorder writes `locks/tiktok.lock`; archiver skips
   TikTok download while a LIVE recorder holds it. The lock is a pid-stamped
   heartbeat, so the read is liveness-gated: a crashed recorder's stale lock
   self-heals to not-held. One-way (`recorder.lock` ↔ `archiver.lock_reader`).
3. **content_hash** — every producer stamps it at enqueue so dedup works across
   producers/paths.
4. **Orphaned chat_id folders** — folder name `-100…[.t<topic>]` IS the
   destination; `parse_route` canonicalizes it on both ingest and send.
5. **Split-part group_key** — oversize split parts share a key so they album.
6. **On-disk status files** — `dispatcher/progress.json`, archiver
   `loop_state`; ops reads these + launchd only, importing no worker.
7. **Editable `core`** — all four venvs import the same editable core; a frozen
   copy breaks schema-bump compatibility.

## 9. Design patterns & idioms in use

- **Template Method** — `archiver.orchestrator` (fixed cycle, variable steps).
- **Strategy** — `archiver.platforms.Platform`, `dispatcher.send.SendStrategy`.
- **Protocol / structural typing** — `recorder.platforms.base.LivePlatform`,
  the `ProducerStore`/`QueueStore`/`AdminStore` store views.
- **State machine** — `recorder.state`, the items `status` column.
- **Producer/consumer** — recorder capture→uploader thread; `fast_upload`'s
  bounded queue.
- **Circuit breaker** — dispatcher send loop + archiver per-platform.
- **Watchdog/heartbeat** — stuck-`sending`, stall deadline, progress/loop_state
  files.
- **Safebrake (guard)** — `DeletionGuard` threaded through every delete path.
- **Fail-fast** — config loads crash loud at startup; `SessionUnauthorized`
  stops the daemon rather than spin-looping doomed sends.
- **Single choke point** — `fast_upload` (all big-file uploads), `core.ingest`
  (all enqueues), `core.ffprobe`/`core.ffmpeg` (all media subprocesses),
  `core.heartbeat` (all status files), `media_prep` (all compat).
- **Graceful degradation** — every ffprobe/ffmpeg/thumbnail/fast-path failure
  degrades to a working fallback, never an error that loses a file.

## 10. Where to look first

| To understand… | Start at |
|---|---|
| The data model & lifecycle | `core/core/schema.py`, `core/core/models.py` |
| How an upload is decided & sent | `dispatcher/dispatcher/drain.py` → `send.py` |
| How a download cycle runs | `archiver/archiver/orchestrator.py` |
| How a live stream is captured | `recorder/recorder/state.py` → `capture.py` |
| Media compatibility rules | `core/core/media_prep.py`, `dispatcher/dispatcher/image_fix.py` |
| What can heal itself | §7 above + `drain.run_housekeeping` |
| Cross-worker contracts | `tests/test_seams.py` |

---

### Recent change set (branch `dispatcher-compat-hardening`)

- Photos made Telegram-compatible **proactively on the single-send path** too,
  not just in albums; un-fixable photos ship as documents.
- **Fail-fast on session/auth loss** (startup auth check + mid-send fatal),
  replacing a silent interactive-prompt hang / circuit-breaker spin-loop.
- **`core.ffprobe`** consolidates three duplicated ffprobe wrappers.
- Removed dead `tg_router` helpers (`explain`/`chat_id_for`/`peer_for`).

Codebase-wide consolidation pass (same branch):
- **`core.heartbeat`** unifies the JSON status-file + pid-liveness pattern that
  was duplicated in five places (dispatcher progress, archiver loop, ops ×3).
- **`core.ffmpeg`** unifies the ffmpeg subprocess wrapper across media_prep,
  image_fix, and media_meta.
- **`core.InstanceLock`** absorbed the dispatcher's duplicated flock mechanism;
  `DispatcherInstanceLock` is now a thin session-keyed subclass.
- **`core.env`** unifies env-var parsing across all worker configs (+ lenient
  warn-and-default for optional tunables, so a config typo can't crash a daemon).
- **`core.paths`** centralizes every cross-process artifact path (tiktok lock,
  progress, loop, pid) that was duplicated between each writer and ops — the
  seam contracts now have one definition each.
- **Stale-lock self-healing** at the recorder↔archiver seam: the TikTok lock
  read is now liveness-gated (`core.heartbeat`), so a crashed recorder no longer
  starves TikTok archiving forever. `core.heartbeat` liveness was also made
  precise (ProcessLookupError = dead, PermissionError = alive).

Deliberately left as-is (consolidation would add indirection or change
user-visible output for little gain): the per-worker CLI `main()` dispatch
(archiver is bespoke, ops trivial), the `_human_bytes`/`_human_secs` status-line
formatters (intentionally binary/no-space vs termui's SI style), the watch
dashboards (different data sources), and the dir-snapshot diffs.
- (Earlier on main) the still-image gate that stops photos being re-encoded into
  unplayable 0-second "videos".
