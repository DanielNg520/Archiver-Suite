# Architecture & Design Note

Dense map of the Media Archiver Suite for fast navigation + revision. Companion
to `README.md` (what) / `USER-GUIDE.md` (how). Lookup-oriented, not narrative.
Verify `file:line` against code before trusting — comments may lag.

## TL;DR topology

Four processes + ops, pivoting on **one SQLite file** (`~/.config/archiver-suite/suite.db`).
No IPC sockets; they coordinate via the DB + on-disk artifacts.

```
archiver (download+reconcile) ─┐                   ┌─ recorder (TikTok live capture)
                               ├─► suite.db ◄──────┤
        writes pending rows ───┘  items/checkpoints/circuit/metadata (WAL)
                                          │ dispatcher claims pending
                                          ▼
                                   dispatcher ──► Telegram (Telethon/MTProto)
   ops: reads on-disk artifacts + launchd ONLY (imports core, never a worker)
   core: the shared lib every worker imports
```

**Rules:** producers (archiver, recorder) + dispatcher all import `core`, never each
other. ops imports no *worker* (core is fine). Installs are pipx venvs with an
**editable** `core` (a frozen non-editable core crashes on schema bump).

## core/core/ — the shared spine

| module | purpose | key symbols |
|---|---|---|
| `schema.py` | DDL + connection factory; WAL+busy_timeout; `SCHEMA_VERSION=4`, keyed migrations via `user_version` | `connect`, `db_path`, `DEFAULT_DB_PATH`, `SchemaVersionError` |
| `models.py` | Item dataclass + status state machine | `Item`, `Status{PENDING,SENDING,SENT,FAILED}`, `TERMINAL` |
| `store.py` / `stores.py` | concrete `ItemStore` + role views (`ProducerStore`/`QueueStore`/`AdminStore`); `claim_batch` (homogeneous album claim; bucket is **source-aware** via `album_bucket` — orphaned rows group mixed `media` vs `document`, and a document group is held while a same-subfolder media sibling is still pending = media-before-documents), `add_pending`, `reset_stuck_sending`, dedup queries | `ItemStore`, `claim_batch`, `sent_twin`, `mark_*` |
| `ingest.py` | THE enqueue primitive: stabilize→hash→dedup-collapse→insert (every producer) | `register_file`, `IngestResult`, `IngestOutcome` |
| `dedup.py`/`hashing.py`/`backfill.py` | content-hash dedup; backfill hashes for legacy rows | `dedup_user`, `backfill_content_hashes` |
| `identity.py` | resolve (identifier,date,title) from sidecar/filename/hash | `resolve` |
| `stability.py` | skip half-written files | `is_stable` |
| `orphaned.py` | chat_id-folder ingest + subfolder→album routing | `ingest_chat_id_dirs`, `ORPHANED_SOURCE`, `subfolder_of` |
| `routing.py` | canonicalize chat_id/`.t<topic>` token (dash-free→`-100…`) | `parse_route`, `Route`, `is_chat_id` |
| `grouping.py` | split-part album group keys | `split_group_key`, `is_split_group` |
| `files.py` | media-type sets (`PHOTO_EXTS`/`VIDEO_EXTS`/`MEDIA_EXTENSIONS`, `ALBUM_MAX`) + album buckets: `media_bucket` (photo/video/single, all producers) and `orphaned_kind` (`media` = photos+inline video, `document` = `.mkv`/`.gif`/other — chat_id folders), unified by source-aware `album_bucket`; sidecar-aware delete. Leaf module — also THE home of `ORPHANED_SOURCE_NAME` (re-exported by `orphaned.py` as `ORPHANED_SOURCE`) | `media_bucket`, `orphaned_kind`, `album_bucket`, `cleanup_sidecars` |
| `media_prep.py` | make file Telegram-compatible pre-enqueue: remux/re-encode video, split >ceiling (or a caller-supplied `split_threshold_bytes` — recorder split mode); `streamable_temp` (send-time net), `is_nonstreamable_video` (doc decision). Gated to `PREP_VIDEO_EXTS` (photos never become video) | `prepare`, `streamable_temp`, `is_nonstreamable_video` |
| **`ffprobe.py`** | shared ffprobe: subprocess+json+timeout | `probe_json` |
| **`ffmpeg.py`** | shared ffmpeg runner (bool, never raises) | `run_ffmpeg` |
| **`heartbeat.py`** | cross-proc status files: atomic write + liveness/staleness read + **`pid_alive`** (the one liveness primitive) | `write_atomic`, `read_live`, `clear`, `pid_alive` |
| **`paths.py`** | single source for cross-proc artifact paths | `tiktok_lock`, `dispatcher_progress`, `archiver_loop`, `recorder_pid`, `locks_dir` |
| **`env.py`** | env parsing; req=fail-loud, opt*=warn+default (self-healing tunables) | `req`, `opt`, `opt_int/float/bool`, `MissingEnvVar` |
| `instance_lock.py` | generic singleton flock; `_already_running_error` hook | `InstanceLock`, `InstanceAlreadyRunning` |
| `deletion.py` | safebrake guard on every delete path | `DeletionGuard` |
| `policy_store.py`/`policies.py` | config.toml scoped resolution (user>platform>default) | `PolicyStore`, `DeletePolicy`,`BatchPolicy`,`DedupPolicy`,`AutoIngestPolicy`,`DownloadPolicy`,`SortPolicy`,`FailedRetryPolicy`,`ProtectionPolicy` |
| `sanitize.py` | banned-word strip (names+captions) | `Sanitizer`, `ReloadingSanitizer` |
| `sorter.py` | move loose files into platform/user homes | `sort_unsorted` |
| `termui.py` | shared terminal UI + `human_size`/`human_duration`/`age` (SI style) | `banner`, `field`, `setup_logging` |
| `migrate.py` | one-time 2-DB→1-DB migration | `migrate` |

State machine: `pending →claim→ sending →ok→ sent` / `→fail→ pending|failed` /
`→floodwait/watchdog→ pending`. No `queued` (writing the row IS the handoff).

## archiver/archiver/

| module | purpose |
|---|---|
| `orchestrator.py` | Template Method cycle: circuit→health→recover→per-user{reconcile→download→checkpoint}; post-run reconcile-all + orphaned-ingest + auto-sort + backfill. Checkpoint = `date_floor=MAX(upload_date WHERE sent)` |
| `platforms.py` | Strategy `Platform` (X/IG gallery-dl, TikTok yt-dlp); `LocalPlatform` = folder, no download. new-download = before/after dir diff |
| `reconcile.py` | walk disk→register stable files via `register_file`→seed extractor archives |
| `cookies.py` | Firefox cookies.sqlite→Netscape txt (copy-first); cookie-refresh self-heal |
| `lock_reader.py` | read tiktok soft-lock; **liveness-gated** (self-heals stale lock) |
| `loop_state.py` | loop phase heartbeat (via core.heartbeat + core.paths) |
| `config.py` | env (core.env) + config.toml |
| CLI: `start/run/loop`, `ingest`, `sort`, `backfill`, `bootstrap`, `reset{failed,uploads,user,all}`, `local`, `cookies`, `config`, auto-{ingest,sort,retry}, `download`, `banned` |

## recorder/recorder/

| module | purpose |
|---|---|
| `state.py` | state machine LISTENING→RECORDING→HANDOFF→STOPPED + producer/consumer uploader thread; **lock held only around active recording** |
| `capture.py` | yt-dlp wrapper (ffmpeg HLS, MPEG-TS, --no-part, infinite retries); **process-group kill** (no orphaned ffmpeg); reconnect on premature still-live exit |
| `platforms/base.py` | `LivePlatform` Protocol (structural) |
| `platforms/tiktok.py` | TikTokLive lib (sync↔async bridge per call) |
| `enqueue.py` | `register_file` at `priority=5` (before archiver's 10), min-batch exempt |
| `startup_sweep.py` | reconcile disk↔queue once at start (sent→del, pending/sending→leave, failed→re-arm, new→ingest, drop empty dirs) |
| `lock.py` | `TikTokLock` writes pid-stamped heartbeat (via core.heartbeat + core.paths) |
| `watch.py` | dashboard snapshot/render; `_pid` via `heartbeat.pid_alive` |
| CLI: `start[--daemon]`, `record`, `stop`, `status`, `watch`, `config` |

## dispatcher/dispatcher/

| module | purpose |
|---|---|
| `drain.py` | `drain_forever`: serial claim→send→mark; circuit breaker (`_CIRCUIT_TRIP_AT=8`/60s); `run_housekeeping` (failed-missing GC→prune→**transient auto-recover** (`reset_failed_transient`, default on)→opt-in blanket `auto_retry_failed`→stuck watchdog, every 15min); missing-file+dedup pre-filter; `recover_media_empty` (atomic-album per-item fallback) |
| `send.py` | `TelethonSendStrategy`: FloodWait+backoff+stall-watchdog+reconnect; video/photo/doc paths; **proactive photo compat** (single+album); **fail-fast `SessionUnauthorized`** (startup `is_user_authorized`, mid-send `UnauthorizedError`/`AuthKeyError`). Single video sends attach explicit ffprobe attrs; native video **albums** rely on Telethon's own per-item geometry → **`hachoir` is a hard dep** (without it album videos ship as 1×1 images; `cli._assert_video_metadata_backend` fails fast). Chat_id-folder albums: MIXED photo+video in one group (`_send_mixed_album`, photo preflight kept) and grouped `.mkv`/`.gif` originals as a **document album** (`send_album(as_documents=True)` → `force_document`), shipped after the subfolder's media. **Optional burner**: `_client_for(peer)` picks primary vs burner per destination (`_sender`/`_active_client`; serial drain ⇒ single field safe); burner built lazily, unauthorized→log+primary fallback; `None` burner short-circuits to primary (inert) |
| `fast_upload.py` | FastTelethon parallel multi-conn upload (home DC, shared auth key); always falls back to serial |
| `tg_router.py` | (platform,user)→`Destination(chat_id,topic_id)` env chain; row chat_id overrides; `peer_chat_id` = inverse of `_resolve_peer` (match a peer against the burner chat set) | 
| `media_meta.py` | ffprobe geometry→`DocumentAttributeVideo` + poster thumbnail (via core.ff*) |
| `image_fix.py` | normalize photos Telegram rejects → baseline JPEG (via core.ff*) |
| `delete.py` | `maybe_delete`: re-read status='sent' gate → DeletionGuard |
| `progress.py` | upload heartbeat (via core.heartbeat + core.paths) |
| `instance_lock.py` | session-keyed singleton = thin `core.InstanceLock` subclass |
| `config.py` | env (core.env) + creds + tunables (lenient parse); `BurnerCreds` (optional 2nd account, `parse_route`-normalized chat set, primary-inherited api creds); `upsert_env_vars` (idempotent dotenv writer, the burner CLI's only persistence) |
| CLI: `start`, `status`, `stats`, `check-routes`, `banned-words`, `queue{list,retry,cancel}`, `config`, `burner{login,chats,status}` |

## ops/ops/
`health.py` (reads suite.db RO + core.paths artifacts + launchd; liveness via core.heartbeat), `logrotate.py` (copytruncate), `cli.py` (`install/uninstall/health/watch/load/unload/restart/logrotate`). launchd labels `com.duy.{dispatcher,recorder,archiver}`.

## Seams (cross-process contracts; tests/test_seams.py, 210 checks, 30 seams)
1. **DB handoff** — producer writes `pending`, dispatcher claims. One table.
2. **TikTok soft-lock** — recorder writes `paths.tiktok_lock()`; archiver/ops read **liveness-gated** (stale = self-heal). recorder owns write/remove.
3. **content_hash** — all producers via `register_file` → global dedup.
4. **orphaned chat_id folders** — folder name = dest; `parse_route` both sides.
5. **split group_key** — oversize parts album together.
6. **status files** — `paths.*` written by writer, read by ops; core.heartbeat liveness.
7. **editable core** — all venvs import the same core (schema-bump compat).
8. **video-metadata backend** — native album send leans on Telethon+`hachoir` for per-item geometry; missing → 1×1 image videos (startup fail-fast).

## Choke points (one definition each — change here, not at call sites)
`register_file` (enqueue) · `claim_batch` (claim) · `fast_upload` (big upload) ·
`media_prep` (compat) · `core.ffprobe`/`core.ffmpeg` (media subprocess) ·
`core.heartbeat` (status files + `pid_alive`) · `core.paths` (artifact paths) ·
`core.env` (env parse) · `core.InstanceLock` (flock) · `DeletionGuard` (delete).

## Self-healing map
stuck `sending`→pending (startup + 15min watchdog + auth-loss revert) · global
dedup (never re-upload) · delete gate re-reads sent + safebrake · album MediaEmpty
→ per-item recover · FloodWait sleep outside budget · stall watchdog→reconnect ·
circuit breaker (systemic) · **fail-fast SessionUnauthorized** (no spin-loop/no
interactive hang) · **single reconnect authority** (Telethon `auto_reconnect=False`
+ client/fast_upload; `_force_reconnect` on stall AND network error — kills the
dual-authority `'NoneType'.connect` race that hung the drain) · **TCP keepalive**
(`dispatcher.keepalive`, 20/5/3) resets a half-open MTProto socket in ~35s →
ConnectionError → `_force_reconnect`, instead of an infinite silent hang (dead
VPN/Tailscale exit, sleep/wake; macOS default keepidle is 2h) · **stale
tiktok-lock self-heals** (pid liveness) · lenient env
(typo'd tunable won't crash) · is_stable skip · process-group kill · reconnect on
premature exit · cookie-refresh + per-platform breaker · auto-ban gone accounts ·
failed-queue prune-before-retry (no storm) · **transient failed-row auto-recover**
(`is_transient_failure`-gated, default on; permanent poison stays quarantined) ·
logrotate.

## Invariants
- One row per file (`file_path` UNIQUE) and per post (`(platform,identifier)` UNIQUE).
- Never delete unless status='sent' AND policy AND not safebraked.
- Never upload bytes already sent (content_hash), suppression AFTER delivery.
- Albums homogeneous by media-bucket; photos never re-encoded to video (`PREP_VIDEO_EXTS` gate).
- Required env fails loud; optional env warns+defaults.

## Run / test (from a NEUTRAL cwd — repo root lets ./core shadow the install)
```
PP=core:archiver:recorder:dispatcher:ops
PY=~/.local/pipx/venvs/dispatcher/bin/python   # only venv with all of core+dispatcher+telethon
# seam suite + any selftest:
PYTHONPATH=$PP $PY tests/test_seams.py
PYTHONPATH=$PP $PY core/core/_selftest_media_prep.py   # etc.
# logrotate selftest is module-mode:
PYTHONPATH=ops $PY -m ops._selftest_logrotate
```
Full battery = 13 files: core{_selftest,_fixes,_media_prep,_safebrake,_termui},
dispatcher/_fast_upload, recorder{_capture,_reconnect,_ui,_watch,platforms/_tiktok_browser},
ops/_logrotate(-m), tests/test_seams.

## Gotchas
- **Editable installs import the working tree** → a worker restart loads whatever
  branch is checked out. (archiver/recorder run under launchd; dispatcher manual.)
- No single pipx venv has every package; use `media-archiver` venv OR `PYTHONPATH`
  over the dispatcher venv for cross-worker tests.
- ops soft-imports core (try/except + sys.path fallback to repo); deps=[].
- `recorder_pid` default only; a custom STATE_DIR makes ops blind to the pid.
- `hachoir` must be in the **dispatcher** venv (declared dep). If album videos
  arrive as 1×1 images, that's the first thing to check.
