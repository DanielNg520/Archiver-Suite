# Media Archiver

Unified downloader for X (Twitter), TikTok, and Instagram. It discovers and
downloads media, then writes pending rows into the shared
`~/.config/archiver-suite/suite.db` database. The dispatcher is the only
process that talks to Telegram.

Version 1.1 highlights:
- **Instagram support** (posts + reels by default; stories/highlights opt-in)
- **Per-(platform, user) `delete-after-upload`** — 3-level resolution chain
- **`bootstrap` subcommand** — absorb an existing on-disk media library
- **Manual media in subfolders** is auto-detected, queued, and (optionally) cleaned up by the dispatcher after send
- **Checkpoint = `MAX(upload_date) WHERE status='sent'`** — survives deletion, robust across long gaps
- **Sidecar-aware identity resolution** — sidecars (`.json` / `.info.json`) drive identifiers/dates/captions where available

## Layout

```
.
├── archiver/                       # The Python package
│   ├── config.py                   # Frozen dataclass config (X / TikTok / Instagram)
│   ├── cookies.py                  # Firefox cookie export (TikTok + Instagram)
│   ├── reconcile.py                # Reconcile v2 (recursive, sidecar-aware, archive-seeding)
│   ├── platforms.py                # Platform ABC + X / TikTok / Instagram strategies
│   ├── orchestrator.py             # Template Method + circuit breaker + bootstrap
│   └── cli.py                      # Subcommand CLI
├── downloads/                      # Created at runtime
│   ├── x/<username>/...
│   ├── tiktok/<username>/...
│   └── instagram/<username>/...   ← manual files in subfolders also picked up
├── .archiver/                      # Hidden runtime state
│   ├── archiver.log
│   ├── loop.log
│   ├── gallery_dl/
│   │   ├── x/<user>_archive.sqlite3
│   │   ├── instagram/<user>_archive.sqlite3
│   │   └── tiktok/<user>_photo_archive.sqlite3   ← photo carousels (NEW: prevents re-fetch)
│   └── yt_dlp/
│       └── tiktok/<user>_archive.txt
├── cookies/
│   ├── tiktok.txt
│   └── instagram.txt
├── .env.example
└── pyproject.toml
```

**User config lives in `~/.config/archiver-suite/.env`** (outside the project).
User lists and behavior policies live in
`~/.config/archiver-suite/config.toml`.

## First-time setup (new install)

```bash
pipx install . --python 3.13
pipx inject --editable media-archiver ../core
mkdir -p ~/.config/archiver-suite
cp .env.example ~/.config/archiver-suite/.env
# Fill in env vars — see Env reference below.
archiver health
archiver run
```

## Migrating an EXISTING archive

If you already have an `output_dir` with X/TikTok/Instagram media on disk —
either from a prior version of this tool or downloaded by hand — **run
`archiver bootstrap` once before your first `archiver run`**. It scans your
folders, registers every file in the DB, seeds the per-platform extractor
archives so the next run doesn't re-fetch what you already have, and sets
each user's `date_floor` so the first real run is incremental.

```bash
archiver bootstrap                       # absorb all configured platforms+users
archiver bootstrap --platform instagram  # just one platform
archiver bootstrap --user alice          # just one user (across all platforms)
```

Bootstrap is idempotent — re-run it any time. It never makes network
calls; it just reads disk + writes DB.

## Manually adding media to a subfolder

Drop any media file into `downloads/<platform>/<user>/` or any subfolder
beneath it (e.g. `downloads/instagram/carol/stories_2025/`). The next
`archiver run` will:

1. **Reconcile pass** walks recursively. Files with no sidecar and a
   non-standard filename get a `manual_<hash>` identifier and use
   their mtime as the `upload_date`.
2. **Queue pass** inserts pending `items` rows for the dispatcher.
3. With delete-after-upload enabled, they're deleted just like
   extractor-downloaded files after the dispatcher sends them.

No special command needed; this is part of every run.

## Manually adding loose root media

Media files dropped directly into `downloads/<platform>/` are treated as
loose root files. Reconcile clusters them by the longest shared filename-stem
prefix, ignoring punctuation/underscores and case. At least 5 shared
alphanumeric prefix characters are required for a cluster; otherwise the file
is queued by itself. The dispatcher sends each loose-root cluster in chunks of
10 or fewer items, using the display prefix as the Telegram caption.

## Env vars

### Always required
```bash
ENABLED_PLATFORMS=x,tiktok,instagram
```

Telegram credentials and chat routing now belong to the dispatcher, not the
archiver.

### Run behavior
```bash
RECONCILE_AFTER_RUN=false
```

When true, each `archiver run` finishes with a disk sweep that dedups platform
folders, then queues any stable files missing from the shared dispatcher DB.
That sweep also checks the recorder output directory from
`~/.config/recorder/config.toml`.

### Delete after upload (3-level chain)
```bash
DELETE_AFTER_UPLOAD=false                  # global default
DELETE_AFTER_UPLOAD_INSTAGRAM=true         # per-platform
DELETE_AFTER_UPLOAD_X_ALICE=false          # per-user
```

Run `archiver policy` to see the resolved decision per (platform, user).
Policy changes are stored in `~/.config/archiver-suite/config.toml`.

### X
```bash
X_USERS=alice,bob
X_AUTH_TOKEN=...
X_CT0=...
X_TWID=...
```

### TikTok
```bash
TIKTOK_USERS=cara
TIKTOK_COOKIES_FILE=./cookies/tiktok.txt
FIREFOX_PROFILE=archiver
COOKIE_REFRESH_DAYS=3
```

### Instagram (NEW)
```bash
INSTAGRAM_USERS=dan
INSTAGRAM_COOKIES_FILE=./cookies/instagram.txt
INSTAGRAM_INCLUDE=posts,reels                # default; can add stories,highlights,tagged,channel
FIREFOX_PROFILE=archiver                     # shared with TikTok
```

Note: stories/highlights are opt-in. They have higher ban risk and the
"posts expire" semantic complicates incremental checkpoints. Start with
the defaults; add subcategories only if you accept the tradeoffs.

## Daily commands

```bash
archiver start                              # run continuously (alias of `loop`)
archiver start --once                       # single cycle (alias of `run`)
archiver run                                # everything, all platforms (legacy verb)
archiver run --platform instagram           # one platform
archiver run --platform x --user alice      # one user

archiver bootstrap                          # one-shot import (existing archive)
archiver backfill                           # one-time: content-hash existing rows

# Local platforms (hand-managed folders, no download)
archiver local add mylibrary                # then drop files in output_dir/mylibrary/<user>/
archiver local list ; archiver local remove mylibrary

# Per-platform download toggle (off = reconcile/upload only, no cookies)
archiver download set --platform instagram --enabled false
archiver download                           # show resolved on/off per platform

# chat_id (orphaned) folders → loose files to a specific chat
archiver ingest                             # scan output_dir/<chat_id>/ folders
archiver ingest --path "/any/dir" --chat -100123   # ingest an arbitrary folder
archiver auto-ingest set --enabled true     # auto-ingest every cycle (default off)

# Shared queue noun (same as dispatcher)
archiver queue list --status failed ; archiver queue retry <id>

archiver stats                              # totals + per-platform date_floor
archiver stats --platform tiktok --user u

archiver policy                             # resolved delete-after-upload per user
archiver health                             # check credentials
archiver reconcile                          # scan disk → DB (subset of `run`)
archiver run-settings show
archiver run-settings reconcile-after-run on
archiver run-settings delete-records-after-upload on

archiver loop                               # see "Automation" below
archiver loop --min 3600 --max 7200

archiver reset failed                       # re-queue failed uploads
archiver reset failed --platform instagram
archiver reset uploads --platform x --user u  # re-upload everything (no re-download)
archiver reset user --platform x --user u   # full wipe of one user
archiver reset all                          # nuke EVERY user (prompts y/N)
archiver reset all --yes                    # cron-safe

archiver cookies refresh                                # default: TikTok
archiver cookies refresh --platform instagram           # IG
archiver cookies refresh --platform tiktok --profile a  # override profile

archiver config list
archiver config list --platform instagram
archiver config add --platform instagram --user newuser
archiver config remove --platform instagram --user olduser
```

## The "incremental + auto-deletion" guarantee

With `DELETE_AFTER_UPLOAD=true` (any level), the local file is deleted
after successful upload by the dispatcher. But the system still knows what's
been archived:

1. The shared `suite.db` row for that file persists with `status='sent'` and the
   post's `upload_date`. That row alone tells the next run "we've seen
   this post."
2. Each platform's extractor archive file (gallery-dl sqlite or
   yt-dlp txt) ALSO holds the post's canonical ID — so the extractor
   itself short-circuits the download before any I/O.
3. The checkpoint stores `date_floor = MAX(upload_date WHERE status='sent')`.
   The next run's `date-min` / `dateafter` is
   `date_floor - 1 day` (slack for timezones). Posts older than that
   are never fetched.

So even after months without running, with deletion on, the next
`archiver run` walks the user's timeline from "newest" until it hits
the saved `date_floor`, stops, and proceeds. No full-history re-walk,
no rate-limit risk.

`archiver stats` shows the current `date_floor` for every user — useful
for sanity-checking what "incremental from where?" means at any point.

## Self-healing behaviors

| Failure                                | Action                                                                       |
|----------------------------------------|------------------------------------------------------------------------------|
| TikTok / Instagram cookies expired     | Re-export from Firefox immediately, retry user once                          |
| TikTok / Instagram cookies stale (>N d)| Re-export pre-emptively at run start                                         |
| X cookies expired                      | Trip circuit, surface clear remediation, skip platform                       |
| Dispatcher send failure                 | Row remains pending/failed in `suite.db`; dispatcher handles retry/send state |
| Disk full during download              | Purge already-sent local files, retry once                                   |
| File vanished before queueing          | Mark as failed, continue                                                     |
| File still being written               | Stability check skips it; next reconcile catches it                          |
| Crashed mid-download                   | Next run's `reconcile` step catches orphaned files                           |
| Multiple consecutive auth fails        | Circuit breaker trips → skip platform for rest of run                        |
| Sidecar JSON malformed                 | Fall through to filename → mtime+hash                                        |
| Manual file with no sidecar / pattern  | Resolver assigns `manual_<hash>` identifier; uploaded normally                |
| Refusal-to-delete safety violation     | ERROR log; file NOT deleted (defense in depth against future regressions)    |

## Automation

Same as before. `archiver loop` for long-running mode, cron for fixed
schedules. See the loop subcommand's `--help` for flags.

```bash
caffeinate -d -i archiver loop              # macOS: also prevent sleep
tail -f .archiver/loop.log                  # watch loop health
tail -f .archiver/archiver.log              # watch what each run does
```

Cron alternative:
```cron
0 */6 * * * /Users/duynguyen/.local/bin/archiver run >> /tmp/archiver-cron.log 2>&1
```
