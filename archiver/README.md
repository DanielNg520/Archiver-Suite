# Media Archiver

Unified archiver for X (Twitter), TikTok, and Instagram → Telegram. One CLI,
one DB, one Telegram client per run, with self-healing.

Version 1.1 highlights:
- **Instagram support** (posts + reels by default; stories/highlights opt-in)
- **Per-platform Telegram channels** — each platform's media can go to its own chat
- **Per-(platform, user) `delete-after-upload`** — 3-level resolution chain
- **`bootstrap` subcommand** — absorb an existing on-disk media library
- **Manual media in subfolders** is auto-detected, uploaded, and (optionally) cleaned up
- **Checkpoint = `MAX(upload_date) WHERE sent=1`** — survives deletion, robust across long gaps
- **Sidecar-aware identity resolution** — sidecars (`.json` / `.info.json`) drive identifiers/dates/captions where available

## Layout

```
.
├── archiver/                       # The Python package
│   ├── config.py                   # Frozen dataclass config (X / TikTok / Instagram)
│   ├── db.py                       # Unified SQLite + auto-migration to checkpoints v2
│   ├── cookies.py                  # Firefox cookie export (TikTok + Instagram)
│   ├── identity.py                 # Sidecar → filename → hash resolver chain
│   ├── stability.py                # "Is this file safely closed?" check
│   ├── reconcile.py                # Reconcile v2 (recursive, sidecar-aware, archive-seeding)
│   ├── delete_policy.py            # 3-level delete-after-upload resolution
│   ├── tg_router.py                # 3-level Telegram destination resolution
│   ├── platforms.py                # Platform ABC + X / TikTok / Instagram strategies
│   ├── telegram.py                 # Persistent Telethon uploader (gated cleanup)
│   ├── orchestrator.py             # Template Method + circuit breaker + bootstrap
│   └── cli.py                      # Subcommand CLI
├── downloads/                      # Created at runtime
│   ├── x/<username>/...
│   ├── tiktok/<username>/...
│   └── instagram/<username>/...   ← manual files in subfolders also picked up
├── .archiver/                      # Hidden runtime state
│   ├── archive.db                  # SQLite (+ -wal / -shm)
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

**User config lives in `~/.config/archiver/.env`** (outside the project).

## First-time setup (new install)

```bash
pipx install . --python 3.13
mkdir -p ~/.config/archiver
cp .env.example ~/.config/archiver/.env
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
2. **Upload pass** sends them through Telegram normally (to the
   correct per-platform channel).
3. With delete-after-upload enabled, they're deleted just like
   extractor-downloaded files.

No special command needed; this is part of every run.

## Env vars

### Always required
```bash
TELEGRAM_API_ID=12345
TELEGRAM_API_HASH=abcd...
TELEGRAM_PHONE=+1234567890
TELEGRAM_CHAT_ID=-1001234567890      # global default destination
ENABLED_PLATFORMS=x,tiktok,instagram
```

### Per-platform Telegram channels (NEW, optional)
```bash
# Overrides the global TELEGRAM_CHAT_ID per platform.
TELEGRAM_CHAT_ID_X=-1001111111111
TELEGRAM_CHAT_ID_TIKTOK=-1002222222222
TELEGRAM_CHAT_ID_INSTAGRAM=-1003333333333

# Per-user override (rarely needed) — most specific wins:
# resolution order: user → platform → global
TELEGRAM_CHAT_ID_X_ALICE=-100xxx
```

Run `archiver chats` to see the resolved destination for every (platform, user).

### Delete after upload (3-level chain)
```bash
DELETE_AFTER_UPLOAD=false                  # global default
DELETE_AFTER_UPLOAD_INSTAGRAM=true         # per-platform
DELETE_AFTER_UPLOAD_X_ALICE=false          # per-user
```

Run `archiver policy` to see the resolved decision per (platform, user). Typo'd
env var names (e.g. `DELETE_AFTER_UPLOAD_X_ALCIE`) are detected and warned about at run start.

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
archiver run                                # everything, all platforms
archiver run --platform instagram           # one platform
archiver run --platform x --user alice      # one user

archiver bootstrap                          # one-shot import (existing archive)

archiver stats                              # totals + per-platform date_floor
archiver stats --platform tiktok --user u

archiver policy                             # resolved delete-after-upload per user
archiver chats                              # resolved Telegram destination per user

archiver health                             # check credentials
archiver reconcile                          # scan disk → DB (subset of `run`)

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
after successful upload. But the system still knows what's been
archived:

1. The DB row for that file persists with `telegram_sent=1` and the
   post's `upload_date`. That row alone tells the next run "we've seen
   this post."
2. Each platform's extractor archive file (gallery-dl sqlite or
   yt-dlp txt) ALSO holds the post's canonical ID — so the extractor
   itself short-circuits the download before any I/O.
3. The checkpoint stores `date_floor = MAX(upload_date WHERE
   telegram_sent=1)`. The next run's `date-min` / `dateafter` is
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
| Telegram FloodWait ≤ 10min             | Sleep exactly the server-requested duration, retry                           |
| Telegram FloodWait > 10min             | Bail this send; row stays pending for next run                               |
| Telegram transient send failure        | Exponential backoff (2/4/8/16s), then mark failed                            |
| Disk full during download              | Purge already-uploaded local files, retry once                               |
| File vanished before upload            | Mark as failed, continue                                                     |
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
