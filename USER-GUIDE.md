# User Guide

Task-oriented reference for daily use. Architecture is in **README.md**;
upgrading is in **UPGRADE.md**.

The model in one line: **producers write file-rows into one `suite.db`; the
dispatcher uploads them to Telegram.** You mostly drop files in the right place
and let it run.

---

## The ways content gets uploaded

| You want to… | Put files here | Routed to |
|---|---|---|
| Archive a social account | (downloaded) `output_dir/<platform>/<user>/` | per-platform/user chat |
| Capture a TikTok live | (recorded automatically) | TikTok-live chat |
| Send a hand-managed library like a platform | `output_dir/<localname>/<user>/` after `archiver local add` | per-`localname`/user chat |
| Keep a built-in platform but manage files yourself | `output_dir/instagram/<user>/` + `archiver download set --platform instagram --enabled false` | that platform's chat |
| Send loose files to a specific channel | `output_dir/<chat_id>/…` | that chat_id |

---

## Platforms (downloaded)

```bash
archiver config add --platform instagram --user someone
archiver start                # run continuously (was: loop)
archiver start --once         # single cycle (was: run)
```

Platform uploads **batch**: an album is held until 10 items accumulate (or 7
days), then sent. Tune with `dispatcher config set min_batch_size`.

### Download off, still upload (manual backup of a platform)

```bash
archiver download set --platform instagram --enabled false   # stop fetching
archiver download                       # show resolved on/off per platform
archiver download unset --platform instagram   # back to default (on)
```
With download off, every run still walks `output_dir/instagram/` and uploads
everything — configured users, disk-discovered users, and loose root files —
and needs no cookies. (Keep `instagram` in `ENABLED_PLATFORMS` with ≥1
configured user so the platform still exists.)

## Local platforms (no download, you manage the files)

```bash
archiver local add mylibrary       # then drop files under output_dir/mylibrary/<username>/
archiver local list
archiver local remove mylibrary    # files on disk kept
```
Each subfolder is a username; routed via `TELEGRAM_CHAT_ID_MYLIBRARY[_<USER>]`.

## Loose files → a specific chat (chat_id folders)

```
output_dir/-1001234567890/holiday clip.mp4      → sent individually, caption "holiday clip"
output_dir/-1001234567890/Beach day/John.jpg    → album, caption "Beach day\nJohn\nJess"
                         /Beach day/Jess.jpg
```
- **Directly in the chat_id folder** → one message each, filename as caption.
- **In a subfolder** → one album per subfolder, subfolder name + filenames.

```bash
archiver ingest                                  # scan chat_id folders under output_dir
archiver ingest --path "/any/folder" --chat -100123   # ingest an arbitrary folder
archiver auto-ingest set --enabled true          # do it automatically every cycle
```
A top-level folder that's neither a known/local platform nor a valid chat_id is
skipped with a warning — never guessed.

---

## Dedup & cleanup (automatic)

- **No duplicate is ever uploaded.** Every file is content-hashed; if those
  bytes were already sent, the dispatcher suppresses the copy and deletes it.
- **Move an old, already-uploaded file back in** → recognized by content (even
  renamed) and deleted from disk instead of re-uploaded.
- **One-time:** `archiver backfill` after upgrading so this covers pre-upgrade
  files.

## Delete-after-upload

```bash
archiver policy set --delete true                    # global ON
archiver policy set --platform x --delete false      # …except X
dispatcher config set delete_after_upload true --platform orphaned   # chat_id folders
dispatcher config set delete_after_upload_records true               # live recordings
```
Restart the dispatcher after changing delete/batch policies.

---

## Inspecting & fixing the queue

```bash
<app> stats                          # DB counts (archiver/dispatcher/recorder)
dispatcher queue list --status failed --limit 100
dispatcher queue retry <id>          # failed/sent → pending
dispatcher queue cancel <id>         # pending/sending → failed
archiver reset failed                # re-queue all failed
archiver reset uploads --platform x  # re-send everything (no re-download)
```

## Settings

```bash
dispatcher config set <key> <value> [--platform P] [--user U]
dispatcher config get <key> [--platform P]
dispatcher config list
```
Common keys: `min_batch_size`, `min_batch_max_wait_h`, `delete_after_upload`,
`delete_after_upload_records`, `dedup_after_download`, `auto_ingest_orphaned`,
`download_enabled`, `local_platforms`.

---

## "Why isn't my file uploading?"

1. **Platform file, <10 pending?** Batching — waits for 10 or 7 days.
   `dispatcher config set min_batch_size 1` to send now.
2. **Duplicate?** If its bytes were already sent, it's suppressed by design
   (and the copy deleted). Check the dispatcher log for "suppressed as
   duplicate".
3. **Loose folder not a chat_id?** Folders that aren't a chat_id or known
   platform are skipped — rename to the chat_id or use `ingest --path … --chat`.
4. **Dispatcher running?** The queue is durable; rows wait at `pending`.
