# RUNBOOK — archiver / recorder / dispatcher

Operations reference for the three-process system. Keep this where you'll
find it at 2am.

```
recorder ──┐
           ├──→ suite.db ──→ dispatcher ──→ Telegram
archiver ──┘
```

All three run under launchd as user agents (`com.duy.*`). Manage them with
the `ops` tool, not raw launchctl, unless debugging launchd itself.

---

## Quick reference

```
ops health                 # is everything alive? queue depth? disk?
ops watch                  # same, auto-refreshing
ops load                   # load all three launchd jobs
ops unload                 # stop all three
ops restart dispatcher     # restart one (dispatcher|recorder|archiver)
```

Log locations:
- App logs (rotated, 50MB×5): `~/.local/log/{dispatcher,recorder,archiver}.log`
- launchd crash catchers (NOT rotated): `~/.local/log/{name}.{out,err}.log`

When something's wrong, read the app log first; fall back to the `.err`
log for crashes that happened before logging started.

---

## First-time install order

Install order no longer matters: the shared `core` package owns the schema
and initializes `suite.db` idempotently.

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

Authenticate Telegram ONCE interactively before handing control to launchd
(launchd can't answer the SMS prompt):

```
dispatcher start          # complete SMS auth, see "connected", then Ctrl-C
```

Copy plists and load:

```
cp ~/code/ops/launchd/com.duy.*.plist ~/Library/LaunchAgents/
ops load
ops health
```

---

## Telegram session died (dispatcher can't send)

Symptom: `ops health` shows dispatcher running but queue `pending` only
climbs, never `sent`. App log shows auth/session errors.

Telethon sessions expire or get invalidated (password change, Telegram
logout-all-devices, too-long offline). Re-auth is interactive, so launchd
must be out of the way:

```
ops unload                                  # or: launchctl unload the dispatcher plist
dispatcher start                            # complete the SMS code prompt
# wait for "connected", confirm a test send drains, then Ctrl-C
ops load
```

The queue is durable — nothing is lost while the session is down. Jobs
just wait at `pending` until the dispatcher can send again.

---

## TikTok cookies expired (recorder stops detecting / archiver TikTok fails)

Symptom: recorder never finds anyone live even when they are; or archiver
TikTok health check fails. Cookies from your browser go stale.

```
archiver cookies refresh                    # pulls fresh cookies from Firefox
```

The recorder reads `TIKTOK_COOKIES_FILE` from `~/.config/recorder/.env`.
If you symlinked it to archiver's cookies, the refresh covers both. If
not, refresh/copy the recorder's copy too. Then:

```
ops restart recorder
```

---

## suite.db is corrupt

Symptom: dispatcher crashes on startup with `database disk image is
malformed`, or `ops health` can't read queue counts.

SQLite WAL corruption is rare but recoverable. Stop everything that writes
first (all three), then attempt recovery:

```
ops unload
cd ~/.config/archiver-suite
sqlite3 suite.db ".recover" | sqlite3 suite_recovered.db
mv suite.db suite.db.corrupt
mv suite_recovered.db suite.db
ops load
ops health
```

If `.recover` fails entirely: keep `suite.db.corrupt`, recreate an empty
`suite.db`, then run `archiver bootstrap`/`archiver reconcile` for configured
users to register files still on disk. You may need to retry or re-send some
items manually, but the media files remain the durable source.

```
ops unload
rm ~/.config/archiver-suite/suite.db*       # removes -wal and -shm too
ops load
```

---

## Recorder is stuck (recording that never ends, or won't pick up new lives)

Symptom: `tiktok.lock` shows HELD for hours, recorder pid alive, no new
files appearing.

A yt-dlp capture can hang if a stream half-dies (socket open, no data).
Restart the recorder — it terminates the capture cleanly and releases the
lock on shutdown:

```
ops restart recorder
```

If the lock is STILL held after restart (stale lock — recorder was
SIGKILLed previously and `__exit__` never ran):

```
cat ~/.config/archiver-suite/locks/tiktok.lock     # check the pid inside
# if that pid is dead:
rm ~/.config/archiver-suite/locks/tiktok.lock
```

The archiver only skips TikTok *downloads* while this lock exists; a stale
lock silently blocks TikTok archiving, so clear it promptly. (The pid
field exists precisely so you can verify it's stale before removing.)

---

## Drain the queue manually (dispatcher won't start at all)

If the dispatcher is broken but you need files sent, there is no manual
send path by design (the dispatcher is the only process that talks to Telegram).
The correct move is to fix the dispatcher, not bypass it. To inspect what's
stuck while you debug:

```
dispatcher status                            # counts + top pending
dispatcher queue list --status failed --limit 100
dispatcher queue retry <id>                  # reset a failed row to pending
dispatcher queue cancel <id>                 # give up on a row
```

There is no direct-upload fallback after the single-source migration. The
dispatcher owns the Telegram session(s) and routing. If it is down, keep
the durable rows in `pending`, fix or re-authenticate the dispatcher, then
let it drain normally. (If a burner account is registered, the primary is
already the fallback for its chats — a logged-out burner never wedges the
queue; see dispatcher/README.md.)

---

## Uploads look "stuck" but nothing is failing (min-batch holding)

Symptom: `dispatcher stats` shows `pending` flat, none going to `failed`,
dispatcher healthy. Usually the **min-batch gate**: platform albums are held
until `min_batch_size` (default 10) files accumulate, or `min_batch_max_wait_h`
(default 168h = 7 days) elapses.

```
dispatcher config set min_batch_size 1          # send whatever's pending now
ops restart dispatcher                          # policies read at startup
```

Recorder (live) and chat_id (orphaned) rows are exempt and never held — if those
aren't draining, it's a real problem (session/route), not batching.

---

## A file vanished without uploading (dedup suppression)

If a file you dropped in is gone with no new Telegram message, and its **bytes
were already sent**, this is by design: the dispatcher suppresses the duplicate
and deletes it. The log shows `suppressed as duplicate of id=…`. Only the
redundant copy is removed; the originally-sent file is untouched.

---

## Disk filling up

`ops health` shows the free-space figure. If it's getting tight:

- Confirm `delete_after_upload` policy is ON for the users you don't want
  to keep locally (`archiver policy` / dispatcher delete policy).
- The archiver self-purges already-sent files on ENOSPC, but that's a
  last resort, not a strategy.
- Recorder output (`~/recorder-output`) is NOT auto-deleted unless the
  dispatcher's delete policy covers `source=recorder` files. Check there
  first — live recordings are large.

---

## Sanity checklist after any intervention

```
ops health
```

Expect: all three `running`, queue `pending` trending toward 0, lock
`not held` (unless actively recording), disk healthy. Watch one drain
cycle with `ops watch` before walking away.
