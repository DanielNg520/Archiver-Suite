# AUTOMATION.md — Running the suite unattended

This is the complete guide to automating all four processes via macOS
launchd, plus what each automated piece does and how to verify it.

> **Read this first.** You had a kernel panic (`lifs` filesystem driver,
> tag-check fault, triggered by python3.13) before this redesign. The new
> system runs THREE Python processes doing concurrent disk I/O — strictly
> more filesystem pressure than the single archiver that panicked you.
> The rollout below is therefore **staged**: one service under launchd at a
> time, with stability windows between. Do not skip to "load all three."

---

## What "automated" means here

Three launchd user agents, each defined by a plist in
`~/Library/LaunchAgents/`:

| Service | Label | What launchd does |
|---------|-------|-------------------|
| dispatcher | `com.duy.dispatcher` | starts at login, restarts on any exit (`KeepAlive=true`), drains the queue forever |
| recorder | `com.duy.recorder` | starts at login, restarts on any exit, watches for lives forever |
| archiver | `com.duy.archiver` | starts at login, runs `archiver loop` (download cycle → sleep 2–4h → repeat) PLUS a background ingest sweeper that enqueues drop-folder files every ~3 min; restarts only on *crash* (`KeepAlive{SuccessfulExit:false}`) |
| logrotate | `com.duy.logrotate` | a calendar job (not a daemon), daily 04:05, gzip-rotates `~/.local/log/*.log` so launchd's captured output can't grow unbounded |

`ops install` writes all four plists; `ops uninstall` removes them.
`ThrottleInterval=30` on the three services: if one crash-loops on startup, launchd
waits 30s between respawns so it can't fill your disk with crash logs before
you intervene.

`EnvironmentVariables.PATH` includes `/opt/homebrew/bin` because launchd does
NOT source your shell rc — without it, the recorder and archiver can't find
ffmpeg / yt-dlp / gallery-dl and fail silently.

---

## Prerequisites (all must be true before ANY launchd step)

Run from the suite root. Every check must pass.

```
# 1. All four installed, entry points resolve to ~/.local/bin
which dispatcher archiver recorder ops

# 2. All import cleanly
dispatcher --help >/dev/null && echo "dispatcher OK"
archiver   --help >/dev/null && echo "archiver OK"
recorder   --help >/dev/null && echo "recorder OK"
ops health        >/dev/null && echo "ops OK"

# 3. ffmpeg present (recorder + archiver need it)
which ffmpeg

# 4. The shared suite database can be initialized
PYTHONPATH=core python3.13 -c "from core import ItemStore; s=ItemStore.open(); s.close(); print('suite.db OK')"

# 5. The external output drive is mounted
ls /Volumes/StorEDGE/archiver_downloads >/dev/null && echo "drive OK"
```

If `which archiver` shows anything under `/opt/homebrew/bin`, the OLD shim is
shadowing the new one — remove it: `rm /opt/homebrew/bin/archiver`.

---

## Step 1 — Telegram auth (one time, interactive, BEFORE launchd)

launchd cannot type the SMS code. Authenticate the dispatcher's session by
hand once:

```
dispatcher start
```

Enter the code Telegram sends. When you see `telethon: connected` and it
idles on the queue, Ctrl-C. Confirm the session file exists:

```
ls ~/.config/dispatcher/session.session
```

The archiver has no Telegram session after the single-source migration. The
dispatcher owns Telegram credentials and routing.

**Optional burner account.** If you route some chats through a second (burner)
account, register it now too — its login is also interactive and launchd can't
type the code, so it must be done BEFORE the daemon runs headless:

```
dispatcher burner login --phone +49…     # interactive, one time
dispatcher burner chats add -100123      # chats the burner should send
dispatcher burner status                 # confirm authorized + chats
```

A burner that isn't logged in never blocks the daemon — those chats just fall
back to the primary. See **dispatcher/README.md**.

---

## Step 2 — Install the plists (log rotation is automatic)

`ops install` generates all four launchd plists with absolute paths and writes
them to `~/Library/LaunchAgents/`, then creates `~/.local/log`:

```
ops install
```

This does NOT start anything — the service plists activate only on
`launchctl load` (next step), which lets you stage the rollout. The
**logrotate** calendar job is the exception: it is harmless on its own and keeps
launchd's captured `~/.local/log/*.log` from growing unbounded (gzip, keep 7,
copytruncate so launchd's append fd is never orphaned). No per-package log
wiring is needed — `ops logrotate` is the single mechanism.

---

## Step 3 — STAGED ROLLOUT (the panic-aware part)

### Stage A — dispatcher only (lowest risk)

The dispatcher idle-polling an empty queue is near-zero disk load. Load it
alone and let it run while you keep using the archiver manually.

```
launchctl load ~/Library/LaunchAgents/com.duy.dispatcher.plist
ops health
```

Expect `dispatcher: running`. Now feed it manually:

```
archiver run          # downloads, inserts pending rows into suite.db, exits
```

Watch the dispatcher drain via `ops watch`. The dispatcher (launchd) and the
archiver (manual, one-shot) overlap only briefly during the enqueue write —
minimal concurrency.

**Run this way for at least 2–3 days.** If the machine stays up, proceed.
If it panics, you've isolated the trigger to the dispatcher's send path under
load, and we debug there before adding more.

### Stage B — add the archiver loop

Once Stage A is stable, hand the archiver to launchd too:

```
launchctl load ~/Library/LaunchAgents/com.duy.archiver.plist
ops health
```

Now dispatcher and archiver run concurrently on launchd's schedule. This is
the first sustained two-process concurrency. **Watch another 2–3 days.**

### Stage C — add the recorder

The recorder is the heaviest I/O (live video capture). Add it last:

```
launchctl load ~/Library/LaunchAgents/com.duy.recorder.plist
ops health
```

All three now run unattended. This is full automation.

> If you're confident the OS update (you're on 26.5, past the 25E build that
> panicked) fixed the `lifs` bug, you may compress A→C into one session. The
> staging is insurance, not dogma. But do at least one `ops health` between
> each load to confirm the new service came up before adding the next.

---

## Step 4 — Verify full automation

```
ops health
```

Expect all three `running`, queue `pending` trending toward 0, `tiktok.lock`
`not held` (unless recording), disk healthy. Then:

```
ops watch
```

Leave it open through one archiver cycle and confirm: archiver enqueues →
dispatcher sends → rows go `sent`. Walk away.

To confirm restart-on-crash works, kill the dispatcher and watch launchd
respawn it within ~30s:

```
launchctl kill SIGKILL gui/$(id -u)/com.duy.dispatcher
sleep 5 && ops health
```

---

## What each automated piece does, end to end

1. **At login**, launchd starts dispatcher, recorder, archiver.
2. **Dispatcher** connects to Telegram and begins polling `items`
   every 2s. On startup it runs the watchdog (reverts stuck `sending` rows).
3. **Archiver** runs a cycle: for each configured user on each platform, it
   downloads new media and inserts pending `items` rows (priority 10). Then it
   sleeps 2–4h and repeats. If the recorder holds the TikTok lock, it skips
   TikTok downloads that cycle.
   - **Ingest sweeper (background, every ~3 min).** The heavy download cycle
     above only reconciles the *drop folders* (record folder, orphaned chat_id
     dirs, local platforms) at its tail — hours apart, and never mid-download.
     So `archiver loop` also runs a background thread that sweeps just those
     folders every `--ingest-interval` seconds (default 180, min 30; `0`
     disables) on its own DB connection, decoupled from downloads. A
     hand-dropped file is enqueued within minutes instead of waiting for the
     next full cycle. It shares a lock with the heavy run so the two never
     prep/split the same file at once; the heavy run remains a backstop if the
     thread ever dies.
4. **Recorder** polls its TikTok user list every 60s. When someone's live, it
   acquires the lock, records with yt-dlp until the stream ends, releases the
   lock, enqueues the file (priority 5), and re-scans.
5. **Dispatcher** claims queued rows in priority order: recorder (5), chat_id
   folders (6), then normal archive media (10). It sends each to
   the Telegram chat resolved for that platform/user, marks `sent`, and (if
   the delete policy is on) removes the local file + sidecars.
6. **Auto-ban of gone accounts** — when an archiver cycle's extractor reports
   that an account itself is gone (suspended/banned/deleted, distinct from our
   cookies expiring), the archiver moves that user off the active list into a
   per-platform banned roster (`config.toml`) and prints a "Banned / deleted
   accounts this run" block at the end of the run. Already-queued uploads for
   that user still deliver. Inspect with `archiver banned list`; restore with
   `archiver banned unban --platform <p> --user <u> --re-add`. Detection is
   deliberately conservative (auth failures and per-item 404s never ban), so a
   live account is never retired by mistake.
7. **You** run `ops health` whenever you want to check, and consult
   `RUNBOOK.md` if something breaks.

---

## Managing users and policies (no restart needed)

```
# Archiver VOD users
archiver config add --platform x --user someone
archiver config list

# Recorder live users (order = priority)
recorder config add --user tiktoker
recorder config list
recorder config priority --user tiktoker --rank 1

# Delete-after-upload (dispatcher honors this)
archiver policy set --delete true --platform tiktok

# Local platforms (hand-managed folders, no download)
archiver local add mylibrary

# Per-platform download toggle (off = reconcile/upload only)
archiver download set --platform instagram --enabled false

# Auto-ingest chat_id folders each cycle (default off)
archiver auto-ingest set --enabled true

# Banned/deleted accounts (auto-retired during runs)
archiver banned list
archiver banned unban --platform x --user someone --re-add

# Upload batching (dispatcher; restart it to apply)
dispatcher config set min_batch_size 10
```

Archiver-side settings (users, local platforms, auto-ingest, download toggle,
delete *policy*) are read on the next cycle — no reload. The **dispatcher**
reads its policies (delete, min-batch) at startup, so `ops restart dispatcher`
after changing those.

> **Note on batching:** with the default `min_batch_size = 10`, platform albums
> are held until 10 files accumulate (or 7 days). Set `min_batch_size 1` to send
> immediately. Recorder and chat_id uploads are never held.

---

## Turning it off

```
ops unload            # stops all three, removes from launchd
```

To stop just one: `launchctl unload ~/Library/LaunchAgents/com.duy.<svc>.plist`.

There is no direct-send rollback path after this migration. If the
dispatcher is unhealthy, stop the services, fix or re-authenticate the
dispatcher, and let the durable `pending` rows drain when it is healthy.
