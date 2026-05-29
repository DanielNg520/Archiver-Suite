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
| archiver | `com.duy.archiver` | starts at login, runs `archiver loop` (cycle → sleep 2–4h → repeat); restarts only on *crash* (`KeepAlive{SuccessfulExit:false}`) |

`ThrottleInterval=30` on all three: if one crash-loops on startup, launchd
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

# 4. The archiver dispatcher flag is set
grep -E "USE_DISPATCHER|DISPATCHER_DB" ~/.config/archiver/.env

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

The archiver only needs its own session if you ever use the legacy fallback
(`ARCHIVER_USE_DISPATCHER=false`). In dispatcher mode it opens no session.

---

## Step 2 — Wire rotating logs (optional but recommended)

Without this, the only logs are launchd's `.out`/`.err` files, which grow
**unbounded**. The fix: each service uses a `RotatingFileHandler` (50 MB × 5).

`log_setup.py` is a vendorable file. Copy it into each package and call it:

```
cp ops/log_setup.py dispatcher/dispatcher/log_setup.py
cp ops/log_setup.py recorder/recorder/log_setup.py
cp ops/log_setup.py archiver/archiver/log_setup.py
```

Then in each package's `cli.py`, replace the `logging.basicConfig(...)` call
inside `main()` (or `_setup_logging`) with:

```python
from .log_setup import setup_file_logging
setup_file_logging("dispatcher", verbose=args.verbose)   # name per package
```

Use the matching name: `"dispatcher"`, `"recorder"`, `"archiver"`. Then
reinstall each:

```
pipx reinstall dispatcher --python 3.13
pipx reinstall recorder --python 3.13
pipx reinstall media-archiver --python 3.13
```

You can defer this and do it after the staged rollout — it's not required for
correctness, only for log hygiene.

---

## Step 3 — Create the log directory and install plists

```
mkdir -p ~/.local/log
cp ops/launchd/com.duy.dispatcher.plist ~/Library/LaunchAgents/
cp ops/launchd/com.duy.recorder.plist   ~/Library/LaunchAgents/
cp ops/launchd/com.duy.archiver.plist   ~/Library/LaunchAgents/
```

Copying the plists does NOT start anything. They activate only on
`launchctl load`. This lets you stage the rollout below.

---

## Step 4 — STAGED ROLLOUT (the panic-aware part)

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

## Step 5 — Verify full automation

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
4. **Recorder** polls its TikTok user list every 60s. When someone's live, it
   acquires the lock, records with yt-dlp until the stream ends, releases the
   lock, enqueues the file (priority 20), and re-scans.
5. **Dispatcher** claims queued rows (priority 10 before 20), sends each to
   the Telegram chat resolved for that platform/user, marks `sent`, and (if
   the delete policy is on) removes the local file + sidecars.
6. **You** run `ops health` whenever you want to check, and consult
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
```

Config changes are read on the next cycle/poll — no reload required.

---

## Turning it off

```
ops unload            # stops all three, removes from launchd
```

To stop just one: `launchctl unload ~/Library/LaunchAgents/com.duy.<svc>.plist`.

To temporarily revert the archiver to direct-send (bypassing the queue):

```
# in ~/.config/archiver/.env
ARCHIVER_USE_DISPATCHER=false
launchctl kickstart -k gui/$(id -u)/com.duy.archiver
```

Set it back to `true` once the dispatcher is healthy again.
