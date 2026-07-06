# dispatcher

Telegram upload dispatcher. The only process that talks to Telegram; owns the
primary session and an optional second *burner* account (see below). Drains
pending rows from the shared `suite.db` / `items` table populated by
`recorder` (priority 5), chat_id folders (priority 6), and `archiver`
(priority 10). One file at a time;
FloodWait-aware; crash-safe via a startup watchdog.

Architectural context: see the root `README.md` and `DESIGN.md`.

## Install

pipx-managed, with the shared `core` package injected into the isolated
dispatcher venv:

```
pipx install ./dispatcher --python 3.13
pipx inject --editable dispatcher ./core
```

That puts `dispatcher` on your PATH via `~/.local/bin/dispatcher`, isolated
in its own venv at `~/.local/pipx/venvs/dispatcher/`.

`hachoir` is a declared dependency and installs automatically — it is Telethon's
video-metadata backend. Without it, native album sends emit a degenerate 1×1/0s
video attribute and Telegram renders every album video as a static image, so the
dispatcher **refuses to start** if it's missing (`pipx inject dispatcher hachoir`
to repair an old venv).

After dispatcher source edits, reinstall to pick them up:

```
pipx reinstall dispatcher --python 3.13
```

Edits to `core` are picked up immediately because it is injected editable.

## First-run setup

```
mkdir -p ~/.config/dispatcher
cp .env.example ~/.config/dispatcher/.env
chmod 600 ~/.config/dispatcher/.env
```

Edit `~/.config/dispatcher/.env` to fill in `TELEGRAM_API_ID`,
`TELEGRAM_API_HASH`, `TELEGRAM_PHONE`, and `TELEGRAM_CHAT_ID`.

Optional Telegram routing overrides live in the same file. TikTok videos use
`TELEGRAM_CHAT_ID_TIKTOK`; TikTok live recordings produced by the recorder
use `TELEGRAM_CHAT_ID_TIKTOK_LIVE`, or
`TELEGRAM_CHAT_ID_TIKTOK_LIVE_<USER>` for a single recorded account.

First time you run `dispatcher start`, Telethon will prompt for the SMS
auth code interactively and write a session file at
`~/.config/dispatcher/session`. After that, sessions persist.

## Commands

```
dispatcher start                 # foreground drain loop
dispatcher status                # process/queue health
dispatcher stats                 # DB counts (pending/sending/sent/failed)
dispatcher queue list --status pending --limit 100
dispatcher queue retry <id>      # failed/sent -> pending
dispatcher queue cancel <id>     # pending/sending -> failed
dispatcher config show           # effective .env + paths
dispatcher config get  <key> [--platform P] [--user U]
dispatcher config set  <key> <value> [--platform P] [--user U]
dispatcher config unset <key> [--platform P] [--user U]
dispatcher config list           # all scoped overrides
dispatcher burner login          # register the optional 2nd (burner) account
dispatcher burner chats add <id…># route those chats via the burner
dispatcher burner chats list     # (remove <id…> also)
dispatcher burner status         # show burner config without connecting
```

### Optional burner account

A second, **entirely optional** Telegram account that becomes the sender for a
dedicated set of chats; the primary account stays the sender for everything else
and is the **fallback** for the burner's chats if the burner can't come up. When
no burner is registered nothing changes — the pipeline is byte-for-byte the
single-account path.

Set it up through the CLI only (no hand-editing `.env`):

```
dispatcher burner login --phone +49…          # interactive; creates the session
dispatcher burner chats add -100123 100456    # dash-free numeric ok → -100456
dispatcher burner status                       # verify active + authorized
```

`login` reuses the primary's `TELEGRAM_API_ID`/`TELEGRAM_API_HASH` unless you
pass `--api-id`/`--api-hash`, and persists `TELEGRAM_BURNER_SESSION` /
`TELEGRAM_BURNER_PHONE` / `BURNER_CHAT_IDS` to the dispatcher `.env`. The burner
client is built **lazily** on the first send to one of its chats, so a
misconfigured or logged-out burner never blocks startup or any primary send —
it just logs a warning and the send goes out on the primary. **Restart the
dispatcher** after registering, so `start` picks the burner up.

### Queue-shaping behaviors (in the drain loop)

- **Global content dedup.** Each claimed row is checked against an indexed
  `content_hash`; if those bytes were already `sent`, the row is suppressed and
  the redundant file is **deleted unconditionally** (independent of
  `delete_after_upload`). Cleans up re-introduced already-uploaded files.
- **Minimum-batch gate** (platform / `source=archiver` only). An album is held
  until `min_batch_size` items (default 10) accumulate in the same
  user+media-bucket; a partial flushes after `min_batch_max_wait_h` (default
  168 = 7 days). Recorder and orphaned rows are exempt.
  ```
  dispatcher config set min_batch_size 10        # or 1 to disable
  dispatcher config set min_batch_max_wait_h 168 --platform x
  ```
- **chat_id routing.** Rows from `output_dir/<chat_id>/…` carry an explicit
  `chat_id` and route there; an unresolvable chat_id fails the batch cleanly.

Policies are read at startup — **restart the dispatcher** after changing them.

## Smoke test (no archiver involvement)

```
dispatcher status

sqlite3 ~/.config/archiver-suite/suite.db
```

Then in the sqlite shell:

```
INSERT INTO items
  (source, platform, username, identifier, file_path, discovered_at, status, priority, attempts)
VALUES
  ('test', 'x', 'testuser', 'manual_smoke', '/tmp/test_image.jpg',
   strftime('%Y-%m-%dT%H:%M:%SZ','now'), 'pending', 10, 0);
.quit
```

Drop a real image at `/tmp/test_image.jpg`, then:

```
dispatcher start
```

You should see the file get picked up, uploaded, and marked sent.

## Failure modes to verify

- Ctrl-C mid-send. Restart `dispatcher start`. Watchdog should reset the
  stuck `sending` row back to `pending`. Note: a duplicate upload is
  possible if the crash happened after the Telegram send-success but
  before `mark_sent` committed. Accepted tradeoff (see drain.py).
- Insert a row with a non-existent file path. After max_retries it should
  end up in `failed` status with a clear `last_error`.
- Insert rows at different priorities — drain order is priority ASC,
  then discovered_at ASC.

## Files

```
dispatcher/
├── __init__.py
├── __main__.py        # python -m dispatcher
├── cli.py             # argparse entry point
├── config.py          # frozen-dataclass config + .env loading
├── send.py            # SendStrategy ABC + TelethonSendStrategy
├── drain.py           # the main loop (Template Method)
└── delete.py          # safety-gated cleanup after successful upload

../core/core/
├── store.py           # ItemStore: WAL, atomic claim, watchdog, status changes
├── schema.py          # shared suite.db schema
├── policy_store.py    # TOML-backed PolicyStore
└── policies.py        # DeletePolicy / DedupPolicy
```
