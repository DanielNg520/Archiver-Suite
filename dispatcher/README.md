# dispatcher

Telegram upload dispatcher. Owns the single Telegram session. Drains a
shared SQLite queue populated by `archiver` (priority 10) and `recorder`
(priority 20). One file at a time; FloodWait-aware; crash-safe via a
startup watchdog.

Architectural context: see `IMPLEMENTATION_GUIDE.md` in the media-archiver
project.

## Install

pipx-managed, no virtualenv:

```
cd ~/code/dispatcher
pipx install . --python 3.13
```

That puts `dispatcher` on your PATH via `~/.local/bin/dispatcher`, isolated
in its own venv at `~/.local/pipx/venvs/dispatcher/`.

After source edits, reinstall to pick them up:

```
pipx reinstall dispatcher --python 3.13
```

pipx does not support editable installs the way pip's `-e` flag does, so
the edit → reinstall loop is the supported workflow.

## First-run setup

```
mkdir -p ~/.config/dispatcher
cp .env.example ~/.config/dispatcher/.env
chmod 600 ~/.config/dispatcher/.env
```

Edit `~/.config/dispatcher/.env` to fill in `TELEGRAM_API_ID`,
`TELEGRAM_API_HASH`, `TELEGRAM_PHONE`, and `TELEGRAM_CHAT_ID`.

First time you run `dispatcher start`, Telethon will prompt for the SMS
auth code interactively and write a session file at
`~/.config/dispatcher/session`. After that, sessions persist.

## Commands

```
dispatcher start                 # foreground drain loop
dispatcher status                # queue counts + top pending
dispatcher queue list --status pending --limit 100
dispatcher queue retry <id>      # failed/sent -> pending
dispatcher queue cancel <id>     # pending/sending -> failed
dispatcher config show
```

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
├── db.py              # QueueDB: WAL, atomic claim, watchdog
├── send.py            # SendStrategy ABC + TelethonSendStrategy
├── drain.py           # the main loop (Template Method)
├── delete.py          # safety-gated cleanup after successful upload
├── policy_store.py    # TOML-backed PolicyStore (ported from archiver)
├── policies.py        # DeletePolicy (ported from archiver)
└── tg_router.py       # per-(platform,user) chat resolution (ported)
```
