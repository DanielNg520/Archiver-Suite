"""
dispatcher.config
─────────────────
Frozen-dataclass config, identical pattern to archiver.config:
  - Secrets (Telegram creds) in .env, loaded by python-dotenv
  - Behavior (retry policy, delete policy) in config.toml via PolicyStore

The PolicyStore reference IS mutable (the CLI writes to it). frozen=True
freezes the dataclass's references, not the referents — so the store
instance is fixed but its contents can be mutated through .set/.unset.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from core import PolicyStore, DEFAULT_DB_PATH, Sanitizer, ReloadingSanitizer
from core import env
from core.routing import parse_route

# Load dispatcher's own .env BEFORE any os.environ reads. This is a side
# effect on import; matches archiver's pattern. Test code that needs a
# different env should monkeypatch os.environ after import.
load_dotenv(Path.home() / ".config" / "dispatcher" / ".env")


# ── env-var primitives (shared parsing lives in core.env) ──────────────────

_req = env.req
_opt = env.opt


def banned_words_file_path() -> Path:
    """Path to the banned-word list the sanitizer reads (BANNED_WORDS_FILE, else
    ~/.config/dispatcher/banned_words.txt). One word per line, '#' comments. The
    single source of truth shared by config-load and the `banned-words` CLI."""
    return Path(_opt(
        "BANNED_WORDS_FILE",
        str(Path.home() / ".config" / "dispatcher" / "banned_words.txt")
    )).expanduser()


def session_name_or_default() -> str:
    """Session name as `dispatcher start` would resolve it, without requiring
    the full Telegram credentials. Lets read-only commands (status) locate the
    instance lock even when creds aren't loadable."""
    return _opt("TELEGRAM_SESSION",
                os.path.expanduser("~/.config/dispatcher/session"))


def dispatcher_env_path() -> Path:
    """The dispatcher's own .env file — the single store the `burner` CLI writes.
    Kept as a function (not a constant) so tests can point HOME elsewhere."""
    return Path.home() / ".config" / "dispatcher" / ".env"


def burner_session_name_or_default() -> str:
    """Where the burner session file lives: TELEGRAM_BURNER_SESSION if set, else
    a sibling of the primary session. Available without full creds so the
    `burner` CLI and `burner status` can locate/report it."""
    explicit = _opt("TELEGRAM_BURNER_SESSION")
    if explicit:
        return explicit
    return session_name_or_default() + "-burner"


def upsert_env_vars(path: Path, values: dict[str, str]) -> None:
    """Idempotently set KEY=VALUE lines in a dotenv file, preserving unrelated
    lines, comments, and ordering. A key already present is rewritten in place;
    a new key is appended. This is how the `burner` CLI persists registration —
    the ONLY writer of the burner env vars, so users never hand-edit .env.
    Also updates os.environ so the running process sees the change immediately."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    remaining = dict(values)
    out: list[str] = []
    for ln in lines:
        stripped = ln.strip()
        key = stripped.split("=", 1)[0].strip() if "=" in stripped else None
        if key in remaining and not stripped.startswith("#"):
            out.append(f"{key}={remaining.pop(key)}")
        else:
            out.append(ln)
    for key, val in remaining.items():
        out.append(f"{key}={val}")
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    os.environ.update(values)


# ── Telegram credentials ──────────────────────────────────────────────────

@dataclass(frozen=True)
class TelegramCreds:
    """Telethon MTProto credentials. Routing lives in tg_router, not here."""
    api_id:       int
    api_hash:     str
    phone:        str
    session_name: str

    @classmethod
    def from_env(cls) -> "TelegramCreds":
        session = session_name_or_default()
        os.makedirs(os.path.dirname(session) or ".", exist_ok=True)
        return cls(
            api_id       = int(_req("TELEGRAM_API_ID")),
            api_hash     = _req("TELEGRAM_API_HASH"),
            phone        = _req("TELEGRAM_PHONE"),
            session_name = session,
        )


# ── Optional burner account ────────────────────────────────────────────────

@dataclass(frozen=True)
class BurnerCreds:
    """A second, optional Telegram account. The burner is the SENDER for its
    dedicated chats (`chat_ids`); the primary account is the fallback for those
    chats and the default sender for everything else.

    Credentials mirror TelegramCreds. api_id/api_hash default to the primary's
    when their own vars are unset (the common case: one Telegram app, two
    logins) — only session_name and phone must differ. The feature is OFF (this
    returns None) unless BURNER_CHAT_IDS is non-empty AND a distinct burner
    session or phone is configured; an inert burner would just be a dead client.
    """
    api_id:       int
    api_hash:     str
    phone:        str
    session_name: str
    # Canonical (parse_route-normalized) chat_ids routed through the burner.
    chat_ids:     frozenset[str]

    def routes(self, chat_id: str) -> bool:
        """True iff `chat_id` (as it appears in items.chat_id) is dedicated to
        the burner. Compared against the normalized set, so dash-free/@handle
        input forms match the canonical value the dispatcher routes on."""
        return chat_id in self.chat_ids

    @classmethod
    def from_env(cls, primary: "TelegramCreds") -> "BurnerCreds | None":
        raw_chats = _opt("BURNER_CHAT_IDS")
        chat_ids = frozenset(
            r.chat_id
            for tok in raw_chats.replace(",", " ").split()
            if (r := parse_route(tok)) is not None
        )
        session = _opt("TELEGRAM_BURNER_SESSION")
        phone   = _opt("TELEGRAM_BURNER_PHONE")
        # Inert unless there is both something to route AND a distinct login.
        if not chat_ids or not (session or phone):
            return None
        if session:
            os.makedirs(os.path.dirname(session) or ".", exist_ok=True)
        else:
            session = primary.session_name + "-burner"
        return cls(
            api_id       = env.opt_int("TELEGRAM_BURNER_API_ID", primary.api_id),
            api_hash     = _opt("TELEGRAM_BURNER_API_HASH") or primary.api_hash,
            phone        = phone or primary.phone,
            session_name = session,
            chat_ids     = chat_ids,
        )


# ── Top-level config ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class DispatcherConfig:
    telegram:           TelegramCreds | None
    default_chat_id:    str | None
    db_path:            str
    policy_store:       PolicyStore
    # Banned-word sanitizer, applied to upload filenames + captions at send time.
    # Empty (no words / no file) → a no-op. Word list path via BANNED_WORDS_FILE
    # (default ~/.config/dispatcher/banned_words.txt): one word per line, '#'
    # comments allowed.
    sanitizer:          Sanitizer = field(default_factory=lambda: Sanitizer([]))
    # Optional second account. None ⇒ feature off (current behavior byte-for-byte).
    burner:             BurnerCreds | None = None
    poll_interval_s:    float = 2.0
    max_retries:        int   = 4
    retry_base_delay:   float = 2.0
    max_flood_wait_s:   int   = 600
    inter_album_sleep:  float = 2.0
    stuck_claim_min:    int   = 10    # watchdog threshold
    # Retention: auto-delete terminal 'failed' rows older than this many days
    # (e.g. tombstones for files deleted off disk and never restored). 0
    # disables. Tune via FAILED_RETENTION_DAYS in ~/.config/dispatcher/.env.
    failed_retention_days: float = 7.0
    # Stall watchdog: per-attempt send deadline = base + bytes/rate. Catches
    # silent TCP freezes (sleep/wake, VPN drop) that raise nothing and would
    # otherwise hang the serial drain loop forever. Tune via
    # STALL_BASE_TIMEOUT_S / STALL_MIN_RATE_KIB_S in .env.
    # The floor rate must sit WELL below the link's real worst case (observed
    # ~115 KiB/s through the VPN path) — a floor that's too optimistic kills
    # and re-uploads legitimately slow transfers from scratch.
    stall_base_timeout_s: float = 600.0
    stall_min_rate_kib_s: float = 64.0
    # Parallel upload: number of concurrent MTProto connections used to push a
    # big file's parts (the "FastTelethon" fan-out). 1 restores Telethon's stock
    # single-connection serial upload. Default 8 = fast_upload.MAX_CONNECTIONS,
    # the optimal-and-safe ceiling for heavy media: it saturates a fast uplink
    # (each connection is throttled by Telegram, so N connections ≈ N× until the
    # link's bandwidth caps), while staying within what the home DC tolerates on
    # one auth key — past 8 is diminishing returns + FloodWait/politeness risk,
    # which is why fast_upload caps there. Bounded memory regardless (≤ workers×2
    # parts of 512 KiB). Lower it via UPLOAD_CONNECTIONS in ~/.config/dispatcher/
    # .env on a constrained/metered link.
    upload_connections: int = 8
    # Fast video albums: build a video album from pre-uploaded documents
    # (fast_upload fan-out → messages.UploadMedia → SendMultiMedia) instead of
    # Telethon's native list send, which uploads every item SERIALLY. The big
    # win is split-original albums (parts of one oversize video) and any batch
    # of large clips: each item rides the same multi-connection fan-out that
    # single sends use. Falls back to the native list send when the Telethon
    # internals the fast path reaches into are absent. A per-item Telegram
    # rejection still surfaces as media_empty → the drain's per-item
    # recover_media_empty (which re-encodes) fires identically, so the
    # converter's fallback tier is unchanged. Disable with FAST_ALBUM=0 to pin
    # the native serial path.
    fast_album: bool = True

    @classmethod
    def load(cls, *, require_telegram: bool = True) -> "DispatcherConfig":
        """
        Build the full config from .env + config.toml.

        Crash loud on missing required values — this is run at startup,
        before the drain loop, so failing here is the right time to fail.
        """
        store = PolicyStore()
        default_db = os.path.expanduser(DEFAULT_DB_PATH)
        telegram = TelegramCreds.from_env() if require_telegram else None
        burner = BurnerCreds.from_env(telegram) if telegram is not None else None
        default_chat_id = _req("TELEGRAM_CHAT_ID") if require_telegram else None
        banned_words_file = banned_words_file_path()
        return cls(
            telegram          = telegram,
            burner            = burner,
            default_chat_id   = default_chat_id,
            db_path           = _opt("ARCHIVER_DB", _opt("DISPATCHER_DB", default_db)),
            policy_store      = store,
            sanitizer         = ReloadingSanitizer(banned_words_file),
            poll_interval_s   = env.opt_float("POLL_INTERVAL_S", 2.0, min_value=0.0),
            max_retries       = env.opt_int("MAX_RETRIES", 4, min_value=1),
            retry_base_delay  = env.opt_float("RETRY_BASE_DELAY", 2.0, min_value=0.0),
            max_flood_wait_s  = env.opt_int("MAX_FLOOD_WAIT_S", 600, min_value=0),
            inter_album_sleep = env.opt_float("INTER_ALBUM_SLEEP", 2.0, min_value=0.0),
            stuck_claim_min   = env.opt_int("STUCK_CLAIM_MIN", 10, min_value=1),
            failed_retention_days = env.opt_float("FAILED_RETENTION_DAYS", 7.0, min_value=0.0),
            stall_base_timeout_s  = env.opt_float("STALL_BASE_TIMEOUT_S", 600.0, min_value=1.0),
            stall_min_rate_kib_s  = env.opt_float("STALL_MIN_RATE_KIB_S", 64.0, min_value=1.0),
            upload_connections    = env.opt_int("UPLOAD_CONNECTIONS", 8, min_value=1),
            fast_album            = env.opt_bool("FAST_ALBUM", True),
        )

    def config_toml_path(self) -> Path:
        return self.policy_store.path

    def env_path(self) -> Path:
        return Path.home() / ".config" / "dispatcher" / ".env"
