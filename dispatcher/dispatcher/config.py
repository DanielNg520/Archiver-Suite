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
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from core import PolicyStore, DEFAULT_DB_PATH

# Load dispatcher's own .env BEFORE any os.environ reads. This is a side
# effect on import; matches archiver's pattern. Test code that needs a
# different env should monkeypatch os.environ after import.
load_dotenv(Path.home() / ".config" / "dispatcher" / ".env")


# ── env-var primitives ────────────────────────────────────────────────────

def _req(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        raise RuntimeError(
            f"Missing required env var: {key}. See .env.example."
        )
    return val


def _opt(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


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
        default_session = os.path.expanduser("~/.config/dispatcher/session")
        os.makedirs(os.path.dirname(default_session), exist_ok=True)
        return cls(
            api_id       = int(_req("TELEGRAM_API_ID")),
            api_hash     = _req("TELEGRAM_API_HASH"),
            phone        = _req("TELEGRAM_PHONE"),
            session_name = _opt("TELEGRAM_SESSION", default_session),
        )


# ── Top-level config ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class DispatcherConfig:
    telegram:           TelegramCreds | None
    default_chat_id:    str | None
    db_path:            str
    policy_store:       PolicyStore
    poll_interval_s:    float = 2.0
    max_retries:        int   = 4
    retry_base_delay:   float = 2.0
    max_flood_wait_s:   int   = 600
    inter_album_sleep:  float = 2.0
    stuck_claim_min:    int   = 10    # watchdog threshold

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
        default_chat_id = _req("TELEGRAM_CHAT_ID") if require_telegram else None
        return cls(
            telegram          = telegram,
            default_chat_id   = default_chat_id,
            db_path           = _opt("ARCHIVER_DB", _opt("DISPATCHER_DB", default_db)),
            policy_store      = store,
            poll_interval_s   = float(_opt("POLL_INTERVAL_S", "2.0")),
            max_retries       = int(_opt("MAX_RETRIES", "4")),
            retry_base_delay  = float(_opt("RETRY_BASE_DELAY", "2.0")),
            max_flood_wait_s  = int(_opt("MAX_FLOOD_WAIT_S", "600")),
            inter_album_sleep = float(_opt("INTER_ALBUM_SLEEP", "2.0")),
            stuck_claim_min   = int(_opt("STUCK_CLAIM_MIN", "10")),
        )

    def config_toml_path(self) -> Path:
        return self.policy_store.path

    def env_path(self) -> Path:
        return Path.home() / ".config" / "dispatcher" / ".env"
