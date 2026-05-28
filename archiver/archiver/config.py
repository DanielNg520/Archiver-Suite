"""
archiver.config
───────────────
Unified configuration. Two storage backends, with a clear split:

  .env (secrets):
      All credentials (API tokens, session paths, telegram chat IDs).
      Loaded by python-dotenv on import.

  config.toml (behavior):
      User lists per platform, plus behavior policies (delete-after-
      upload, dedup-after-download, ...) in a hierarchical structure
      that supports per-user overrides without env-var encoding hazards.
      Loaded by PolicyStore.

The split is deliberate:
  - Secrets benefit from .env's tight ecosystem (.gitignore, shell tools).
  - Behavior + user lists benefit from TOML's structured + unicode-safe
    representation. Usernames like "user.name!" or "正常用户" work natively.

Per-platform CONFIG SECTIONS are loaded only when both:
  (a) the platform is in ENABLED_PLATFORMS, and
  (b) at least one user is configured for it in config.toml.

This makes enable/disable a one-line toggle without scrubbing TOML.

Why frozen dataclasses?
  - Immutable: config can't be accidentally mutated mid-run.
  - Hashable for the value parts.
  - dataclasses.asdict() for diagnostics.

NOTE on frozen + PolicyStore:
  Config.policy_store IS a mutable object (the CLI mutates it via
  store.set/unset). frozen=True freezes the dataclass's REFERENCES,
  not the referents. So Config.policy_store always points at the
  same store instance, but that store's internal state can change.
  This is exactly what we want.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

from .policy_store import PolicyStore

load_dotenv(Path.home() / ".config" / "archiver" / ".env")


# ── env-var primitives (secrets only) ─────────────────────────────────────────

def _req(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        raise RuntimeError(
            f"Missing required env var: {key}. See .env.example."
        )
    return val


def _opt(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


# ── Telegram (always required) ────────────────────────────────────────────────

@dataclass(frozen=True)
class TelegramConfig:
    """
    Telethon MTProto config — credentials only. The chat_id here is the
    GLOBAL default. Per-platform / per-user overrides are still env vars
    (TELEGRAM_CHAT_ID_*) resolved by TelegramRouter at send time —
    chat IDs are always numeric or @username, so env-var encoding works
    cleanly for them.
    """
    api_id:       int
    api_hash:     str
    phone:        str
    chat_id:      str
    session_name: str = "archiver"

    @classmethod
    def from_env(cls) -> "TelegramConfig":
        default_session = os.path.expanduser("~/.config/archiver/session")
        os.makedirs(os.path.dirname(default_session), exist_ok=True)
        return cls(
            api_id       = int(_req("TELEGRAM_API_ID")),
            api_hash     = _req("TELEGRAM_API_HASH"),
            phone        = _req("TELEGRAM_PHONE"),
            chat_id      = _req("TELEGRAM_CHAT_ID"),
            session_name = _opt("TELEGRAM_SESSION", default_session),
        )


# ── X / Twitter ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class XConfig:
    users:      tuple[str, ...]
    auth_token: str
    ct0:        str
    twid:       str

    @classmethod
    def from_store(cls, store: PolicyStore) -> "XConfig":
        return cls(
            users      = store.list_users("x"),
            auth_token = _req("X_AUTH_TOKEN"),
            ct0        = _req("X_CT0"),
            twid       = _req("X_TWID"),
        )


# ── TikTok ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TikTokConfig:
    users:               tuple[str, ...]
    cookies_file:        str
    firefox_profile:     str
    cookie_refresh_days: float

    @classmethod
    def from_store(cls, store: PolicyStore) -> "TikTokConfig":
        return cls(
            users               = store.list_users("tiktok"),
            cookies_file        = _opt("TIKTOK_COOKIES_FILE", "./cookies/tiktok.txt"),
            firefox_profile     = _opt("FIREFOX_PROFILE", ""),
            cookie_refresh_days = float(_opt("COOKIE_REFRESH_DAYS", "3")),
        )


# ── Instagram ─────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class InstagramConfig:
    """
    Posts + reels only by default. Stories/highlights are opt-in via
    INSTAGRAM_INCLUDE — higher ban risk + they disappear, which
    complicates incremental-checkpoint logic.
    """
    users:               tuple[str, ...]
    cookies_file:        str
    firefox_profile:     str
    cookie_refresh_days: float
    include:             str

    _ALLOWED_INCLUDE = frozenset({
        "posts", "reels", "stories", "highlights", "tagged", "channel",
    })

    def __post_init__(self):
        for sub in (s.strip().lower() for s in self.include.split(",")):
            if sub and sub not in self._ALLOWED_INCLUDE:
                raise RuntimeError(
                    f"INSTAGRAM_INCLUDE: unknown subcategory '{sub}'. "
                    f"Allowed: {sorted(self._ALLOWED_INCLUDE)}"
                )

    @classmethod
    def from_store(cls, store: PolicyStore) -> "InstagramConfig":
        return cls(
            users               = store.list_users("instagram"),
            cookies_file        = _opt("INSTAGRAM_COOKIES_FILE", "./cookies/instagram.txt"),
            firefox_profile     = _opt("FIREFOX_PROFILE", ""),
            cookie_refresh_days = float(_opt("COOKIE_REFRESH_DAYS", "3")),
            include             = _opt("INSTAGRAM_INCLUDE", "posts,reels"),
        )


# ── Master config ─────────────────────────────────────────────────────────────

# eq=False on the master config to avoid the frozen-dataclass requirement
# that every field be hashable. PolicyStore wraps an RLock and isn't
# hashable. We don't use Config as a dict key anywhere — equality and
# hashing aren't useful here.
@dataclass(frozen=True, eq=False)
class Config:
    telegram:     TelegramConfig
    x:            XConfig         | None
    tiktok:       TikTokConfig    | None
    instagram:    InstagramConfig | None
    policy_store: PolicyStore

    output_dir: str = "./downloads"
    db_path:    str = "./.archiver/archive.db"
    log_file:   str = "./.archiver/archiver.log"
    state_dir:  str = "./.archiver"

    sleep_min: float = 3.0
    sleep_max: float = 8.0

    max_retries:       int   = 4
    retry_base_delay:  float = 2.0
    inter_album_sleep: float = 2.0

    auth_failure_threshold: int = 3

    use_dispatcher:     bool = True
    dispatcher_db_path: str  = "~/.config/dispatcher/dispatcher.db"

    enabled_platforms: frozenset[str] = field(default_factory=frozenset)

    @classmethod
    def load(cls) -> "Config":
        # 1. Build the store first — it sources user lists for the rest.
        store = PolicyStore()

        # 2. Resolve enabled platforms (env var; secrets-adjacent setting).
        enabled = frozenset(
            p.strip().lower()
            for p in _opt("ENABLED_PLATFORMS", "x,tiktok,instagram").split(",")
            if p.strip()
        )

        # 3. Build per-platform config blocks only for enabled platforms
        #    with at least one configured user in config.toml.
        x_cfg = None
        if "x" in enabled and store.list_users("x"):
            x_cfg = XConfig.from_store(store)

        tt_cfg = None
        if "tiktok" in enabled and store.list_users("tiktok"):
            tt_cfg = TikTokConfig.from_store(store)

        ig_cfg = None
        if "instagram" in enabled and store.list_users("instagram"):
            ig_cfg = InstagramConfig.from_store(store)

        if not (x_cfg or tt_cfg or ig_cfg):
            raise RuntimeError(
                "No platforms configured. Add users via "
                "`archiver config add --platform <x|tiktok|instagram> "
                "--user <name>`, and ensure ENABLED_PLATFORMS includes "
                f"that platform in .env. (config.toml: {store.path})"
            )

        return cls(
            telegram          = TelegramConfig.from_env(),
            x                 = x_cfg,
            tiktok            = tt_cfg,
            instagram         = ig_cfg,
            policy_store      = store,
            output_dir        = _opt("OUTPUT_DIR", "./downloads"),
            db_path           = _opt("DB_PATH",    "./.archiver/archive.db"),
            log_file          = _opt("LOG_FILE",   "./.archiver/archiver.log"),
            state_dir         = _opt("STATE_DIR",  "./.archiver"),
            sleep_min         = float(_opt("SLEEP_MIN", "3")),
            sleep_max         = float(_opt("SLEEP_MAX", "8")),
            use_dispatcher    = _opt("ARCHIVER_USE_DISPATCHER", "true").lower()
                                 in ("1", "true", "yes", "on"),
            dispatcher_db_path = _opt("DISPATCHER_DB",
                                      "~/.config/dispatcher/dispatcher.db"),
            enabled_platforms = enabled,
        )
