"""
recorder.config
───────────────
Frozen-dataclass config, same split as archiver: secrets in .env,
behavior + the priority-ordered user list in config.toml.

The TikTok user list order IS the priority order — index 0 is highest
priority. The recorder records one stream at a time and, between
recordings, re-scans this list top-to-bottom.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path.home() / ".config" / "recorder" / ".env")

CONFIG_TOML = Path.home() / ".config" / "recorder" / "config.toml"


def _opt(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


@dataclass(frozen=True)
class RecorderConfig:
    poll_interval_s:     float
    dispatcher_db_path:  str
    output_dir:          str
    state_dir:           str
    lock_path:           str
    tiktok_users:        tuple[str, ...]
    tiktok_cookies_file: str | None

    @classmethod
    def load(cls) -> "RecorderConfig":
        toml_data: dict = {}
        if CONFIG_TOML.exists():
            with CONFIG_TOML.open("rb") as f:
                toml_data = tomllib.load(f)

        rec = toml_data.get("recorder", {})
        tt  = rec.get("tiktok", {})
        users = tuple(tt.get("users", []))

        cookies = _opt("TIKTOK_COOKIES_FILE") or None

        return cls(
            poll_interval_s     = float(rec.get("poll_interval_s",
                                       _opt("POLL_INTERVAL_S", "60"))),
            dispatcher_db_path  = _opt("DISPATCHER_DB",
                                       "~/.config/dispatcher/dispatcher.db"),
            output_dir          = rec.get("output_dir",
                                       _opt("OUTPUT_DIR",
                                            os.path.expanduser("~/recorder-output"))),
            state_dir           = _opt("STATE_DIR",
                                       os.path.expanduser("~/.recorder")),
            lock_path           = _opt("LOCK_PATH",
                                       os.path.expanduser(
                                           "~/.config/archiver/locks/tiktok.lock")),
            tiktok_users        = users,
            tiktok_cookies_file = cookies,
        )
