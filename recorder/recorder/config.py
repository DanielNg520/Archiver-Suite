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

from core import db_path as _core_db_path

load_dotenv(Path.home() / ".config" / "recorder" / ".env")

CONFIG_TOML = Path.home() / ".config" / "recorder" / "config.toml"


def _opt(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


@dataclass(frozen=True)
class RecorderConfig:
    poll_interval_s:     float
    db_path:             str
    output_dir:          str
    state_dir:           str
    lock_path:           str
    tiktok_users:        tuple[str, ...]
    tiktok_cookies_file: str | None
    # ── Reconnect-on-premature-exit (see state._wait_for_recording_done) ──
    # yt-dlp can exit while the broadcast is still live (rotated m3u8 URL,
    # expired token, transient ffmpeg input error). Rather than finalize a
    # truncated recording, re-confirm liveness and relaunch on a fresh URL.
    reconnect_enabled:        bool  = True
    live_confirm_samples:     int   = 3     # is_live() polls to confirm still-live
    live_confirm_interval_s:  float = 2.0   # gap between those polls
    reconnect_backoff_base_s: float = 2.0   # backoff = base·2^streak, capped 30s
    max_zero_byte_reconnects: int   = 3     # consecutive no-data reconnects → stop
    max_session_minutes:      float = 0.0   # 0 = no cap on total session length

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
            db_path             = str(_core_db_path()),
            output_dir          = rec.get("output_dir",
                                       _opt("OUTPUT_DIR",
                                            os.path.expanduser("~/recorder-output"))),
            state_dir           = _opt("STATE_DIR",
                                       os.path.expanduser("~/.recorder")),
            lock_path           = _opt("LOCK_PATH",
                                       os.path.expanduser(
                                           "~/.config/archiver-suite/locks/tiktok.lock")),
            tiktok_users        = users,
            tiktok_cookies_file = cookies,
            reconnect_enabled        = bool(rec.get("reconnect_enabled", True)),
            live_confirm_samples     = int(rec.get("live_confirm_samples", 3)),
            live_confirm_interval_s  = float(rec.get("live_confirm_interval_s", 2.0)),
            reconnect_backoff_base_s = float(rec.get("reconnect_backoff_base_s", 2.0)),
            max_zero_byte_reconnects = int(rec.get("max_zero_byte_reconnects", 3)),
            max_session_minutes      = float(rec.get("max_session_minutes", 0.0)),
        )
