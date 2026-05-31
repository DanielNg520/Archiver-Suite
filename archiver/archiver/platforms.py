"""
archiver.platforms
──────────────────
Strategy pattern: a uniform interface (`Platform`) implemented per source.
The orchestrator only knows about Platform; it doesn't care whether it's
talking to X via gallery-dl, TikTok via yt-dlp+gallery-dl, or Instagram
via gallery-dl.

Common interface (every platform must implement):
  - name                                 : str
  - users                                : tuple[str]
  - health_check()                       : HealthStatus
  - attempt_recovery()                   : bool
  - download(username, db)               : int (new files registered)
  - seed_archive(username, entries)      : int (entries newly added)
  - archive_path(username)               : Path (where the archive lives)

The `download()` / `seed_archive()` / `archive_path()` triple is what
lets `archiver.reconcile` bootstrap an existing on-disk archive into
the extractor's own dedup store without each platform's caller having
to know whether the archive is sqlite or txt.

Detection of "new" downloads: snapshot the user dir BEFORE the run,
diff AFTER. Robust against extractor-set mtimes and the extractor's
own archive (which doesn't tell us what was new THIS run vs. ever).
"""

from __future__ import annotations

import abc
import logging
import sqlite3
import subprocess
from dataclasses import dataclass
from datetime import timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .config import Config, XConfig, TikTokConfig, InstagramConfig
    from core import ItemStore

log = logging.getLogger(__name__)

MEDIA_EXTENSIONS = {
    ".mp4", ".mov", ".webm", ".mkv",
    ".jpg", ".jpeg", ".png", ".webp",
    ".gif",
}


# ── Status types ──────────────────────────────────────────────────────────────

@dataclass
class HealthStatus:
    healthy: bool
    reason:  str = ""

    @classmethod
    def ok(cls)         -> "HealthStatus": return cls(True)
    @classmethod
    def fail(cls, msg)  -> "HealthStatus": return cls(False, msg)


class AuthError(RuntimeError):
    """Raised by Platform.download when credentials are bad/expired."""


# ── Platform ABC ──────────────────────────────────────────────────────────────

class Platform(abc.ABC):
    """
    Strategy interface for all platforms.

    Subclasses MUST set `name` as a class attribute — used as a stable
    identifier in the DB and the on-disk folder structure.
    """
    name: str

    def __init__(self, config: "Config"):
        self.config = config

    def download_root(self, username: str) -> Path:
        """Returns {output_dir}/{platform}/{username}/, ensures it exists."""
        p = Path(self.config.output_dir) / self.name / username
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    @abc.abstractmethod
    def users(self) -> tuple[str, ...]: ...

    @abc.abstractmethod
    def health_check(self) -> HealthStatus: ...

    @abc.abstractmethod
    def attempt_recovery(self) -> bool: ...

    @abc.abstractmethod
    def download(self, username: str, db: "ItemStore") -> int: ...

    @abc.abstractmethod
    def archive_path(self, username: str) -> Path:
        """Where this platform's per-user extractor archive lives."""
        ...

    @abc.abstractmethod
    def seed_archive(self, username: str, entries: Iterable[str]) -> int:
        """
        Insert `entries` into the per-user extractor archive (sqlite for
        gallery-dl, txt for yt-dlp). Idempotent: re-seeding existing
        entries is a no-op. Returns the count actually inserted.

        Used by reconcile/bootstrap to teach the extractor "you already
        have these locally; don't re-fetch."
        """
        ...


class LocalPlatform(Platform):
    """
    A user-managed folder treated like a platform — but with NO download.

    Files live under {output_dir}/{name}/{username}/ and are managed by hand
    (you drop them in). Each immediate subfolder is a "username". Every cycle,
    reconcile walks them and enqueues new files exactly as for a real platform,
    so they inherit the full pipeline: platform-style captions ("@user · name"),
    routing (TELEGRAM_CHAT_ID_<NAME>_<USER> / config), the min-batch gate, and
    the delete-after-upload policy. Only the fetch step is skipped.

    No auth (always healthy) and no extractor archive (manual files have no
    upstream id to seed).
    """

    def __init__(self, config: "Config", name: str):
        super().__init__(config)
        self.name = name   # per-instance, unlike the built-in platforms

    @property
    def users(self) -> tuple[str, ...]:
        """Auto-discovered: each non-hidden subfolder of {output_dir}/{name}/
        is a username. No registration step — make a folder, it's a user."""
        root = Path(self.config.output_dir) / self.name
        if not root.is_dir():
            return ()
        return tuple(sorted(
            d.name for d in root.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ))

    def health_check(self) -> HealthStatus:
        return HealthStatus.ok()           # nothing to authenticate

    def attempt_recovery(self) -> bool:
        return True

    def download(self, username: str, db: "ItemStore") -> int:
        return 0                            # user-managed; reconcile enqueues

    def archive_path(self, username: str) -> Path:
        # Unused (seed_archive is a no-op) but the interface requires a path.
        d = Path(self.config.state_dir) / "local" / self.name
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{username}.txt"

    def seed_archive(self, username: str, entries: Iterable[str]) -> int:
        return 0                            # no extractor → nothing to seed


# ── Shared helpers ────────────────────────────────────────────────────────────

def _snapshot_media_files(root: Path) -> set[Path]:
    return {
        f.resolve()
        for f in root.rglob("*")
        if f.is_file() and f.suffix.lower() in MEDIA_EXTENSIONS
    }


def _cookies_file_health(path_str: str,
                         required_cookies: set[str]) -> HealthStatus:
    """Shared cookies.txt validation used by TikTok and Instagram."""
    path = Path(path_str)
    if not path.exists():
        return HealthStatus.fail(f"cookies file missing: {path}")
    if path.stat().st_size < 200:
        return HealthStatus.fail(
            f"cookies file suspiciously small ({path.stat().st_size} bytes)"
        )

    text = path.read_text(encoding="utf-8", errors="replace")
    found: set[str] = set()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) >= 6:
            found.add(fields[5])

    missing = required_cookies - found
    if missing:
        return HealthStatus.fail(f"missing auth cookies: {sorted(missing)}")
    return HealthStatus.ok()


def _seed_gallery_dl_sqlite(archive_path: Path, entries: Iterable[str]) -> int:
    """
    Idempotently insert entries into a gallery-dl archive.sqlite3.
    Schema: `CREATE TABLE archive (entry PRIMARY KEY) WITHOUT ROWID`.
    """
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(archive_path, timeout=10.0)
    try:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS archive (entry PRIMARY KEY) "
            "WITHOUT ROWID"
        )
        added = 0
        for e in entries:
            cur = conn.execute(
                "INSERT OR IGNORE INTO archive VALUES (?)", (e,),
            )
            added += cur.rowcount
        conn.commit()
        return added
    finally:
        conn.close()


def _seed_ytdlp_txt(archive_path: Path, entries: Iterable[str]) -> int:
    """
    Idempotently insert entries into a yt-dlp download-archive.txt.
    Format: one line per entry, "<extractor> <id>" (already formatted by caller).
    """
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    existing: set[str] = set()
    if archive_path.exists():
        existing = set(
            line.strip()
            for line in archive_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    new = [e for e in entries if e not in existing]
    if not new:
        return 0
    # Append-only; safer than rewrite.
    with archive_path.open("a", encoding="utf-8") as f:
        for e in new:
            f.write(e + "\n")
    return len(new)


# ══════════════════════════════════════════════════════════════════════════════
#  X / Twitter — via gallery-dl Python API
# ══════════════════════════════════════════════════════════════════════════════

class XPlatform(Platform):
    """
    Downloads X/Twitter media via gallery-dl with direct cookie injection.

    Health check: verifies all 3 cookie env vars are non-empty and ct0
    is long enough (~160 hex chars).

    Recovery: no automatic recovery — X cookies are 1FA + device-bound.
    Returns False → circuit breaker will skip this platform for the run.
    """
    name = "x"

    def __init__(self, config: "Config"):
        super().__init__(config)
        assert config.x is not None, "XPlatform requires Config.x to be set"
        self.x_cfg: XConfig = config.x

    @property
    def users(self) -> tuple[str, ...]:
        return self.x_cfg.users

    def health_check(self) -> HealthStatus:
        if not self.x_cfg.auth_token:
            return HealthStatus.fail("X_AUTH_TOKEN is empty")
        if not self.x_cfg.ct0:
            return HealthStatus.fail("X_CT0 is empty")
        if not self.x_cfg.twid:
            return HealthStatus.fail("X_TWID is empty")
        if len(self.x_cfg.ct0) < 50:
            return HealthStatus.fail(
                f"X_CT0 looks too short ({len(self.x_cfg.ct0)} chars) — "
                "likely a copy mistake; re-paste from DevTools."
            )
        return HealthStatus.ok()

    def attempt_recovery(self) -> bool:
        log.error(
            "X auth cannot be auto-refreshed. To recover:\n"
            "  1. Open Firefox → x.com (logged in) → F12 → Storage → Cookies\n"
            "  2. Copy auth_token, ct0, twid values\n"
            "  3. Update X_AUTH_TOKEN, X_CT0, X_TWID in .env\n"
            "  4. Re-run."
        )
        return False

    def archive_path(self, username: str) -> Path:
        d = Path(self.config.state_dir) / "gallery_dl" / self.name
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{username}_archive.sqlite3"

    def seed_archive(self, username: str, entries: Iterable[str]) -> int:
        return _seed_gallery_dl_sqlite(self.archive_path(username), entries)

    def download(self, username: str, db: "ItemStore") -> int:
        import gallery_dl.config
        import gallery_dl.job
        import gallery_dl.exception

        user_dir = self.download_root(username)
        before   = _snapshot_media_files(user_dir)

        date_min_ts = _compute_date_min(db, self.name, username, slack_days=1)
        if date_min_ts:
            log.info("  Incremental: date-min=%s",
                     _ts_to_date_str(date_min_ts))
        else:
            log.info("  First run: fetching all media for @%s", username)

        extractor_cfg: dict = {
            "cookies": {
                "auth_token": self.x_cfg.auth_token,
                "ct0":        self.x_cfg.ct0,
                "twid":       self.x_cfg.twid,
            },
            "ytdl-format": (
                "bestvideo[ext=mp4]+bestaudio[ext=m4a]"
                "/bestvideo[ext=mp4]+bestaudio"
                "/bestvideo+bestaudio/best"
            ),
            "image-quality": "orig",
            "retweets":  False,
            "replies":   False,
            "quoted":    False,
            "videos":    True,
            "filename":       "{date:%Y%m%d}_{tweet_id}_{num}.{extension}",
            "directory":      [self.name, username],
            "base-directory": self.config.output_dir,
            "archive":        str(self.archive_path(username)),
            "mtime":          False,  # don't trust the post's Last-Modified
            "postprocessors": [{
                "name":     "metadata",
                "event":    "post",
                "filename": "{date:%Y%m%d}_{tweet_id}_{num}.json",
            }],
            "sleep":         self.config.sleep_min,
            "sleep-request": self.config.sleep_max,
            "retries":       6,
        }
        if date_min_ts:
            extractor_cfg["date-min"] = date_min_ts

        gallery_dl.config.clear()
        for key, value in extractor_cfg.items():
            gallery_dl.config.set(("extractor", "twitter"), key, value)

        auth_failed = False

        class _AuthDetector(logging.Handler):
            def emit(_self, record):
                nonlocal auth_failed
                msg = record.getMessage()
                if any(s in msg for s in
                       ("AuthRequired", "AuthenticationError", "401", "403")):
                    auth_failed = True

        detector   = _AuthDetector()
        gdl_logger = logging.getLogger("gallery_dl")
        gdl_logger.addHandler(detector)

        url = f"https://x.com/{username}/media"
        log.info("  gallery-dl → @%s", username)

        try:
            job = gallery_dl.job.DownloadJob(url)
            job.run()
        except gallery_dl.exception.AuthenticationError as e:
            auth_failed = True
            log.error("  gallery-dl auth error: %s", e)
        except Exception as e:
            msg = str(e)
            if any(s in msg for s in ("AuthRequired", "AuthenticationError")):
                auth_failed = True
            log.error("  gallery-dl error for @%s: %s", username, e)
        finally:
            gdl_logger.removeHandler(detector)

        if auth_failed:
            raise AuthError(
                "X cookies appear expired or invalid (gallery-dl reported auth error)."
            )

        return _register_new_files(self.name, username, user_dir, before, db)


# ══════════════════════════════════════════════════════════════════════════════
#  TikTok — yt-dlp for videos, gallery-dl subprocess for photo carousels
# ══════════════════════════════════════════════════════════════════════════════

class TikTokPlatform(Platform):
    """
    TikTok needs two extractors: yt-dlp (videos with curl_cffi TLS
    impersonation) + gallery-dl (photo carousels). Both share the same
    cookies.txt and can be auto-refreshed from a Firefox profile.

    The seed_archive() method targets the yt-dlp txt archive — that's
    what blocks TikTok video re-downloads. Photo carousels are
    deduplicated by gallery-dl, with its OWN archive at a separate path
    (see `_photo_archive_path`) — fixing a v1 oversight where the photo
    pass had no archive flag set.
    """
    name = "tiktok"

    AUTH_COOKIES = {"sessionid", "sid_tt"}

    def __init__(self, config: "Config"):
        super().__init__(config)
        assert config.tiktok is not None, "TikTokPlatform requires Config.tiktok"
        self.tt_cfg: TikTokConfig = config.tiktok

    @property
    def users(self) -> tuple[str, ...]:
        return self.tt_cfg.users

    def health_check(self) -> HealthStatus:
        return _cookies_file_health(self.tt_cfg.cookies_file, self.AUTH_COOKIES)

    def attempt_recovery(self) -> bool:
        if not self.tt_cfg.firefox_profile:
            log.error(
                "TikTok cookies unhealthy and no FIREFOX_PROFILE set — "
                "can't auto-recover. Set FIREFOX_PROFILE in .env or "
                "manually refresh %s.", self.tt_cfg.cookies_file,
            )
            return False
        try:
            from .cookies import refresh_for_domain
            n = refresh_for_domain(
                domain           = "tiktok.com",
                profile_name     = self.tt_cfg.firefox_profile,
                output_path      = self.tt_cfg.cookies_file,
                required_cookies = self.AUTH_COOKIES,
            )
            log.info("  Cookie auto-refresh: %d cookies → %s",
                     n, self.tt_cfg.cookies_file)
            return True
        except Exception as e:
            log.error("  Cookie refresh failed: %s", e)
            return False

    def archive_path(self, username: str) -> Path:
        d = Path(self.config.state_dir) / "yt_dlp" / self.name
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{username}_archive.txt"

    def _photo_archive_path(self, username: str) -> Path:
        """Gallery-dl-format archive for TikTok PHOTO carousels (separate
        from yt-dlp's video archive). Fixes the v1 oversight where photo
        re-fetches weren't deduplicated at the extractor level."""
        d = Path(self.config.state_dir) / "gallery_dl" / self.name
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{username}_photo_archive.sqlite3"

    def seed_archive(self, username: str, entries: Iterable[str]) -> int:
        """
        Most TikTok entries from identity.archive_entry_for are of the
        form `tiktok <id>` (yt-dlp txt format). We can't tell from the
        entry string alone whether the original was a video or a photo,
        but yt-dlp's archive is the right place for both video IDs to
        live (photos won't be in there, but gallery-dl will dedup them
        via its own archive on first re-walk).
        """
        return _seed_ytdlp_txt(self.archive_path(username), entries)

    def download(self, username: str, db: "ItemStore") -> int:
        import yt_dlp

        user_dir = self.download_root(username)
        before   = _snapshot_media_files(user_dir)

        date_after = _compute_date_after_str(db, self.name, username)
        if date_after:
            log.info("  Incremental: posts on/after %s", date_after)
        else:
            log.info("  First run: fetching all available posts for @%s", username)

        ydl_opts = self._build_ydl_opts(user_dir, date_after, username)

        profile_url = f"https://www.tiktok.com/@{username}"
        log.info("  yt-dlp → @%s", username)

        auth_failed = False
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ret = ydl.download([profile_url])
                if ret:
                    log.warning("  yt-dlp returned non-zero exit code: %s", ret)
        except yt_dlp.utils.DownloadError as e:
            msg = str(e).lower()
            if any(s in msg for s in
                   ("login", "unauthorized", "403", "401",
                    "private account", "verify", "captcha")):
                auth_failed = True
            log.error("  yt-dlp DownloadError: %s", e)
        except Exception as e:
            log.error("  yt-dlp unexpected error: %s", e, exc_info=True)

        self._download_photo_posts(username, user_dir)

        if auth_failed:
            raise AuthError("TikTok auth appears expired (yt-dlp reported login failure).")

        return _register_new_files(self.name, username, user_dir, before, db)

    def _build_ydl_opts(self, user_dir: Path, date_after: str | None,
                        username: str) -> dict:
        from yt_dlp.networking.impersonate import ImpersonateTarget

        TIKTOK_FORMAT = (
            "bestvideo[format_id!*=download_addr][ext=mp4]+bestaudio[ext=m4a]"
            "/bestvideo[format_id!*=download_addr]+bestaudio"
            "/best[format_id!*=download_addr][ext=mp4]"
            "/best[format_id!*=download_addr]"
            "/best"
        )

        opts: dict = {
            "outtmpl":             str(user_dir / "%(upload_date)s_%(id)s.%(ext)s"),
            "format":              TIKTOK_FORMAT,
            "merge_output_format": "mp4",
            "cookiefile":          self.tt_cfg.cookies_file,
            "impersonate":         ImpersonateTarget.from_str("chrome"),
            "writeinfojson":       True,
            "overwrites":          False,
            # Tell yt-dlp about its archive — same one we seed during reconcile.
            "download_archive":    str(self.archive_path(username)),
            "sleep_interval":              self.config.sleep_min,
            "max_sleep_interval":          self.config.sleep_max,
            "sleep_interval_requests":     self.config.sleep_min,
            "concurrent_fragment_downloads": 1,
            "retries":             5,
            "fragment_retries":    5,
            "retry_sleep_functions": {
                "http":     lambda n: 2 ** n,
                "fragment": lambda n: 2 ** n,
            },
            "quiet":         True,
            "no_warnings":   False,
            "logger":        _YtdlpLogger(),
            "geo_bypass":    True,
            "playlistreverse": False,
        }
        if date_after:
            from yt_dlp.utils import DateRange
            opts["daterange"] = DateRange(start=date_after, end=None)
        return opts

    def _download_photo_posts(self, username: str, user_dir: Path) -> None:
        cmd = [
            "gallery-dl",
            "--cookies",     self.tt_cfg.cookies_file,
            "--directory",   str(user_dir),
            "--filename",    "{date:%Y%m%d}_{id}_{num}.{extension}",
            # v1 oversight fix: photo carousels now have their OWN archive,
            # preventing re-fetches even when files exist on disk.
            "--download-archive", str(self._photo_archive_path(username)),
            "--sleep",         str(int(self.config.sleep_min)),
            "--sleep-request", str(int(self.config.sleep_max)),
            f"https://www.tiktok.com/@{username}",
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
            if result.returncode not in (0, 1):
                log.warning("  gallery-dl exit %d: %s",
                            result.returncode, result.stderr[:300])
        except FileNotFoundError:
            log.debug("  gallery-dl not in PATH; skipping photo posts.")
        except subprocess.TimeoutExpired:
            log.warning("  gallery-dl timed out (>10 min)")
        except Exception as e:
            log.error("  gallery-dl error: %s", e)


class _YtdlpLogger:
    def debug(self,   msg: str):
        if not msg.startswith("[debug]"):
            log.debug("yt-dlp: %s", msg)
    def warning(self, msg: str): log.warning("yt-dlp: %s", msg)
    def error(self,   msg: str): log.error("yt-dlp: %s", msg)


# ══════════════════════════════════════════════════════════════════════════════
#  Instagram — gallery-dl Python API (posts + reels)
# ══════════════════════════════════════════════════════════════════════════════

class InstagramPlatform(Platform):
    """
    Downloads Instagram media via gallery-dl with cookies.txt auth.

    Why gallery-dl (and not yt-dlp): IG support is unified — posts and
    reels under one extractor with per-subcategory routing. yt-dlp's IG
    extractor is video-only and flakier for carousels.

    Why posts+reels by default (not stories/highlights):
      - Smaller surface area = lower ban risk.
      - Stories disappear; their checkpointing logic would be its own
        special case ("don't fail just because the story expired").
      - INSTAGRAM_INCLUDE lets you opt into more later if you accept it.
    """
    name = "instagram"

    AUTH_COOKIES = {"sessionid", "csrftoken", "ds_user_id"}

    def __init__(self, config: "Config"):
        super().__init__(config)
        assert config.instagram is not None, \
            "InstagramPlatform requires Config.instagram to be set"
        self.ig_cfg: InstagramConfig = config.instagram

    @property
    def users(self) -> tuple[str, ...]:
        return self.ig_cfg.users

    def health_check(self) -> HealthStatus:
        return _cookies_file_health(self.ig_cfg.cookies_file, self.AUTH_COOKIES)

    def attempt_recovery(self) -> bool:
        if not self.ig_cfg.firefox_profile:
            log.error(
                "Instagram cookies unhealthy and no FIREFOX_PROFILE set — "
                "can't auto-recover. Set FIREFOX_PROFILE in .env or "
                "manually refresh %s.", self.ig_cfg.cookies_file,
            )
            return False
        try:
            from .cookies import refresh_for_domain
            n = refresh_for_domain(
                domain           = "instagram.com",
                profile_name     = self.ig_cfg.firefox_profile,
                output_path      = self.ig_cfg.cookies_file,
                required_cookies = self.AUTH_COOKIES,
            )
            log.info("  Cookie auto-refresh: %d cookies → %s",
                     n, self.ig_cfg.cookies_file)
            return True
        except Exception as e:
            log.error("  Cookie refresh failed: %s", e)
            return False

    def archive_path(self, username: str) -> Path:
        d = Path(self.config.state_dir) / "gallery_dl" / self.name
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{username}_archive.sqlite3"

    def seed_archive(self, username: str, entries: Iterable[str]) -> int:
        return _seed_gallery_dl_sqlite(self.archive_path(username), entries)

    def download(self, username: str, db: "ItemStore") -> int:
        import gallery_dl.config
        import gallery_dl.job
        import gallery_dl.exception

        user_dir = self.download_root(username)
        before   = _snapshot_media_files(user_dir)

        date_min_ts = _compute_date_min(db, self.name, username, slack_days=1)
        if date_min_ts:
            log.info("  Incremental: date-min=%s",
                     _ts_to_date_str(date_min_ts))
        else:
            log.info("  First run: fetching all %s for @%s",
                     self.ig_cfg.include, username)

        # Per-subcategory filename — `{post_shortcode}_{num}` keeps carousel
        # images uniquely named AND parseable by identity.resolve.
        ig_filename = "{date:%Y%m%d}_{post_shortcode}_{num}.{extension}"

        extractor_cfg: dict = {
            "cookies":        self.ig_cfg.cookies_file,
            "include":        self.ig_cfg.include,
            "videos":         True,
            "previews":       False,
            "filename":       ig_filename,
            "directory":      [self.name, username],
            "base-directory": self.config.output_dir,
            "archive":        str(self.archive_path(username)),
            "mtime":          False,
            # IG bans aggressively — bias to higher pacing than X/TikTok.
            "sleep":         self.config.sleep_max,
            "sleep-request": [self.config.sleep_min, self.config.sleep_max],
            "retries":       6,
        }
        if date_min_ts:
            extractor_cfg["date-min"] = date_min_ts

        gallery_dl.config.clear()
        for key, value in extractor_cfg.items():
            gallery_dl.config.set(("extractor", "instagram"), key, value)

        auth_failed = False

        class _AuthDetector(logging.Handler):
            def emit(_self, record):
                nonlocal auth_failed
                msg = record.getMessage()
                if any(s in msg for s in (
                    "AuthRequired", "AuthenticationError",
                    "login_required", "challenge_required",
                    "checkpoint_required",
                    "401", "403",
                )):
                    auth_failed = True

        detector   = _AuthDetector()
        gdl_logger = logging.getLogger("gallery_dl")
        gdl_logger.addHandler(detector)

        url = f"https://www.instagram.com/{username}/"
        log.info("  gallery-dl → @%s", username)

        try:
            job = gallery_dl.job.DownloadJob(url)
            job.run()
        except gallery_dl.exception.AuthenticationError as e:
            auth_failed = True
            log.error("  gallery-dl auth error: %s", e)
        except Exception as e:
            msg = str(e)
            if any(s in msg for s in (
                "AuthRequired", "AuthenticationError",
                "login_required", "challenge_required",
            )):
                auth_failed = True
            log.error("  gallery-dl error for @%s: %s", username, e)
        finally:
            gdl_logger.removeHandler(detector)

        if auth_failed:
            raise AuthError(
                "Instagram cookies appear expired or the account is challenged "
                "(checkpoint/2FA). Open Firefox → instagram.com, clear any "
                "pending verification, then re-run."
            )

        return _register_new_files(self.name, username, user_dir, before, db)


# ── Checkpoint helpers (shared across platforms) ──────────────────────────────

def _compute_date_min(db: "ItemStore", platform: str, username: str,
                      slack_days: int = 1) -> int | None:
    """
    Compute a Unix timestamp `date-min` for the next extractor call.

    Strategy:
      1. Prefer `MAX(upload_date WHERE status='sent')` — the actual
         frontier of confirmed-archived content. This is what makes
         incremental work even with `delete_after_upload=true`.
      2. Fall back to `last_run_utc` if there's no completed upload yet.
      3. Apply slack_days as a safety subtraction (covers timezone slop
         and posts edited late in a day).

    Returns None on first-ever run (no DB knowledge of this user).
    """
    floor_str = db.max_sent_upload_date(platform, username)
    if floor_str:
        try:
            dt = _date_str_to_datetime(floor_str)
            cutoff = dt - timedelta(days=slack_days)
            return int(cutoff.timestamp())
        except ValueError:
            pass  # fall through to last_run

    last_run = db.get_last_run(platform, username)
    if last_run:
        cutoff = last_run - timedelta(days=slack_days)
        ts = (cutoff.replace(tzinfo=timezone.utc).timestamp()
              if cutoff.tzinfo is None else cutoff.timestamp())
        return int(ts)

    return None


def _compute_date_after_str(db: "ItemStore", platform: str, username: str,
                            slack_days: int = 1) -> str | None:
    """
    Same as _compute_date_min but returns YYYYMMDD string for yt-dlp
    DateRange. Returns None on first-ever run.
    """
    ts = _compute_date_min(db, platform, username, slack_days)
    if ts is None:
        return None
    return _ts_to_date_str(ts)


def _ts_to_date_str(ts: int) -> str:
    from datetime import datetime as _dt
    return _dt.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%d")


def _date_str_to_datetime(s: str):
    from datetime import datetime as _dt
    return _dt.strptime(s, "%Y%m%d").replace(tzinfo=timezone.utc)


# ── Shared registration helper ────────────────────────────────────────────────

def _register_new_files(
    platform: str,
    username: str,
    user_dir: Path,
    before:   set[Path],
    db:       "ItemStore",
) -> int:
    """
    Diff before/after, register new files in DB. Delegates to the
    identity resolver so we use the SAME identifier logic as reconcile.
    Returns count of NEW files registered.
    """
    from core import identity
    from core.hashing import full_hash

    after = _snapshot_media_files(user_dir)
    new_files = sorted(after - before)

    added = 0
    for f in new_files:
        try:
            size = f.stat().st_size
        except OSError:
            log.warning("  Skipping vanished file: %s", f)
            continue
        if size < 100:
            log.warning("  Skipping tiny file (%d bytes): %s", size, f.name)
            try: f.unlink()
            except OSError: pass
            continue

        ident = identity.resolve(f)
        inserted = db.add_item(
            source          = "archiver",
            platform        = platform,
            username        = username,
            identifier      = ident.identifier,
            file_path       = str(f),
            upload_date     = ident.upload_date,
            file_size_bytes = size,
            title           = ident.title,
            priority        = 10,
            # Stamp the content hash so the dispatcher's global dedup guarantee
            # covers platform downloads too. The file was just written, so it's
            # warm in cache — the hash is cheap.
            content_hash    = full_hash(f),
        )
        if inserted:
            added += 1
            log.info("  + %s (%.1f MB)", f.name, size / 1_048_576)
    log.info("  Download done: @%s — %d new file(s)", username, added)
    return added
