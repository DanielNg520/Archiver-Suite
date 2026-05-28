"""
archiver.policy_store
─────────────────────
Single source of truth for behavior config + user lists, backed by a
TOML file at ~/.config/archiver/config.toml. Secrets stay in .env;
this file holds everything else.

WHY TOML AND NOT ENV VARS:
  The previous DELETE_AFTER_UPLOAD_<PLATFORM>_<USER> scheme silently
  fails for usernames containing characters that aren't valid in POSIX
  env-var names (dots, dashes, unicode). E.g. an Instagram user
  "user.name!" produces DELETE_AFTER_UPLOAD_INSTAGRAM_USER.NAME!
  which different shells/loaders handle inconsistently — typically
  it gets dropped, and the override silently falls through to the
  platform default with no error.

  TOML quoted keys take any string: ["user.name!"], ["正常用户"]. No
  encoding loss. No shell hazard.

  Other wins:
    - Native booleans/integers (no _parse_bool ceremony)
    - Hierarchy matches resolution chain visually
    - Adding a new policy = one new key, zero schema change

DESIGN PATTERN: Repository.
  PolicyStore owns the file and atomic writes. Every policy (Delete,
  Dedup, ...) is a thin Specification on top — it knows its KEY and
  DEFAULT and asks the Repository for resolved values. Adding a new
  policy is a one-class addition; storage and persistence are free.

ATOMIC WRITES:
  Temp file in same dir → fsync → os.replace. POSIX guarantees rename
  is atomic on the same filesystem, so a crash mid-write leaves either
  the old file fully intact OR the new file fully written, never half.
  Same-dir tempfile is critical: rename across filesystems is not
  atomic (it becomes copy+unlink).

CONCURRENCY:
  An RLock guards the in-memory cache. The cache is loaded once at
  construction and is the authoritative copy — we never reload from
  disk after init. This prevents the classic lost-update bug where
  two writers each read→modify→write and the first change is clobbered.
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
import tomllib
from pathlib import Path
from typing import Any, Iterator

import tomli_w

log = logging.getLogger(__name__)


_HEADER = """\
# archiver config — machine-managed but human-readable.
# Edits are safe; the CLI may rewrite this file. Values are preserved on
# rewrite but comments OUTSIDE this header are not (no Python TOML writer
# round-trips comments losslessly). Don't put secrets here — those live
# in .env. Resolution order: user → platform → global.

"""


def default_config_path() -> Path:
    """The canonical location. Overridable via $ARCHIVER_CONFIG (tests, dev)."""
    override = os.environ.get("ARCHIVER_CONFIG")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".config" / "archiver" / "config.toml"


class PolicyStore:
    """
    Owns config.toml. Thread-safe via a single RLock.

    Public surface:
      .get(key, *, platform=None, username=None, default=None)
      .explain(key, *, platform=None, username=None, default=None)
        → (value, source) for diagnostics

      .set(key, value, *, platform=None, username=None)
      .unset(key, *, platform=None, username=None)

      .list_users(platform)
      .add_user(platform, username)
      .remove_user(platform, username)

      .iter_user_overrides()
        → (platform, username, dict) per per-user section
    """

    def __init__(self, path: Path | None = None):
        self._path  = path or default_config_path()
        self._lock  = threading.RLock()
        self._data: dict[str, Any] = self._load()

    # ── Loading / persistence ─────────────────────────────────────────────────

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            log.info("policy_store: %s does not exist — starting empty", self._path)
            return {}
        try:
            with self._path.open("rb") as f:
                return tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            # Fail loud at startup. Silent fallback would produce wrong
            # behavior (everything resolves to defaults) without surfacing
            # the parse error — much harder to debug later.
            raise RuntimeError(
                f"config.toml is malformed: {e}. "
                f"Fix or delete {self._path}."
            ) from e

    def _persist(self) -> None:
        """
        Atomic write: NamedTemporaryFile in same dir → fsync → os.replace.
        Same dir = same filesystem = atomic rename on POSIX.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(
            mode     = "w",
            dir      = self._path.parent,
            prefix   = ".config.toml.",
            suffix   = ".tmp",
            delete   = False,
            encoding = "utf-8",
        )
        try:
            tmp.write(_HEADER)
            tmp.write(tomli_w.dumps(self._data))
            tmp.flush()
            os.fsync(tmp.fileno())  # bytes hit disk before we rename
            tmp.close()
            os.replace(tmp.name, self._path)
        except Exception:
            # Best-effort cleanup. Don't swallow the original exception.
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            raise

    # ── Hierarchical lookup ───────────────────────────────────────────────────

    def get(
        self,
        key:      str,
        *,
        platform: str | None = None,
        username: str | None = None,
        default:  Any        = None,
    ) -> Any:
        """
        Walk user → platform → global. Return first hit.

        A *present* False does NOT fall through; that's the whole point
        of an override. Only absent keys fall through to the next level.
        """
        with self._lock:
            if platform and username:
                user_section = (
                    self._data.get("platform", {})
                              .get(platform, {})
                              .get("user", {})
                              .get(username, {})
                )
                if key in user_section:
                    return user_section[key]
            if platform:
                plat_section = self._data.get("platform", {}).get(platform, {})
                if key in plat_section:
                    return plat_section[key]
            global_section = self._data.get("global", {})
            if key in global_section:
                return global_section[key]
            return default

    def explain(
        self,
        key:      str,
        *,
        platform: str | None = None,
        username: str | None = None,
        default:  Any        = None,
    ) -> tuple[Any, str]:
        """
        Returns (value, source). Mirrors get() exactly — keep in lockstep.
        `source` is one of:
          "user:<plat>/<user>", "platform:<plat>", "global", "default"
        """
        with self._lock:
            if platform and username:
                user_section = (
                    self._data.get("platform", {})
                              .get(platform, {})
                              .get("user", {})
                              .get(username, {})
                )
                if key in user_section:
                    return user_section[key], f"user:{platform}/{username}"
            if platform:
                plat_section = self._data.get("platform", {}).get(platform, {})
                if key in plat_section:
                    return plat_section[key], f"platform:{platform}"
            global_section = self._data.get("global", {})
            if key in global_section:
                return global_section[key], "global"
            return default, "default"

    # ── Mutation ──────────────────────────────────────────────────────────────

    def set(
        self,
        key:      str,
        value:    Any,
        *,
        platform: str | None = None,
        username: str | None = None,
    ) -> None:
        with self._lock:
            target = self._resolve_section(platform, username, create=True)
            target[key] = value
            self._persist()

    def unset(
        self,
        key:      str,
        *,
        platform: str | None = None,
        username: str | None = None,
    ) -> bool:
        """Return True iff a key was actually removed."""
        with self._lock:
            target = self._resolve_section(platform, username, create=False)
            if target is None or key not in target:
                return False
            del target[key]
            self._prune_empty(platform, username)
            self._persist()
            return True

    def _resolve_section(
        self,
        platform: str | None,
        username: str | None,
        *,
        create: bool,
    ) -> dict[str, Any] | None:
        """
        Walk to the right section. With create=True, build missing
        intermediate dicts. With create=False, return None on any miss.
        """
        if platform and username:
            path: tuple[str, ...] = ("platform", platform, "user", username)
        elif platform:
            path = ("platform", platform)
        else:
            path = ("global",)

        node: dict[str, Any] = self._data
        for seg in path:
            if seg not in node:
                if not create:
                    return None
                node[seg] = {}
            node = node[seg]
        return node

    def _prune_empty(self, platform: str | None, username: str | None) -> None:
        """Drop now-empty per-user sections so the file stays tidy."""
        if platform and username:
            user_dict = (
                self._data.get("platform", {})
                          .get(platform, {})
                          .get("user", {})
            )
            if username in user_dict and not user_dict[username]:
                del user_dict[username]

    # ── User-list management ──────────────────────────────────────────────────
    #
    # `users` per platform lives at [platform.<name>].users as a TOML
    # array of strings. These typed accessors are the ONLY way callers
    # should touch user lists — no module should poke at the raw dict.

    def list_users(self, platform: str) -> tuple[str, ...]:
        with self._lock:
            return tuple(
                self._data.get("platform", {})
                          .get(platform, {})
                          .get("users", [])
            )

    def add_user(self, platform: str, username: str) -> bool:
        """Return True if added, False if already present."""
        with self._lock:
            section = self._resolve_section(platform, None, create=True)
            users = list(section.get("users", []))
            if username in users:
                return False
            users.append(username)
            section["users"] = users
            self._persist()
            return True

    def remove_user(self, platform: str, username: str) -> bool:
        """
        Return True if removed, False if not present.

        Also drops any per-user overrides for the removed user. Otherwise
        you accumulate dead overrides referencing users that no longer exist.
        """
        with self._lock:
            section = self._resolve_section(platform, None, create=False)
            if section is None:
                return False
            users = list(section.get("users", []))
            if username not in users:
                return False
            users.remove(username)
            section["users"] = users
            user_dict = section.get("user", {})
            if username in user_dict:
                del user_dict[username]
            self._persist()
            return True

    # ── Diagnostics ───────────────────────────────────────────────────────────

    def iter_user_overrides(self) -> Iterator[tuple[str, str, dict[str, Any]]]:
        """Yield (platform, username, overrides_dict) for every per-user section."""
        with self._lock:
            for plat_name, plat_data in self._data.get("platform", {}).items():
                if not isinstance(plat_data, dict):
                    continue
                for user_name, user_data in plat_data.get("user", {}).items():
                    if isinstance(user_data, dict):
                        yield plat_name, user_name, dict(user_data)

    @property
    def path(self) -> Path:
        """Read-only accessor for the underlying file path."""
        return self._path
