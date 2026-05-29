"""
core.policy_store
───────────────────────
Shared Repository for config.toml. Same atomic write semantics and same
hierarchical lookup shape everywhere: user → platform → global.

Storage layout:
  ~/.config/archiver-suite/config.toml

Overridable via $ARCHIVER_SUITE_CONFIG for tests / alternate setups.
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
# archiver-suite config — machine-managed but human-readable.
# Edits are safe; the CLI may rewrite this file. Values are preserved on
# rewrite but comments OUTSIDE this header are not. Don't put secrets
# here — those live in .env. Resolution order: user → platform → global.

"""


def default_config_path() -> Path:
    """Canonical config location. Override with $ARCHIVER_SUITE_CONFIG."""
    override = os.environ.get("ARCHIVER_SUITE_CONFIG")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".config" / "archiver-suite" / "config.toml"


class PolicyStore:
    """
    Owns config.toml. Thread-safe via a single RLock.

    Public surface (identical to archiver's):
      .get(key, *, platform=None, username=None, default=None)
      .explain(key, *, platform=None, username=None, default=None)
      .set(key, value, *, platform=None, username=None)
      .unset(key, *, platform=None, username=None)
      .list_users(platform)
      .add_user(platform, username)
      .remove_user(platform, username)
      .iter_user_overrides()
    """

    def __init__(self, path: Path | None = None):
        self._path  = path or default_config_path()
        self._lock  = threading.RLock()
        self._data: dict[str, Any] = self._load()

    # ── Loading / persistence ─────────────────────────────────────────────

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            log.info("policy_store: %s does not exist — starting empty", self._path)
            return {}
        try:
            with self._path.open("rb") as f:
                return tomllib.load(f)
        except tomllib.TOMLDecodeError as e:
            raise RuntimeError(
                f"config.toml is malformed: {e}. Fix or delete {self._path}."
            ) from e

    def _persist(self) -> None:
        """Atomic write: tempfile (same dir) → fsync → os.replace."""
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
            os.fsync(tmp.fileno())
            tmp.close()
            os.replace(tmp.name, self._path)
        except Exception:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass
            raise

    # ── Hierarchical lookup ───────────────────────────────────────────────

    def get(
        self,
        key:      str,
        *,
        platform: str | None = None,
        username: str | None = None,
        default:  Any        = None,
    ) -> Any:
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

    # ── Mutation ──────────────────────────────────────────────────────────

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
        if platform and username:
            user_dict = (
                self._data.get("platform", {})
                          .get(platform, {})
                          .get("user", {})
            )
            if username in user_dict and not user_dict[username]:
                del user_dict[username]

    # ── User-list management ──────────────────────────────────────────────

    def list_users(self, platform: str) -> tuple[str, ...]:
        with self._lock:
            return tuple(
                self._data.get("platform", {})
                          .get(platform, {})
                          .get("users", [])
            )

    def add_user(self, platform: str, username: str) -> bool:
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

    # ── Diagnostics ───────────────────────────────────────────────────────

    def iter_user_overrides(self) -> Iterator[tuple[str, str, dict[str, Any]]]:
        with self._lock:
            for plat_name, plat_data in self._data.get("platform", {}).items():
                if not isinstance(plat_data, dict):
                    continue
                for user_name, user_data in plat_data.get("user", {}).items():
                    if isinstance(user_data, dict):
                        yield plat_name, user_name, dict(user_data)

    @property
    def path(self) -> Path:
        return self._path
