"""
core.env
────────
Environment-variable parsing, in one place. Every worker's config module grew
its own `_req`/`_opt` (and media_prep its own `_env_int`/`_env_bool`) — the same
read-strip-default-or-raise logic copied four times. Centralized here so the
parsing rules (and the "bad value → warn and fall back" robustness) are
consistent and fixed once.

REQUIRED vs OPTIONAL philosophy:
  • `req()` fails loud — a missing credential should stop startup, the right
    time to fail. It raises MissingEnvVar (a RuntimeError, so the CLIs' existing
    top-level handler turns it into a clean message + non-zero exit).
  • the typed optionals (`opt_int`/`opt_float`/`opt_bool`) are SELF-HEALING: a
    typo'd tunable logs a warning and uses the default rather than crashing a
    long-running daemon. Tunables are not worth a hard down.
"""

from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)


class MissingEnvVar(RuntimeError):
    """A required environment variable is unset or empty."""


def req(key: str) -> str:
    """Required string env var. Raises MissingEnvVar if unset/empty."""
    val = os.environ.get(key, "").strip()
    if not val:
        raise MissingEnvVar(f"Missing required env var: {key}. See .env.example.")
    return val


def opt(key: str, default: str = "") -> str:
    """Optional string env var (stripped), or `default`."""
    return os.environ.get(key, default).strip()


def opt_int(key: str, default: int, *, min_value: int | None = None) -> int:
    """Optional int env var, or `default`. A non-integer value — or one below
    `min_value` when given — logs a warning and falls back to `default` rather
    than raising (a wedged tunable must not crash a daemon)."""
    raw = os.environ.get(key, "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
    except ValueError:
        log.warning("env: %s=%r is not an int — using %d", key, raw, default)
        return default
    if min_value is not None and v < min_value:
        log.warning("env: %s=%d is below the minimum %d — using %d",
                    key, v, min_value, default)
        return default
    return v


def opt_float(key: str, default: float, *, min_value: float | None = None) -> float:
    """Optional float env var, or `default` (warn-and-fall-back on a bad value)."""
    raw = os.environ.get(key, "").strip()
    if not raw:
        return default
    try:
        v = float(raw)
    except ValueError:
        log.warning("env: %s=%r is not a number — using %s", key, raw, default)
        return default
    if min_value is not None and v < min_value:
        log.warning("env: %s=%s is below the minimum %s — using %s",
                    key, v, min_value, default)
        return default
    return v


def opt_bool(key: str, default: bool) -> bool:
    """Optional boolean env var. An unset var → `default`; otherwise anything
    other than the falsy tokens (0/false/no/off/empty) is True."""
    raw = os.environ.get(key)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")
