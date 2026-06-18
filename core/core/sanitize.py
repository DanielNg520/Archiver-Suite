r"""
core.sanitize
─────────────
Strip configured words from the text Telegram actually shows — the upload
FILENAME and the message CAPTION — at send time. The motivation is harm
reduction: a banned/hate token glued into a downloaded filename (e.g.
`clip_4horlover___.mp4`) would otherwise ride straight into a Telegram message
on auto-upload, and that visible text can have real consequences. Removing it
before upload keeps the archive flowing without broadcasting the token.

MATCH SCHEME (deliberately blunt, operator-controlled):
  - case-insensitive SUBSTRING match, so a word is caught even when glued into a
    larger token (`xxx4horloveryyy`) or padded with junk (`4horlover___`). The
    operator owns the list and accepts the over-match risk that a substring of a
    legitimate word could be hit.
  - on a match the word is REMOVED ALONG WITH the run of filler characters
    hugging it on EITHER side — spaces, tabs, `_`, `-`, `.` — so `4horlover___`,
    `_4horlover_`, and ` 4horlover ` all vanish cleanly with no leftover
    separator. The gap is replaced with a single space, intra-line whitespace is
    then collapsed, and each line is trimmed of edge filler.

SCOPE / NON-GOALS:
  - operates on TEXT ONLY — it never renames files on disk (the on-disk name and
    the content hash are untouched; only what Telegram displays changes).
  - the file EXTENSION is preserved: callers that sanitize a filename should pass
    the stem (see sanitize_stem) so a trailing match can't eat the `.mp4`.
  - an empty list → a no-op Sanitizer (falsy), so the whole feature costs nothing
    when unconfigured.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

log = logging.getLogger(__name__)

# Filler/separator characters that hug a token. A banned word is removed
# TOGETHER with the filler run on either side (NOT including newlines, so
# multi-line captions keep their line breaks).
_FILL      = r"[ \t_.\-]"
_SPACE_RUN = re.compile(r"[ \t]{2,}")


class Sanitizer:
    """Immutable, reusable. Built once from a word list and applied to every
    caption/filename. Thread-safe (compiled patterns are read-only)."""

    def __init__(self, words: "list[str] | tuple[str, ...]") -> None:
        cleaned = []
        for w in words:
            w = (w or "").strip()
            if w and not w.startswith("#"):     # allow # comments in a list/file
                cleaned.append(w)
        self.words: tuple[str, ...] = tuple(cleaned)
        # Each pattern eats the filler hugging the word on both sides. Longest
        # word first: removing a longer banned phrase before a shorter token it
        # contains avoids leaving a fragment behind.
        self._patterns = [
            re.compile(f"{_FILL}*{re.escape(w)}{_FILL}*", re.IGNORECASE)
            for w in sorted(set(cleaned), key=len, reverse=True)
        ]

    def __bool__(self) -> bool:
        return bool(self._patterns)

    def sanitize(self, text: str | None) -> str | None:
        """Remove every banned word (and its hugging separators) from `text`,
        then collapse intra-line whitespace and trim. Preserves newlines
        (multi-line orphaned captions stay multi-line). None/empty/no-words →
        returned unchanged."""
        if not text or not self._patterns:
            return text
        out = text
        for p in self._patterns:
            out = p.sub(" ", out)            # gap → single space (keeps neighbors apart)
        # Collapse whitespace and trim edge filler, per line so '\n' survives.
        return "\n".join(
            _SPACE_RUN.sub(" ", line).strip(" \t_-.")
            for line in out.split("\n")
        )

    def sanitize_stem(self, filename: str, *, fallback: str = "file") -> str:
        """Sanitize a FILENAME while protecting its extension: only the stem is
        cleaned, then the suffix is re-appended. If the stem is wholly removed,
        `fallback` is used so the upload is never left nameless."""
        p = Path(filename)
        stem = self.sanitize(p.stem) or ""
        if not stem.strip():
            stem = fallback
        return stem + p.suffix


def load_words(path: "str | Path") -> list[str]:
    """Read a banned-word list file: one entry per line, blank lines and
    `#` comments ignored. A missing file → [] (feature simply off)."""
    p = Path(path).expanduser()
    if not p.exists():
        return []
    try:
        return p.read_text(encoding="utf-8").splitlines()
    except OSError as e:
        log.warning("sanitize: could not read banned-word list %s: %s", p, e)
        return []
