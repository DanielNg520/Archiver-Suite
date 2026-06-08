"""
core.identity
─────────────
Resolve `(identifier, upload_date, title)` for any media file on disk.

Lives in `core` (not archiver) because every producer — the archiver
downloader, the reconcile pass, and the loose/orphaned-file ingestor —
must resolve identity by the IDENTICAL logic, or the same file gets two
different identifiers and the UNIQUE(platform, identifier) constraint
silently rejects one, masking a duplicate-row bug.

The resolver is a Chain of Responsibility: try the most-trusted source
first, fall through on failure. Output is always a complete
`MediaIdentity` — no platform code has to handle "what if I can't get
an identifier" because we ALWAYS produce one (path-hash fallback).

Sources, in order of trust:

  1. Sidecar JSON  (gallery-dl writes `*.json`, yt-dlp writes `*.info.json`)
     - Highest fidelity: canonical post ID, exact timestamp, full caption.
     - Field names differ per extractor; we probe a known set.

  2. Filename pattern  `YYYYMMDD_<id>_<num>.<ext>` (what our downloaders emit)
     - Reliable for files our system produced.
     - The `_<num>` is optional for backwards compat with older files.

  3. mtime + path hash  (manual files, partial filenames, anything else)
     - mtime is best-effort upload_date (set when the file landed on disk;
       we configure gallery-dl with `mtime: false` so it reflects
       download time, not the post's Last-Modified header).
     - Identifier is `manual_<sha1(abs_path)[:10]>`. The "manual_" prefix
       means: "this file did not come through an extractor; do NOT try to
       seed its ID into the extractor's archive file." See `platforms.seed_archive`.

WHY one resolver instead of per-platform parsing:
  - The reconcile pass + the download pass need identical logic so a
    file's identifier is stable whether it was inserted by the
    downloader or by reconcile. If those drift, you get duplicate DB
    rows on the same file — the UNIQUE(platform, identifier) constraint
    will catch it, but only by silently rejecting one, masking the bug.
  - Three platforms × two code paths = six places to get this right
    today, more later. Centralising is strictly cheaper.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

log = logging.getLogger(__name__)


# Match YYYYMMDD prefix and OPTIONAL `_<num>` at the end. The middle is
# the identifier — captured greedy because IG shortcodes can contain
# underscores. We anchor at start AND end of stem to avoid surprises.
_FILENAME_RE = re.compile(
    r"^(?P<date>\d{8})_(?P<ident>.+?)(?:_(?P<num>\d+))?$"
)

# Instagram (instaloader / gallery-dl default) filename:
#   <username>_<taken_at_unixts>_<media_pk>_<owner_id>.<ext>
# e.g. fit_miness_1736258696_3540317000569885880_50348444507.jpg
# The username may contain underscores; what's reliable is the rigid numeric
# TAIL — a 9-11-digit unix timestamp, then the post's media PK (17-20 digits),
# then the owner id. These files carry NO sidecar and don't start with our
# YYYYMMDD prefix, so without this they all fall to `manual_<pathhash>`: a
# DIFFERENT identifier per path. That defeats UNIQUE(platform, identifier), so
# the SAME post re-downloaded under a renamed handle (different user folder) or
# re-encoded by IG (different bytes) slips past every dedup layer and uploads
# again. Extracting the media PK as the identifier collapses those onto one row.
# Tried only AFTER _FILENAME_RE fails, so our own downloads are unaffected.
_IG_PK_RE = re.compile(
    r"^.+?_(?P<takenat>\d{9,11})_(?P<pk>\d{16,21})_(?P<owner>\d{3,})$"
)

# Video containers — a video post is a single asset, used to collapse the
# `<id>` vs `<id>_0` split two TikTok extractors produce for the same clip.
_VIDEO_EXTS = {".mp4", ".mov", ".webm", ".mkv", ".ts", ".m4v"}


@dataclass(frozen=True)
class MediaIdentity:
    """
    Output of the resolver. Always complete — no Optionals on identifier.

    `is_manual` is True when the resolver fell all the way through to the
    hash fallback. The platform layer uses this signal to decide whether
    to write an entry into the extractor's archive sqlite/txt. Manual
    files don't belong in those archives — the extractor doesn't know
    their IDs and we don't want to fabricate fake ones in its namespace.
    """
    identifier:  str
    upload_date: str             # YYYYMMDD; never None — falls back to mtime
    title:       str
    is_manual:   bool            # True if we used the hash fallback


# ── Sidecar parsing ───────────────────────────────────────────────────────────

# Keys to try, in trust order. The first present key wins.
# This list covers gallery-dl twitter/instagram and yt-dlp tiktok.
_SIDECAR_ID_KEYS = (
    # gallery-dl
    "tweet_id",        # twitter
    "post_shortcode",  # instagram (URL-stable, preferred over numeric)
    "post_id",         # instagram (numeric, fallback if shortcode missing)
    "media_id",        # instagram per-asset / tiktok photo carousel
    # yt-dlp
    "id",              # tiktok video, generic
)

_SIDECAR_DATE_KEYS_INT = (
    # gallery-dl gives a python datetime serialized as ISO string under
    # "date", but unix timestamps under these keys.
    "timestamp",        # gallery-dl twitter, instagram
    "post_date",        # generic
)

_SIDECAR_DATE_KEYS_STR = (
    "upload_date",      # yt-dlp — already YYYYMMDD
    "date",             # gallery-dl — ISO datetime string
)

_SIDECAR_TITLE_KEYS = (
    "content",          # twitter tweet text
    "description",      # instagram caption, yt-dlp
    "title",            # generic, yt-dlp tiktok
    "caption",          # safety
)


def _read_sidecar(media_file: Path) -> dict | None:
    """Try both sidecar naming conventions. Return parsed JSON or None."""
    for sidecar in (
        media_file.with_suffix(".json"),
        media_file.with_suffix(".info.json"),
        # gallery-dl sometimes appends .json to the full name including ext:
        # e.g. 20260101_abc_1.jpg.json — handle that too.
        media_file.parent / (media_file.name + ".json"),
    ):
        if not sidecar.exists():
            continue
        try:
            # errors="replace": TikTok captions/usernames routinely carry
            # non-UTF-8 bytes (e.g. a stray 0xb0 "°"); a strict decode would
            # raise UnicodeDecodeError (a ValueError, NOT an OSError) and abort
            # the whole fetch. Tolerate the bad byte — JSON structure survives.
            return json.loads(sidecar.read_text(encoding="utf-8", errors="replace"))
        except (json.JSONDecodeError, OSError, ValueError) as e:
            log.debug("sidecar %s unreadable: %s", sidecar, e)
            continue
    return None


def _extract_id_from_sidecar(meta: dict) -> str | None:
    for key in _SIDECAR_ID_KEYS:
        v = meta.get(key)
        if v not in (None, "", 0):
            return str(v)
    return None


def _extract_date_from_sidecar(meta: dict) -> str | None:
    # Prefer integer timestamps — unambiguous.
    for key in _SIDECAR_DATE_KEYS_INT:
        v = meta.get(key)
        if isinstance(v, (int, float)) and v > 0:
            try:
                return datetime.fromtimestamp(v, tz=timezone.utc).strftime("%Y%m%d")
            except (ValueError, OSError, OverflowError):
                continue
    # Then strings — could be YYYYMMDD already, or ISO.
    for key in _SIDECAR_DATE_KEYS_STR:
        v = meta.get(key)
        if not isinstance(v, str) or not v:
            continue
        # Already YYYYMMDD?
        if len(v) == 8 and v.isdigit():
            return v
        # ISO datetime?
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            return dt.strftime("%Y%m%d")
        except ValueError:
            continue
    return None


def _extract_title_from_sidecar(meta: dict) -> str:
    for key in _SIDECAR_TITLE_KEYS:
        v = meta.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


# ── Filename parsing ──────────────────────────────────────────────────────────

def _parse_filename(stem: str) -> tuple[str | None, str | None]:
    """
    Parse `YYYYMMDD_<id>[_<num>]` → (date, id). Either field may be None
    if the regex doesn't match.
    """
    m = _FILENAME_RE.match(stem)
    if not m:
        return (None, None)
    date  = m.group("date")
    ident = m.group("ident")
    num   = m.group("num")
    # Compose final identifier. We include _num so that carousel images
    # from the same post each get a distinct identifier (matching the
    # extractor's archive entries: e.g. `twitter12345_1`, `twitter12345_2`).
    if num is not None:
        ident = f"{ident}_{num}"
    return (date, ident)


def _parse_ig_pk(stem: str) -> tuple[str | None, str | None]:
    """Parse the instaloader/gallery-dl IG `<user>_<taken_at>_<pk>_<owner>`
    layout → (date, media_pk). (None, None) if it doesn't match. The media PK
    is the stable per-asset post identifier (each carousel image has its own),
    so it's a correct one-row-per-asset key. taken_at gives the upload date."""
    m = _IG_PK_RE.match(stem)
    if not m:
        return (None, None)
    pk = m.group("pk")
    try:
        date = datetime.fromtimestamp(
            int(m.group("takenat")), tz=timezone.utc).strftime("%Y%m%d")
    except (ValueError, OSError, OverflowError):
        date = None
    return (date, pk)


# ── Path-hash fallback ────────────────────────────────────────────────────────

def _path_hash(path: Path) -> str:
    """
    Stable, short identifier derived from the absolute path.
    SHA1 hex prefix — 10 chars = 40 bits ≈ 1 collision per ~1.5M files
    in the same (platform, username) namespace. Way more headroom than
    you'd ever hit in practice for a single user's archive.
    """
    h = hashlib.sha1(str(path.resolve()).encode("utf-8")).hexdigest()
    return f"manual_{h[:10]}"


def _mtime_date(path: Path) -> str:
    """File mtime → YYYYMMDD. Today if stat fails (extremely unlikely)."""
    try:
        return datetime.fromtimestamp(
            path.stat().st_mtime, tz=timezone.utc,
        ).strftime("%Y%m%d")
    except OSError:
        return datetime.now(timezone.utc).strftime("%Y%m%d")


# ── Public API ────────────────────────────────────────────────────────────────

def resolve(media_file: Path) -> MediaIdentity:
    """
    Run the resolver chain and return a complete MediaIdentity.

    NEVER raises for a missing source — it falls through to the hash
    fallback. The only way this raises is if the file itself can't be
    stat'd, which is an unrecoverable upstream bug.
    """
    sidecar = _read_sidecar(media_file)

    sidecar_id   = _extract_id_from_sidecar(sidecar)   if sidecar else None
    sidecar_date = _extract_date_from_sidecar(sidecar) if sidecar else None
    title        = _extract_title_from_sidecar(sidecar) if sidecar else ""

    fn_date, fn_id = _parse_filename(media_file.stem)
    # IG media-PK tail is tried only when our own YYYYMMDD pattern didn't match.
    ig_date = ig_pk = None
    if not fn_id:
        ig_date, ig_pk = _parse_ig_pk(media_file.stem)

    # Identifier resolution: sidecar > our filename > IG media-pk > hash
    if sidecar_id:
        ident = sidecar_id
        is_manual = False
        # gallery-dl IG carousels have ONE shortcode but multiple files.
        # Disambiguate using the filename's num suffix when present.
        # Otherwise the UNIQUE(platform, identifier) constraint collapses
        # the whole carousel to one row.
        m = _FILENAME_RE.match(media_file.stem)
        if m and m.group("num") and not ident.endswith(f"_{m.group('num')}"):
            ident = f"{ident}_{m.group('num')}"
    elif fn_id:
        ident = fn_id
        is_manual = False
    elif ig_pk:
        ident = ig_pk
        is_manual = False
    else:
        ident = _path_hash(media_file)
        is_manual = True

    # Single-video normalization: a video is one asset, but our two TikTok
    # extractors name it differently — yt-dlp writes `<id>.mp4` (no index),
    # gallery-dl writes `<id>_0.mp4` (0-based first asset). Left alone they
    # resolve to `<id>` vs `<id>_0`, two identifiers for ONE video → two rows →
    # the same clip uploads twice (often paired in one album). Stripping a
    # trailing `_0` from VIDEO files collapses them onto `<id>`, so
    # UNIQUE(platform, identifier) drops the second. Photo carousels start their
    # index at _1 (and keep distinct assets), so a `_0`-only rule never merges
    # two real images. Skipped for manual/hash ids (no extractor index there).
    if (not is_manual and ident.endswith("_0")
            and media_file.suffix.lower() in _VIDEO_EXTS):
        ident = ident[:-2]

    # Date resolution: sidecar > our filename > IG taken_at > mtime
    upload_date = sidecar_date or fn_date or ig_date or _mtime_date(media_file)

    return MediaIdentity(
        identifier  = ident,
        upload_date = upload_date,
        title       = title,
        is_manual   = is_manual,
    )


def archive_entry_for(platform: str, identity: MediaIdentity) -> str | None:
    """
    Given a non-manual identity, build the entry string the extractor
    would have written into its archive (sqlite for gallery-dl, txt for
    yt-dlp). Used by the bootstrap/reconcile pass to seed those archives.

    Returns None for manual files (the extractor doesn't know them).

    Entry formats by platform (verified against gallery-dl source +
    yt-dlp behavior):
      x         (gallery-dl twitter):  "twitter<tweet_id>_<num>"
      instagram (gallery-dl):          "instagram<shortcode>_<num>"
      tiktok    (yt-dlp):              "tiktok <video_id>"  (txt, space-sep)
    """
    if identity.is_manual:
        return None
    if platform == "x":
        # gallery-dl twitter default archive-format is "{tweet_id}_{num}"
        # prefixed with category "twitter". Our identifier already includes
        # the _num suffix when applicable.
        return f"twitter{identity.identifier}"
    if platform == "instagram":
        # gallery-dl's IG archive keys on the URL SHORTCODE (e.g. "C1a2b3").
        # A purely-numeric identifier here is a media PK parsed from an
        # instaloader-style filename (_parse_ig_pk), not a shortcode — seeding
        # it would never match a real gallery-dl entry and just pollute the
        # archive, so skip it. The DB's UNIQUE(platform, identifier) still
        # dedups those files; the extractor archive simply isn't involved.
        if identity.identifier.isdigit():
            return None
        return f"instagram{identity.identifier}"
    if platform == "tiktok":
        # yt-dlp uses "<extractor> <id>" — strip any _num suffix our parser
        # may have added (tiktok videos don't have a num concept).
        ident = identity.identifier.split("_")[0]
        return f"tiktok {ident}"
    return None
