"""
core.media_prep
───────────────
Make a loose (orphaned / chat_id-folder) media file ready for Telegram BEFORE
it is enqueued. Two concerns, applied only to video:

  1. FORMAT — Telegram streams a video inline only when it is an mp4/mov
     container carrying h264 video and aac/mp3 audio. Anything else (mkv, avi,
     ts, wmv, flv, HEVC/VP9/AV1, …) either uploads as a non-previewable
     document or is refused outright. We normalize it:
       • lossless REMUX when the codecs are already fine but the container
         isn't (ffmpeg -c copy + faststart) — zero quality loss; or
       • a high-quality RE-ENCODE (libx264 CRF 18 + aac) when the codecs
         themselves aren't streamable. Visually lossless, the necessary cost
         of an incompatible codec.

  2. SIZE — a single Telegram upload is capped (4 GiB on a Premium account).
     A file over the ceiling is split into <=1 GiB chunks by the AutoSplitter
     project (lossless stream-copy segmenting with its own integrity check),
     and each verified chunk is enqueued as its own message.

ORDER: convert FIRST, then split. Re-encoding an incompatible file often drops
it under the ceiling on its own (no split needed), and splitting on the final
streamable bytes gives correct chunk sizes. When both apply, the intermediate
converted file is split and then discarded.

ROBUSTNESS CONTRACT: this module never raises for an expected problem. A probe
that fails, an ffmpeg error, a missing AutoSplitter, or a failed integrity
check all return a PrepResult with ok=False and the original left untouched —
the caller quarantines the file (so it is not retried every sweep) rather than
shipping something broken or oversized. A file we don't need to touch
(compatible + within size) is returned as a no-op passthrough.

OUTPUT PLACEMENT: derived files are written NEXT TO the source so the orphaned
ingester's subfolder→album routing keeps working unchanged. Split parts are
the exception — each chunk of one video is a standalone clip, so they are
registered as individual messages (the caller decides this via .individual).
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from .files import VIDEO_EXTS

log = logging.getLogger(__name__)

# ── Tunables (env-overridable so a deployment can adjust without code edits) ──
#
# ARCHIVER_MEDIA_PREP=0            → disable the whole pre-flight (pure passthrough)
# ARCHIVER_TG_MAX_UPLOAD_BYTES=N  → upload ceiling; over it we split (default 4 GiB, Premium)
# ARCHIVER_SPLIT_CHUNK_BYTES=N    → target chunk size when splitting (default 1 GiB)
# ARCHIVER_DELETE_AFTER_SPLIT=0   → keep the original after a successful transform
# AUTOSPLITTER_HOME=/path         → where the AutoSplitter package lives, if not importable


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except ValueError:
        log.warning("media_prep: %s=%r is not an int — using %d", name, raw, default)
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


def prep_enabled() -> bool:
    return _env_bool("ARCHIVER_MEDIA_PREP", True)


def max_upload_bytes() -> int:
    return _env_int("ARCHIVER_TG_MAX_UPLOAD_BYTES", 4 * 1024 ** 3)


def split_chunk_bytes() -> int:
    return _env_int("ARCHIVER_SPLIT_CHUNK_BYTES", 1 * 1024 ** 3)


def delete_after_split() -> bool:
    return _env_bool("ARCHIVER_DELETE_AFTER_SPLIT", True)


# Extensions we will rescue by conversion even though they are NOT in the
# suite's canonical MEDIA_EXTENSIONS (the orphaned ingester is taught to accept
# these so they can be converted into a streamable .mp4 before enqueue). Kept
# local to prep — the global media set stays untouched so dedup/reconcile can't
# drift.
CONVERTIBLE_VIDEO_EXTS = {
    ".avi", ".ts", ".mts", ".m2ts", ".wmv", ".flv", ".m4v", ".mpg", ".mpeg",
    ".3gp", ".ogv", ".vob",
}

# A file is a prep candidate iff it is video by the canonical set OR one of the
# extra convertible containers above.
PREP_VIDEO_EXTS = VIDEO_EXTS | CONVERTIBLE_VIDEO_EXTS

# Telegram streams a video inline only for these. Everything else is converted.
_STREAMABLE_CONTAINERS = {"mp4", "mov", "m4a"}   # ffprobe format_name tokens
_STREAMABLE_VCODECS    = {"h264"}
_STREAMABLE_ACODECS    = {"aac", "mp3", None}    # None → no audio stream

# ffprobe/ffmpeg time caps. Conversions of a many-GB file can be slow, so the
# convert cap is generous; a probe must answer fast or we treat it as "unknown,
# leave it alone".
_PROBE_TIMEOUT_S   = 30.0
_CONVERT_TIMEOUT_S = 6 * 3600.0

# Marker so prep never re-processes its own output (defensive belt-and-braces;
# converted outputs are already compatible so they passthrough anyway).
_PREP_TAG = ".tgprep"


@dataclass
class PrepResult:
    """Outcome of preparing one file.

    outputs     — files to enqueue. For a no-op this is [original]; for a
                  convert it is [converted]; for a split it is the parts.
    transformed — True when the original was replaced (caller deletes it).
    individual  — True when each output must be its own message (split parts),
                  False when normal subfolder→album grouping applies.
    converted   — True when a FORMAT conversion happened (the original was
                  non-streamable). Distinct from a pure oversize split: it tells
                  the caller the original is a full-quality non-streamable source
                  worth keeping and uploading as a document alongside the
                  streamable copy. False for passthroughs and split-only outputs.
    ok / error  — ok=False means "could not prepare safely"; the original is
                  left on disk and outputs is empty. Caller quarantines.
    """
    outputs:     list[Path]
    transformed: bool       = False
    individual:  bool       = False
    converted:   bool       = False
    ok:          bool       = True
    error:       str | None = None
    temps:       list[Path] = field(default_factory=list)  # intermediates to clean

    @classmethod
    def passthrough(cls, path: Path) -> "PrepResult":
        return cls(outputs=[path])

    @classmethod
    def failed(cls, error: str) -> "PrepResult":
        return cls(outputs=[], ok=False, error=error)


# ── Probe ───────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class _Probe:
    container: str          # ffprobe format_name (comma list)
    vcodec:    str | None
    acodec:    str | None
    size:      int
    duration:  float        # seconds; 0.0 if unknown


def _probe(path: Path) -> _Probe | None:
    """Container + first video/audio codec + size + duration, or None if the
    file isn't a readable video (ffprobe missing/failed, or no video stream)."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=format_name,duration,size:stream=codec_type,codec_name",
        "-of", "json", str(path),
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, timeout=_PROBE_TIMEOUT_S)
    except (OSError, subprocess.SubprocessError) as e:
        log.warning("media_prep: ffprobe failed for %s: %s", path.name, e)
        return None
    if out.returncode != 0:
        return None
    try:
        data = json.loads(out.stdout or b"{}")
    except json.JSONDecodeError:
        return None

    fmt = data.get("format") or {}
    vcodec = acodec = None
    has_video = False
    for s in data.get("streams") or []:
        kind = s.get("codec_type")
        if kind == "video":
            has_video = True
            if vcodec is None:
                vcodec = (s.get("codec_name") or "").lower() or None
        elif kind == "audio" and acodec is None:
            acodec = (s.get("codec_name") or "").lower() or None
    if not has_video:
        return None  # audio-only / image / not a video we should touch

    try:
        size = int(fmt.get("size") or path.stat().st_size)
    except (ValueError, OSError):
        size = -1
    try:
        duration = float(fmt.get("duration") or 0.0)
    except (TypeError, ValueError):
        duration = 0.0
    return _Probe(
        container=(fmt.get("format_name") or "").lower(),
        vcodec=vcodec, acodec=acodec, size=size, duration=duration,
    )


def _is_streamable(p: _Probe) -> bool:
    container_ok = any(tok in _STREAMABLE_CONTAINERS
                       for tok in p.container.split(","))
    return (
        container_ok
        and p.vcodec in _STREAMABLE_VCODECS
        and p.acodec in _STREAMABLE_ACODECS
    )


def _codecs_copyable(p: _Probe) -> bool:
    """Codecs are already Telegram-friendly; only the container is wrong, so a
    lossless remux (stream copy into mp4) suffices."""
    return p.vcodec in _STREAMABLE_VCODECS and p.acodec in _STREAMABLE_ACODECS


# ── Convert ───────────────────────────────────────────────────────────────────

def _run_ffmpeg(cmd: list[str], what: str) -> bool:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                            timeout=_CONVERT_TIMEOUT_S)
    except (OSError, subprocess.SubprocessError) as e:
        log.warning("media_prep: ffmpeg failed (%s): %s", what, e)
        return False
    if r.returncode != 0:
        log.warning("media_prep: ffmpeg rc=%d (%s): %s",
                    r.returncode, what, (r.stderr or "").strip()[:300])
        return False
    return True


def _convert(src: Path, p: _Probe) -> Path | None:
    """Produce a streamable mp4 next to `src`. Lossless remux when the codecs
    already pass; otherwise a visually-lossless libx264/aac re-encode. Returns
    the output path, or None on failure (output cleaned up)."""
    # Prefer a clean "<stem>.mp4" so the UPLOADED filename carries no internal
    # marker — the output path is what every send path names the Telegram file
    # after. Fall back to the "<stem>.tgprep.mp4" tag only when the clean name
    # would clobber bytes we don't own: the source itself (an incompatible-codec
    # .mp4) or a pre-existing sibling. The tag stays a defensive last resort, not
    # the default, so it never reaches Telegram in the common case.
    clean = src.with_name(f"{src.stem}.mp4")
    dst = (clean if clean != src and not clean.exists()
           else src.with_name(f"{src.stem}{_PREP_TAG}.mp4"))
    if _codecs_copyable(p):
        ok = _run_ffmpeg(
            ["ffmpeg", "-y", "-v", "error", "-i", str(src),
             "-map", "0:v:0", "-map", "0:a:0?",
             "-c", "copy", "-movflags", "+faststart", str(dst)],
            what=f"remux {src.name}",
        )
        mode = "remux"
    else:
        ok = _run_ffmpeg(
            ["ffmpeg", "-y", "-v", "error", "-i", str(src),
             "-map", "0:v:0", "-map", "0:a:0?",
             "-c:v", "libx264", "-crf", "18", "-preset", "medium",
             "-pix_fmt", "yuv420p",
             "-c:a", "aac", "-b:a", "256k",
             "-movflags", "+faststart", str(dst)],
            what=f"re-encode {src.name}",
        )
        mode = "re-encode"

    if not ok or not dst.exists() or dst.stat().st_size == 0:
        _unlink(dst)
        return None

    # Integrity: the output must be a readable, streamable video of (about) the
    # same duration. A remux must match closely; a re-encode can drift slightly.
    out = _probe(dst)
    if out is None or not _is_streamable(out):
        log.warning("media_prep: %s of %s produced a non-streamable file — "
                    "discarding", mode, src.name)
        _unlink(dst)
        return None
    if p.duration > 0 and out.duration > 0:
        tol = max(2.0, p.duration * 0.02)
        if abs(out.duration - p.duration) > tol:
            log.warning("media_prep: %s of %s changed duration %.1fs→%.1fs "
                        "(>%.1fs) — discarding", mode, src.name,
                        p.duration, out.duration, tol)
            _unlink(dst)
            return None
    log.info("media_prep: %s %s → %s (%.2f GB)", mode, src.name, dst.name,
             dst.stat().st_size / 1e9)
    return dst


def streamable_temp(path: Path) -> Path | None:
    """Send-time safety net: if `path` is a video Telegram can't stream inline
    (wrong container and/or codec), produce a streamable .mp4 next to it and
    return that temp path; otherwise return None.

    Returns None for the common cases that need no work — a probe failure (not
    a readable video: image, audio-only, ffprobe missing), an already-streamable
    video, or a conversion that fails — so the caller simply sends the original.
    The caller owns the returned temp and must unlink it after sending.

    Unlike prepare(), this never splits: it is a last-line guard for producers
    that bypass ingest-time prep (chiefly the recorder, whose remux is allowed
    to fall back to the raw container so a recording is never lost). Oversize
    handling stays prepare()'s job; a too-large remux fails to send exactly as
    the raw file would have, which is no worse than the status quo."""
    if not prep_enabled():
        return None
    p = _probe(path)
    if p is None or _is_streamable(p):
        return None
    return _convert(path, p)


def clean_upload_name(path: "str | Path") -> str:
    """The basename to show on Telegram, with the internal '.tgprep' marker
    stripped. _convert stores its output as '<stem>.tgprep.mp4' whenever the
    clean '<stem>.mp4' would clobber bytes we don't own — chiefly an
    incompatible-codec .mp4 source (clean name == source) or a pre-existing
    sibling. That on-disk tag is fine, but it must NEVER reach the upload, so
    every send path names the Telegram file after this. Idempotent for names
    that already carry no tag."""
    name = Path(path).name
    tag = f"{_PREP_TAG}."
    return name.replace(tag, ".", 1) if tag in name else name


def is_nonstreamable_video(path: Path) -> bool:
    """True when `path` is a readable video Telegram CAN'T stream inline (wrong
    container/codec). Used by producers that deliberately ship a non-streamable
    file as-is (a .mkv kept as a full-quality document alongside its .mp4
    preview): such a file should go up as a downloadable DOCUMENT, not as a
    half-broken streaming video that just duplicates its own preview.

    False for non-videos (images/audio/probe failure) and already-streamable
    videos — those keep the normal streaming-video send path."""
    p = _probe(path)
    return p is not None and not _is_streamable(p)


# ── Split (AutoSplitter) ──────────────────────────────────────────────────────
#
# AutoSplitter ships two ways and we support both. Typically it is installed
# stand-alone (pipx → its own isolated interpreter), so it CANNOT be imported
# into this process; we drive its CLI as a subprocess. If it instead happens to
# be importable in this same venv (editable install / sibling checkout on the
# path) we call run_split() in-process to skip the subprocess hop. Either way a
# missing AutoSplitter degrades to "can't split" — never a crash.

_run_split_cache: "object | None" = None
_cli_cache: "str | None | bool" = None


def _load_run_split():
    """Return an importable AutoSplitter run_split(), or None. Tried first; the
    CLI is the fallback when AutoSplitter lives in its own (e.g. pipx) venv."""
    global _run_split_cache
    if _run_split_cache is not None:
        return None if _run_split_cache is False else _run_split_cache
    try:
        from autosplitter.splitter import run_split  # type: ignore
        _run_split_cache = run_split
        return run_split
    except ImportError:
        pass
    # Sibling source checkout on the path (dev convenience).
    candidates = []
    home = os.environ.get("AUTOSPLITTER_HOME")
    if home:
        candidates.append(Path(home))
    candidates.append(Path(__file__).resolve().parents[3] / "autosplitter")
    for cand in candidates:
        if (cand / "autosplitter" / "splitter.py").exists():
            sys.path.insert(0, str(cand))
            try:
                from autosplitter.splitter import run_split  # type: ignore
                _run_split_cache = run_split
                return run_split
            except ImportError:
                continue
    _run_split_cache = False
    return None


def _find_cli() -> str | None:
    """The AutoSplitter CLI executable (AUTOSPLITTER_BIN override, else on PATH),
    or None if it isn't installed."""
    global _cli_cache
    if _cli_cache is not None:
        return None if _cli_cache is False else _cli_cache
    import shutil
    cli = os.environ.get("AUTOSPLITTER_BIN") or shutil.which("autosplitter")
    _cli_cache = cli or False
    return cli


def _segment_parts(src: Path) -> list[Path]:
    """Read AutoSplitter's <stem>_segments.txt to learn the exact part files it
    wrote (basenames, one per line), resolved against src's directory."""
    listing = src.parent / f"{src.stem}_segments.txt"
    try:
        lines = listing.read_text().splitlines()
    except OSError:
        return []
    return [src.parent / ln.strip() for ln in lines if ln.strip()]


def _cleanup_split_debris(src: Path) -> None:
    """Remove a failed split's partial parts + segment list so a retry (or the
    next sweep) starts clean and nothing half-written gets enqueued."""
    for stray in src.parent.glob(f"{src.stem}_part*"):
        _unlink(stray)
    _unlink(src.parent / f"{src.stem}_segments.txt")


def _split(src: Path) -> list[Path] | None:
    """Split `src` into <=chunk-size parts beside it via AutoSplitter. Returns
    the verified part paths, or None on any failure (no parts trusted)."""
    target_gib = split_chunk_bytes() / (1024 ** 3)

    run_split = _load_run_split()
    if run_split is not None:
        return _split_in_process(run_split, src, target_gib)

    cli = _find_cli()
    if cli is not None:
        return _split_via_cli(cli, src, target_gib)

    log.error("media_prep: AutoSplitter not found (import or CLI; set "
              "AUTOSPLITTER_BIN/AUTOSPLITTER_HOME) — cannot split oversized files")
    return None


def _split_in_process(run_split, src: Path, target_gib: float) -> list[Path] | None:
    try:
        result = run_split(
            input_path=str(src),
            target_size_gb=target_gib,
            output_dir=str(src.parent),
        )
    except Exception as e:   # AutoSplitter raises ValueError/RuntimeError
        log.warning("media_prep: AutoSplitter failed on %s: %s", src.name, e)
        _cleanup_split_debris(src)
        return None
    if not result.integrity_ok or not result.parts:
        log.warning("media_prep: AutoSplitter integrity check FAILED on %s — "
                    "discarding parts", src.name)
        _cleanup_split_debris(src)
        return None
    _unlink(src.parent / f"{src.stem}_segments.txt")
    log.info("media_prep: split %s → %d part(s) of <=%.2f GiB",
             src.name, len(result.parts), target_gib)
    return [Path(p) for p in result.parts]


def _split_via_cli(cli: str, src: Path, target_gib: float) -> list[Path] | None:
    """Run the AutoSplitter CLI in single-file mode. Exit 0 means it both split
    and passed its own integrity check; exit 2 is an integrity failure. We learn
    the parts from the segment-list it writes, then verify they exist."""
    cmd = [
        cli, str(src),
        "--size", repr(target_gib), "--size-unit", "GiB",
        "--output", str(src.parent),
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=_CONVERT_TIMEOUT_S)
    except (OSError, subprocess.SubprocessError) as e:
        log.warning("media_prep: AutoSplitter CLI failed on %s: %s", src.name, e)
        _cleanup_split_debris(src)
        return None
    if r.returncode != 0:
        log.warning("media_prep: AutoSplitter CLI rc=%d on %s (integrity?): %s",
                    r.returncode, src.name, (r.stderr or "").strip()[:300])
        _cleanup_split_debris(src)
        return None
    parts = _segment_parts(src)
    if not parts or not all(p.exists() and p.stat().st_size > 0 for p in parts):
        log.warning("media_prep: AutoSplitter CLI produced no usable parts for "
                    "%s — discarding", src.name)
        _cleanup_split_debris(src)
        return None
    _unlink(src.parent / f"{src.stem}_segments.txt")
    log.info("media_prep: split %s → %d part(s) of <=%.2f GiB (cli)",
             src.name, len(parts), target_gib)
    return parts


# ── Orchestrator ──────────────────────────────────────────────────────────────

def prepare(path: Path) -> PrepResult:
    """Prepare one file for Telegram. See module docstring for the contract.

    Never raises. Cleans up its own intermediate files before returning; the
    caller is responsible only for deleting the ORIGINAL when .transformed."""
    if not prep_enabled():
        return PrepResult.passthrough(path)
    if path.suffix.lower() not in PREP_VIDEO_EXTS:
        return PrepResult.passthrough(path)        # images / non-video untouched

    probe = _probe(path)
    if probe is None:
        # Unreadable or not actually a video. Leave it to the normal ingest path
        # (it will hash/skip as today). Not our failure to own.
        return PrepResult.passthrough(path)

    streamable = _is_streamable(probe)
    ceiling = max_upload_bytes()
    oversize = probe.size > ceiling if probe.size > 0 else False

    if streamable and not oversize:
        return PrepResult.passthrough(path)        # already perfect

    temps: list[Path] = []

    # 1. Convert first (only when needed). The to-split source becomes the
    #    converted file so we split final, streamable bytes.
    to_split = path
    converted: Path | None = None
    if not streamable:
        converted = _convert(path, probe)
        if converted is None:
            return PrepResult.failed(f"conversion failed: {path.name}")
        to_split = converted
        # Re-encoding may have brought it under the ceiling.
        try:
            oversize = converted.stat().st_size > ceiling
        except OSError:
            oversize = False

    # 2. Split if still oversized.
    if oversize:
        parts = _split(to_split)
        if parts is None:
            _unlink(converted)                     # clean the intermediate
            return PrepResult.failed(f"split failed: {path.name}")
        if converted is not None:
            _unlink(converted)                     # intermediate consumed by split
        return PrepResult(outputs=parts, transformed=True, individual=True,
                          converted=converted is not None)

    # Converted but within size → single streamable output.
    assert converted is not None
    return PrepResult(outputs=[converted], transformed=True, individual=False,
                      converted=True)


def _unlink(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except OSError as e:
        log.warning("media_prep: could not remove %s: %s", path, e)
