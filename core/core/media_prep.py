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
     and each verified chunk is enqueued as its own message. A caller may pass
     prepare(split_threshold_bytes=N) to LOWER that trigger below the ~3.9 GiB
     ceiling — e.g. the recorder-output "split mode" cuts every recording over
     2 GiB into <=2 GiB parts (see archiver.reconcile).

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
the exception — the .individual flag marks them as the chunks of ONE oversize
video; the caller (orphaned/reconcile) stamps every part with a shared
split_group_key so the dispatcher ships them as a single ordered album.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

from . import env, ffmpeg, ffprobe, paths
from .files import VIDEO_EXTS

log = logging.getLogger(__name__)

# ── Tunables (env-overridable so a deployment can adjust without code edits) ──
#
# ARCHIVER_MEDIA_PREP=0            → disable the whole pre-flight (pure passthrough)
# ARCHIVER_TG_MAX_UPLOAD_BYTES=N  → upload ceiling; over it we split (default below)
# ARCHIVER_SPLIT_CHUNK_BYTES=N    → target chunk size when splitting (default 1 GiB)
# ARCHIVER_DELETE_AFTER_SPLIT=0   → keep the original after a successful transform
# AUTOSPLITTER_HOME=/path         → where the AutoSplitter package lives, if not importable


def prep_enabled() -> bool:
    return env.opt_bool("ARCHIVER_MEDIA_PREP", True)


# The real ceiling is NOT the account's 4 GiB file-size cap — it's the upload
# PART-COUNT limit. The uploader (dispatcher.fast_upload) sends fixed 512 KiB
# parts, and Telegram's SaveBigFilePart accepts at most ~8000 parts (the Premium
# "4 GB" tier). 8000 × 512 KiB = 4,194,304,000 B ≈ 3.906 GiB is the largest a
# single shot can carry; a file above it needs ≥8001 parts and Telegram rejects
# the whole upload with FilePartsInvalidError. The old 4 GiB default sat ABOVE
# this wall, so a file in the 3.906–4.0 GiB band converted fine, never tripped
# the split, then died at upload. We default to 8000 parts minus a small margin
# so such files split into ≤1 GiB chunks (which upload trivially) instead.
_PART_SIZE = 512 * 1024          # must track dispatcher.fast_upload.PART_SIZE
_SAFE_MAX_PARTS = 7936           # 8000 server cap − 64-part safety margin


def max_upload_bytes() -> int:
    return env.opt_int(
        "ARCHIVER_TG_MAX_UPLOAD_BYTES", _SAFE_MAX_PARTS * _PART_SIZE, min_value=1)


def split_chunk_bytes() -> int:
    return env.opt_int("ARCHIVER_SPLIT_CHUNK_BYTES", 1 * 1024 ** 3, min_value=1)


def delete_after_split() -> bool:
    return env.opt_bool("ARCHIVER_DELETE_AFTER_SPLIT", True)


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
    busy        — True when ANOTHER worker is already converting/splitting this
                  same source (the per-file prep lock is held). NOT a failure:
                  outputs is empty, ok stays True, the original is untouched, and
                  the caller must simply SKIP the file this cycle and retry next
                  sweep — never quarantine it, never register it raw. This is what
                  stops two workers (recorder sweep + archiver sweep) from
                  re-encoding the same file into the same output at once.
    """
    outputs:     list[Path]
    transformed: bool       = False
    individual:  bool       = False
    converted:   bool       = False
    ok:          bool       = True
    error:       str | None = None
    busy:        bool       = False
    temps:       list[Path] = field(default_factory=list)  # intermediates to clean

    @classmethod
    def passthrough(cls, path: Path) -> "PrepResult":
        return cls(outputs=[path])

    @classmethod
    def failed(cls, error: str) -> "PrepResult":
        return cls(outputs=[], ok=False, error=error)

    @classmethod
    def busy_(cls) -> "PrepResult":
        return cls(outputs=[], busy=True)


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
    data = ffprobe.probe_json(
        path,
        show_entries="format=format_name,duration,size:stream=codec_type,codec_name",
        timeout=_PROBE_TIMEOUT_S,
    )
    if data is None:
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
    return ffmpeg.run_ffmpeg(cmd, what=what, timeout=_CONVERT_TIMEOUT_S)


def _remux_cmd(src: Path, dst: Path) -> list[str]:
    return ["ffmpeg", "-y", "-v", "error", "-i", str(src),
            "-map", "0:v:0", "-map", "0:a:0?",
            "-c", "copy", "-movflags", "+faststart", str(dst)]


def _reencode_cmd(src: Path, dst: Path) -> list[str]:
    # -fflags +genpts rebuilds presentation timestamps from a source whose own
    # are missing/irregular (chiefly a live-recorded .mkv), so the output mp4
    # carries a real duration instead of a 0-length moov.
    return ["ffmpeg", "-y", "-v", "error", "-fflags", "+genpts", "-i", str(src),
            "-map", "0:v:0", "-map", "0:a:0?",
            "-c:v", "libx264", "-crf", "18", "-preset", "medium",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "256k",
            "-movflags", "+faststart", str(dst)]


def _accept(p: _Probe, dst: Path, mode: str, src_name: str) -> bool:
    """Integrity gate for a freshly produced `dst`: a non-empty, readable,
    streamable mp4 with a POSITIVE duration of (about) the source length. A
    remux must match closely; a re-encode can drift slightly. Returns False
    (caller discards) on any failure; never unlinks."""
    try:
        if not dst.exists() or dst.stat().st_size == 0:
            return False
    except OSError:
        return False
    out = _probe(dst)
    if out is None or not _is_streamable(out):
        log.warning("media_prep: %s of %s produced a non-streamable file",
                    mode, src_name)
        return False
    # A 0-duration output is the silent-failure case that motivated this gate:
    # a stream-copy of a timestamp-less .mkv yields an mp4 that plays in tolerant
    # local players but reads as 0:00 on Telegram. The source duration is often
    # unknown (0) for such files, so this MUST be checked independently of the
    # tolerance comparison below — never gate it behind p.duration > 0.
    if out.duration <= 0:
        log.warning("media_prep: %s of %s produced a 0-duration file — "
                    "discarding", mode, src_name)
        return False
    if p.duration > 0:
        tol = max(2.0, p.duration * 0.02)
        if abs(out.duration - p.duration) > tol:
            log.warning("media_prep: %s of %s changed duration %.1fs→%.1fs "
                        "(>%.1fs) — discarding", mode, src_name,
                        p.duration, out.duration, tol)
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

    # A lossless remux is preferred whenever the codecs already pass, but a
    # stream-copy CANNOT repair a source with missing/irregular timestamps — it
    # can produce a 0-duration mp4 that fails _accept. When the cheap remux is
    # unusable, fall back to a full re-encode, which rebuilds timestamps and
    # durations from decoded frames. (-c copy ignores -fflags +genpts, so the
    # re-encode is the only path that actually fixes such a source.)
    if _codecs_copyable(p):
        if (_run_ffmpeg(_remux_cmd(src, dst), what=f"remux {src.name}")
                and _accept(p, dst, "remux", src.name)):
            log.info("media_prep: remux %s → %s (%.2f GB)", src.name, dst.name,
                     dst.stat().st_size / 1e9)
            return dst
        log.info("media_prep: remux of %s unusable — re-encoding", src.name)

    if (_run_ffmpeg(_reencode_cmd(src, dst), what=f"re-encode {src.name}")
            and _accept(p, dst, "re-encode", src.name)):
        log.info("media_prep: re-encode %s → %s (%.2f GB)", src.name, dst.name,
                 dst.stat().st_size / 1e9)
        return dst

    _unlink(dst)
    return None


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
    # Extension gate, matching prepare(). WITHOUT it a still image (.jpg/.webp)
    # slips through: ffprobe reports a single-frame mjpeg/png "video" stream, so
    # _is_streamable says False and we'd "re-encode" the photo into a 0-second
    # h264 .mp4 Telegram can't play. Only real video containers are candidates.
    if path.suffix.lower() not in PREP_VIDEO_EXTS:
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


def clean_upload_stem(path: "str | Path") -> str:
    """The display STEM (no extension) with the internal '.tgprep' marker
    stripped — for captions that list filenames (orphaned-folder batches, split
    albums). Path(...).stem alone leaves the tag visible (a '<stem>.tgprep.mp4'
    file stems to '<stem>.tgprep'), leaking the internal marker into the message;
    this is the caption-side companion to clean_upload_name (which keeps the
    extension for the file's display name)."""
    return Path(clean_upload_name(path)).stem


def is_nonstreamable_video(path: Path) -> bool:
    """True when `path` is a readable video Telegram CAN'T stream inline (wrong
    container/codec). Used by producers that deliberately ship a non-streamable
    file as-is (a .mkv kept as a full-quality document alongside its .mp4
    preview): such a file should go up as a downloadable DOCUMENT, not as a
    half-broken streaming video that just duplicates its own preview.

    False for non-videos (images/audio/probe failure) and already-streamable
    videos — those keep the normal streaming-video send path."""
    # Extension gate, matching prepare()/streamable_temp: a still image probes as
    # a single-frame mjpeg/png "video", which would otherwise be misreported here
    # as a non-streamable video and shipped as a document.
    if path.suffix.lower() not in PREP_VIDEO_EXTS:
        return False
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


def _split(src: Path, chunk_bytes: int | None = None) -> list[Path] | None:
    """Split `src` into <=chunk-size parts beside it via AutoSplitter. Returns
    the verified part paths, or None on any failure (no parts trusted).

    `chunk_bytes` overrides the default target part size (split_chunk_bytes);
    used when a caller drives a smaller split trigger (recorder split mode)."""
    target_gib = (chunk_bytes if chunk_bytes is not None
                  else split_chunk_bytes()) / (1024 ** 3)

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
    # AutoSplitter's CLI auto-classifies the positional arg as file-vs-folder and
    # always writes parts NEXT TO the source (no --output flag); for a file that
    # is src.parent, exactly where _segment_parts() looks. (The in-process
    # run_split still takes an explicit output_dir; only the CLI dropped it.)
    # We deliberately do NOT pass --remove-original: media_prep owns retiring the
    # source after its outputs are registered, so the split must leave it in place.
    cmd = [
        cli, str(src),
        "--size", repr(target_gib), "--size-unit", "GiB",
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

@contextlib.contextmanager
def _prep_lock(src: Path):
    """Cross-process exclusive lock for preparing ONE source file. Yields True if
    we acquired it, False if another worker already holds it (is converting the
    same file right now).

    Why this exists: recorder and archiver BOTH run an ingest sweep over the same
    record folder, and a large HEVC re-encode takes far longer than the 180s
    sweep interval — so without serialization the next sweep (in either worker)
    launches a SECOND ffmpeg on the same source, writing the SAME deterministic
    '<stem>.mp4'/'.tgprep.mp4' output. They clobber each other, neither ever
    produces an acceptable file, the source is never registered, and the loop
    repeats forever at 2×CPU. The per-worker InstanceLock can't help: recorder
    and archiver are DIFFERENT named singletons. The contended resource is the
    FILE, so the lock is keyed on the file.

    flock (not a PID file) so the kernel frees it if the holder crashes/SIGKILLs —
    no stale-lock wedge. Non-blocking: a busy file is skipped this cycle and
    retried next sweep, never queued behind a multi-minute encode.
    The lock file name is a hash of the resolved source path (paths can contain
    spaces / arbitrary chars; a hash is a safe fixed-width filename)."""
    key = hashlib.sha1(str(src.resolve()).encode("utf-8", "surrogatepass")).hexdigest()[:16]
    lock_path = paths.locks_dir() / f"prep-{key}.lock"
    handle = None
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            yield False
            return
        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()} {src.name}\n")
        handle.flush()
        yield True
    finally:
        if handle is not None and not handle.closed:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()


def prepare(path: Path, *, split_threshold_bytes: int | None = None) -> PrepResult:
    """Prepare one file for Telegram. See module docstring for the contract.

    `split_threshold_bytes` (optional) lowers the size at which we split below
    the default ~3.9 GiB upload ceiling AND caps each part to that size — so a
    caller can force every file over, say, 2 GiB into <=2 GiB parts (recorder
    split mode). Clamped to the hard ceiling: it can only make the trigger
    SMALLER, never larger (a single upload above the ceiling would be rejected).

    Never raises. Cleans up its own intermediate files before returning; the
    caller is responsible only for deleting the ORIGINAL when .transformed."""
    if not prep_enabled():
        return PrepResult.passthrough(path)
    if path.suffix.lower() not in PREP_VIDEO_EXTS:
        return PrepResult.passthrough(path)        # images / non-video untouched

    probe = _probe(path)
    if probe is None:
        # Unreadable or not actually a video. Leave it to the normal ingest path
        # (it will hash/skip as today). Not our failure to own — and crucially the
        # recorder's raw-fallback contract DEPENDS on this: a non-streamable or
        # not-yet-probeable recording is enqueued RAW and the dispatcher's
        # send-time streamable net converts it at upload, so a recording is never
        # lost. Refusing here would strand legitimate raw recordings. The
        # incomplete-file case that wedged the queue is caught upstream by the
        # ingest layer's stabilize-before-prep gate (register_media, orphaned, and
        # reconcile all is_stable() first), so a half-written file never reaches
        # prep to be passthrough-registered raw.
        return PrepResult.passthrough(path)

    streamable = _is_streamable(probe)
    ceiling = max_upload_bytes()
    chunk_target = split_chunk_bytes()
    # Optional lower split trigger: split at the caller's threshold instead of
    # the upload ceiling, and never produce a part larger than it. Only ever
    # tightens the limits (min), so a misconfigured huge value can't raise the
    # trigger above the hard ceiling.
    if split_threshold_bytes is not None and split_threshold_bytes > 0:
        ceiling = min(ceiling, split_threshold_bytes)
        chunk_target = min(chunk_target, split_threshold_bytes)
    oversize = probe.size > ceiling if probe.size > 0 else False

    if streamable and not oversize:
        return PrepResult.passthrough(path)        # already perfect

    # Real work (convert and/or split) needed. Serialize it per source file
    # across ALL workers: if another sweep is already preparing this exact file,
    # skip it this cycle rather than launch a second clobbering encode. The lock
    # spans convert AND split so the whole deterministic-output section is
    # single-writer; it releases the instant we return.
    with _prep_lock(path) as acquired:
        if not acquired:
            log.info("media_prep: %s already being prepared by another worker "
                     "— skipping this cycle", path.name)
            return PrepResult.busy_()

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
            parts = _split(to_split, chunk_target)
            if parts is None:
                _unlink(converted)                 # clean the intermediate
                return PrepResult.failed(f"split failed: {path.name}")
            if converted is not None:
                _unlink(converted)                 # intermediate consumed by split
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
