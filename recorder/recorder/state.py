"""
recorder.state
──────────────
Explicit state machine + producer/consumer uploader thread.

STATES:
  LISTENING — poll the priority list for a live user.
  RECORDING — a capture is running; wait for it to finish.
  HANDOFF   — a recording just ended; re-scan the priority list ONCE
              (someone else, higher or lower priority, may be live now)
              before dropping back to LISTENING.
  STOPPED   — terminal.

PRODUCER/CONSUMER (guide lesson, kept):
  The state machine PRODUCES finished files onto a queue.Queue; a daemon
  uploader thread CONSUMES them and enqueues into the shared items table. This
  decouples "recording" from "enqueuing" so a slow DB write
  can't make us miss the next stream start. queue.Queue is thread-safe by
  construction — no manual locks.

LOCK PLACEMENT (deviates from the guide's literal code):
  The guide wraps the entire run_forever loop in `with self.lock:`, which
  would hold the TikTok download-lock even while merely LISTENING and
  starve the archiver's TikTok backlog the whole time the recorder is up.
  Instead we acquire the lock ONLY around an active recording (enter on
  start, release when the capture ends). Archiver skips TikTok downloads
  exactly when a capture is in flight, and is free to drain TikTok
  backlog while the recorder idles in LISTENING. This matches the design
  intent in §0 ("skip download while recorder runs [a recording]").
"""

from __future__ import annotations

import logging
import queue
import subprocess
import threading
import time
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Callable

from core import split_group_key

from . import ui
from .capture import StreamCapture
from .config import RecorderConfig
from .lock import TikTokLock
from .platforms.base import LivePlatform

log = logging.getLogger(__name__)

_VIDEO_SUFFIXES = frozenset({".mp4", ".ts", ".mkv", ".webm", ".flv", ".m4v"})


def _safe_size(p: Path) -> int:
    """Byte size of `p`, or 0 if it vanished/can't be stat'd. Used to total a
    recording session without letting a transient FS error abort the tally."""
    try:
        return p.stat().st_size
    except OSError:
        return 0


def _remux_for_telegram(src: Path) -> Path:
    """Remux src to a progressive MP4 with moov atom at the front.

    HLS live streams recorded with --hls-use-mpegts land as MPEG-TS content
    (no valid MP4 moov structure), and even .mp4-extension files from ffmpeg's
    HLS downloader are often fragmented/streaming-layout. Telegram can't play
    either format inline. A -c copy remux to +faststart MP4 takes seconds
    (no re-encode, just container surgery) and fixes both cases.

    Returns the path to upload — remuxed on success, unchanged src on failure.
    Never raises; a failed remux falls back to the original so no recording
    is ever lost."""
    if src.suffix.lower() not in _VIDEO_SUFFIXES:
        return src

    # Integrity guard: only remux a source that's actually present and non-empty.
    # A vanished/zero-byte source here means the capture was already lost (e.g. a
    # subprocess orphaned past termination and its file unlinked); remuxing it
    # would just produce an ffmpeg "No such file" and a misleading fallback.
    if not src.exists() or _safe_size(src) == 0:
        log.warning("remux: %s missing or empty — nothing to convert", src.name)
        return src

    # If src is already .mp4 we can't overwrite it while ffmpeg reads it,
    # so write to a temp name and rename over it after. The temp is a DOTFILE
    # ('.<stem>._tmp.mp4'): if a kill interrupts the remux mid-write, the leftover
    # must NOT look like a recording — both output_files() and the startup sweep
    # skip dotfiles, so a hidden temp can't be re-enqueued as a phantom row.
    if src.suffix.lower() == ".mp4":
        tmp = src.with_name("." + src.stem + "._tmp.mp4")
        final = src
    else:
        tmp = src.with_suffix(".mp4")
        final = tmp

    cmd = [
        "ffmpeg", "-y", "-loglevel", "error",
        "-i", str(src), "-c", "copy",
        "-movflags", "+faststart",
        str(tmp),
    ]
    try:
        result = subprocess.run(cmd, timeout=600, capture_output=True)
        if result.returncode != 0:
            stderr = result.stderr.decode(errors="replace").strip()
            raise RuntimeError(f"ffmpeg rc={result.returncode}: {stderr}")
    except (subprocess.TimeoutExpired, OSError, RuntimeError) as e:
        log.warning("remux: %s: failed (%s) — uploading original", src.name, e)
        tmp.unlink(missing_ok=True)
        return src

    # Never destroy the source until the remux output is proven real. An ffmpeg
    # rc=0 that nonetheless left no/empty output (rare, but it would otherwise
    # trade a good .flv for a 0-byte .mp4) must keep the original instead.
    if not tmp.exists() or _safe_size(tmp) == 0:
        log.warning("remux: %s produced no output despite rc=0 — keeping original",
                    src.name)
        tmp.unlink(missing_ok=True)
        return src

    try:
        src.unlink()
    except OSError as e:
        log.debug("remux: could not remove source %s: %s", src.name, e)

    if tmp != final:
        try:
            tmp.rename(final)
        except OSError as e:
            log.warning("remux: rename %s → %s failed: %s — using tmp path",
                        tmp.name, final.name, e)
            return tmp

    log.info("remux → %s", final.name, extra={"ev": "remux"})
    return final


class RecorderState(Enum):
    LISTENING = auto()
    RECORDING = auto()
    HANDOFF   = auto()
    STOPPED   = auto()


@dataclass
class _Job:
    """A finished file awaiting enqueue. `group_key`, when set, albums this file
    with the other segments of the same broadcast (a reconnect-stitched session
    that produced more than one file); None → the file sends on its own."""
    username: str
    file_path: Path
    group_key: str | None = None


# enqueue_fn signature: (platform, username, file_path, caption, group_key) -> None
EnqueueFn = Callable[[str, str, str, str, str | None], None]


class StateMachine:
    def __init__(
        self,
        config:    RecorderConfig,
        platform:  LivePlatform,
        capture:   StreamCapture,
        enqueue_fn: EnqueueFn,
        lock:      TikTokLock,
    ):
        self.config    = config
        self.platform  = platform
        self.capture   = capture
        self.enqueue   = enqueue_fn
        self.lock      = lock
        self.state     = RecorderState.LISTENING
        self.current_user: str | None = None
        self._stop     = threading.Event()
        self._upload_q: "queue.Queue[_Job]" = queue.Queue()
        self._lock_held = False

    # ── lifecycle ─────────────────────────────────────────────────────────

    def run_forever(self) -> None:
        uploader_thread = threading.Thread(
            target=self._uploader_loop, daemon=True, name="uploader",
        )
        uploader_thread.start()
        log.info("watching %d user%s — listening for live streams",
                 len(self.config.tiktok_users),
                 "" if len(self.config.tiktok_users) == 1 else "s",
                 extra={"ev": "start"})
        try:
            while not self._stop.is_set():
                try:
                    self._tick()
                except Exception as e:
                    # A single bad tick must never permanently stop the
                    # recorder. Log with traceback, pause one poll interval
                    # so we don't hot-loop on a persistent fault, then carry
                    # on listening.
                    log.error("recorder: tick failed in state %s: %s — recovering",
                              self.state.name, e, exc_info=True)
                    self._release_lock_if_held()
                    self.state = RecorderState.LISTENING
                    self._stop.wait(self.config.poll_interval_s)
        finally:
            # Ensure the lock is released even if we exit mid-recording.
            self._release_lock_if_held()
            self._stop.set()
            uploader_thread.join(timeout=15.0)
            if uploader_thread.is_alive():
                log.warning("recorder: uploader still draining after shutdown; "
                            "remaining files stay on disk for manual recovery")
            self.state = RecorderState.STOPPED
            log.info("stopped", extra={"ev": "stop"})

    def request_stop(self) -> None:
        """Signal a clean shutdown. Safe to call from a signal handler."""
        log.info("stop requested — finishing up", extra={"ev": "stop"})
        self._stop.set()

    def record_once(self, username: str) -> bool:
        """Manual one-shot: if @username is live, record until the stream ends
        (or a stop is requested), enqueue the file(s), and return — no LISTENING
        loop, no priority re-scan, no uploader thread.

        Reuses the same record + enqueue mechanics as the loop so behavior is
        identical to a live capture; only the scheduling differs. Returns True
        if a recording was produced, False if the user wasn't live (or the
        stream couldn't be started). A Ctrl-C mid-recording still enqueues the
        partial file (yt-dlp's --no-part keeps it usable)."""
        username = username.lstrip("@")
        if self._stop.is_set():
            return False
        try:
            live = self.platform.is_live(username)
        except Exception as e:
            log.error("record-once: is_live(@%s) failed: %s", username, e)
            return False
        if not live:
            log.info("@%s is not live right now — nothing to record",
                     username, extra={"ev": "listen"})
            return False

        log.info("@%s is LIVE — recording until the stream ends "
                 "(Ctrl-C to stop early)", username, extra={"ev": "live"})
        self._start_recording(username)
        if self.state != RecorderState.RECORDING:
            return False                    # stream_url/capture failed (logged)

        # Blocks until the stream ends or stop fires; releases the lock and
        # queues the finished file(s) onto _upload_q (same as the loop path).
        self._wait_for_recording_done()

        # No uploader thread in one-shot mode — drain synchronously so the
        # process can exit once the file is registered.
        drained = 0
        while not self._upload_q.empty():
            try:
                self._enqueue_job(self._upload_q.get_nowait())
                drained += 1
            except queue.Empty:
                break
        log.info("done — %d file%s queued for upload", drained,
                 "" if drained == 1 else "s", extra={"ev": "queued"})
        self.state = RecorderState.STOPPED
        return True

    # ── tick dispatch ─────────────────────────────────────────────────────

    def _tick(self) -> None:
        if self.state == RecorderState.LISTENING:
            self._poll_for_live()
        elif self.state == RecorderState.RECORDING:
            self._wait_for_recording_done()
        elif self.state == RecorderState.HANDOFF:
            self._scan_priority_list_once()

    # ── states ────────────────────────────────────────────────────────────

    def _poll_for_live(self) -> None:
        for username in self.config.tiktok_users:   # priority order
            if self._stop.is_set():
                return
            if self.platform.is_live(username):
                log.info("@%s is LIVE — recording", username, extra={"ev": "live"})
                self._start_recording(username)
                return
        # Nobody live — sleep, but wake on stop.
        self._stop.wait(self.config.poll_interval_s)

    def _start_recording(self, username: str) -> None:
        # Commit to recording this user: take the download-lock, then open a
        # capture. _open_capture resolves a fresh URL and launches yt-dlp; on
        # any failure we release the lock and fall back to listening.
        self._acquire_lock(username)
        if not self._open_capture(username):
            self._release_lock_if_held()
            self.state = RecorderState.LISTENING
            return
        self.current_user = username
        self.state = RecorderState.RECORDING

    def _open_capture(self, username: str) -> bool:
        """Resolve a FRESH stream URL and launch a capture for `username`.
        Returns True on success. Touches neither the lock nor self.state — the
        caller owns those (initial start acquires the lock; a reconnect keeps
        the one it already holds). Re-resolving the URL each time is essential
        for reconnects: a rotated/expired m3u8 URL is the usual reason a live
        capture exits early, so reusing the old one would just re-fail."""
        try:
            url = self.platform.stream_url(username)
        except Exception as e:
            log.error("recorder: stream_url(%s) failed: %s", username, e)
            return False
        try:
            self.capture.start(url, username)
        except Exception as e:
            log.error("recorder: capture start for @%s failed: %s", username, e)
            return False
        return True

    def _confirm_still_live(self, username: str | None) -> bool:
        """Re-check whether `username` is still broadcasting after a capture
        exit. Biased toward CONTINUING: returns True as soon as ANY of
        live_confirm_samples polls (spaced live_confirm_interval_s) reports
        live, and False only when EVERY sample says offline.

        Rationale — the fault we are fixing is a premature STOP, so when
        uncertain we keep recording. A false 'still live' (is_live() lagging
        after a real end) costs only one bounded relaunch that the capture's
        dead-stream guard (rc=-2) terminates — never lost data. A stop request
        short-circuits to False; is_live() is contracted not to raise, but a
        raise is treated defensively as 'not live for this sample'."""
        if not username:
            return False
        samples = max(1, self.config.live_confirm_samples)
        for i in range(samples):
            if self._stop.is_set():
                return False
            try:
                if self.platform.is_live(username):
                    return True
            except Exception as e:
                log.debug("recorder: is_live(%s) raised during confirm: %s",
                          username, e)
            if i < samples - 1 and self._stop.wait(self.config.live_confirm_interval_s):
                return False
        return False

    def _wait_for_recording_done(self) -> None:
        """Record current_user until the broadcast GENUINELY ends.

        yt-dlp can exit while the user is STILL live — TikTok rotates the m3u8
        URL, a token expires, or ffmpeg (our live-HLS downloader) hits a
        transient input error that --fragment-retries doesn't cover. Finalizing
        on that exit truncates the recording. So after each capture exit we
        re-confirm liveness and, if still live, relaunch on a FRESH URL and keep
        recording — accumulating segment files — until the stream is confirmed
        offline (or a stop is requested, or a reconnect budget trips). The
        download-lock is held across the WHOLE session; the segment files are
        handed to the uploader exactly once, at the end."""
        session_files: "dict[Path, None]" = {}     # insertion-ordered de-dup
        session_start = time.monotonic()
        zero_byte_streak = 0
        reconnects = 0

        while True:
            rc = self.capture.wait(self._stop)

            # Snapshot THIS segment BEFORE any relaunch: output_files() is keyed
            # to the latest capture.start()'s mtime, so a reconnect hides it.
            segment = self.capture.output_files()
            seg_bytes = sum(_safe_size(f) for f in segment)
            for f in segment:
                session_files[f] = None
            # Pair this segment's yt-dlp log with its media (or drop it if dead)
            # before the next start() opens a new log.
            self.capture.finalize()

            # ── Terminal conditions: never reconnect ──
            if self._stop.is_set() or rc == -1:        # clean shutdown
                break
            if rc == -2:                               # dead stream (zero bytes)
                break
            if not self.config.reconnect_enabled:
                break
            if not self._confirm_still_live(self.current_user):
                break                                  # genuine end

            # Still live but the capture dropped → premature. Bound pathological
            # loops: count reconnects yielding NO new bytes, and an optional
            # whole-session cap. (A dead relaunch is also caught by the capture's
            # own rc=-2 guard above; this is belt-and-braces for a flapping one.)
            zero_byte_streak = zero_byte_streak + 1 if seg_bytes == 0 else 0
            if zero_byte_streak > self.config.max_zero_byte_reconnects:
                log.warning("@%s still flagged live but %d reconnect(s) produced "
                            "no data — finalizing", self.current_user,
                            zero_byte_streak)
                break
            if (self.config.max_session_minutes > 0 and
                    time.monotonic() - session_start
                    >= self.config.max_session_minutes * 60.0):
                log.warning("@%s session passed the %.0f-min cap — finalizing",
                            self.current_user, self.config.max_session_minutes)
                break

            reconnects += 1
            backoff = min(
                self.config.reconnect_backoff_base_s * (2 ** zero_byte_streak),
                30.0)
            log.warning("@%s capture exited (rc=%d) but still LIVE — "
                        "reconnecting in %.0fs (#%d)", self.current_user, rc,
                        backoff, reconnects, extra={"ev": "reconnect"})
            if self._stop.wait(backoff):               # stop during backoff
                break
            if not self._open_capture(self.current_user):
                log.warning("@%s reconnect failed to start — finalizing",
                            self.current_user)
                break
            # loop: lock still held, recording resumed on the fresh URL

        # ── Session over: release the lock so the archiver can resume TikTok
        #    downloads during handoff + upload, then hand off the files once. ──
        self._release_lock_if_held()
        elapsed = time.monotonic() - session_start
        files = list(session_files)
        user = self.current_user or "?"
        # A broadcast that dropped and reconnected leaves >1 segment file. Stamp
        # them all with ONE album key so the dispatcher ships them as a single
        # ordered batch instead of scattered clips — the disk-level counterpart
        # to Fix 1 (which keeps a single broadcast in one ffmpeg file when it
        # can; this covers the genuine URL-rotation reconnects that still split).
        # A single-file session gets no key: it sends on its own as before.
        session_gk = (split_group_key("tiktok", user, files[0].stem)
                      if len(files) > 1 else None)
        if files:
            total = sum(_safe_size(f) for f in files)
            extra_seg = "" if reconnects == 0 else f" · {reconnects} reconnect(s)"
            log.info("@%s ended — %s · %d file%s · %s%s",
                     user, ui.human_duration(elapsed), len(files),
                     "" if len(files) == 1 else "s", ui.human_size(total),
                     extra_seg, extra={"ev": "rec_end"})
        else:
            # rc=-2 dead-stream guard (no bytes ever arrived).
            log.info("@%s ended — %s · no data (dead stream)",
                     user, ui.human_duration(elapsed), extra={"ev": "rec_end"})
        for f in files:
            self._upload_q.put(_Job(username=self.current_user or "",
                                    file_path=f, group_key=session_gk))

        if self._stop.is_set():
            return
        self.state = RecorderState.HANDOFF

    def _scan_priority_list_once(self) -> None:
        # One immediate pass: someone may have gone live during the last
        # recording. If so, record them; else return to normal listening.
        for username in self.config.tiktok_users:
            if self._stop.is_set():
                return
            if self.platform.is_live(username):
                log.info("handoff → @%s is live, recording next",
                         username, extra={"ev": "handoff"})
                self._start_recording(username)
                return
        self.state = RecorderState.LISTENING

    # ── lock helpers ──────────────────────────────────────────────────────

    def _acquire_lock(self, username: str | None = None) -> None:
        if not self._lock_held:
            # Stamp the user onto the lock so the lockfile names who's recording.
            self.lock.username = username
            self.lock.__enter__()
            self._lock_held = True

    def _release_lock_if_held(self) -> None:
        if self._lock_held:
            self.lock.__exit__(None, None, None)
            self._lock_held = False

    # ── consumer thread ───────────────────────────────────────────────────

    def _uploader_loop(self) -> None:
        while not self._stop.is_set() or not self._upload_q.empty():
            try:
                job = self._upload_q.get(timeout=1.0)
            except queue.Empty:
                continue
            self._enqueue_job(job)

    def _enqueue_job(self, job: _Job) -> None:
        """Register one finished recording in the shared queue. Shared by the
        daemon uploader thread and the one-shot record_once drain so both build
        the caption and handle failures identically."""
        # A file present at handoff that's gone by enqueue means it was lost
        # (e.g. an orphaned writer's unlinked inode). Report it honestly instead
        # of enqueuing a phantom row the dispatcher will fail as "file missing".
        if not job.file_path.exists():
            log.error("recorder: %s vanished before enqueue — recording lost, "
                      "not enqueued", job.file_path.name, extra={"ev": "lost"})
            return
        try:
            upload_path = _remux_for_telegram(job.file_path)
            caption = (f"@{job.username} · tiktok · live · "
                       f"{upload_path.stem}")
            self.enqueue("tiktok", job.username, str(upload_path), caption,
                         job.group_key)
        except Exception as e:
            # Keep the file on disk; ops can re-enqueue via the dispatcher
            # CLI. Losing the recording is the only unacceptable outcome,
            # and we avoid it.
            log.error("recorder: enqueue failed for %s: %s — file kept "
                      "on disk for manual recovery", job.file_path, e)
