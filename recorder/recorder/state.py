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
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Callable

from .capture import StreamCapture
from .config import RecorderConfig
from .lock import TikTokLock
from .platforms.base import LivePlatform

log = logging.getLogger(__name__)

_VIDEO_SUFFIXES = frozenset({".mp4", ".ts", ".mkv", ".webm", ".flv", ".m4v"})


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

    # If src is already .mp4 we can't overwrite it while ffmpeg reads it,
    # so write to a temp name and rename over it after.
    if src.suffix.lower() == ".mp4":
        tmp = src.with_name(src.stem + "._tmp.mp4")
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

    log.info("remux: %s → %s (telegram-ready mp4)", src.name, final.name)
    return final


class RecorderState(Enum):
    LISTENING = auto()
    RECORDING = auto()
    HANDOFF   = auto()
    STOPPED   = auto()


@dataclass
class _Job:
    """A finished file awaiting enqueue."""
    username: str
    file_path: Path


# enqueue_fn signature: (platform, username, file_path, caption) -> None
EnqueueFn = Callable[[str, str, str, str], None]


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
        log.info("recorder: state machine started (%d tiktok users, poll=%.0fs)",
                 len(self.config.tiktok_users), self.config.poll_interval_s)
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
            log.info("recorder: stopped")

    def request_stop(self) -> None:
        """Signal a clean shutdown. Safe to call from a signal handler."""
        log.info("recorder: stop requested")
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
            log.info("record-once: @%s is not live right now — nothing to record",
                     username)
            return False

        log.info("record-once: @%s is LIVE — recording until the stream ends "
                 "(Ctrl-C to stop early)", username)
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
        log.info("record-once: done — %d file(s) enqueued for upload", drained)
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
                log.info("recorder: @%s is LIVE — starting recording", username)
                self._start_recording(username)
                return
        # Nobody live — sleep, but wake on stop.
        self._stop.wait(self.config.poll_interval_s)

    def _start_recording(self, username: str) -> None:
        try:
            url = self.platform.stream_url(username)
        except Exception as e:
            log.error("recorder: stream_url(%s) failed: %s — back to listening",
                      username, e)
            self.state = RecorderState.LISTENING
            return
        self._acquire_lock()
        try:
            self.capture.start(url, username)
        except Exception as e:
            self._release_lock_if_held()
            log.error("recorder: capture start for @%s failed: %s — back to listening",
                      username, e)
            self.state = RecorderState.LISTENING
            return
        self.current_user = username
        self.state = RecorderState.RECORDING

    def _wait_for_recording_done(self) -> None:
        rc = self.capture.wait(self._stop)
        # Release the download-lock as soon as the capture ends — archiver
        # may resume TikTok downloads during our handoff scan + upload.
        self._release_lock_if_held()

        files = self.capture.output_files()
        # Pair the yt-dlp log with the recording (or drop it if the stream was
        # dead) so it's cleaned up with the media instead of piling up forever.
        self.capture.finalize()
        log.info("recorder: recording of @%s ended (rc=%d, %d file(s))",
                 self.current_user, rc, len(files))
        for f in files:
            self._upload_q.put(_Job(username=self.current_user or "", file_path=f))

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
                log.info("recorder: handoff → @%s is live, recording next",
                         username)
                self._start_recording(username)
                return
        self.state = RecorderState.LISTENING

    # ── lock helpers ──────────────────────────────────────────────────────

    def _acquire_lock(self) -> None:
        if not self._lock_held:
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
        try:
            upload_path = _remux_for_telegram(job.file_path)
            caption = (f"@{job.username} · tiktok · live · "
                       f"{upload_path.stem}")
            self.enqueue("tiktok", job.username, str(upload_path), caption)
        except Exception as e:
            # Keep the file on disk; ops can re-enqueue via the dispatcher
            # CLI. Losing the recording is the only unacceptable outcome,
            # and we avoid it.
            log.error("recorder: enqueue failed for %s: %s — file kept "
                      "on disk for manual recovery", job.file_path, e)
