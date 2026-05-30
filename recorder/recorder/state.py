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
            try:
                caption = (f"@{job.username} · tiktok · live · "
                           f"{job.file_path.stem}")
                self.enqueue("tiktok", job.username, str(job.file_path), caption)
            except Exception as e:
                # Keep the file on disk; ops can re-enqueue via the
                # dispatcher CLI. Losing the recording is the only
                # unacceptable outcome, and we avoid it.
                log.error("recorder: enqueue failed for %s: %s — file kept "
                          "on disk for manual recovery", job.file_path, e)
