"""
recorder.capture
────────────────
yt-dlp subprocess wrapper for recording one live stream.

FLAG CHOICES (corrected vs the guide — verified against yt-dlp 2026):
  --downloader ffmpeg   TikTok live HLS is NOT supported by yt-dlp's
                        native downloader; ffmpeg is required for live
                        HLS. Without this you get a few seconds then a
                        "Live HLS not supported by native downloader"
                        bail.
  --downloader-args ffmpeg:-reconnect …
                        ffmpeg does the actual live-HLS download and, unlike
                        yt-dlp's own --retries, does NOT reconnect the HTTP
                        transport by default. A socket stall / rotated m3u8 /
                        mid-stream EOF then kills ffmpeg, ends the recording
                        file, and the state machine relaunches into a NEW file —
                        one broadcast becomes many short clips. These reconnect
                        flags keep one ffmpeg process alive across blips so the
                        broadcast stays in a single file.
  --hls-use-mpegts      MPEG-TS container survives mid-stream disconnects
                        (TikTok lives drop often). NOTE: yt-dlp already
                        enables this by default for live, but we set it
                        explicitly so behavior is pinned regardless of
                        yt-dlp default changes.
  --no-part             Write directly to the final filename, no .part
                        rename at the end. We want a usable file even if
                        the recorder is killed mid-stream — the partial
                        TS is still playable/uploadable.
  --retries infinite
  --fragment-retries infinite
                        A live stream that blips shouldn't end the
                        recording. Keep retrying fragments until the
                        stream genuinely ends (yt-dlp then exits 0).

  We deliberately DROP --live-from-start: it's a YouTube DVR-rewind
  feature, has caused live regressions (yt-dlp #15751), and isn't
  meaningful for TikTok where you record from join point forward.

PROCESS CONTROL (guide lesson, kept):
  Never call proc.wait() with no timeout — it's uninterruptible and the
  recorder could never be shut down mid-recording. wait() polls in a loop
  against a threading.Event so the main thread can terminate cleanly.

OUTPUT DISCOVERY:
  yt-dlp's final filename isn't perfectly predictable (extension depends
  on container negotiation). We snapshot the output dir's mtime before
  start and, after the process exits, return files in our run's directory
  newer than that snapshot. Simpler and more robust than parsing yt-dlp
  stdout.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
import time
from pathlib import Path

log = logging.getLogger(__name__)


class StreamCapture:
    def __init__(self, output_dir: str, cookies_file: str | None,
                 start_timeout_s: float = 120.0):
        self.output_dir = Path(output_dir).expanduser()
        self.cookies_file = cookies_file
        # Dead-stream guard: terminate yt-dlp if it produces ZERO bytes
        # within this many seconds of starting. Zero bytes means there is no
        # recording to lose, so this can never drop captured data; set to 0
        # to disable.
        self.start_timeout_s = start_timeout_s
        self._proc: subprocess.Popen | None = None
        self._run_dir: Path | None = None
        self._started_at: float = 0.0
        self._log_fh = None  # yt-dlp stdout/stderr sink; closed in wait()
        self._log_path: Path | None = None  # the sink's path, for finalize()

    def start(self, stream_url: str, username: str) -> None:
        """Launch yt-dlp. Files land in output_dir/<username>/."""
        self._run_dir = self.output_dir / username
        self._run_dir.mkdir(parents=True, exist_ok=True)
        self._started_at = time.time()

        # %(epoch)s keeps successive recordings of the same user from
        # colliding; the dispatcher later sends each file individually.
        out_template = str(
            self._run_dir / f"{username}_%(epoch)s.%(ext)s"
        )

        cmd = [
            "yt-dlp",
            "--downloader", "ffmpeg",
            # ffmpeg (our live-HLS downloader) does NOT retry the HTTP transport
            # on its own — any socket stall, rotated m3u8, or mid-stream EOF ends
            # the ffmpeg process, yt-dlp exits, and that output file is closed. On
            # TikTok live those blips happen constantly, so without this the state
            # machine relaunches yt-dlp on every drop and one broadcast lands as a
            # pile of short clips. These flags keep a SINGLE ffmpeg process alive
            # across blips by re-opening the connection internally, so a whole
            # broadcast stays in one file. yt-dlp's --retries/--fragment-retries
            # are its own knobs and don't reach the child ffmpeg — hence this.
            # (reconnect_* apply to http/https inputs, exactly the HLS case.)
            "--downloader-args",
            "ffmpeg:-reconnect 1 -reconnect_streamed 1 "
            "-reconnect_at_eof 1 -reconnect_delay_max 30",
            "--hls-use-mpegts",
            "--no-part",
            "--retries", "infinite",
            "--fragment-retries", "infinite",
            "-o", out_template,
        ]
        if self.cookies_file:
            cmd += ["--cookies", self.cookies_file]
        cmd.append(stream_url)

        log.debug("capture: starting yt-dlp for @%s → %s", username, self._run_dir)
        log.debug("capture cmd: %s", " ".join(cmd))
        # yt-dlp/ffmpeg emit continuous progress on stderr while recording.
        # Capturing with subprocess.PIPE and never reading it fills the OS
        # pipe buffer (~16-64KB) within minutes, blocking the child's next
        # write() and silently freezing the recording. Redirect to a regular
        # file fd instead: kernel appends never block the writer. stderr is
        # merged into stdout so one file holds the full diagnostic stream.
        self._log_path = self._run_dir / f"{username}_{int(self._started_at)}_ytdlp.log"
        self._log_fh = open(self._log_path, "ab", buffering=0)
        # start_new_session puts yt-dlp in its OWN process group so we can later
        # signal the whole group. yt-dlp does the actual download via a child
        # ffmpeg (--downloader ffmpeg); without the group, terminating only the
        # yt-dlp pid orphans that ffmpeg — it keeps the recording file open and
        # writing, and a remux that then unlinks the source drains live footage
        # into a deleted inode (silent data loss, observed in prod).
        self._proc = subprocess.Popen(
            cmd, stdout=self._log_fh, stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def elapsed_s(self) -> float:
        """Wall-clock seconds since this run's yt-dlp was launched (0 if it
        never started). Used for the recording-ended summary line."""
        return max(0.0, time.time() - self._started_at) if self._started_at else 0.0

    def wait(self, stop_event: threading.Event) -> int:
        """Block until yt-dlp exits OR stop_event is set.

        Returns the process exit code, or -1 if we terminated it because
        stop_event fired. Polls every 2s so a shutdown request is honored
        within ~2s rather than hanging on an uninterruptible wait()."""
        if self._proc is None:
            return -1
        while self.is_running():
            if stop_event.wait(timeout=2.0):
                log.debug("capture: stop requested — terminating yt-dlp")
                self._terminate()
                self._close_log()
                return -1
            # Dead-stream guard. After a live ends, the recorder's handoff
            # re-scan can re-detect the just-ended user as live (TikTok lag)
            # and we relaunch yt-dlp on a dead URL. With --retries infinite
            # that call would hang here forever and the recorder would never
            # return to LISTENING. If no bytes have arrived within the
            # startup window the stream is dead — bail. Safe: zero bytes
            # means there is no recording to lose.
            if (self.start_timeout_s > 0
                    and time.time() - self._started_at > self.start_timeout_s
                    and self._recorded_bytes() == 0):
                log.warning("capture: no data after %.0fs — assuming dead "
                            "stream, terminating yt-dlp", self.start_timeout_s)
                self._terminate()
                self._close_log()
                return -2
        rc = self._proc.returncode
        log.debug("capture: yt-dlp exited rc=%d", rc)
        self._close_log()
        return rc

    def _terminate(self) -> None:
        """Stop the capture AND every subprocess it spawned.

        SIGTERM the whole process group (yt-dlp + its child ffmpeg), escalating
        to SIGKILL if the group ignores us. Group-signalling is what guarantees
        the ffmpeg downloader dies with yt-dlp instead of being orphaned and
        left writing the recording file (see start()). Falls back to signalling
        the lone pid if the group can't be reached (already gone / no killpg)."""
        if self._proc is None:
            return
        if not self._signal_group(signal.SIGTERM):
            self._proc.terminate()
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            log.warning("capture: yt-dlp group ignored SIGTERM — killing")
            if not self._signal_group(signal.SIGKILL):
                self._proc.kill()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log.error("capture: yt-dlp survived SIGKILL — possible orphan")

    def _signal_group(self, sig: int) -> bool:
        """Send `sig` to the capture's entire process group. Returns False if
        the group can't be signalled — already exited, not permitted, or the
        platform has no killpg — so the caller can fall back to the bare pid."""
        if self._proc is None or not hasattr(os, "killpg"):
            return False
        try:
            os.killpg(os.getpgid(self._proc.pid), sig)
            return True
        except (ProcessLookupError, PermissionError, OSError):
            return False

    def _recorded_bytes(self) -> int:
        """Total bytes written so far for this run. output_files() already
        excludes logs/sidecars, so this measures only the recording."""
        total = 0
        for p in self.output_files():
            try:
                total += p.stat().st_size
            except OSError:
                pass
        return total

    def _close_log(self) -> None:
        """Release the yt-dlp log fd. Idempotent — safe to call twice."""
        if self._log_fh is not None:
            try:
                self._log_fh.close()
            finally:
                self._log_fh = None

    def finalize(self) -> None:
        """Settle this run's yt-dlp log so it never orphans. Call once after
        wait() returns.

          recording produced → rename the log to share the media's stem
            (<user>_<epoch>_ytdlp.log) so cleanup_sidecars deletes it when the
            recording is deleted post-upload. The log is named off our wall
            clock but the media off yt-dlp's own %(epoch)s; the two can drift a
            second, so we re-key the log to the actual file rather than trust
            the names to match.
          no recording (dead stream) → there is no record to ever pair with or
            delete, so the log would pile up forever. Remove it now.

        The media stem survives the later -c copy remux (suffix-only change),
        so the pairing still holds for the uploaded .mp4."""
        if self._log_path is None:
            return
        media = self.output_files()
        if not media:
            self._log_path.unlink(missing_ok=True)
            self._log_path = None
            return
        target = media[0].with_name(media[0].stem + "_ytdlp.log")
        if target != self._log_path and self._log_path.exists():
            try:
                self._log_path.rename(target)
                self._log_path = target
            except OSError as e:
                log.debug("capture: could not pair log with recording: %s", e)

    def output_files(self) -> list[Path]:
        """Files written by this run: anything in the run dir with mtime
        at or after our start time. Excludes yt-dlp sidecars/temp."""
        if self._run_dir is None or not self._run_dir.exists():
            return []
        out: list[Path] = []
        for p in self._run_dir.iterdir():
            if not p.is_file():
                continue
            # Skip dotfiles — chiefly macOS AppleDouble companions (._name)
            # the OS auto-creates next to every file on exFAT/FAT volumes.
            # They carry the recording's extension but are 4KB metadata stubs,
            # not recordings; enqueuing them spawns phantom rows that vanish
            # (→ "file missing on disk") or upload as junk.
            if p.name.startswith("."):
                continue
            if p.suffix in (".part", ".ytdl", ".temp", ".log"):
                continue
            try:
                if p.stat().st_mtime >= self._started_at - 1:
                    out.append(p)
            except OSError:
                continue
        return sorted(out)
