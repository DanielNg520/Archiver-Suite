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
import subprocess
import threading
import time
from pathlib import Path

log = logging.getLogger(__name__)


class StreamCapture:
    def __init__(self, output_dir: str, cookies_file: str | None):
        self.output_dir = Path(output_dir).expanduser()
        self.cookies_file = cookies_file
        self._proc: subprocess.Popen | None = None
        self._run_dir: Path | None = None
        self._started_at: float = 0.0

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
            "--hls-use-mpegts",
            "--no-part",
            "--retries", "infinite",
            "--fragment-retries", "infinite",
            "-o", out_template,
        ]
        if self.cookies_file:
            cmd += ["--cookies", self.cookies_file]
        cmd.append(stream_url)

        log.info("capture: starting yt-dlp for @%s → %s", username, self._run_dir)
        log.debug("capture cmd: %s", " ".join(cmd))
        self._proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )

    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def wait(self, stop_event: threading.Event) -> int:
        """Block until yt-dlp exits OR stop_event is set.

        Returns the process exit code, or -1 if we terminated it because
        stop_event fired. Polls every 2s so a shutdown request is honored
        within ~2s rather than hanging on an uninterruptible wait()."""
        if self._proc is None:
            return -1
        while self.is_running():
            if stop_event.wait(timeout=2.0):
                log.info("capture: stop requested — terminating yt-dlp")
                self._proc.terminate()
                try:
                    self._proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    log.warning("capture: yt-dlp ignored SIGTERM — killing")
                    self._proc.kill()
                return -1
        rc = self._proc.returncode
        log.info("capture: yt-dlp exited rc=%d", rc)
        return rc

    def output_files(self) -> list[Path]:
        """Files written by this run: anything in the run dir with mtime
        at or after our start time. Excludes yt-dlp sidecars/temp."""
        if self._run_dir is None or not self._run_dir.exists():
            return []
        out: list[Path] = []
        for p in self._run_dir.iterdir():
            if not p.is_file():
                continue
            if p.suffix in (".part", ".ytdl", ".temp"):
                continue
            try:
                if p.stat().st_mtime >= self._started_at - 1:
                    out.append(p)
            except OSError:
                continue
        return sorted(out)
