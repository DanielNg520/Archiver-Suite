"""
core.ffprobe
────────────
One place that shells out to ffprobe. Three call sites used to each carry their
own subprocess + JSON + timeout + error-swallowing boilerplate (media_prep's
streamability probe, media_meta's display-geometry probe, image_fix's dimension
probe); they differed only in which `-show_entries` they asked for and how they
parsed the result. The plumbing is now shared here; each caller keeps its own
interpretation of the returned dict.

CONTRACT: never raises for an expected problem. ffprobe missing, a timeout, a
non-zero exit (corrupt/odd file), or unparseable output all return None — the
caller degrades to its own "couldn't probe" path. This preserves every site's
existing robustness guarantee verbatim.
"""

from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path

log = logging.getLogger(__name__)

# ffprobe answers in well under a second for a healthy file; the cap is a guard
# against a wedged probe stalling a caller (drain loop / ingest sweep), not a
# normal-path timeout. A timeout is reported as None like any other failure.
DEFAULT_TIMEOUT_S = 20.0


def probe_json(
    path: "str | Path",
    *,
    show_entries: str,
    select_streams: str | None = None,
    timeout: float = DEFAULT_TIMEOUT_S,
) -> dict | None:
    """Run `ffprobe -show_entries <show_entries> -of json` on `path` and return
    the parsed dict, or None on any failure (missing ffprobe, timeout, non-zero
    exit, unreadable output).

    select_streams maps to ffprobe's `-select_streams` (e.g. "v:0" for the first
    video stream). Callers pass exactly the entries they need so the probe stays
    cheap.
    """
    cmd = ["ffprobe", "-v", "error"]
    if select_streams is not None:
        cmd += ["-select_streams", select_streams]
    cmd += ["-show_entries", show_entries, "-of", "json", str(path)]
    try:
        out = subprocess.run(cmd, capture_output=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        log.warning("ffprobe: failed for %s: %s", Path(path).name, e)
        return None
    if out.returncode != 0:
        return None
    try:
        data = json.loads(out.stdout or b"{}")
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None
