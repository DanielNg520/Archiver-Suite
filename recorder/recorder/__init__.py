"""
recorder
────────
TikTok live recorder. Monitors a priority-ordered user list, records one
stream at a time via yt-dlp, and enqueues finished files into
the shared suite items table (source='recorder', priority=5 by default).

Coordinates with archiver via a soft lockfile (~/.config/archiver-suite/locks/
tiktok.lock) held only during active recording.

See IMPLEMENTATION_GUIDE.md (Slice 4) for architecture.
"""

__version__ = "0.1.0"
