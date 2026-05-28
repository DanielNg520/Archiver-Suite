"""
dispatcher
──────────
Telegram upload dispatcher. Owns the Telegram session; drains a shared
SQLite queue populated by archiver (priority 10) and recorder (priority 20).

See IMPLEMENTATION_GUIDE.md for architecture.
"""

__version__ = "0.1.0"
