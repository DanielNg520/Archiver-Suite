"""
dispatcher
──────────
Telegram upload dispatcher. Owns the Telegram session; drains a shared
SQLite queue populated by recorder (priority 5), chat_id folders (priority 6),
and archiver (priority 10).

See IMPLEMENTATION_GUIDE.md for architecture.
"""

__version__ = "0.1.0"
