"""
core
────
Shared data layer for the Archiver Suite. ONE database, ONE schema, ONE
copy of the state machine and policies — imported by archiver, dispatcher,
recorder, and ops instead of being copied into each.
"""

from .models import Item, Status, TERMINAL
from .schema import connect, db_path, DEFAULT_DB_PATH
from .store import ItemStore, now_iso
from .policy_store import PolicyStore
from .policies import DeletePolicy, DedupPolicy, BooleanPolicy, validate_overrides
from .files import cleanup_sidecars

__all__ = [
    "Item", "Status", "TERMINAL",
    "connect", "db_path", "DEFAULT_DB_PATH",
    "ItemStore", "now_iso",
    "PolicyStore", "DeletePolicy", "DedupPolicy", "BooleanPolicy",
    "validate_overrides", "cleanup_sidecars",
]
