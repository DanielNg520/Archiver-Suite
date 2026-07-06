"""
core
────
Shared data layer for the Archiver Suite. ONE database, ONE schema, ONE
copy of the state machine and policies — imported by archiver, dispatcher,
recorder, and ops instead of being copied into each.
"""

from .models import Item, Status, TERMINAL
from .schema import connect, db_path, DEFAULT_DB_PATH, SchemaVersionError
from .store import (
    ClaimContentionError, IllegalTransition, ItemStore, now_iso, CANCELLED_MARKER,
    is_transient_failure,
)
from .instance_lock import InstanceLock, InstanceAlreadyRunning
from .stores import ProducerStore, QueueStore, AdminStore
from .policy_store import PolicyStore
from .policies import (
    DeletePolicy, RecorderDeletePolicy, DedupPolicy, BooleanPolicy,
    BatchPolicy, AutoIngestPolicy, DownloadPolicy, ProtectionPolicy,
    SortPolicy, FailedRetryPolicy, validate_overrides,
)
from .files import cleanup_sidecars, orphaned_kind, album_bucket
from .deletion import DeletionGuard
from .dedup import dedup_user, DedupReport, DupGroup
from .ingest import (
    register_file, register_media, IngestResult, IngestOutcome, PreparedResult,
)
from .routing import is_chat_id, CHAT_ID_RE, Route, parse_route
from .grouping import split_group_key, is_split_group, SPLIT_GROUP_PREFIX
from .sanitize import Sanitizer, ReloadingSanitizer, load_words
from .orphaned import (
    ingest_chat_id_dirs, ingest_folder, OrphanedReport, subfolder_of,
    ORPHANED_SOURCE, ORPHANED_PLATFORM, CHAT_ID_PRIORITY,
)
from .backfill import backfill_content_hashes, BackfillReport
from .sorter import sort_unsorted, SortReport, extract_username

__all__ = [
    "Item", "Status", "TERMINAL",
    "connect", "db_path", "DEFAULT_DB_PATH", "SchemaVersionError",
    "ClaimContentionError", "IllegalTransition", "ItemStore", "now_iso",
    "CANCELLED_MARKER", "is_transient_failure",
    "InstanceLock", "InstanceAlreadyRunning",
    "ProducerStore", "QueueStore", "AdminStore",
    "PolicyStore", "DeletePolicy", "RecorderDeletePolicy", "DedupPolicy",
    "BooleanPolicy", "BatchPolicy", "AutoIngestPolicy", "DownloadPolicy",
    "ProtectionPolicy", "SortPolicy", "FailedRetryPolicy",
    "validate_overrides", "cleanup_sidecars", "orphaned_kind", "album_bucket",
    "DeletionGuard",
    "dedup_user", "DedupReport", "DupGroup",
    "register_file", "register_media", "IngestResult", "IngestOutcome",
    "PreparedResult",
    "is_chat_id", "CHAT_ID_RE", "Route", "parse_route",
    "split_group_key", "is_split_group", "SPLIT_GROUP_PREFIX",
    "Sanitizer", "ReloadingSanitizer", "load_words",
    "ingest_chat_id_dirs", "ingest_folder", "OrphanedReport", "subfolder_of",
    "ORPHANED_SOURCE", "ORPHANED_PLATFORM", "CHAT_ID_PRIORITY",
    "backfill_content_hashes", "BackfillReport",
    "sort_unsorted", "SortReport", "extract_username",
]
