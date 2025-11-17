"""Constants for the ingestion framework"""

from enum import StrEnum


class ExecutionStatus(StrEnum):
    """Status values for resource execution"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    NO_DATA = "no_data"
    DUPLICATE = "duplicate"
    SKIPPED = "skipped"
    REJECTED = "rejected"


class ImportPattern(StrEnum):
    """Import pattern - tracking strategy"""

    INCREMENTAL = "incremental"  # Only process new items, check for duplicates
    FULL = "full"  # Always process everything in the path


class BatchBy(StrEnum):
    """Batching strategy - how to group items"""

    FILE = "file"  # One batch per file
    FOLDER = "folder"  # One batch per subfolder (all files in folder together)
    ALL = "all"  # One batch with everything in the directory (recursive)


class DuplicateHandling(StrEnum):
    """Duplicate file handling modes"""

    ALLOW = "allow"
    SKIP = "skip"
    FAIL = "fail"


class WriteMode(StrEnum):
    """Delta table write modes"""

    OVERWRITE = "overwrite"
    APPEND = "append"
    MERGE = "merge"


class LoadType(StrEnum):
    """Target table load types for merge operations"""

    INCREMENTAL = "incremental"  # Only insert/update records from batch (don't infer deletes)
    FULL = "full"  # Batch is complete snapshot, mark missing records as deleted


class DatastoreType(StrEnum):
    """Target datastore types"""

    LAKEHOUSE = "lakehouse"
    WAREHOUSE = "warehouse"
