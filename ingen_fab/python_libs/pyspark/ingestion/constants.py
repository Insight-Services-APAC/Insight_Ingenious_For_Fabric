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


class ImportPattern(StrEnum):
    """File import patterns"""

    SINGLE_FILE = "single_file"
    INCREMENTAL_FILES = "incremental_files"
    INCREMENTAL_FOLDERS = "incremental_folders"


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


class DatastoreType(StrEnum):
    """Target datastore types"""

    LAKEHOUSE = "lakehouse"
    WAREHOUSE = "warehouse"
