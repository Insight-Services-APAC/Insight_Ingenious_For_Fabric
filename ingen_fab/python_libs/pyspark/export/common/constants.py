"""Constants for the export framework."""

from enum import StrEnum


class ExecutionStatus(StrEnum):
    """Status values for export execution."""

    # Operational states (transient)
    PENDING = "pending"
    RUNNING = "running"

    # Outcome states (final)
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"


class SourceType(StrEnum):
    """Source datastore types for exports."""

    LAKEHOUSE = "lakehouse"
    WAREHOUSE = "warehouse"


class FileFormat(StrEnum):
    """Supported output file formats."""

    CSV = "csv"
    PARQUET = "parquet"
    JSON = "json"


class CompressionType(StrEnum):
    """Supported compression types."""

    NONE = "none"
    GZIP = "gzip"
    ZIP = "zip"
    ZIPDEFLATE = "zipdeflate"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    BROTLI = "brotli"
