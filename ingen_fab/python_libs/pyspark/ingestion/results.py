# Shared Result Classes for Data Integration
# Used by both Extraction and File Loading frameworks

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

# Import ExecutionStatus for type hints
from ingen_fab.python_libs.pyspark.ingestion.constants import ExecutionStatus


@dataclass
class BatchExtractionResult:
    """Result of a single batch extraction event"""

    extraction_id: str                      # Unique batch ID (becomes extract_batch_id)
    source_path: str                        # Where batch came from (inbound path)
    destination_path: str                   # Where batch was promoted to (raw path)
    status: ExecutionStatus                 # ExecutionStatus enum (convert to str only when logging to table)

    # Batch metrics
    file_count: int = 1                     # Number of files in this batch
    file_size_bytes: int = 0                # Total size of batch
    promoted_count: int = 0                 # Successfully promoted files
    failed_count: int = 0                   # Failed files
    duplicate_count: int = 0                # Skipped duplicates
    duration_ms: int = 0                    # Extraction duration

    # Error tracking
    error_message: Optional[str] = None     # Error if batch failed


@dataclass
class BatchInfo:
    """Information about a batch that was processed"""

    batch_id: str                           # Unique identifier
    extract_batch_id: str                   # FK to log_resource_extract_batch.extract_batch_id (loading always driven by extraction)

    # Source info (flexible for different batch types)
    file_paths: List[str] = field(default_factory=list)  # For file loading: list of file paths
    destination_path: Optional[str] = None  # Full path with Hive partitions (e.g., abfss://.../ds=2025-11-14/file.csv)

    # Metadata
    size_bytes: int = 0
    modified_time: Optional[datetime] = None
    date_partition: Optional[str] = None    # Extracted date (for date-based loading)
    folder_name: Optional[str] = None       # For folder-based loading

    # Flexible metadata
    source_metadata: Optional[Dict[str, Any]] = None
    control_file_path: Optional[str] = None


@dataclass
class ProcessingMetrics:
    """Metrics for processing performance"""

    # Timing
    read_duration_ms: int = 0               # File loading: time to read files
    extract_duration_ms: int = 0            # Extraction: time to extract from source
    write_duration_ms: int = 0
    total_duration_ms: int = 0

    # Row counts
    source_row_count: int = 0
    records_processed: int = 0

    # Write metrics
    records_inserted: int = 0
    records_updated: int = 0
    records_deleted: int = 0

    # Target metrics
    target_row_count_before: int = 0
    target_row_count_after: int = 0

    # Validation
    row_count_reconciliation_status: str = "not_verified"
    corrupt_records_count: int = 0
    validation_errors: List[str] = field(default_factory=list)

    # Operational metadata
    completed_at: Optional[datetime] = None


@dataclass
class BatchResult:
    """Result of processing a batch (read + write operation)"""

    status: str  # "success" | "rejected" | "failed"
    metrics: 'ProcessingMetrics'  # Always provided (all status paths)
    df: Optional[Any] = None  # DataFrame (use Any to avoid importing pyspark.sql.DataFrame) - typically None after write
    rejection_reason: str = ""  # Required for rejected/failed status, empty for success
    corrupt_count: int = 0


@dataclass
class ResourceExtractionResult:
    """Result of extraction for a resource"""

    resource_name: str
    status: ExecutionStatus                 # ExecutionStatus enum (convert to str only when logging to table)

    # Batch tracking
    batches_processed: int = 0              # Successfully extracted batches
    batches_failed: int = 0                 # Failed batches

    # Extraction metrics
    total_items_count: int = 0              # Total items (files, etc.) across all batches
    total_bytes: int = 0                    # Total bytes extracted

    # Errors
    error_message: Optional[str] = None

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


@dataclass
class ResourceExecutionResult:
    """Result of loading/processing a resource (file loading framework)"""

    resource_name: str
    status: ExecutionStatus                 # ExecutionStatus enum (convert to str only when logging to table)

    # Batch tracking
    batches_processed: int = 0
    batches_failed: int = 0
    batches_rejected: int = 0
    batches_discovered: int = 0
    batches_skipped: int = 0

    # Loading-specific metrics (optional, populated after processing)
    metrics: Optional[ProcessingMetrics] = None

    # Errors
    error_message: Optional[str] = None
    errors: List[str] = field(default_factory=list)

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
