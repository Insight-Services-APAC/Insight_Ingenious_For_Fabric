# Core Ingestion Interfaces
# Defines the interface for extracting data from any source type

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


# ============================================================================
# EXCEPTIONS
# ============================================================================

class IngestionError(Exception):
    """Base exception for ingestion errors"""
    pass


class ConfigurationError(IngestionError):
    """Raised when configuration is invalid"""
    pass


class ExtractionError(IngestionError):
    """Raised when extraction fails"""
    pass


class StateError(IngestionError):
    """Raised when state management fails"""
    pass


# ============================================================================
# CORE RESULT CLASSES
# ============================================================================

@dataclass
class BatchInfo:
    """Information about a batch that was extracted"""

    # Identity
    batch_id: str                           # Unique identifier for this batch
    batch_type: str                         # 'file', 'table_full', 'table_incremental', 'api_page', etc.
    source_path: str                        # Path/reference to the source (file path, table name, API endpoint)

    # Metadata
    size_bytes: int = 0                     # Size of the batch
    modified_time: Optional[datetime] = None  # When source was last modified

    # Partitioning/filtering info
    partition_value: Optional[str] = None   # Date partition, partition column value, etc.

    # Source-specific metadata (flexible dict for any additional info)
    source_metadata: Optional[Dict[str, Any]] = None

    # Control/validation files
    control_file_path: Optional[str] = None


@dataclass
class ProcessingMetrics:
    """Metrics for extraction and loading performance"""

    # Timing metrics
    extract_duration_ms: int = 0            # Time to extract/read data
    write_duration_ms: int = 0              # Time to write data to target
    total_duration_ms: int = 0              # Total end-to-end time

    # Row counts
    source_row_count: int = 0               # Rows extracted from source
    records_processed: int = 0              # Rows processed (may differ if filtering applied)

    # Write operation metrics
    records_inserted: int = 0               # Rows inserted (append/merge)
    records_updated: int = 0                # Rows updated (merge only)
    records_deleted: int = 0                # Rows deleted (overwrite/merge)

    # Target table metrics
    target_row_count_before: int = 0        # Target table rows before write
    target_row_count_after: int = 0         # Target table rows after write

    # Validation
    row_count_reconciliation_status: str = "not_verified"  # 'passed', 'failed', 'not_verified'
    corrupt_records_count: int = 0          # Number of corrupt/invalid records
    validation_errors: List[str] = field(default_factory=list)  # Validation error messages


@dataclass
class ResourceExecutionResult:
    """Result of executing extraction for a single resource"""

    # Identity
    resource_name: str                      # Resource name (primary identifier)
    execution_id: str                       # Unique ID for this execution run

    # Status
    status: str                             # 'pending', 'running', 'completed', 'failed', 'no_data'

    # Metrics
    metrics: ProcessingMetrics

    # Batches processed
    batches_discovered: int = 0
    batches_processed: int = 0
    batches_failed: int = 0
    batches_skipped: int = 0

    # Errors
    errors: List[str] = field(default_factory=list)

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


# ============================================================================
# CORE EXTRACTOR INTERFACE
# ============================================================================

class ResourceExtractorInterface(ABC):
    """
    Interface for extracting data from any source type.

    Each source type (filesystem, database, API, etc.) implements this interface
    and handles all extraction logic internally.

    Examples:
    - FileSystemExtractor: Discovers files, reads each file
    - DatabaseExtractor: Determines incremental/full load, executes query
    - APIExtractor: Handles pagination, fetches all pages
    """

    @abstractmethod
    def extract(self) -> List[Tuple[BatchInfo, Any, ProcessingMetrics]]:
        """
        Extract data from this resource.

        Returns:
            List of (batch_info, dataframe, metrics) tuples
            - batch_info: Metadata about what was extracted
            - dataframe: The actual data (PySpark DataFrame or other format)
            - metrics: Performance metrics for this batch

        Each source type implements this method according to its needs:
        - Filesystem: Discover files → read each file
        - Database: Check watermark → query incrementally or full
        - API: Paginate → fetch all pages

        Raises:
            ExtractionError: If extraction fails
        """
        pass

    @abstractmethod
    def validate_config(self) -> List[str]:
        """
        Validate the resource configuration.

        Returns:
            List of validation error messages (empty if valid)
        """
        pass

    @abstractmethod
    def get_extraction_state(self) -> Optional[Dict[str, Any]]:
        """
        Get the current extraction state for incremental loading.

        Returns:
            State dict containing watermark, last processed file, etc.
            None if no state exists or state not applicable for this source.

        Examples:
        - Filesystem: {"last_modified_time": "2024-01-15 10:30:00"}
        - Database: {"last_watermark": "2024-01-15 10:30:00"}
        - API: {"last_cursor": "abc123", "last_page": 42}
        """
        pass

    @abstractmethod
    def update_extraction_state(self, batch: BatchInfo, metrics: ProcessingMetrics) -> None:
        """
        Update the extraction state after successfully processing a batch.

        Args:
            batch: Information about the batch that was processed
            metrics: Metrics from processing the batch

        Raises:
            StateError: If state update fails
        """
        pass


# ============================================================================
# LOGGING INTERFACE
# ============================================================================

class IngestionLoggingInterface(ABC):
    """Interface for logging ingestion activities"""

    @abstractmethod
    def log_resource_execution_start(
        self,
        resource_name: str,
        execution_id: str,
    ) -> None:
        """Log the start of a resource execution"""
        pass

    @abstractmethod
    def log_resource_execution_completion(
        self,
        result: ResourceExecutionResult,
    ) -> None:
        """Log the completion of a resource execution"""
        pass

    @abstractmethod
    def log_resource_execution_error(
        self,
        resource_name: str,
        execution_id: str,
        error_message: str,
    ) -> None:
        """Log an error during resource execution"""
        pass

    @abstractmethod
    def log_batch_start(
        self,
        resource_name: str,
        execution_id: str,
        batch: BatchInfo,
    ) -> None:
        """Log the start of processing a batch"""
        pass

    @abstractmethod
    def log_batch_completion(
        self,
        resource_name: str,
        execution_id: str,
        batch: BatchInfo,
        metrics: ProcessingMetrics,
        status: str,
    ) -> None:
        """Log the completion of processing a batch"""
        pass

    @abstractmethod
    def log_batch_error(
        self,
        resource_name: str,
        execution_id: str,
        batch: BatchInfo,
        error_message: str,
    ) -> None:
        """Log an error during batch processing"""
        pass
