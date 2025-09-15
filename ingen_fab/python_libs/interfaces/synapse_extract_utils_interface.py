"""
Standard interface for Synapse Extract utilities.

This module defines the common interface for Synapse Extraction utilities
including enhanced logging, retry mechanisms, and SQL template generation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface


class SynapseExtractUtilsInterface(ABC):
    """Abstract interface for Synapse Extract utilities."""

    @abstractmethod
    def __init__(self, lakehouse: DataStoreInterface):
        """Initialise the extract utils with a data store."""
        pass

    @abstractmethod
    def get_extract_sql_template(
        self,
        source_schema_name: str,
        source_table_name: str,
        extract_mode: str,
        single_date_filter: Optional[str] = None,
        date_range_filter: Optional[str] = None,
        custom_select_sql: Optional[str] = None,
        extract_start_dt: Optional[str] = None,
        extract_end_dt: Optional[str] = None
    ) -> str:
        """
        Generate SQL template for extraction with custom SQL support.

        Args:
            source_schema_name: Schema name of the source table
            source_table_name: Table name to extract from
            extract_mode: 'incremental' or 'snapshot'
            single_date_filter: SQL filter for single date extracts
            date_range_filter: SQL filter for date range extracts
            custom_select_sql: Custom SQL for complex extractions
            extract_start_dt: Start date for incremental extraction
            extract_end_dt: End date for incremental extraction

        Returns:
            SQL template string for the extraction
        """
        pass

    @abstractmethod
    def bulk_insert_queued_extracts(
        self,
        extract_records: List[Dict[str, Any]],
        max_retries: int = 5
    ) -> Dict[str, str]:
        """
        Bulk insert extraction records with validation and retry logic.

        Args:
            extract_records: List of extraction record dictionaries
            max_retries: Maximum number of retry attempts for Delta operations

        Returns:
            Mapping of external table name -> execution_id for downstream processing
        """
        pass

    @abstractmethod
    def build_pipeline_parameters_from_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build pipeline parameters for Fabric from a prepared extraction record.

        Implementations should resolve Synapse external data source information
        using record-provided values first (e.g., from variable library), then
        fall back to configuration tables when not available.

        Args:
            record: Prepared extraction record containing table, output path, and optional datasource info

        Returns:
            Dictionary payload suitable for pipeline execution (e.g., CETAS script and paths)
        """
        pass

    @abstractmethod
    def update_log_record(
        self,
        master_execution_id: str,
        execution_id: str,
        updates: Dict[str, Any],
        max_retries: int = 5
    ) -> bool:
        """
        Update log record with merge operations and retry logic.

        Notes:
            - Composite key merge is enforced: (master_execution_id AND execution_id)

        Args:
            master_execution_id: The master execution ID for the record
            execution_id: The execution ID to update
            updates: Dictionary of field updates
            max_retries: Maximum number of retry attempts

        Returns:
            True if successful, False otherwise
        """
        pass

    @abstractmethod
    def get_queued_extracts(
        self,
        master_execution_id: str,
        execution_group: Optional[int] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get queued extracts for processing.

        Args:
            master_execution_id: The master execution ID to filter by
            execution_group: Optional execution group to filter by
            limit: Optional limit on number of records returned

        Returns:
            List of queued extraction records
        """
        pass

    @abstractmethod
    def get_failed_extracts(
        self,
        master_execution_id: Optional[str] = None,
        status_filter: Optional[List[str]] = None,
        hours_back: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get failed extracts for retry processing.

        Args:
            master_execution_id: Optional master execution ID to filter by
            status_filter: Optional list of statuses to include (defaults to ["Failed"])
            hours_back: Optional number of hours back to filter by time window

        Returns:
            List of failed extraction records
        """
        pass

    @abstractmethod
    def create_extraction_record(
        self,
        work_item: Dict[str, Any],
        master_execution_id: str,
    ) -> Dict[str, Any]:
        """
        Create an extraction record from a work item.

        Args:
            work_item: The work item containing extraction details
            master_execution_id: The master execution ID for grouping

        Returns:
            Complete extraction record dictionary
        """
        pass

    @abstractmethod
    def validate_extraction_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate an extraction record against the expected schema.

        Args:
            record: The extraction record to validate

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        pass

    @abstractmethod
    def build_path_components(
        self,
        source_schema_name: str,
        source_table_name: str,
        extract_mode: str,
        export_base_dir: Optional[str] = None,
        extract_start_dt: Optional[str] = None,
        extract_end_dt: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Build path components for file storage.

        Args:
            source_schema_name: Schema name of the source table
            source_table_name: Table name
            extract_mode: 'incremental' or 'snapshot'
            export_base_dir: Optional base directory name (defaults to 'exports' if not provided)
            extract_start_dt: Start date for incremental extraction
            extract_end_dt: End date for incremental extraction

        Returns:
            Dictionary containing path components (output_path, file_name, etc.)
        """
        pass

    @abstractmethod
    def build_partition_clause(
        self,
        extract_mode: str,
        single_date_filter: Optional[str] = None,
        date_range_filter: Optional[str] = None,
        extract_start_dt: Optional[str] = None,
        extract_end_dt: Optional[str] = None
    ) -> str:
        """
        Build SQL WHERE clause for partitioning.

        Args:
            extract_mode: 'incremental' or 'snapshot'
            single_date_filter: SQL filter template for single date
            date_range_filter: SQL filter template for date range
            extract_start_dt: Start date for incremental extraction
            extract_end_dt: End date for incremental extraction

        Returns:
            SQL WHERE clause string
        """
        pass

    @abstractmethod
    def get_log_schema(self) -> Dict[str, str]:
        """
        Get the schema definition for the extraction log table.

        Returns:
            Dictionary mapping column names to data types
        """
        pass

    @abstractmethod
    def prepare_extract_payloads(
        self,
        work_items: List[Dict[str, Any]],
        master_execution_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Prepare extraction payloads for batch processing.

        Args:
            work_items: List of work items to process
            master_execution_id: The master execution ID for grouping

        Returns:
            List of prepared extraction payloads
        """
        pass

    @abstractmethod
    def with_retry(
        self,
        operation: callable,
        max_retries: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: bool = True
    ) -> Any:
        """
        Execute an operation with exponential backoff retry logic.

        Args:
            operation: The operation to execute
            max_retries: Maximum number of retry attempts
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
            jitter: Whether to add random jitter to delays

        Returns:
            Result of the operation
        """
        pass

    @abstractmethod
    def is_concurrent_write_error(self, error: Exception) -> bool:
        """
        Check if an error is a Delta Lake concurrent write error.

        Args:
            error: The exception to check

        Returns:
            True if it's a concurrent write error, False otherwise
        """
        pass

    @abstractmethod
    def get_execution_summary(
        self,
        master_execution_id: str
    ) -> Dict[str, Any]:
        """
        Deprecated: Prefer orchestrator-produced summaries.

        This method is retained for compatibility but is not referenced by the
        Synapse package notebooks. Use the summary returned by the orchestrator
        (e.g., `SynapseOrchestrator.run_async_orchestration(...)`) for the 
        execution summary.

        Args:
            master_execution_id: The master execution ID to summarise

        Returns:
            Dictionary containing execution statistics
        """
        pass
