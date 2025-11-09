# Extraction Framework - Logging Service
# Handles state tracking in log_resource_extract_batch and log_resource_extract tables

import logging
import uuid
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from ingen_fab.python_libs.common.extract_resource_schema import (
    get_extract_resource_schema,
)
from ingen_fab.python_libs.common.extract_resource_batch_schema import (
    get_extract_resource_batch_schema,
)
from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)


class ExtractionResult:
    """Container for extraction metrics (generic across all source types)"""

    def __init__(
        self,
        batches_extracted: int = 0,
        batches_failed: int = 0,
        items_count: int = 0,  # Total items (files, rows, records, etc.) across all batches
        total_bytes: int = 0,
        duration_ms: int = 0,
        completed_at: Optional[datetime] = None,
    ):
        self.batches_extracted = batches_extracted
        self.batches_failed = batches_failed
        self.items_count = items_count
        self.total_bytes = total_bytes
        self.duration_ms = duration_ms
        self.completed_at = completed_at or datetime.now()


class ExtractionLogger:
    """
    Logging service for extraction framework.

    Handles:
    - Batch-level logging (log_extract_batch table) - work queue for loading
    - Resource-level logging (log_extract_run table) - execution summary
    - UPDATE pattern for state tracking (Airflow-style)
    """

    def __init__(self, lakehouse_utils_instance: lakehouse_utils, auto_create_tables: bool = True):
        """
        Initialize logger with lakehouse utilities.

        Args:
            lakehouse_utils_instance: Lakehouse utilities for accessing log tables
            auto_create_tables: If True, automatically create log tables if they don't exist
        """
        self.lakehouse = lakehouse_utils_instance
        self.spark = lakehouse_utils_instance.spark

        if auto_create_tables:
            self.ensure_log_tables_exist()

    def ensure_log_tables_exist(self) -> None:
        """Create log tables if they don't exist"""
        try:
            # Check and create log_resource_extract_batch table
            try:
                self.spark.sql("DESCRIBE TABLE log_resource_extract_batch")
                logger.debug("Table 'log_resource_extract_batch' already exists")
            except Exception:
                logger.info("Creating table 'log_resource_extract_batch' with partitioning by (source_name, resource_name)")
                schema = get_extract_resource_batch_schema()
                empty_df = self.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_resource_extract_batch",
                    mode="overwrite",
                    partition_by=["source_name", "resource_name"],
                )

            # Check and create log_resource_extract table
            try:
                self.spark.sql("DESCRIBE TABLE log_resource_extract")
                logger.debug("Table 'log_resource_extract' already exists")
            except Exception:
                logger.info("Creating table 'log_resource_extract' with partitioning by (source_name, resource_name)")
                schema = get_extract_resource_schema()
                empty_df = self.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_resource_extract",
                    mode="overwrite",
                    partition_by=["source_name", "resource_name"],
                )

        except Exception as e:
            logger.warning(f"Could not create log tables: {e}")

    # ========================================================================
    # BATCH-LEVEL LOGGING (log_resource_extract_batch table)
    # ========================================================================

    def log_extraction_batch(
        self,
        config: ResourceConfig,
        execution_id: str,
        extract_run_id: str,
        extraction_id: str,
        status: str,
        destination_path: str,
        source_path: Optional[str] = None,
        file_count: int = 0,
        file_size_bytes: int = 0,
        promoted_count: int = 0,
        failed_count: int = 0,
        duplicate_count: int = 0,
        duration_ms: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Log extraction batch event (file or folder level).

        This is the work queue for loading - each row says "file X is ready at path Y".

        Args:
            config: Resource configuration
            execution_id: Master execution ID (orchestrator run ID)
            extract_run_id: Parent resource extraction run ID (FK to log_resource_extract)
            extraction_id: Unique batch ID (becomes extract_batch_id)
            status: 'running', 'completed', 'failed', 'duplicate'
            destination_path: WHERE file was promoted to (raw/landing) - CRITICAL for loading
            source_path: Where file came from (inbound, API, etc)
            file_count: Number of files in batch
            file_size_bytes: Total size in bytes
            promoted_count: Successfully promoted files
            failed_count: Failed files
            duplicate_count: Skipped duplicates
            duration_ms: Extraction duration
            error_message: Error if failed
        """
        try:
            # Convert status to string
            status = str(status)

            # Determine load_state based on extraction status
            if status == 'completed':
                load_state = 'pending'  # Ready for loading
            else:
                load_state = 'skipped'  # Don't load failed/duplicate/running extractions

            # Timestamps
            now = datetime.now()
            started_at = now
            completed_at = now if status in ('completed', 'failed', 'duplicate') else None

            # Build log row
            log_data = [
                (
                    extraction_id,          # extract_batch_id - PK
                    extract_run_id,         # FK to log_resource_extract
                    execution_id,           # master_execution_id (denormalized)
                    config.source_name,     # source_name (denormalized for partitioning)
                    config.resource_name,   # resource_name (denormalized for partitioning)
                    status,
                    load_state,             # Track downstream loading status
                    source_path,
                    destination_path,       # KEY: where loader reads from
                    file_count,
                    file_size_bytes,
                    promoted_count,
                    failed_count,
                    duplicate_count,
                    started_at,
                    completed_at,
                    duration_ms,
                    error_message,
                )
            ]

            schema = get_extract_resource_batch_schema()
            log_df = self.spark.createDataFrame(log_data, schema)

            # Append to log table (partitioned by source_name, resource_name for isolation)
            self.lakehouse.write_to_table(
                df=log_df,
                table_name="log_resource_extract_batch",
                mode="append",
            )

        except Exception as e:
            logger.warning(f"Failed to log extraction batch ({status}): {e}")

    # ========================================================================
    # RESOURCE-LEVEL LOGGING (log_resource_extract table)
    # ========================================================================

    def log_extraction_config_start(
        self,
        config: ResourceConfig,
        execution_id: str,
    ) -> str:
        """
        Log the start of resource extraction.

        Returns:
            extract_run_id: The generated run ID for this resource extraction (use for batch logging)
        """
        return self._log_extraction_config_event(
            config=config,
            execution_id=execution_id,
            status="running",
        )

    def log_extraction_config_completion(
        self,
        config: ResourceConfig,
        execution_id: str,
        extract_run_id: str,
        result: ExtractionResult,
    ) -> None:
        """Log the completion of resource extraction"""
        self._log_extraction_config_event(
            config=config,
            execution_id=execution_id,
            extract_run_id=extract_run_id,
            status="completed",
            completed_at=result.completed_at,
        )

    def log_extraction_config_error(
        self,
        config: ResourceConfig,
        execution_id: str,
        extract_run_id: str,
        error_message: str,
        result: ExtractionResult,
    ) -> None:
        """Log a resource extraction error"""
        self._log_extraction_config_event(
            config=config,
            execution_id=execution_id,
            extract_run_id=extract_run_id,
            status="failed",
            error_message=error_message,
        )

    def log_extraction_config_no_data(
        self,
        config: ResourceConfig,
        execution_id: str,
        extract_run_id: str,
    ) -> None:
        """Log extraction with no data found"""
        self._log_extraction_config_event(
            config=config,
            execution_id=execution_id,
            extract_run_id=extract_run_id,
            status="no_data",
        )

    def _log_extraction_config_event(
        self,
        config: ResourceConfig,
        execution_id: str,
        status: str,
        extract_run_id: Optional[str] = None,
        completed_at: Optional[datetime] = None,
        error_message: Optional[str] = None,
    ) -> str:
        """
        Unified method to log config extraction events using UPDATE pattern (Airflow-style).

        - On "running": INSERT new row with new extract_run_id
        - On "completed/failed/no_data": UPDATE existing row with final metrics

        Returns:
            extract_run_id: The run ID for this resource extraction
        """
        try:
            # Convert status to string
            status = str(status)

            # Timestamps
            now = datetime.now()
            completed_at = completed_at if completed_at else (
                now if status in ('completed', 'failed', 'no_data') else None
            )

            if status == "running":
                # Generate new extract_run_id for INSERT
                extract_run_id = str(uuid.uuid4())

                # INSERT new row for "running" status
                log_data = [
                    (
                        extract_run_id,         # PK
                        execution_id,           # master_execution_id
                        config.source_name,     # source_name
                        config.resource_name,   # resource_name
                        status,
                        error_message,
                        now,                    # started_at
                        now,                    # updated_at
                        None,                   # completed_at (NULL for running)
                    )
                ]

                schema = get_extract_resource_schema()
                log_df = self.spark.createDataFrame(log_data, schema)

                self.lakehouse.write_to_table(
                    df=log_df,
                    table_name="log_resource_extract",
                    mode="append",
                )
            else:
                # UPDATE existing row with final status (completed/failed/no_data)
                update_query = f"""
                    UPDATE log_resource_extract
                    SET
                        status = '{status}',
                        error_message = {'NULL' if error_message is None else f"'{error_message}'"},
                        updated_at = CURRENT_TIMESTAMP(),
                        completed_at = CURRENT_TIMESTAMP()
                    WHERE extract_run_id = '{extract_run_id}'
                      AND source_name = '{config.source_name}'
                      AND resource_name = '{config.resource_name}'
                """
                self.spark.sql(update_query)

            # Return the extract_run_id (either newly generated or passed in)
            return extract_run_id

        except Exception as e:
            logger.warning(f"Failed to log extraction config event ({status}): {e}")
            # Return a generated ID even on error to prevent cascading failures
            return extract_run_id if extract_run_id else str(uuid.uuid4())
