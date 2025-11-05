# File Loading Framework - Logging Service
# Handles state tracking in log_file_load and log_config_execution tables

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from ingen_fab.python_libs.common.config_execution_logging_schema import (
    get_config_execution_log_schema,
)
from ingen_fab.python_libs.common.file_load_logging_schema import (
    get_file_load_log_schema,
)
from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchInfo,
    ProcessingMetrics,
    ResourceExecutionResult,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)


class FileLoadingLogger:
    """
    Unified logging service for file loading framework.

    Handles:
    - Batch-level logging (log_file_load table)
    - Resource-level logging (log_config_execution table)
    - Merge/upsert pattern for state updates
    - Airflow-style single-row-per-execution pattern
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
            # Check and create log_file_load table
            try:
                self.spark.sql("DESCRIBE TABLE log_file_load")
                logger.debug("Table 'log_file_load' already exists")
            except Exception:
                logger.info("Creating table 'log_file_load'")
                schema = get_file_load_log_schema()
                empty_df = self.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_file_load",
                    mode="overwrite",
                )

            # Check and create log_config_execution table
            try:
                self.spark.sql("DESCRIBE TABLE log_config_execution")
                logger.debug("Table 'log_config_execution' already exists")
            except Exception:
                logger.info("Creating table 'log_config_execution'")
                schema = get_config_execution_log_schema()
                empty_df = self.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_config_execution",
                    mode="overwrite",
                )

        except Exception as e:
            logger.warning(f"Could not create log tables: {e}")

    # ========================================================================
    # BATCH-LEVEL LOGGING (log_file_load table)
    # ========================================================================

    def log_batch_start(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_id: str,
        batch_info: BatchInfo,
    ) -> None:
        """Log the start of a batch load"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_id=load_id,
            status="running",
            batch_info=batch_info,
        )

    def log_batch_completion(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_id: str,
        metrics: ProcessingMetrics,
        batch_info: BatchInfo,
    ) -> None:
        """Log the completion of a batch load"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_id=load_id,
            status="completed",
            batch_info=batch_info,
            metrics=metrics,
        )

    def log_batch_error(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_id: str,
        error_message: str,
        batch_info: BatchInfo,
    ) -> None:
        """Log a batch load error"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_id=load_id,
            status="failed",
            batch_info=batch_info,
            error_message=error_message,
        )

    def log_batch_duplicate(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_id: str,
        batch_info: BatchInfo,
    ) -> None:
        """Log a duplicate batch that was skipped"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_id=load_id,
            status="duplicate",
            batch_info=batch_info,
        )

    def _log_batch_event(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_id: str,
        status: str,
        batch_info: BatchInfo,
        metrics: Optional[ProcessingMetrics] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Unified method to log batch events using merge/upsert pattern.

        Uses Airflow-style single-row-per-execution pattern where:
        - First call creates row with status='running'
        - Subsequent calls update same row with latest status/metrics
        - Timestamps: started_at (immutable), updated_at (always updated), completed_at (set on completion)
        """
        try:
            # Convert status to string if it's an enum (defensive programming)
            status = str(status)

            # Extract batch metadata
            source_file_path = batch_info.file_paths[0] if batch_info.file_paths else None
            source_file_size_bytes = batch_info.size_bytes
            source_file_modified_time = batch_info.modified_time
            date_partition = batch_info.date_partition

            # Populate metrics fields
            records_processed = metrics.records_processed if metrics else (0 if status != "running" else None)
            records_inserted = metrics.records_inserted if metrics else (0 if status != "running" else None)
            records_updated = metrics.records_updated if metrics else (0 if status != "running" else None)
            records_deleted = metrics.records_deleted if metrics else (0 if status != "running" else None)
            source_row_count = metrics.source_row_count if metrics else (0 if status != "running" else None)
            target_row_count_before = metrics.target_row_count_before if metrics else (0 if status != "running" else None)
            target_row_count_after = metrics.target_row_count_after if metrics else (0 if status != "running" else None)
            row_count_reconciliation_status = (
                metrics.row_count_reconciliation_status if metrics
                else ("not_verified" if status != "running" else None)
            )
            data_read_duration_ms = metrics.read_duration_ms if metrics else None
            total_duration_ms = metrics.total_duration_ms if metrics else None
            execution_duration_seconds = (
                int(metrics.total_duration_ms / 1000) if metrics and metrics.total_duration_ms
                else None
            )

            # Timestamp logic
            now = datetime.now()
            started_at = now  # For first insert only (protected by immutable_columns)
            updated_at = now  # Always current time
            completed_at = metrics.completed_at if metrics else None  # From caller
            attempt_count = 1

            # Build log row
            log_data = [
                (
                    load_id,                              # PK
                    execution_id,
                    config.resource_name,                 # config_id (using resource_name)
                    status,
                    source_file_path,
                    source_file_size_bytes,
                    source_file_modified_time,
                    config.target_table_name,
                    records_processed,
                    records_inserted,
                    records_updated,
                    records_deleted,
                    source_row_count,
                    target_row_count_before,
                    target_row_count_after,
                    row_count_reconciliation_status,
                    0,                                    # corrupt_records_count (not used yet)
                    data_read_duration_ms,
                    total_duration_ms,
                    error_message,
                    None,                                 # error_details
                    execution_duration_seconds,
                    None,                                 # source_file_partition_cols (not used yet)
                    None,                                 # source_file_partition_values (not used yet)
                    date_partition,
                    None,                                 # filename_attributes_json (not used yet)
                    None,                                 # control_file_path (not used yet)
                    started_at,
                    updated_at,
                    completed_at,
                    attempt_count,
                )
            ]

            schema = get_file_load_log_schema()
            log_df = self.spark.createDataFrame(log_data, schema)

            # Merge into log table
            self.lakehouse.merge_to_table(
                df=log_df,
                table_name="log_file_load",
                merge_keys=["load_id"],
                immutable_columns=[
                    "load_id",
                    "execution_id",
                    "config_id",
                    "source_file_path",
                    "started_at",
                ],
                custom_update_expressions={
                    "attempt_count": "TARGET.attempt_count + 1",
                    "updated_at": "SOURCE.updated_at",
                },
            )

        except Exception as e:
            logger.warning(f"Failed to log batch event ({status}): {e}")

    # ========================================================================
    # RESOURCE-LEVEL LOGGING (log_config_execution table)
    # ========================================================================

    def log_config_execution_start(
        self,
        config: ResourceConfig,
        execution_id: str,
    ) -> None:
        """Log the start of a resource execution"""
        self._log_config_execution_event(
            config=config,
            execution_id=execution_id,
            status="running",
        )

    def log_config_execution_completion(
        self,
        config: ResourceConfig,
        execution_id: str,
        result: ResourceExecutionResult,
    ) -> None:
        """Log the completion of a resource execution"""
        # Count files based on result
        files_discovered = result.batches_processed + result.batches_failed
        files_processed = result.batches_processed
        files_failed = result.batches_failed
        files_skipped = 0  # Could track this separately if needed

        self._log_config_execution_event(
            config=config,
            execution_id=execution_id,
            status=result.status,
            files_discovered=files_discovered,
            files_processed=files_processed,
            files_failed=files_failed,
            files_skipped=files_skipped,
            metrics=result.metrics,
        )

    def log_config_execution_error(
        self,
        config: ResourceConfig,
        execution_id: str,
        error_message: str,
        result: ResourceExecutionResult,
    ) -> None:
        """Log a resource execution error"""
        files_discovered = result.batches_processed + result.batches_failed
        files_processed = result.batches_processed
        files_failed = result.batches_failed

        self._log_config_execution_event(
            config=config,
            execution_id=execution_id,
            status="failed",
            files_discovered=files_discovered,
            files_processed=files_processed,
            files_failed=files_failed,
            error_message=error_message,
        )

    def _log_config_execution_event(
        self,
        config: ResourceConfig,
        execution_id: str,
        status: str,
        files_discovered: int = 0,
        files_processed: int = 0,
        files_failed: int = 0,
        files_skipped: int = 0,
        metrics: Optional[ProcessingMetrics] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Unified method to log config execution events using merge/upsert pattern.

        Uses Airflow-style single-row-per-execution pattern where:
        - First call creates row with status='running'
        - Subsequent calls update same row with latest status/metrics
        """
        try:
            # Convert status to string if it's an enum (defensive programming)
            status = str(status)

            # Populate metrics fields
            records_processed = metrics.records_processed if metrics else (0 if status != "running" else None)
            records_inserted = metrics.records_inserted if metrics else (0 if status != "running" else None)
            records_updated = metrics.records_updated if metrics else (0 if status != "running" else None)
            records_deleted = metrics.records_deleted if metrics else (0 if status != "running" else None)
            total_duration_ms = metrics.total_duration_ms if metrics else None

            # Timestamp logic
            now = datetime.now()
            started_at = now  # For first insert only (protected by immutable_columns)
            updated_at = now  # Always current time
            completed_at = metrics.completed_at if metrics else None  # From caller

            # Build log row
            log_data = [
                (
                    execution_id,                         # PK part 1
                    config.resource_name,                 # PK part 2 (config_id)
                    status,
                    files_discovered,
                    files_processed,
                    files_failed,
                    files_skipped,
                    records_processed,
                    records_inserted,
                    records_updated,
                    records_deleted,
                    total_duration_ms,
                    error_message,
                    None,                                 # error_details
                    started_at,
                    updated_at,
                    completed_at,
                )
            ]

            schema = get_config_execution_log_schema()
            log_df = self.spark.createDataFrame(log_data, schema)

            # Merge into log table
            self.lakehouse.merge_to_table(
                df=log_df,
                table_name="log_config_execution",
                merge_keys=["execution_id", "config_id"],
                immutable_columns=[
                    "execution_id",
                    "config_id",
                    "started_at",
                ],
                custom_update_expressions={
                    "updated_at": "SOURCE.updated_at",
                },
            )

        except Exception as e:
            logger.warning(f"Failed to log config execution event ({status}): {e}")
