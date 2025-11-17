# File Loading Framework - Logging Service
# Handles state tracking in log_resource_load_batch and log_resource_load tables

import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, row_number
from pyspark.sql.window import Window

from ingen_fab.python_libs.common.load_resource_schema import (
    get_load_resource_schema,
)
from ingen_fab.python_libs.common.load_resource_batch_schema import (
    get_load_resource_batch_schema,
)
from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchInfo,
    ProcessingMetrics,
    ResourceExecutionResult,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)


class LoadingLogger:
    """
    Unified logging service for file loading framework.

    Handles:
    - Batch-level logging (log_resource_load_batch table)
    - Resource-level logging (log_resource_load table)
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

        if auto_create_tables:
            self.ensure_log_tables_exist()

    def ensure_log_tables_exist(self) -> None:
        """Create log tables if they don't exist"""
        try:
            # Check and create log_resource_load_batch table
            if self.lakehouse.check_if_table_exists("log_resource_load_batch"):
                logger.debug("Table 'log_resource_load_batch' already exists")
            else:
                logger.info("Creating table 'log_resource_load_batch' with partitioning by (source_name, resource_name)")
                schema = get_load_resource_batch_schema()
                empty_df = self.lakehouse.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_resource_load_batch",
                    mode="overwrite",
                    partition_by=["source_name", "resource_name"],
                )

            # Check and create log_resource_load table
            if self.lakehouse.check_if_table_exists("log_resource_load"):
                logger.debug("Table 'log_resource_load' already exists")
            else:
                logger.info("Creating table 'log_resource_load' with partitioning by (source_name, resource_name)")
                schema = get_load_resource_schema()
                empty_df = self.lakehouse.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_resource_load",
                    mode="overwrite",
                    partition_by=["source_name", "resource_name"],
                )

        except Exception as e:
            logger.warning(f"Could not create log tables: {e}")

    # ========================================================================
    # BATCH-LEVEL LOGGING (log_resource_load_batch table)
    # ========================================================================

    def log_batch_start(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        load_id: str,
        batch_info: BatchInfo,
    ) -> None:
        """Log the start of a batch load"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_run_id=load_run_id,
            load_id=load_id,
            status=ExecutionStatus.RUNNING,
            batch_info=batch_info,
        )

    def log_batch_completion(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        load_id: str,
        metrics: ProcessingMetrics,
        batch_info: BatchInfo,
    ) -> None:
        """Log the completion of a batch load"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_run_id=load_run_id,
            load_id=load_id,
            status=ExecutionStatus.COMPLETED,
            batch_info=batch_info,
            metrics=metrics,
        )

    def log_batch_error(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        load_id: str,
        error_message: str,
        batch_info: BatchInfo,
    ) -> None:
        """Log a batch load error"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_run_id=load_run_id,
            load_id=load_id,
            status=ExecutionStatus.FAILED,
            batch_info=batch_info,
            error_message=error_message,
        )

    def log_batch_duplicate(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        load_id: str,
        batch_info: BatchInfo,
    ) -> None:
        """Log a duplicate batch that was skipped"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_run_id=load_run_id,
            load_id=load_id,
            status=ExecutionStatus.DUPLICATE,
            batch_info=batch_info,
        )

    def log_batch_no_data(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        load_id: str,
        batch_info: BatchInfo,
    ) -> None:
        """Log a batch with no data"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_run_id=load_run_id,
            load_id=load_id,
            status=ExecutionStatus.NO_DATA,
            batch_info=batch_info,
        )

    def log_batch_rejected(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        load_id: str,
        rejection_reason: str,
        batch_info: BatchInfo,
    ) -> None:
        """Log a batch rejected due to data quality issues"""
        self._log_batch_event(
            config=config,
            execution_id=execution_id,
            load_run_id=load_run_id,
            load_id=load_id,
            status=ExecutionStatus.REJECTED,
            batch_info=batch_info,
            error_message=rejection_reason,
        )

    def _log_batch_event(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        load_id: str,
        status: str,
        batch_info: BatchInfo,
        metrics: Optional[ProcessingMetrics] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Unified method to log batch events using UPDATE pattern (Airflow-style).

        - On "running": INSERT new row
        - On "completed/failed/no_data/duplicate": UPDATE existing row with final metrics
        """
        try:
            # Extract batch metadata
            extract_batch_id = batch_info.extract_batch_id  # FK to log_resource_extract_batch
            source_file_path = batch_info.file_paths[0] if batch_info.file_paths else None
            source_file_size_bytes = batch_info.size_bytes
            source_file_modified_time = batch_info.modified_time

            # Populate metrics fields
            records_processed = metrics.records_processed if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            records_inserted = metrics.records_inserted if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            records_updated = metrics.records_updated if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            records_deleted = metrics.records_deleted if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            source_row_count = metrics.source_row_count if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            target_row_count_before = metrics.target_row_count_before if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            target_row_count_after = metrics.target_row_count_after if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            row_count_reconciliation_status = (
                metrics.row_count_reconciliation_status if metrics
                else ("not_verified" if status != ExecutionStatus.RUNNING else None)
            )
            data_read_duration_ms = metrics.read_duration_ms if metrics else None
            total_duration_ms = metrics.total_duration_ms if metrics else None
            execution_duration_seconds = (
                int(metrics.total_duration_ms / 1000) if metrics and metrics.total_duration_ms
                else None
            )

            now = datetime.now()
            completed_at = metrics.completed_at if metrics else None

            if status == ExecutionStatus.RUNNING:
                # INSERT new row for "running" status
                log_data = [
                    (
                        load_id,                              # load_batch_id - PK
                        load_run_id,                          # FK to log_resource_load
                        execution_id,                         # master_execution_id (denormalized)
                        extract_batch_id,                     # FK to log_resource_extract_batch
                        config.source_name,                   # source_name (denormalized for partitioning)
                        config.resource_name,                 # resource_name (denormalized for partitioning)
                        str(status),                          # Convert enum to string for Spark
                        source_file_path,
                        source_file_size_bytes,
                        source_file_modified_time,
                        config.target_table,
                        None,                                 # records_processed (NULL for running)
                        None,                                 # records_inserted
                        None,                                 # records_updated
                        None,                                 # records_deleted
                        None,                                 # source_row_count
                        None,                                 # target_row_count_before
                        None,                                 # target_row_count_after
                        None,                                 # row_count_reconciliation_status
                        0,                                    # corrupt_records_count
                        None,                                 # data_read_duration_ms
                        None,                                 # total_duration_ms
                        None,                                 # error_message
                        None,                                 # execution_duration_seconds
                        None,                                 # filename_attributes_json
                        now,                                  # started_at
                        now,                                  # updated_at
                        None,                                 # completed_at (NULL for running)
                        1,                                    # attempt_count
                    )
                ]

                schema = get_load_resource_batch_schema()
                log_df = self.lakehouse.spark.createDataFrame(log_data, schema)

                self.lakehouse.write_to_table(
                    df=log_df,
                    table_name="log_resource_load_batch",
                    mode="append",
                )
            else:
                # UPDATE existing row with final metrics (completed/failed/no_data/duplicate)
                set_values = {
                    "load_state": lit(str(status)),  # Convert enum to string for Spark
                    "records_processed": lit(records_processed if records_processed is not None else 0),
                    "records_inserted": lit(records_inserted if records_inserted is not None else 0),
                    "records_updated": lit(records_updated if records_updated is not None else 0),
                    "records_deleted": lit(records_deleted if records_deleted is not None else 0),
                    "source_row_count": lit(source_row_count if source_row_count is not None else 0),
                    "target_row_count_before": lit(target_row_count_before if target_row_count_before is not None else 0),
                    "target_row_count_after": lit(target_row_count_after if target_row_count_after is not None else 0),
                    "row_count_reconciliation_status": lit(row_count_reconciliation_status),
                    "updated_at": current_timestamp(),
                    "completed_at": current_timestamp(),
                }

                # Add optional fields only if they have values
                if data_read_duration_ms is not None:
                    set_values["data_read_duration_ms"] = lit(data_read_duration_ms)
                if total_duration_ms is not None:
                    set_values["total_duration_ms"] = lit(total_duration_ms)
                if error_message is not None:
                    set_values["error_message"] = lit(error_message)
                if execution_duration_seconds is not None:
                    set_values["execution_duration_seconds"] = lit(execution_duration_seconds)

                self.lakehouse.update_table(
                    table_name="log_resource_load_batch",
                    condition=(
                        (col("load_batch_id") == load_id) &
                        (col("source_name") == config.source_name) &
                        (col("resource_name") == config.resource_name)
                    ),
                    set_values=set_values,
                )

        except Exception as e:
            logger.warning(f"Failed to log batch event ({status}): {e}")

    # ========================================================================
    # RESOURCE-LEVEL LOGGING (log_resource_load table)
    # ========================================================================

    def log_resource_execution_start(
        self,
        config: ResourceConfig,
        execution_id: str,
    ) -> str:
        """
        Log the start of a resource execution.

        Returns:
            load_run_id: The generated run ID for this resource load (use for batch logging)
        """
        return self._log_resource_execution_event(
            config=config,
            execution_id=execution_id,
            status=ExecutionStatus.RUNNING,
        )

    def log_resource_execution_completion(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        result: ResourceExecutionResult,
    ) -> None:
        """Log the completion of a resource execution"""
        # Count files based on result
        files_discovered = result.batches_processed + result.batches_failed
        files_processed = result.batches_processed
        files_failed = result.batches_failed
        files_skipped = 0  # Could track this separately if needed

        self._log_resource_execution_event(
            config=config,
            execution_id=execution_id,
            load_run_id=load_run_id,
            status=result.status,
            files_discovered=files_discovered,
            files_processed=files_processed,
            files_failed=files_failed,
            files_skipped=files_skipped,
            metrics=result.metrics,
        )

    def log_resource_execution_error(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        error_message: str,
        result: ResourceExecutionResult,
    ) -> None:
        """Log a resource execution error"""
        files_discovered = result.batches_processed + result.batches_failed
        files_processed = result.batches_processed
        files_failed = result.batches_failed

        self._log_resource_execution_event(
            config=config,
            execution_id=execution_id,
            load_run_id=load_run_id,
            status=ExecutionStatus.FAILED,
            files_discovered=files_discovered,
            files_processed=files_processed,
            files_failed=files_failed,
            error_message=error_message,
        )

    def _log_resource_execution_event(
        self,
        config: ResourceConfig,
        execution_id: str,
        status: str,
        load_run_id: Optional[str] = None,
        files_discovered: int = 0,
        files_processed: int = 0,
        files_failed: int = 0,
        files_skipped: int = 0,
        metrics: Optional[ProcessingMetrics] = None,
        error_message: Optional[str] = None,
    ) -> str:
        """
        Unified method to log config execution events using UPDATE pattern (Airflow-style).

        - On "running": INSERT new row with new load_run_id
        - On "completed/failed/no_data": UPDATE existing row with final metrics

        Returns:
            load_run_id: The run ID for this resource load
        """
        try:
            # Populate metrics fields
            records_processed = metrics.records_processed if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            records_inserted = metrics.records_inserted if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            records_updated = metrics.records_updated if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            records_deleted = metrics.records_deleted if metrics else (0 if status != ExecutionStatus.RUNNING else None)
            total_duration_ms = metrics.total_duration_ms if metrics else None

            now = datetime.now()
            completed_at = metrics.completed_at if metrics else None

            if status == ExecutionStatus.RUNNING:
                # Generate new load_run_id for INSERT
                load_run_id = str(uuid.uuid4())

                # INSERT new row for "running" status
                log_data = [
                    (
                        load_run_id,                          # PK
                        execution_id,                         # master_execution_id
                        config.source_name,                   # source_name
                        config.resource_name,                 # resource_name
                        str(status),                          # Convert enum to string for Spark
                        files_discovered,
                        files_processed,
                        files_failed,
                        files_skipped,
                        None,                                 # records_processed (NULL for running)
                        None,                                 # records_inserted
                        None,                                 # records_updated
                        None,                                 # records_deleted
                        None,                                 # total_duration_ms
                        None,                                 # error_message
                        now,                                  # started_at
                        now,                                  # updated_at
                        None,                                 # completed_at (NULL for running)
                    )
                ]

                schema = get_load_resource_schema()
                log_df = self.lakehouse.spark.createDataFrame(log_data, schema)

                self.lakehouse.write_to_table(
                    df=log_df,
                    table_name="log_resource_load",
                    mode="append",
                )
            else:
                # UPDATE existing row with final metrics (completed/failed/no_data)
                set_values = {
                    "load_state": lit(str(status)),  # Convert enum to string for Spark
                    "files_discovered": lit(files_discovered),
                    "files_processed": lit(files_processed),
                    "files_failed": lit(files_failed),
                    "files_skipped": lit(files_skipped),
                    "records_processed": lit(records_processed if records_processed is not None else 0),
                    "records_inserted": lit(records_inserted if records_inserted is not None else 0),
                    "records_updated": lit(records_updated if records_updated is not None else 0),
                    "records_deleted": lit(records_deleted if records_deleted is not None else 0),
                    "updated_at": current_timestamp(),
                    "completed_at": current_timestamp(),
                }

                # Add optional fields only if they have values
                if total_duration_ms is not None:
                    set_values["total_duration_ms"] = lit(total_duration_ms)
                if error_message is not None:
                    set_values["error_message"] = lit(error_message)

                self.lakehouse.update_table(
                    table_name="log_resource_load",
                    condition=(
                        (col("load_run_id") == load_run_id) &
                        (col("source_name") == config.source_name) &
                        (col("resource_name") == config.resource_name)
                    ),
                    set_values=set_values,
                )

            # Return the load_run_id (either newly generated or passed in)
            return load_run_id

        except Exception as e:
            logger.warning(f"Failed to log config execution event ({status}): {e}")
            # Return a generated ID even on error to prevent cascading failures
            return load_run_id if load_run_id else str(uuid.uuid4())

    # ========================================================================
    # CROSS-WORKFLOW METHODS (interact with extraction logs)
    # ========================================================================

    def query_pending_extraction_batches(self, config: ResourceConfig):
        """
        Query extraction batches ready for loading (pure data access).

        Returns raw Spark DataFrame from log_resource_extract_batch table.
        Filters for: status='completed', load_state='pending'.

        Args:
            config: Resource configuration

        Returns:
            Spark DataFrame with extraction batch rows (or empty DataFrame if table doesn't exist)
        """
        try:
            # Check if log_resource_extract_batch table exists
            if not self.lakehouse.check_if_table_exists("log_resource_extract_batch"):
                logger.warning("Table 'log_resource_extract_batch' not found")
                # Return empty DataFrame with expected schema
                return self.lakehouse.spark.createDataFrame([], "extract_batch_id string, destination_path string, file_size_bytes long, completed_at timestamp")

            # Query for completed extractions ready for loading
            return (
                self.lakehouse.read_table("log_resource_extract_batch")
                .filter(col("source_name") == config.source_name)
                .filter(col("resource_name") == config.resource_name)
                .filter(col("extract_state") == str(ExecutionStatus.COMPLETED))
                .filter(col("load_state") == str(ExecutionStatus.PENDING))
                .orderBy(col("completed_at").asc())
            )

        except Exception as e:
            logger.warning(f"Could not query extraction logs: {e}")
            # Return empty DataFrame on error
            return self.lakehouse.spark.createDataFrame([], "extract_batch_id string, destination_path string, file_size_bytes long, completed_at timestamp")

    def query_stale_batches(self, config: ResourceConfig, threshold_hours: int):
        """
        Query batches stuck in 'processing' status (pure data access).

        Returns raw Spark DataFrame from log_resource_load_batch table.
        Finds batches in 'processing' status older than threshold_hours.

        Args:
            config: Resource configuration
            threshold_hours: Hours threshold for considering batch stale

        Returns:
            Spark DataFrame with stale batch rows (or empty DataFrame if table doesn't exist)
        """
        try:
            # Check if log table exists
            if not self.lakehouse.check_if_table_exists("log_resource_load_batch"):
                logger.debug("Table 'log_resource_load_batch' not found")
                # Return empty DataFrame
                return self.lakehouse.spark.createDataFrame([], get_load_resource_batch_schema())

            # Calculate threshold timestamp
            threshold_time = datetime.now() - timedelta(hours=threshold_hours)

            # Get latest status for each batch
            window_spec = Window.partitionBy("load_batch_id").orderBy(col("started_at").desc())

            return (
                self.lakehouse.read_table("log_resource_load_batch")
                .filter(col("source_name") == config.source_name)
                .filter(col("resource_name") == config.resource_name)
                .withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .filter(col("load_state") == "processing")
                .filter(col("started_at") < threshold_time)
            )

        except Exception as e:
            logger.warning(f"Could not query stale batches: {e}")
            # Return empty DataFrame on error
            return self.lakehouse.spark.createDataFrame([], get_load_resource_batch_schema())

    def log_batch_recovery(self, config: ResourceConfig, stale_batch_row) -> None:
        """
        Insert recovery record for stale batch (pure data write).

        Creates new row in log_resource_load_batch with status='pending'
        and incremented attempt_count.

        Args:
            config: Resource configuration
            stale_batch_row: Row from query_stale_batches() to recover
        """
        try:
            now = datetime.now()

            # INSERT new row with status='pending' for recovery
            recovery_data = [(
                stale_batch_row.load_batch_id if hasattr(stale_batch_row, 'load_batch_id') else stale_batch_row.load_id,
                "",  # load_run_id (empty for recovery)
                "",  # master_execution_id (empty for recovery)
                stale_batch_row.extract_batch_id if hasattr(stale_batch_row, 'extract_batch_id') else None,
                config.source_name,
                config.resource_name,
                "pending",  # status (recovered)
                stale_batch_row.source_file_path,
                stale_batch_row.source_file_size_bytes if hasattr(stale_batch_row, 'source_file_size_bytes') else None,
                stale_batch_row.source_file_modified_time if hasattr(stale_batch_row, 'source_file_modified_time') else None,
                (stale_batch_row.target_table_name if hasattr(stale_batch_row, 'target_table_name') else config.target_table) or "",
                None,  # records_processed
                None,  # records_inserted
                None,  # records_updated
                None,  # records_deleted
                None,  # source_row_count
                None,  # target_row_count_before
                None,  # target_row_count_after
                None,  # row_count_reconciliation_status
                0,     # corrupt_records_count
                None,  # data_read_duration_ms
                None,  # total_duration_ms
                "Recovered from stale processing state",  # error_message
                None,  # execution_duration_seconds
                None,  # filename_attributes_json
                now,   # started_at
                now,   # updated_at
                None,  # completed_at
                (stale_batch_row.attempt_count + 1) if hasattr(stale_batch_row, 'attempt_count') else 1,  # attempt_count
            )]

            recovery_df = self.lakehouse.spark.createDataFrame(recovery_data, get_load_resource_batch_schema())

            self.lakehouse.write_to_table(
                df=recovery_df,
                table_name="log_resource_load_batch",
                mode="append",
            )

            logger.debug(f"Logged recovery for batch {stale_batch_row.load_batch_id if hasattr(stale_batch_row, 'load_batch_id') else stale_batch_row.load_id}")

        except Exception as e:
            logger.warning(f"Could not log batch recovery: {e}")

    def update_extraction_batch_load_state(
        self,
        extract_batch_id: str,
        source_name: str,
        resource_name: str,
        new_state: ExecutionStatus
    ) -> None:
        """
        Update load_state for an extraction batch.

        IMPORTANT: Includes source_name and resource_name in WHERE clause for partition isolation.
        This prevents concurrent update conflicts when processing multiple resources in parallel.

        Args:
            extract_batch_id: The extract_batch_id to update
            source_name: Source name (partition key)
            resource_name: Resource name (partition key)
            new_state: New load_state (ExecutionStatus.RUNNING/COMPLETED/FAILED)
        """
        try:
            self.lakehouse.update_table(
                table_name="log_resource_extract_batch",
                condition=(
                    (col("extract_batch_id") == extract_batch_id) &
                    (col("source_name") == source_name) &
                    (col("resource_name") == resource_name)
                ),
                set_values={"load_state": lit(str(new_state))}
            )
            logger.debug(f"Updated extraction {extract_batch_id} to load_state='{new_state}'")
        except Exception as e:
            logger.warning(f"Could not update load_state for {extract_batch_id}: {e}")
