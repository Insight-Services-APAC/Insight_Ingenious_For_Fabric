# File Loading Framework - Logging Service
# Handles state tracking in log_resource_load_batch and log_resource_load tables

import logging
import uuid
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession

from ingen_fab.python_libs.common.load_resource_schema import (
    get_load_resource_schema,
)
from ingen_fab.python_libs.common.load_resource_batch_schema import (
    get_load_resource_batch_schema,
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
        self.spark = lakehouse_utils_instance.spark

        if auto_create_tables:
            self.ensure_log_tables_exist()

    def ensure_log_tables_exist(self) -> None:
        """Create log tables if they don't exist"""
        try:
            # Check and create log_resource_load_batch table
            try:
                self.spark.sql("DESCRIBE TABLE log_resource_load_batch")
                logger.debug("Table 'log_resource_load_batch' already exists")
            except Exception:
                logger.info("Creating table 'log_resource_load_batch' with partitioning by (source_name, resource_name)")
                schema = get_load_resource_batch_schema()
                empty_df = self.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_resource_load_batch",
                    mode="overwrite",
                    partition_by=["source_name", "resource_name"],
                )

            # Check and create log_resource_load table
            try:
                self.spark.sql("DESCRIBE TABLE log_resource_load")
                logger.debug("Table 'log_resource_load' already exists")
            except Exception:
                logger.info("Creating table 'log_resource_load' with partitioning by (source_name, resource_name)")
                schema = get_load_resource_schema()
                empty_df = self.spark.createDataFrame([], schema)
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
            status="running",
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
            status="completed",
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
            status="failed",
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
            status="duplicate",
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
            status="no_data",
            batch_info=batch_info,
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
            # Convert status to string if it's an enum (defensive programming)
            status = str(status)

            # Extract batch metadata
            extract_batch_id = batch_info.extract_batch_id  # FK to log_resource_extract_batch
            source_file_path = batch_info.file_paths[0] if batch_info.file_paths else None
            source_file_size_bytes = batch_info.size_bytes
            source_file_modified_time = batch_info.modified_time

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

            now = datetime.now()
            completed_at = metrics.completed_at if metrics else None

            if status == "running":
                # INSERT new row for "running" status
                log_data = [
                    (
                        load_id,                              # load_batch_id - PK
                        load_run_id,                          # FK to log_resource_load
                        execution_id,                         # master_execution_id (denormalized)
                        extract_batch_id,                     # FK to log_resource_extract_batch
                        config.source_name,                   # source_name (denormalized for partitioning)
                        config.resource_name,                 # resource_name (denormalized for partitioning)
                        status,
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
                        None,                                 # error_details
                        None,                                 # execution_duration_seconds
                        None,                                 # filename_attributes_json
                        now,                                  # started_at
                        now,                                  # updated_at
                        None,                                 # completed_at (NULL for running)
                        1,                                    # attempt_count
                    )
                ]

                schema = get_load_resource_batch_schema()
                log_df = self.spark.createDataFrame(log_data, schema)

                self.lakehouse.write_to_table(
                    df=log_df,
                    table_name="log_resource_load_batch",
                    mode="append",
                )
            else:
                # UPDATE existing row with final metrics (completed/failed/no_data/duplicate)
                update_query = f"""
                    UPDATE log_resource_load_batch
                    SET
                        status = '{status}',
                        records_processed = {records_processed if records_processed is not None else 0},
                        records_inserted = {records_inserted if records_inserted is not None else 0},
                        records_updated = {records_updated if records_updated is not None else 0},
                        records_deleted = {records_deleted if records_deleted is not None else 0},
                        source_row_count = {source_row_count if source_row_count is not None else 0},
                        target_row_count_before = {target_row_count_before if target_row_count_before is not None else 0},
                        target_row_count_after = {target_row_count_after if target_row_count_after is not None else 0},
                        row_count_reconciliation_status = '{row_count_reconciliation_status}',
                        data_read_duration_ms = {data_read_duration_ms if data_read_duration_ms is not None else 'NULL'},
                        total_duration_ms = {total_duration_ms if total_duration_ms is not None else 'NULL'},
                        error_message = {'NULL' if error_message is None else f"'{error_message.replace(chr(39), chr(39) + chr(39))}'"},
                        execution_duration_seconds = {execution_duration_seconds if execution_duration_seconds is not None else 'NULL'},
                        updated_at = CURRENT_TIMESTAMP(),
                        completed_at = CURRENT_TIMESTAMP()
                    WHERE load_batch_id = '{load_id}'
                      AND source_name = '{config.source_name}'
                      AND resource_name = '{config.resource_name}'
                """
                self.spark.sql(update_query)

        except Exception as e:
            logger.warning(f"Failed to log batch event ({status}): {e}")

    # ========================================================================
    # RESOURCE-LEVEL LOGGING (log_resource_load table)
    # ========================================================================

    def log_config_execution_start(
        self,
        config: ResourceConfig,
        execution_id: str,
    ) -> str:
        """
        Log the start of a resource execution.

        Returns:
            load_run_id: The generated run ID for this resource load (use for batch logging)
        """
        return self._log_config_execution_event(
            config=config,
            execution_id=execution_id,
            status="running",
        )

    def log_config_execution_completion(
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

        self._log_config_execution_event(
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

    def log_config_execution_error(
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

        self._log_config_execution_event(
            config=config,
            execution_id=execution_id,
            load_run_id=load_run_id,
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
            # Convert status to string if it's an enum (defensive programming)
            status = str(status)

            # Populate metrics fields
            records_processed = metrics.records_processed if metrics else (0 if status != "running" else None)
            records_inserted = metrics.records_inserted if metrics else (0 if status != "running" else None)
            records_updated = metrics.records_updated if metrics else (0 if status != "running" else None)
            records_deleted = metrics.records_deleted if metrics else (0 if status != "running" else None)
            total_duration_ms = metrics.total_duration_ms if metrics else None

            now = datetime.now()
            completed_at = metrics.completed_at if metrics else None

            if status == "running":
                # Generate new load_run_id for INSERT
                load_run_id = str(uuid.uuid4())

                # INSERT new row for "running" status
                log_data = [
                    (
                        load_run_id,                          # PK
                        execution_id,                         # master_execution_id
                        config.source_name,                   # source_name
                        config.resource_name,                 # resource_name
                        status,
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
                        None,                                 # error_details
                        now,                                  # started_at
                        now,                                  # updated_at
                        None,                                 # completed_at (NULL for running)
                    )
                ]

                schema = get_load_resource_schema()
                log_df = self.spark.createDataFrame(log_data, schema)

                self.lakehouse.write_to_table(
                    df=log_df,
                    table_name="log_resource_load",
                    mode="append",
                )
            else:
                # UPDATE existing row with final metrics (completed/failed/no_data)
                update_query = f"""
                    UPDATE log_resource_load
                    SET
                        status = '{status}',
                        files_discovered = {files_discovered},
                        files_processed = {files_processed},
                        files_failed = {files_failed},
                        files_skipped = {files_skipped},
                        records_processed = {records_processed if records_processed is not None else 0},
                        records_inserted = {records_inserted if records_inserted is not None else 0},
                        records_updated = {records_updated if records_updated is not None else 0},
                        records_deleted = {records_deleted if records_deleted is not None else 0},
                        total_duration_ms = {total_duration_ms if total_duration_ms is not None else 'NULL'},
                        error_message = {'NULL' if error_message is None else f"'{error_message.replace(chr(39), chr(39) + chr(39))}'"},
                        updated_at = CURRENT_TIMESTAMP(),
                        completed_at = CURRENT_TIMESTAMP()
                    WHERE load_run_id = '{load_run_id}'
                      AND source_name = '{config.source_name}'
                      AND resource_name = '{config.resource_name}'
                """
                self.spark.sql(update_query)

            # Return the load_run_id (either newly generated or passed in)
            return load_run_id

        except Exception as e:
            logger.warning(f"Failed to log config execution event ({status}): {e}")
            # Return a generated ID even on error to prevent cascading failures
            return load_run_id if load_run_id else str(uuid.uuid4())
