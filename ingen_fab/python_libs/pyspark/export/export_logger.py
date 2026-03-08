"""ExportLogger for tracking export execution state in Delta tables."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingen_fab.python_libs.common.export_resource_config_schema import (
    get_log_resource_export_schema,
)
from ingen_fab.python_libs.pyspark.export.common.config import ExportConfig
from ingen_fab.python_libs.pyspark.export.common.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.export.common.results import ExportResult
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)


class ExportLogger:
    """
    Logger for tracking export execution state in Delta tables.

    Creates log entries in log_resource_export table to track:
    - Export start (pending state)
    - Export progress (running state)
    - Export completion (success/warning/error state)
    """

    LOG_TABLE_NAME = "log_resource_export"
    WATERMARK_TABLE_NAME = "log_resource_export_watermark"

    def __init__(
        self,
        config_lakehouse: lakehouse_utils,
        spark: SparkSession = None,
        auto_create_tables: bool = True,
    ):
        """
        Initialize ExportLogger.

        Args:
            config_lakehouse: lakehouse_utils instance for log table operations
            spark: SparkSession instance (uses config_lakehouse.spark if not provided)
            auto_create_tables: If True, create log tables if they don't exist
        """
        self.config_lakehouse = config_lakehouse
        self.spark = spark or config_lakehouse.spark
        self.logger = logger

        if auto_create_tables:
            self._ensure_log_table_exists()

    def _ensure_log_table_exists(self):
        """Create log table if it doesn't exist."""
        if not self.config_lakehouse.check_if_table_exists(self.LOG_TABLE_NAME):
            self.logger.info(f"Creating log table: {self.LOG_TABLE_NAME}")
            empty_df = self.spark.createDataFrame([], get_log_resource_export_schema())
            self.config_lakehouse.write_to_table(
                df=empty_df,
                table_name=self.LOG_TABLE_NAME,
                mode="overwrite",
            )
            self.logger.info(f"Created log table: {self.LOG_TABLE_NAME}")

    def log_export_start(
        self,
        config: ExportConfig,
        master_execution_id: str,
        export_run_id: str,
    ):
        """
        Log the start of an export (pending state).

        Args:
            config: Export configuration
            master_execution_id: Master execution ID for the run
            export_run_id: Unique ID for this export run
        """
        now = datetime.utcnow()

        log_data = {
            "export_run_id": export_run_id,
            "master_execution_id": master_execution_id,
            "export_group_name": config.export_group_name,
            "export_name": config.export_name,
            "export_state": str(ExecutionStatus.RUNNING),
            "source_type": config.source_config.source_type,
            "source_workspace": config.source_config.source_workspace,
            "source_datastore": config.source_config.source_datastore,
            "source_table": config.source_config.source_table,
            "target_path": config.target_path,
            "file_format": config.file_format_params.file_format,
            "compression": config.file_format_params.compression,
            "watermark_value": None,
            "period_start_date": None,
            "period_end_date": None,
            "started_at": now,
            "completed_at": None,
            "duration_ms": None,
            "rows_exported": None,
            "files_created": None,
            "total_bytes": None,
            "file_paths": None,
            "trigger_file_path": None,
            "error_message": None,
            "updated_at": now,
        }

        self._insert_log_row(log_data)
        self.logger.info(f"Logged export start: {config.export_name} (run_id: {export_run_id})")

    def log_export_completion(
        self,
        config: ExportConfig,
        master_execution_id: str,
        export_run_id: str,
        result: ExportResult,
    ):
        """
        Log successful export completion.

        Args:
            config: Export configuration
            master_execution_id: Master execution ID
            export_run_id: Export run ID
            result: ExportResult with metrics
        """
        self._update_log_row(
            export_run_id=export_run_id,
            export_group_name=config.export_group_name,
            export_name=config.export_name,
            export_state=str(ExecutionStatus.SUCCESS),
            completed_at=datetime.utcnow(),
            duration_ms=result.metrics.duration_ms,
            rows_exported=result.metrics.rows_exported,
            files_created=result.metrics.files_created,
            total_bytes=result.metrics.total_bytes,
            file_paths=result.file_paths,
            trigger_file_path=result.trigger_file_path,
            error_message=None,
            watermark_value=result.watermark_value,
            period_start_date=result.period_start_date,
            period_end_date=result.period_end_date,
        )
        self.logger.info(
            f"Logged export completion: {config.export_name} "
            f"({result.metrics.rows_exported:,} rows, {result.metrics.files_created} files)"
        )

    def log_export_error(
        self,
        config: ExportConfig,
        master_execution_id: str,
        export_run_id: str,
        error_message: str,
        result: Optional[ExportResult] = None,
    ):
        """
        Log export error.

        Args:
            config: Export configuration
            master_execution_id: Master execution ID
            export_run_id: Export run ID
            error_message: Error message
            result: Optional partial result
        """
        self._update_log_row(
            export_run_id=export_run_id,
            export_group_name=config.export_group_name,
            export_name=config.export_name,
            export_state=str(ExecutionStatus.ERROR),
            completed_at=datetime.utcnow(),
            duration_ms=result.metrics.duration_ms if result else None,
            rows_exported=result.metrics.rows_exported if result else 0,
            files_created=result.metrics.files_created if result else 0,
            total_bytes=result.metrics.total_bytes if result else 0,
            file_paths=result.file_paths if result else [],
            trigger_file_path=result.trigger_file_path if result else None,
            error_message=error_message,
            watermark_value=result.watermark_value if result else None,
            period_start_date=result.period_start_date if result else None,
            period_end_date=result.period_end_date if result else None,
        )
        self.logger.error(f"Logged export error: {config.export_name} - {error_message}")

    def _insert_log_row(self, log_data: dict):
        """Insert a new log row."""
        df = self.spark.createDataFrame([log_data], get_log_resource_export_schema())
        self.config_lakehouse.write_to_table(
            df=df,
            table_name=self.LOG_TABLE_NAME,
            mode="append",
        )

    def _update_log_row(
        self,
        export_run_id: str,
        export_group_name: str,
        export_name: str,
        export_state: str,
        completed_at: datetime,
        duration_ms: Optional[int],
        rows_exported: Optional[int],
        files_created: Optional[int],
        total_bytes: Optional[int],
        file_paths: Optional[List[str]],
        trigger_file_path: Optional[str],
        error_message: Optional[str],
        watermark_value: Optional[str] = None,
        period_start_date: Optional[datetime] = None,
        period_end_date: Optional[datetime] = None,
    ):
        """Update an existing log row using lakehouse update_table."""
        # Build update values
        update_values = {
            "export_state": F.lit(export_state),
            "completed_at": F.lit(completed_at),
            "duration_ms": F.lit(duration_ms),
            "rows_exported": F.lit(rows_exported),
            "files_created": F.lit(files_created),
            "total_bytes": F.lit(total_bytes),
            "file_paths": F.array(*[F.lit(p) for p in (file_paths or [])]) if file_paths else F.lit(None),
            "trigger_file_path": F.lit(trigger_file_path),
            "error_message": F.lit(error_message),
            "watermark_value": F.lit(watermark_value),
            "period_start_date": F.lit(period_start_date),
            "period_end_date": F.lit(period_end_date),
            "updated_at": F.lit(datetime.utcnow()),
        }

        # Use lakehouse_utils update_table (same pattern as ingestion logger)
        # Include partition columns (export_group_name, export_name) in condition
        # to enable Delta partition pruning and avoid ConcurrentAppendException
        self.config_lakehouse.update_table(
            table_name=self.LOG_TABLE_NAME,
            condition=(
                (F.col("export_run_id") == export_run_id) &
                (F.col("export_group_name") == export_group_name) &
                (F.col("export_name") == export_name)
            ),
            set_values=update_values,
        )

    def get_failed_export_keys(
        self, export_keys: List[tuple]
    ) -> set:
        """
        Get set of export keys (export_group_name, export_name) that failed in previous runs.

        Args:
            export_keys: List of (export_group_name, export_name) tuples to check

        Returns:
            Set of (export_group_name, export_name) tuples that have error state in latest run
        """
        if not export_keys:
            return set()

        # Get latest run state for each export
        df = self.config_lakehouse.read_table(table_name=self.LOG_TABLE_NAME)

        # Window to get latest run per export (composite key)
        from pyspark.sql.window import Window
        window = Window.partitionBy("export_group_name", "export_name").orderBy(F.desc("started_at"))

        # Filter to only the export keys we're interested in
        export_group_names = [k[0] for k in export_keys]
        export_names = [k[1] for k in export_keys]

        latest_df = (
            df.filter(
                F.col("export_group_name").isin(export_group_names) &
                F.col("export_name").isin(export_names)
            )
            .withColumn("row_num", F.row_number().over(window))
            .filter(F.col("row_num") == 1)
            .filter(F.col("export_state") == str(ExecutionStatus.ERROR))
            .select("export_group_name", "export_name")
        )

        # Return as set of tuples for efficient lookup
        return {
            (row.export_group_name, row.export_name)
            for row in latest_df.collect()
        }

    # ========== Watermark Methods ==========

    def _get_watermark_schema(self):
        """Get schema for watermark table."""
        from pyspark.sql.types import (
            StringType,
            StructField,
            StructType,
            TimestampType,
        )
        return StructType([
            StructField("export_group_name", StringType(), False),
            StructField("export_name", StringType(), False),
            StructField("incremental_column", StringType(), False),
            StructField("watermark_value", StringType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("export_run_id", StringType(), False),
        ])

    def _ensure_watermark_table_exists(self):
        """Create watermark table if it doesn't exist."""
        if not self.config_lakehouse.check_if_table_exists(self.WATERMARK_TABLE_NAME):
            self.logger.info(f"Creating watermark table: {self.WATERMARK_TABLE_NAME}")
            empty_df = self.spark.createDataFrame([], self._get_watermark_schema())
            self.config_lakehouse.write_to_table(
                df=empty_df,
                table_name=self.WATERMARK_TABLE_NAME,
                mode="overwrite",
            )
            self.logger.info(f"Created watermark table: {self.WATERMARK_TABLE_NAME}")

    def get_watermark(
        self,
        export_group_name: str,
        export_name: str,
    ) -> Optional[str]:
        """
        Get last watermark value for an export.

        Args:
            export_group_name: Export group name
            export_name: Export name

        Returns:
            Watermark value as string, or None if no previous export
        """
        if not self.config_lakehouse.check_if_table_exists(self.WATERMARK_TABLE_NAME):
            return None

        df = self.config_lakehouse.read_table(self.WATERMARK_TABLE_NAME)
        result = (
            df.filter(
                (F.col("export_group_name") == export_group_name) &
                (F.col("export_name") == export_name)
            )
            .orderBy(F.desc("updated_at"))
            .limit(1)
            .select("watermark_value")
            .collect()
        )

        return result[0].watermark_value if result else None

    def update_watermark(
        self,
        export_group_name: str,
        export_name: str,
        incremental_column: str,
        watermark_value: str,
        export_run_id: str,
    ):
        """
        Update watermark value after successful export.

        Args:
            export_group_name: Export group name
            export_name: Export name
            incremental_column: Name of the incremental column
            watermark_value: New watermark value (ISO formatted string)
            export_run_id: Export run ID for audit
        """
        # Ensure table exists
        self._ensure_watermark_table_exists()

        watermark_data = {
            "export_group_name": export_group_name,
            "export_name": export_name,
            "incremental_column": incremental_column,
            "watermark_value": watermark_value,
            "updated_at": datetime.utcnow(),
            "export_run_id": export_run_id,
        }

        df = self.spark.createDataFrame([watermark_data], self._get_watermark_schema())
        self.config_lakehouse.write_to_table(
            df=df,
            table_name=self.WATERMARK_TABLE_NAME,
            mode="append",
        )
        self.logger.info(
            f"Updated watermark for {export_group_name}/{export_name}: {watermark_value}"
        )
