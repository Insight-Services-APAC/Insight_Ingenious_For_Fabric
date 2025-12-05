# Extraction Framework - Logging Service
# Handles state tracking in log_resource_extract_batch and log_resource_extract tables

import logging
import uuid
from datetime import date, datetime
from typing import Any, List, Optional, Set, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, exists, row_number
from pyspark.sql.window import Window

from ingen_fab.python_libs.common.extract_resource_schema import (
    get_extract_resource_schema,
)
from ingen_fab.python_libs.common.extract_resource_batch_schema import (
    get_extract_resource_batch_schema,
)
from ingen_fab.python_libs.common.extract_watermark_schema import (
    get_extract_watermark_schema,
)
from ingen_fab.python_libs.pyspark.ingestion.common.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.common.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)


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

        if auto_create_tables:
            self.ensure_log_tables_exist()

    def ensure_log_tables_exist(self) -> None:
        """Create log tables if they don't exist"""
        try:
            # Check and create log_resource_extract_batch table
            if self.lakehouse.check_if_table_exists("log_resource_extract_batch"):
                logger.debug("Table 'log_resource_extract_batch' already exists")
            else:
                logger.info("Creating table 'log_resource_extract_batch' with partitioning by (source_name, resource_name)")
                schema = get_extract_resource_batch_schema()
                empty_df = self.lakehouse.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_resource_extract_batch",
                    mode="overwrite",
                    partition_by=["source_name", "resource_name"],
                )

            # Check and create log_resource_extract table
            if self.lakehouse.check_if_table_exists("log_resource_extract"):
                logger.debug("Table 'log_resource_extract' already exists")
            else:
                logger.info("Creating table 'log_resource_extract' with partitioning by (source_name, resource_name)")
                schema = get_extract_resource_schema()
                empty_df = self.lakehouse.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_resource_extract",
                    mode="overwrite",
                    partition_by=["source_name", "resource_name"],
                )

            # Check and create log_resource_extract_watermark table
            if self.lakehouse.check_if_table_exists("log_resource_extract_watermark"):
                logger.debug("Table 'log_resource_extract_watermark' already exists")
            else:
                logger.info("Creating table 'log_resource_extract_watermark' with partitioning by (source_name, resource_name)")
                schema = get_extract_watermark_schema()
                empty_df = self.lakehouse.spark.createDataFrame([], schema)
                self.lakehouse.write_to_table(
                    df=empty_df,
                    table_name="log_resource_extract_watermark",
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
        extract_file_paths: list,
        source_path: Optional[str] = None,
        file_count: int = 0,
        file_size_bytes: int = 0,
        duration_ms: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Log extraction batch event (file or folder level).

        This is the work queue for loading - each row says "these files are ready".

        Args:
            config: Resource configuration
            execution_id: Master execution ID (orchestrator run ID)
            extract_run_id: Parent resource extraction run ID (FK to log_resource_extract)
            extraction_id: Unique batch ID (becomes extract_batch_id)
            status: 'running', 'completed', 'failed', 'duplicate'
            extract_file_paths: List of file/folder paths promoted to raw - LOADER READS THESE
            source_path: Where files came from (inbound)
            file_count: Number of files in batch
            file_size_bytes: Total size in bytes
            duration_ms: Extraction duration
            error_message: Error if failed
        """
        try:
            # Determine load_state based on extraction status
            if status == ExecutionStatus.SUCCESS:
                load_state = ExecutionStatus.PENDING  # Ready for loading
            else:
                load_state = status  # Don't load warning/error/running extractions

            # Timestamps
            now = datetime.now()
            started_at = now
            completed_at = now if status in (ExecutionStatus.SUCCESS, ExecutionStatus.WARNING, ExecutionStatus.ERROR) else None

            # Build log row
            log_data = [
                (
                    extraction_id,          # extract_batch_id - PK
                    extract_run_id,         # FK to log_resource_extract
                    execution_id,           # master_execution_id (denormalized)
                    config.source_name,     # source_name (denormalized for partitioning)
                    config.resource_name,   # resource_name (denormalized for partitioning)
                    str(status),            # extract_state - Convert enum to string for Spark
                    str(load_state),        # load_state - Convert enum to string for Spark
                    source_path,
                    extract_file_paths,     # KEY: what loader reads
                    file_count,
                    file_size_bytes,
                    started_at,
                    completed_at,
                    duration_ms,
                    error_message,
                )
            ]

            schema = get_extract_resource_batch_schema()
            log_df = self.lakehouse.spark.createDataFrame(log_data, schema)

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
            status=ExecutionStatus.RUNNING,
        )

    def log_extraction_config_end(
        self,
        config: ResourceConfig,
        execution_id: str,
        extract_run_id: str,
        status: ExecutionStatus,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Log the end of resource extraction with final status.

        Args:
            config: Resource configuration
            execution_id: Master execution ID
            extract_run_id: Run ID from log_extraction_config_start
            status: Final status (COMPLETED, FAILED, NO_DATA, DUPLICATE)
            error_message: Optional error message for failures
        """
        self._log_extraction_config_event(
            config=config,
            execution_id=execution_id,
            extract_run_id=extract_run_id,
            status=status,
            error_message=error_message,
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
            # Timestamps
            now = datetime.now()
            completed_at = completed_at if completed_at else (
                now if status in (ExecutionStatus.SUCCESS, ExecutionStatus.WARNING, ExecutionStatus.ERROR) else None
            )

            if status == ExecutionStatus.RUNNING:
                # Generate new extract_run_id for INSERT
                extract_run_id = str(uuid.uuid4())

                # INSERT new row for "running" status
                log_data = [
                    (
                        extract_run_id,         # PK
                        execution_id,           # master_execution_id
                        config.source_name,     # source_name
                        config.resource_name,   # resource_name
                        str(status),            # extract_state - Convert enum to string for Spark
                        error_message,
                        now,                    # started_at
                        now,                    # updated_at
                        None,                   # completed_at (NULL for running)
                    )
                ]

                schema = get_extract_resource_schema()
                log_df = self.lakehouse.spark.createDataFrame(log_data, schema)

                self.lakehouse.write_to_table(
                    df=log_df,
                    table_name="log_resource_extract",
                    mode="append",
                )
            else:
                # UPDATE existing row with final status (completed/failed/no_data)
                set_values = {
                    "extract_state": lit(str(status)),  # Convert enum to string for Spark
                    "updated_at": current_timestamp(),
                    "completed_at": current_timestamp(),
                }

                # Add optional error_message only if it has a value
                if error_message is not None:
                    set_values["error_message"] = lit(error_message)

                self.lakehouse.update_table(
                    table_name="log_resource_extract",
                    condition=(
                        (col("extract_run_id") == extract_run_id) &
                        (col("source_name") == config.source_name) &
                        (col("resource_name") == config.resource_name)
                    ),
                    set_values=set_values,
                )

            # Return the extract_run_id (either newly generated or passed in)
            return extract_run_id

        except Exception as e:
            logger.warning(f"Failed to log extraction config event ({str(status)}): {e}")
            # Return a generated ID even on error to prevent cascading failures
            return extract_run_id if extract_run_id else str(uuid.uuid4())

    def check_file_already_extracted(
        self,
        config: ResourceConfig,
        filename: str,
    ) -> bool:
        """
        Check if a file has already been successfully extracted (across all partitions/dates).

        Queries log_resource_extract_batch table to see if this filename
        appears in any completed extraction's extract_file_paths array.

        Args:
            config: Resource configuration
            filename: File basename to check (e.g., "pe_Item_Ref_20251112.dat")

        Returns:
            True if file was previously extracted, False otherwise
        """
        try:
            # Check if log table exists
            if not self.lakehouse.check_if_table_exists("log_resource_extract_batch"):
                logger.debug("Table 'log_resource_extract_batch' not found - cannot check duplicates")
                return False

            # Query for any completed extractions with this filename in extract_file_paths array
            # Each path in array format: abfss://.../raw/pe/pe_item_ref/ds=2025-11-13/pe_Item_Ref_20251112.dat
            # Use exists() to check if any element in array ends with the filename
            result_df = (
                self.lakehouse.read_table("log_resource_extract_batch")
                .filter(col("source_name") == config.source_name)
                .filter(col("resource_name") == config.resource_name)
                .filter(col("extract_state") == str(ExecutionStatus.SUCCESS))
                .filter(exists(col("extract_file_paths"), lambda p: p.endswith(f"/{filename}")))
                .limit(1)  # Only need to know if at least one exists
            )

            # If any rows found, file was already extracted
            return result_df.count() > 0

        except Exception as e:
            logger.warning(f"Could not check extraction logs for duplicates: {e}")
            # On error, return False (allow file through - safer than blocking)
            return False

    # ========================================================================
    # WATERMARK MANAGEMENT (log_resource_extract_watermark table)
    # ========================================================================

    def get_watermark(
        self,
        source_name: str,
        resource_name: str,
        incremental_column: Optional[str]
    ) -> Optional[Any]:
        """
        Read last watermark value for incremental extraction.

        Queries log_extract_watermark table for this resource's last watermark.
        Casts watermark_value from string to appropriate type.

        Args:
            source_name: Source identifier
            resource_name: Resource identifier
            incremental_column: Column name for watermark (if None, returns None)

        Returns:
            Last watermark value (typed) or None if:
            - No incremental_column provided
            - First run (no watermark exists)
            - Watermark table doesn't exist
        """
        # Only read watermark if incremental extraction configured
        if not incremental_column:
            return None

        try:
            # Check if watermark table exists
            if not self.lakehouse.check_if_table_exists("log_resource_extract_watermark"):
                logger.debug("Watermark table doesn't exist - treating as first run")
                return None

            # Query for this resource's watermark
            watermark_df = (
                self.lakehouse.read_table("log_resource_extract_watermark")
                .filter(col("source_name") == source_name)
                .filter(col("resource_name") == resource_name)
            )

            # Check if watermark exists
            rows = watermark_df.collect()
            if not rows:
                return None

            row = rows[0]
            watermark_value_str = row.watermark_value
            watermark_type = row.watermark_type

            logger.debug(
                f"Found watermark: {incremental_column} = {watermark_value_str} (type: {watermark_type})"
            )

            # Cast from string to appropriate type
            if watermark_type == "timestamp":
                return datetime.fromisoformat(watermark_value_str)
            elif watermark_type == "date":
                from datetime import date
                return date.fromisoformat(watermark_value_str)
            elif watermark_type == "integer":
                return int(watermark_value_str)
            elif watermark_type == "string":
                return watermark_value_str
            else:
                logger.warning(f"Unknown watermark_type: {watermark_type}, using as string")
                return watermark_value_str

        except Exception as e:
            logger.warning(f"Could not read watermark: {e}. Performing full extraction.")
            return None

    def update_watermark(
        self,
        source_name: str,
        resource_name: str,
        incremental_column: Optional[str],
        new_value: Any,
        extract_batch_id: str
    ) -> None:
        """
        Update watermark value after successful extraction.

        Uses UPDATE + INSERT pattern (not MERGE) to avoid Delta concurrency conflicts
        when multiple chunks complete rapidly.

        Args:
            source_name: Source identifier
            resource_name: Resource identifier
            incremental_column: Column name for watermark
            new_value: New watermark value (max from extracted data)
            extract_batch_id: Batch ID for audit trail
        """
        if not new_value:
            logger.warning("Watermark value is None - skipping update")
            return

        try:
            # Determine watermark type and convert to string for storage
            watermark_type = "string"
            if isinstance(new_value, datetime):
                watermark_value_str = new_value.isoformat()
                watermark_type = "timestamp"
            elif isinstance(new_value, date):
                watermark_value_str = new_value.isoformat()
                watermark_type = "date"
            elif isinstance(new_value, int):
                watermark_value_str = str(new_value)
                watermark_type = "integer"
            else:
                watermark_value_str = str(new_value)
                watermark_type = "string"

            # Check if watermark exists (for UPDATE vs INSERT decision)
            current_watermark = None
            watermark_exists = False

            if self.lakehouse.check_if_table_exists("log_resource_extract_watermark"):
                watermark_df = (
                    self.lakehouse.read_table("log_resource_extract_watermark")
                    .filter(col("source_name") == source_name)
                    .filter(col("resource_name") == resource_name)
                )
                rows = watermark_df.collect()
                if rows:
                    current_watermark = rows[0].watermark_value
                    watermark_exists = True

            if watermark_exists:
                # UPDATE existing row (avoids MERGE concurrency conflicts)
                # Include partition columns in condition for partition pruning
                self.lakehouse.update_table(
                    table_name="log_resource_extract_watermark",
                    condition=(
                        (col("source_name") == source_name) &
                        (col("resource_name") == resource_name)
                    ),
                    set_values={
                        "watermark_column": lit(incremental_column),
                        "watermark_type": lit(watermark_type),
                        "watermark_value": lit(watermark_value_str),
                        "previous_watermark_value": lit(current_watermark),
                        "updated_at": current_timestamp(),
                        "extract_batch_id": lit(extract_batch_id),
                    },
                )
            else:
                # INSERT new row (first run)
                watermark_data = [{
                    "source_name": source_name,
                    "resource_name": resource_name,
                    "watermark_column": incremental_column,
                    "watermark_type": watermark_type,
                    "watermark_value": watermark_value_str,
                    "previous_watermark_value": current_watermark,
                    "updated_at": datetime.now(),
                    "extract_batch_id": extract_batch_id,
                }]

                schema = get_extract_watermark_schema()
                watermark_df = self.lakehouse.spark.createDataFrame(watermark_data, schema)

                self.lakehouse.write_to_table(
                    df=watermark_df,
                    table_name="log_resource_extract_watermark",
                    mode="append",
                )

        except Exception as e:
            # Non-fatal - log warning but don't fail extraction
            logger.warning(f"Could not update watermark: {e}")

    # ========================================================================
    # RETRY SUPPORT
    # ========================================================================

    def get_failed_resource_keys(self, configs: List[ResourceConfig]) -> Set[Tuple[str, str]]:
        """
        Get (source_name, resource_name) tuples for configs whose last extract failed.

        Uses a single query with window function to efficiently check all configs at once.

        Args:
            configs: List of ResourceConfig to check

        Returns:
            Set of (source_name, resource_name) tuples that failed their last extraction
        """
        if not configs:
            return set()

        try:
            if not self.lakehouse.check_if_table_exists("log_resource_extract"):
                return set()

            # Build DataFrame of config keys to filter against
            config_keys = [(c.source_name, c.resource_name) for c in configs]
            keys_df = self.lakehouse.spark.createDataFrame(config_keys, ["source_name", "resource_name"])

            # Get latest status per resource using window function
            window = Window.partitionBy("source_name", "resource_name").orderBy(col("completed_at").desc())

            failed_df = (
                self.lakehouse.read_table("log_resource_extract")
                .join(keys_df, ["source_name", "resource_name"])
                .withColumn("rn", row_number().over(window))
                .filter(col("rn") == 1)
                .filter(col("extract_state") == str(ExecutionStatus.ERROR))
                .select("source_name", "resource_name")
            )

            return {(row.source_name, row.resource_name) for row in failed_df.collect()}

        except Exception as e:
            logger.warning(f"Could not query failed resource keys: {e}")
            return set()
