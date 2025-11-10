# File Loading Framework - FileLoader (Coordinator)
# Coordinates file discovery and batch reading
# Main entry point for the ingestion framework

import json
import logging
import uuid
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number, lit, current_timestamp
from pyspark.sql.window import Window

from ingen_fab.python_libs.common.load_resource_batch_schema import get_load_resource_batch_schema
from ingen_fab.python_libs.pyspark.ingestion.batch_reader import BatchReader
from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.file_discovery import FileDiscovery
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchInfo,
    ProcessingMetrics,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import FileInfo, lakehouse_utils

# Configure logging
logger = logging.getLogger(__name__)


class FileLoader:
    """
    Coordinates file discovery and batch reading.

    This is the main entry point for file loading. It composes:
    - FileDiscovery: Finds files/folders and creates BatchInfo objects
    - BatchReader: Reads batches into DataFrames

    This class maintains backward compatibility while delegating
    all work to specialized components.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ResourceConfig,
        lakehouse_utils_instance: Optional[lakehouse_utils] = None,
    ):
        """
        Initialize FileLoader with configuration.

        Args:
            spark: Spark session for reading files and querying log tables
            config: ResourceConfig with file loading settings
            lakehouse_utils_instance: Optional pre-configured lakehouse_utils instance
                                     (if not provided, will create from config)
        """
        self.spark = spark
        self.config = config

        # Store lakehouse_utils for file movements
        if lakehouse_utils_instance:
            self.lakehouse = lakehouse_utils_instance
        else:
            # Create lakehouse_utils from config
            source_workspace = config.source_config.connection_params.get("workspace_name")
            source_lakehouse = config.source_config.connection_params.get("lakehouse_name")

            if not source_workspace or not source_lakehouse:
                raise ValueError(
                    "source_config.connection_params must have workspace_name and lakehouse_name"
                )

            self.lakehouse = lakehouse_utils(
                target_workspace_name=source_workspace,
                target_lakehouse_name=source_lakehouse,
                spark=spark,
            )

        # Initialize components
        self.discovery = FileDiscovery(spark, config, self.lakehouse)
        self.reader = BatchReader(spark, config, self.lakehouse)

        # Configure logging if not already configured
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(levelname)s - %(message)s',
                force=True
            )

    @property
    def last_duplicate_items(self) -> List[FileInfo]:
        """
        Get list of duplicate items from last discovery operation.

        Forwards to FileDiscovery component.
        """
        return self.discovery.last_duplicate_items

    def _recover_stale_batches(self) -> None:
        """
        Find processing batches older than threshold and reset them to pending.

        This is crash recovery - finds batches stuck in "processing" status
        from a previous crashed run and marks them for retry.
        """
        try:
            # Check if log table exists
            self.spark.sql("DESCRIBE TABLE log_resource_load_batch")
        except Exception:
            # Log table doesn't exist yet - nothing to recover
            logger.info("Log table 'log_resource_load_batch' not found, skipping stale batch recovery")
            return

        try:
            # Find stale batches (processing for more than 1 hour)
            threshold_time = datetime.now() - timedelta(hours=1)

            # Get latest status for each batch (using load_batch_id from schema)
            window_spec = Window.partitionBy("load_batch_id").orderBy(col("started_at").desc())

            stale_batches_df = (
                self.lakehouse.read_table("log_resource_load_batch")
                .filter(col("source_name") == self.config.source_name)
                .filter(col("resource_name") == self.config.resource_name)
                .withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .filter(col("status") == "processing")
                .filter(col("started_at") < threshold_time)
            )

            stale_batches = stale_batches_df.collect()

            if stale_batches:
                logger.warning(f"Found {len(stale_batches)} stale batch(es), resetting to pending")

                for row in stale_batches:
                    now = datetime.now()
                    # INSERT new row with status='pending' for recovery with all 30 schema fields
                    recovery_data = [(
                        row.load_batch_id if hasattr(row, 'load_batch_id') else row.load_id,  # 1. load_batch_id
                        "",                                       # 2. load_run_id (empty string for non-nullable field)
                        "",                                       # 3. master_execution_id (empty string for non-nullable field)
                        row.extract_batch_id if hasattr(row, 'extract_batch_id') else None,  # 4. extract_batch_id
                        self.config.source_name,                  # 5. source_name
                        self.config.resource_name,                # 6. resource_name
                        "pending",                                # 7. status (recovered)
                        row.source_file_path,                     # 8. source_file_path
                        row.source_file_size_bytes if hasattr(row, 'source_file_size_bytes') else None,  # 9. source_file_size_bytes
                        row.source_file_modified_time if hasattr(row, 'source_file_modified_time') else None,  # 10. source_file_modified_time
                        (row.target_table_name if hasattr(row, 'target_table_name') else self.config.target_table) or "",  # 11. target_table_name
                        None,                                     # 12. records_processed
                        None,                                     # 13. records_inserted
                        None,                                     # 14. records_updated
                        None,                                     # 15. records_deleted
                        None,                                     # 16. source_row_count
                        None,                                     # 17. target_row_count_before
                        None,                                     # 18. target_row_count_after
                        None,                                     # 19. row_count_reconciliation_status
                        0,                                        # 20. corrupt_records_count
                        None,                                     # 21. data_read_duration_ms
                        None,                                     # 22. total_duration_ms
                        "Recovered from stale processing state",  # 23. error_message
                        None,                                     # 24. execution_duration_seconds
                        None,                                     # 25. filename_attributes_json
                        now,                                      # 27. started_at
                        now,                                      # 28. updated_at
                        None,                                     # 29. completed_at
                        (row.attempt_count + 1) if hasattr(row, 'attempt_count') else 1,  # 30. attempt_count (increment)
                    )]

                    recovery_df = self.spark.createDataFrame(recovery_data, get_load_resource_batch_schema())

                    self.lakehouse.write_to_table(
                        df=recovery_df,
                        table_name="log_resource_load_batch",
                        mode="append",
                    )
                    logger.info(f"Reset batch {row.load_batch_id if hasattr(row, 'load_batch_id') else row.load_id} to pending")
            else:
                logger.info("No stale batches found")

        except Exception as e:
            logger.warning(f"Error during stale batch recovery: {e}")

    def _start_batch(self, batch_info: BatchInfo, execution_id: str, load_run_id: str) -> None:
        """
        Mark batch as processing in log table (INSERT new row).

        Args:
            batch_info: Batch to start processing
            execution_id: Master execution ID from orchestrator
            load_run_id: Load run ID from orchestrator (FK to log_resource_load)
        """
        now = datetime.now()

        # Get source file path from batch
        source_file_path = batch_info.file_paths[0] if batch_info.file_paths else "unknown"

        # INSERT to log_resource_load_batch with all 30 schema fields
        log_data = [(
            batch_info.batch_id,                      # 1. load_batch_id
            load_run_id,                              # 2. load_run_id (FK to log_resource_load)
            execution_id,                             # 3. master_execution_id
            batch_info.extract_batch_id,              # 4. extract_batch_id
            self.config.source_name,                  # 5. source_name
            self.config.resource_name,                # 6. resource_name
            "processing",                             # 7. status
            source_file_path,                         # 8. source_file_path
            batch_info.size_bytes,                    # 9. source_file_size_bytes
            batch_info.modified_time,                 # 10. source_file_modified_time
            self.config.target_table or "",           # 11. target_table_name
            None,                                     # 12. records_processed (running)
            None,                                     # 13. records_inserted
            None,                                     # 14. records_updated
            None,                                     # 15. records_deleted
            None,                                     # 16. source_row_count
            None,                                     # 17. target_row_count_before
            None,                                     # 18. target_row_count_after
            None,                                     # 19. row_count_reconciliation_status
            0,                                        # 20. corrupt_records_count
            None,                                     # 21. data_read_duration_ms
            None,                                     # 22. total_duration_ms
            None,                                     # 23. error_message
            None,                                     # 24. execution_duration_seconds
            None,                                     # 25. filename_attributes_json
            now,                                      # 26. started_at
            now,                                      # 27. updated_at
            None,                                     # 28. completed_at
            1,                                        # 29. attempt_count
        )]

        log_df = self.spark.createDataFrame(log_data, get_load_resource_batch_schema())

        self.lakehouse.write_to_table(
            df=log_df,
            table_name="log_resource_load_batch",
            mode="append",
        )
        logger.debug(f"Started batch {batch_info.batch_id}")

    def _complete_batch(self, batch_info: BatchInfo, metrics: ProcessingMetrics) -> None:
        """
        Mark batch as completed in logs (UPDATE existing row).

        Files STAY in raw_landing_path (no archiving) to enable easy replay.
        To replay: UPDATE log_extract_batch SET load_state = 'pending' WHERE extraction_id = '...'

        Args:
            batch_info: Batch that completed successfully
            metrics: Processing metrics from the batch
        """
        # Calculate execution duration
        execution_duration_seconds = None
        if metrics.total_duration_ms:
            execution_duration_seconds = int(metrics.total_duration_ms / 1000)

        # UPDATE existing row with completion metrics
        # Use partition keys (source_name, resource_name) for partition isolation
        set_values = {
            "status": lit("completed"),
            "records_processed": lit(metrics.records_processed or 0),
            "records_inserted": lit(metrics.records_inserted or 0),
            "records_updated": lit(metrics.records_updated or 0),
            "records_deleted": lit(metrics.records_deleted or 0),
            "source_row_count": lit(metrics.source_row_count or 0),
            "target_row_count_before": lit(metrics.target_row_count_before or 0),
            "target_row_count_after": lit(metrics.target_row_count_after or 0),
            "updated_at": current_timestamp(),
            "completed_at": current_timestamp(),
        }

        # Add optional fields only if they have values
        if metrics.row_count_reconciliation_status:
            set_values["row_count_reconciliation_status"] = lit(metrics.row_count_reconciliation_status)
        if metrics.read_duration_ms:
            set_values["data_read_duration_ms"] = lit(metrics.read_duration_ms)
        if metrics.total_duration_ms:
            set_values["total_duration_ms"] = lit(metrics.total_duration_ms)
        if execution_duration_seconds:
            set_values["execution_duration_seconds"] = lit(execution_duration_seconds)

        self.lakehouse.update_table(
            table_name="log_resource_load_batch",
            condition=(
                (col("load_batch_id") == batch_info.batch_id) &
                (col("source_name") == self.config.source_name) &
                (col("resource_name") == self.config.resource_name)
            ),
            set_values=set_values,
        )

        logger.debug(f"Completed batch {batch_info.batch_id} - files remain in {self.config.raw_landing_path}")

    def _fail_batch(self, batch_info: BatchInfo, error: Exception) -> None:
        """
        Mark batch as failed and move files to quarantine (UPDATE existing row).

        Args:
            batch_info: Batch that failed
            error: Exception that caused the failure
        """
        error_message = str(error)

        # UPDATE existing row with failure status
        # Use partition keys (source_name, resource_name) for partition isolation
        self.lakehouse.update_table(
            table_name="log_resource_load_batch",
            condition=(
                (col("load_batch_id") == batch_info.batch_id) &
                (col("source_name") == self.config.source_name) &
                (col("resource_name") == self.config.resource_name)
            ),
            set_values={
                "status": lit("failed"),
                "error_message": lit(error_message),
                "updated_at": current_timestamp(),
                "completed_at": current_timestamp(),
            },
        )

        # Move files from landing to quarantine
        landing_base = self.config.raw_landing_path.rstrip('/')
        first_file_relative_path = None

        for file_path in batch_info.file_paths:
            # Extract relative path from landing
            if file_path.startswith(landing_base):
                relative_path = file_path[len(landing_base):].lstrip('/')
            else:
                # File path might be full URI, extract after landing folder name
                landing_folder = landing_base.split('/')[-1]
                if landing_folder in file_path:
                    relative_path = file_path.split(landing_folder + '/', 1)[1]
                else:
                    relative_path = file_path.split('/')[-1]

            # Track first file's relative path for cleanup
            if first_file_relative_path is None:
                first_file_relative_path = f"{landing_base}/{relative_path}"

            # Build quarantine path
            quarantine_path = f"{self.config.raw_quarantined_path.rstrip('/')}/{relative_path}"

            # Move file
            success = self.lakehouse.move_file(file_path, quarantine_path)
            if success:
                logger.info(f"Moved file to quarantine: {relative_path}")

                # Save error metadata as JSON
                error_metadata = {
                    "batch_id": batch_info.batch_id,
                    "error_message": error_message,
                    "timestamp": datetime.now().isoformat(),
                    "config_id": self.config.resource_name,
                }

                metadata_path = f"{quarantine_path}.error.json"
                metadata_json = json.dumps(error_metadata, indent=2)

                # Write error metadata (would need to implement this via spark)
                # For now, just log it
                logger.error(f"Error metadata: {metadata_json}")
            else:
                logger.warning(f"Failed to move file to quarantine: {file_path}")

        # Clean up empty directories in landing ONCE after all files moved
        if first_file_relative_path:
            self.lakehouse.cleanup_empty_directories_recursive(
                first_file_relative_path,
                self.config.raw_landing_path
            )

        logger.error(f"Failed batch {batch_info.batch_id}: {error_message}")

    def discover_and_read_files(self) -> List[Tuple[BatchInfo, DataFrame, ProcessingMetrics]]:
        """
        Main entry point: discover files from raw layer and read them into DataFrames.

        NOTE: This is a backward-compatibility method. Prefer using LoadingOrchestrator.
        Since this bypasses the orchestrator, it generates its own execution_id and load_run_id.

        Returns:
            List of (BatchInfo, DataFrame, ProcessingMetrics) tuples
        """
        # Generate IDs for this direct call (bypassing orchestrator)
        execution_id = str(uuid.uuid4())
        load_run_id = str(uuid.uuid4())
        logger.warning("discover_and_read_files() called directly - prefer using LoadingOrchestrator")

        # Step 0: Recover any stale batches from previous crashed runs
        self._recover_stale_batches()

        # Step 1: Discover files from raw layer
        discovered_batches = self.discovery.discover()

        if not discovered_batches:
            return []

        # Step 2: Read each discovered batch with lifecycle management
        results = []
        for batch_info in discovered_batches:
            try:
                # Mark batch as processing
                self._start_batch(batch_info, execution_id, load_run_id)

                # Read the batch
                df, metrics = self.reader.read_batch(batch_info)

                # Add to results (completion happens in orchestrator after bronze write)
                results.append((batch_info, df, metrics))

            except Exception as e:
                # Mark batch as failed and move to quarantine
                self._fail_batch(batch_info, e)
                logger.error(f"Batch {batch_info.batch_id} failed: {e}")
                # Continue processing other batches

        return results

    def discover_files(self) -> List[BatchInfo]:
        """
        Discover files based on configuration.

        Delegates to FileDiscovery component.

        Returns:
            List of BatchInfo objects representing discovered batches
        """
        return self.discovery.discover()
