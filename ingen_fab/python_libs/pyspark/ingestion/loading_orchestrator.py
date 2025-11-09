# File Loading Framework - Loading Orchestrator
# Orchestrates file loading from raw layer to bronze tables

import logging
import os
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from ingen_fab.python_libs.common.flat_file_ingestion_utils import (
    ErrorHandlingUtils,
    ProcessingMetricsUtils,
)
from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.constants import (
    ExecutionStatus,
    WriteMode,
)
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    ErrorContext,
    FileReadError,
    WriteError,
)
from ingen_fab.python_libs.pyspark.ingestion.loader import FileLoader
from ingen_fab.python_libs.pyspark.ingestion.logging import FileLoadingLogger
from ingen_fab.python_libs.pyspark.ingestion.logging_utils import ConfigLoggerAdapter
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchInfo,
    ProcessingMetrics,
    ResourceExecutionResult,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

# Import warehouse utils for warehouse targets
try:
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
except ImportError:
    warehouse_utils = None  # type: ignore

logger = logging.getLogger(__name__)


class LoadingOrchestrator:
    """
    Orchestrates data loading from raw layer to bronze tables.

    Pattern (dlt-inspired): Raw layer → Bronze tables
    Discovery: Queries extraction logs (log_resource_extract_batch table) for ready batches

    Handles:
    - Discovery from extraction logs (work queue pattern)
    - Loading step (raw → bronze)
    - Execution grouping and sequencing
    - Parallel processing within groups
    - Target table writes (lakehouse or warehouse)
    - State tracking and logging
    - Error handling
    - Metrics aggregation

    File Management (Quarantine-Only Pattern):
    - Successful loads: Files STAY in raw_landing_path (no archiving!)
    - Failed loads: Files move to raw_quarantined_path (corrupt/invalid files only)
    - This enables easy replay by resetting load_state to 'pending'

    Replay Workflow:
        # 1. Find extraction to replay
        SELECT * FROM log_resource_extract_batch
        WHERE source_name = 'my_source'
          AND resource_name = 'my_table'
          AND load_state IN ('completed', 'failed');

        # 2. Reset to pending (will be picked up on next load run)
        UPDATE log_resource_extract_batch
        SET load_state = 'pending'
        WHERE extract_batch_id = '{extract_batch_id}'
          AND source_name = '{source_name}'
          AND resource_name = '{resource_name}';
    """

    def __init__(
        self,
        spark: SparkSession,
        logger_instance: Optional[FileLoadingLogger] = None,
        max_concurrency: int = 4,
    ):
        """
        Initialize orchestrator.

        Args:
            spark: Spark session
            logger_instance: Optional FileLoadingLogger instance for state tracking
                           (if not provided, logging methods will be no-ops)
            max_concurrency: Maximum number of parallel workers per group
        """
        self.spark = spark
        self.logger_instance = logger_instance
        self.max_concurrency = max_concurrency

        # Configure logging if not already configured
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(levelname)s - %(message)s',
                force=True
            )

    def process_resources(
        self,
        configs: List[ResourceConfig],
        execution_id: str,
    ) -> Dict[str, Any]:
        """
        Process multiple resource configurations.

        Args:
            configs: List of ResourceConfig objects to process
            execution_id: Unique execution identifier for this run

        Returns:
            Dictionary with execution summary
        """
        results = {
            "execution_id": execution_id,
            "total_resources": len(configs),
            "successful": 0,
            "failed": 0,
            "no_data": 0,
            "resources": [],
            "execution_groups_processed": [],
        }

        if not configs:
            logger.warning("No resources to process")
            return results

        # Group by execution_group
        execution_groups = defaultdict(list)
        for config in configs:
            execution_groups[config.execution_group].append(config)

        # Sort by execution_group number
        group_numbers = sorted(execution_groups.keys())

        logger.info(
            f"Processing {len(configs)} resource(s) across {len(group_numbers)} execution group(s)"
        )

        # Process each group sequentially
        for group_num in group_numbers:
            group_configs = execution_groups[group_num]

            logger.info(f"Execution group {group_num}: {len(group_configs)} resource(s)")

            # Process group in parallel
            group_results = self._process_group_parallel(group_configs, execution_id, group_num)

            # Aggregate results
            results["resources"].extend(group_results)
            results["execution_groups_processed"].append(group_num)

            # Count statuses
            for result in group_results:
                if result.status == ExecutionStatus.COMPLETED:
                    results["successful"] += 1
                elif result.status == ExecutionStatus.FAILED:
                    results["failed"] += 1
                else:
                    results["no_data"] += 1

        logger.info(
            f"Execution complete: {results['successful']} successful, "
            f"{results['failed']} failed, {results['no_data']} no data"
        )

        return results

    def _process_group_parallel(
        self,
        configs: List[ResourceConfig],
        execution_id: str,
        group_num: int,
    ) -> List[ResourceExecutionResult]:
        """Process a group of resources in parallel"""
        results = []
        start_time = time.time()

        if len(configs) == 1:
            # Single resource - no parallelization needed
            result = self.process_single_resource(configs[0], execution_id)
            results = [result]
        else:
            # Multiple resources - process in parallel
            max_workers = min(len(configs), self.max_concurrency)
            logger.info(f"Using {max_workers} parallel workers")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all configs
                future_to_config = {
                    executor.submit(self.process_single_resource, config, execution_id): config
                    for config in configs
                }

                # Collect results
                for future in as_completed(future_to_config):
                    config = future_to_config[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        error_result = ResourceExecutionResult(
                            resource_name=config.resource_name,
                            status=ExecutionStatus.FAILED,
                            batches_processed=0,
                            batches_failed=0,
                            error_message=f"Thread execution error: {str(e)}",
                        )
                        results.append(error_result)
                        logger.error(f"Failed: {config.resource_name} - {str(e)}")

        # Log summary
        end_time = time.time()
        duration = end_time - start_time

        completed = sum(1 for r in results if r.status == ExecutionStatus.COMPLETED)
        failed = sum(1 for r in results if r.status == ExecutionStatus.FAILED)
        no_data = sum(1 for r in results if r.status == ExecutionStatus.NO_DATA)

        logger.info(f"Execution group {group_num} completed in {duration:.1f}s")
        logger.info(
            f"{len(results)} resource(s) - {completed} completed, {failed} failed, {no_data} no data"
        )

        # Log individual results
        for result in results:
            icon = "✓" if result.status == ExecutionStatus.COMPLETED else "✗" if result.status == ExecutionStatus.FAILED else "○"
            logger.info(
                f"{icon} {result.resource_name} ({result.status}) - "
                f"{result.batches_processed} batch(es)"
            )
            if result.error_message:
                logger.error(f"  Error: {result.error_message}")

        return results

    def process_single_resource(
        self,
        config: ResourceConfig,
        execution_id: str,
    ) -> ResourceExecutionResult:
        """
        Process a single resource configuration.

        Args:
            config: ResourceConfig to process
            execution_id: Unique execution identifier

        Returns:
            ResourceExecutionResult with processing outcome
        """
        # Create logger adapter with resource context
        config_logger = ConfigLoggerAdapter(logger, {
            'source_name': config.source_name,
            'config_name': config.resource_name,
        })

        start_time = time.time()
        result = ResourceExecutionResult(
            resource_name=config.resource_name,
            status=ExecutionStatus.PENDING,
            batches_processed=0,
            batches_failed=0,
        )

        # Get target utilities for writing
        target_utils = self._get_target_utils(config)

        # Log config execution start and capture load_run_id
        load_run_id = self._log_config_execution_start(config, execution_id)

        try:
            config_logger.info(f"Loading: {config.resource_name}")
            # Consolidate metadata into single line
            target_full = f"{config.target_schema}.{config.target_table}" if config.target_schema else config.target_table
            config_logger.info(
                f"Raw: {config.raw_landing_path} → Target: {target_full} "
                f"({config.file_format}, {config.write_mode})"
            )

            # Create FileLoader instance
            file_loader = FileLoader(spark=self.spark, config=config)

            # Discover batches from extraction logs (dlt-style work queue)
            discovered_batches = self._discover_from_extraction_logs(config)

            if not discovered_batches:
                config_logger.info("No files to process")
                result.status = ExecutionStatus.NO_DATA
                self._log_config_execution_completion(config, execution_id, load_run_id, result)
                return result

            # Process batches one at a time
            total_batches = len(discovered_batches)
            all_metrics, batches_processed, batches_failed = self._process_batches(
                discovered_batches, config, execution_id, load_run_id, target_utils, file_loader, config_logger, total_batches
            )

            # Update result with batch counts
            result.batches_processed = batches_processed
            result.batches_failed = batches_failed

            # Finalize result with aggregated metrics and status
            self._finalize_result(result, all_metrics, config, start_time, config_logger)

            # Log config execution completion
            self._log_config_execution_completion(config, execution_id, load_run_id, result)

        except (FileReadError, WriteError) as e:
            # Known loading errors - already have context
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            config_logger.exception(f"Loading failed: {e}")
            self._log_config_execution_error(config, execution_id, load_run_id, str(e), result)

        except Exception as e:
            # Unexpected error
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            config_logger.exception(f"Unexpected error in loading: {e}")
            self._log_config_execution_error(config, execution_id, load_run_id, str(e), result)

        return result

    def _process_batches(
        self,
        batches: List[BatchInfo],
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        target_utils,
        file_loader: FileLoader,
        config_logger: logging.LoggerAdapter,
        total_batches: int,
    ) -> Tuple[List[ProcessingMetrics], int, int]:
        """
        Process all batches one at a time (streaming) and return aggregated metrics.

        Args:
            batches: List of BatchInfo objects to process
            config: ResourceConfig for this resource
            execution_id: Unique execution identifier
            load_run_id: Load run ID for this resource execution
            target_utils: Lakehouse or warehouse utilities
            file_loader: FileLoader instance for reading files
            config_logger: Configured logger adapter

        Returns:
            Tuple of (all_metrics, batches_processed, batches_failed)
        """
        all_metrics = []
        batches_processed = 0
        batches_failed = 0

        for batch_index, batch_info in enumerate(batches, start=1):
            try:
                if batch_info.extract_batch_id:
                    self._update_load_state(
                        batch_info.extract_batch_id,
                        config.source_name,
                        config.resource_name,
                        'loading'
                    )

                file_loader._start_batch(batch_info, execution_id, load_run_id)

                df, read_metrics = file_loader.reader.read_batch(batch_info)

                combined_metrics = self._process_single_batch(
                    batch_info, df, read_metrics, config, execution_id, load_run_id,
                    target_utils, file_loader, config_logger, batch_index, total_batches
                )

                # Complete batch lifecycle (mark as completed in logs, files stay in place)
                file_loader._complete_batch(batch_info, combined_metrics)

                # Update extraction log: mark as completed
                if batch_info.extract_batch_id:
                    self._update_load_state(
                        batch_info.extract_batch_id,
                        config.source_name,
                        config.resource_name,
                        'completed'
                    )

                # Track successful batch
                if combined_metrics.records_processed > 0:
                    all_metrics.append(combined_metrics)
                    batches_processed += 1

            except (WriteError, Exception) as error:
                # Update extraction log: mark as failed
                if batch_info.extract_batch_id:
                    self._update_load_state(
                        batch_info.extract_batch_id,
                        config.source_name,
                        config.resource_name,
                        'failed'
                    )

                # Handle batch error - mark as failed and move to quarantine
                batches_failed += 1
                file_loader._fail_batch(batch_info, error)
                self._handle_batch_error(error, batch_info, config, execution_id, load_run_id, config_logger)

                # Stop processing if any batch fails
                raise

        return all_metrics, batches_processed, batches_failed

    def _process_single_batch(
        self,
        batch_info: BatchInfo,
        df: DataFrame,
        read_metrics: ProcessingMetrics,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        target_utils,
        file_loader: FileLoader,
        config_logger: logging.LoggerAdapter,
        batch_index: int,
        total_batches: int,
    ) -> ProcessingMetrics:
        """
        Process a single batch - cleaner and more testable.

        Args:
            batch_info: Information about this batch
            df: DataFrame to process
            read_metrics: Metrics from file reading
            config: Resource configuration
            execution_id: Unique execution identifier
            load_run_id: Load run ID for this resource execution
            target_utils: Lakehouse or warehouse utilities
            file_loader: FileLoader instance
            config_logger: Configured logger adapter

        Returns:
            Combined ProcessingMetrics for this batch
        """
        batch_start = time.time()
        load_id = batch_info.batch_id

        # Log batch start (delegated to FileLoader now)
        # self._log_batch_start(config, execution_id, load_run_id, load_id, batch_info)

        # Add ingestion metadata
        df = self._add_ingestion_metadata(df, load_id, batch_info)

        # Validate data
        if df.count() == 0:
            config_logger.warning(f"Loading batch {batch_index}/{total_batches}: 0 records - EMPTY (skipped)")
            # Log empty batch so it's not reprocessed on next run
            self._log_batch_no_data(config, execution_id, load_run_id, load_id, batch_info)
            return ProcessingMetrics()

        # Write to target
        write_metrics = self._write_to_target(df, config, target_utils, config_logger)

        # Combine metrics
        combined_metrics = self._combine_metrics(read_metrics, write_metrics)

        # Log batch completion (delegated to FileLoader now)
        # self._log_batch_completion(config, execution_id, load_run_id, load_id, combined_metrics, batch_info)

        batch_duration = time.time() - batch_start
        # Format numbers with commas
        records_formatted = f"{combined_metrics.records_processed:,}"
        config_logger.info(
            f"Loading batch {batch_index}/{total_batches}: {records_formatted} records ({batch_duration:.2f}s)"
        )

        return combined_metrics

    def _combine_metrics(
        self,
        read_metrics: ProcessingMetrics,
        write_metrics: ProcessingMetrics,
    ) -> ProcessingMetrics:
        """
        Combine read and write metrics into a single ProcessingMetrics object.

        Args:
            read_metrics: Metrics from file reading
            write_metrics: Metrics from writing to target

        Returns:
            Combined ProcessingMetrics with completion timestamp
        """
        return ProcessingMetrics(
            read_duration_ms=read_metrics.read_duration_ms,
            write_duration_ms=write_metrics.write_duration_ms,
            records_processed=read_metrics.records_processed,
            records_inserted=write_metrics.records_inserted,
            records_updated=write_metrics.records_updated,
            records_deleted=write_metrics.records_deleted,
            source_row_count=read_metrics.source_row_count,
            target_row_count_before=write_metrics.target_row_count_before,
            target_row_count_after=write_metrics.target_row_count_after,
            completed_at=datetime.now(),
        )

    def _handle_batch_error(
        self,
        error: Exception,
        batch_info: BatchInfo,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        config_logger: logging.LoggerAdapter,
    ) -> None:
        """
        Handle batch processing error with appropriate logging.

        Args:
            error: The exception that occurred
            batch_info: Information about the failed batch
            config: Resource configuration
            execution_id: Unique execution identifier
            load_run_id: Load run ID for this resource execution
            config_logger: Configured logger adapter
        """
        load_id = batch_info.batch_id
        error_msg = str(error)

        # Log with appropriate level based on error type
        if isinstance(error, WriteError):
            config_logger.exception(f"Batch {load_id} write failed: {error_msg}")
        else:
            config_logger.exception(f"Batch {load_id} failed: {error_msg}")

        # Log batch error to state tracking (delegated to FileLoader._fail_batch now)
        # self._log_batch_error(config, execution_id, load_run_id, load_id, error_msg, batch_info)

    def _finalize_result(
        self,
        result: ResourceExecutionResult,
        all_metrics: List[ProcessingMetrics],
        config: ResourceConfig,
        start_time: float,
        config_logger: logging.LoggerAdapter,
    ) -> None:
        """
        Aggregate metrics and update result status.

        Args:
            result: ResourceExecutionResult to update (modified in-place)
            all_metrics: List of ProcessingMetrics from batch processing
            config: ResourceConfig for this resource
            start_time: Start time of processing (from time.time())
            config_logger: Configured logger adapter
        """
        if all_metrics:
            result.metrics = ProcessingMetricsUtils.merge_metrics(
                all_metrics, config.write_mode
            )

            end_time = time.time()
            result.metrics.total_duration_ms = int((end_time - start_time) * 1000)

            result.status = ExecutionStatus.COMPLETED if result.batches_failed == 0 else ExecutionStatus.FAILED

            config_logger.info(f"Completed: {config.resource_name}")
            config_logger.info(f"Batches processed: {result.batches_processed}")
            config_logger.info(f"Records processed: {result.metrics.records_processed}")
            config_logger.info(f"Records inserted: {result.metrics.records_inserted}")
            config_logger.info(f"Records updated: {result.metrics.records_updated}")
            config_logger.info(f"Duration: {result.metrics.total_duration_ms / 1000:.2f}s")
        else:
            result.status = ExecutionStatus.NO_DATA

    def _get_target_utils(self, config: ResourceConfig):
        """Get target utilities (lakehouse or warehouse)"""
        # Infer target type from target_lakehouse field
        # (could also add explicit target_type field if needed)
        return lakehouse_utils(
            target_workspace_name=config.target_workspace,
            target_lakehouse_name=config.target_lakehouse,
            spark=self.spark,
        )

    def _discover_from_extraction_logs(self, config: ResourceConfig) -> List[BatchInfo]:
        """
        Discover batches ready for loading from extraction logs (dlt-style work queue).

        Queries log_resource_extract_batch table for completed extractions not yet loaded.
        This replaces file discovery - extraction logs tell us what's ready.

        Args:
            config: Resource configuration

        Returns:
            List of BatchInfo objects ready to load
        """
        try:
            # Check if log_resource_extract_batch table exists
            try:
                self.spark.sql("DESCRIBE TABLE log_resource_extract_batch")
            except Exception:
                logger.warning("Table 'log_resource_extract_batch' not found, no batches to load")
                return []

            # Query for completed extractions ready for loading (using load_state)
            # This is the work queue pattern (like dlt's load_storage folders)
            query = f"""
                SELECT *
                FROM log_resource_extract_batch
                WHERE source_name = '{config.source_name}'
                  AND resource_name = '{config.resource_name}'
                  AND status = 'completed'
                  AND load_state = 'pending'
                ORDER BY completed_at ASC
            """

            extraction_df = self.spark.sql(query)
            extraction_rows = extraction_df.collect()

            if not extraction_rows:
                return []

            # Get lakehouse utils for building full paths
            source_workspace = config.source_config.connection_params.get("workspace_name")
            source_lakehouse = config.source_config.connection_params.get("lakehouse_name")

            lakehouse = lakehouse_utils(
                target_workspace_name=source_workspace,
                target_lakehouse_name=source_lakehouse,
                spark=self.spark,
            )

            # Convert extraction log rows to BatchInfo objects
            batches = []
            for row in extraction_rows:
                # destination_path is always a full ABFSS URL from the extractor
                destination_path = row.destination_path

                # Validate it's a full path (fail fast if extractor has a bug)
                if not destination_path.startswith("abfss://"):
                    raise ValueError(
                        f"Invalid destination_path in batch table: {destination_path}. "
                        f"Expected full ABFSS URL (e.g., abfss://workspace@onelake.../Files/raw/...)"
                    )

                # Create BatchInfo
                batch_info = BatchInfo(
                    batch_id=str(uuid.uuid4()),
                    extract_batch_id=row.extract_batch_id,  # FK to log_resource_extract_batch
                    file_paths=[destination_path],
                    size_bytes=row.file_size_bytes or 0,
                    modified_time=row.completed_at,
                )

                # Add folder name if this is a folder path
                if destination_path.endswith("/"):
                    folder_name = os.path.basename(destination_path.rstrip("/"))
                    batch_info.folder_name = folder_name

                batches.append(batch_info)

            logger.info(f"Discovered {len(batches)} batch(es) from extraction logs")
            return batches

        except Exception as e:
            logger.warning(f"Could not query extraction logs: {e}, returning empty list")
            return []

    def _update_load_state(
        self,
        extract_batch_id: str,
        source_name: str,
        resource_name: str,
        new_state: str
    ) -> None:
        """
        Update load_state for an extraction batch.

        IMPORTANT: Includes source_name and resource_name in WHERE clause for partition isolation.
        This prevents concurrent update conflicts when processing multiple resources in parallel.

        Args:
            extract_batch_id: The extract_batch_id to update
            source_name: Source name (partition key)
            resource_name: Resource name (partition key)
            new_state: New load_state ('loading', 'completed', 'failed')
        """
        try:
            update_query = f"""
                UPDATE log_resource_extract_batch
                SET load_state = '{new_state}'
                WHERE extract_batch_id = '{extract_batch_id}'
                  AND source_name = '{source_name}'
                  AND resource_name = '{resource_name}'
            """
            self.spark.sql(update_query)
            logger.debug(f"Updated extraction {extract_batch_id} to load_state='{new_state}'")
        except Exception as e:
            logger.warning(f"Could not update load_state for {extract_batch_id}: {e}")

    def _write_to_target(
        self,
        df: DataFrame,
        config: ResourceConfig,
        target_utils,
        config_logger: logging.LoggerAdapter,
    ) -> ProcessingMetrics:
        """Write DataFrame to target table"""
        write_start = time.time()
        metrics = ProcessingMetrics()

        try:
            # Handle merge mode
            if config.write_mode.lower() == WriteMode.MERGE:
                config_logger.info(f"Executing merge with keys: {config.merge_keys}")

                merge_result = target_utils.merge_to_table(
                    df=df,
                    table_name=config.target_table,
                    merge_keys=config.merge_keys,
                    schema_name=config.target_schema,
                    immutable_columns=["_raw_created_at"],
                    enable_schema_evolution=config.enable_schema_evolution,
                    partition_by=config.partition_columns,
                    soft_delete_enabled=config.soft_delete_enabled,
                    cdc_config=config.cdc_config,
                )

                metrics.records_inserted = merge_result["records_inserted"]
                metrics.records_updated = merge_result["records_updated"]
                metrics.records_deleted = merge_result["records_deleted"]
                metrics.target_row_count_before = merge_result["target_row_count_before"]
                metrics.target_row_count_after = merge_result["target_row_count_after"]

            else:
                # Standard write modes (overwrite, append)
                # Get before count
                try:
                    existing_df = target_utils.read_table(config.target_table, schema_name=config.target_schema)
                    metrics.target_row_count_before = existing_df.count()
                    config_logger.debug(f"Target table row count before write: {metrics.target_row_count_before}")
                except Exception as e:
                    config_logger.debug(f"Table does not exist yet (before count): {e}")
                    metrics.target_row_count_before = 0

                # Prepare write options
                write_options = {}
                if config.enable_schema_evolution:
                    write_options["mergeSchema"] = "true"

                # Write data
                target_utils.write_to_table(
                    df=df,
                    table_name=config.target_table,
                    schema_name=config.target_schema,
                    mode=config.write_mode,
                    options=write_options,
                    partition_by=config.partition_columns,
                )

                # Get after count
                try:
                    result_df = target_utils.read_table(config.target_table, schema_name=config.target_schema)
                    metrics.target_row_count_after = result_df.count()
                    config_logger.debug(f"Target table row count after write: {metrics.target_row_count_after}")
                except Exception as e:
                    config_logger.debug(f"Could not read table after write: {e}")
                    metrics.target_row_count_after = 0

                # Calculate inserted/deleted
                if config.write_mode == WriteMode.OVERWRITE:
                    metrics.records_inserted = metrics.target_row_count_after
                    metrics.records_deleted = metrics.target_row_count_before
                elif config.write_mode == WriteMode.APPEND:
                    metrics.records_inserted = (
                        metrics.target_row_count_after - metrics.target_row_count_before
                    )

            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)

            return ProcessingMetricsUtils.calculate_performance_metrics(
                metrics, config.write_mode
            )

        except Exception as e:
            write_end = time.time()
            metrics.write_duration_ms = int((write_end - write_start) * 1000)
            config_logger.exception(f"Write failed: {e}")
            raise WriteError(
                message=f"Failed to write to target table {config.target_table}",
                context=ErrorContext(
                    resource_name=config.resource_name,
                    operation="write_to_target",
                    additional_info={
                        "table": f"{config.target_schema}.{config.target_table}",
                        "write_mode": config.write_mode,
                    },
                ),
            ) from e

    def _add_ingestion_metadata(
        self,
        df: DataFrame,
        load_id: str,
        batch_info,
    ) -> DataFrame:
        """Add ingestion metadata columns to DataFrame"""
        result_df = df

        # Add load_id
        result_df = result_df.withColumn("_raw_load_id", lit(load_id))

        # Add filename
        if batch_info.file_paths:
            import os
            filename = os.path.basename(batch_info.file_paths[0])
            result_df = result_df.withColumn("_raw_filename", lit(filename))

        # Add timestamps
        result_df = result_df.withColumn("_raw_created_at", current_timestamp()) \
                             .withColumn("_raw_updated_at", current_timestamp())

        return result_df

    # ========================================================================
    # LOGGING METHODS (delegate to FileLoadingLogger if provided)
    # ========================================================================

    def _log_config_execution_start(self, config: ResourceConfig, execution_id: str) -> str:
        """Log config execution start and return load_run_id"""
        if self.logger_instance:
            return self.logger_instance.log_config_execution_start(config, execution_id)
        return str(uuid.uuid4())  # Generate ID even if no logger (for consistency)

    def _log_config_execution_completion(
        self, config: ResourceConfig, execution_id: str, load_run_id: str, result: ResourceExecutionResult
    ) -> None:
        """Log config execution completion"""
        if self.logger_instance:
            self.logger_instance.log_config_execution_completion(config, execution_id, load_run_id, result)

    def _log_config_execution_error(
        self, config: ResourceConfig, execution_id: str, load_run_id: str, error_msg: str, result: ResourceExecutionResult
    ) -> None:
        """Log config execution error"""
        if self.logger_instance:
            self.logger_instance.log_config_execution_error(config, execution_id, load_run_id, error_msg, result)

    def _log_batch_start(
        self, config: ResourceConfig, execution_id: str, load_run_id: str, load_id: str, batch_info
    ) -> None:
        """Log batch start"""
        if self.logger_instance:
            self.logger_instance.log_batch_start(config, execution_id, load_run_id, load_id, batch_info)

    def _log_batch_completion(
        self, config: ResourceConfig, execution_id: str, load_run_id: str, load_id: str, metrics: ProcessingMetrics, batch_info
    ) -> None:
        """Log batch completion"""
        if self.logger_instance:
            self.logger_instance.log_batch_completion(config, execution_id, load_run_id, load_id, metrics, batch_info)

    def _log_batch_error(
        self, config: ResourceConfig, execution_id: str, load_run_id: str, load_id: str, error_msg: str, batch_info
    ) -> None:
        """Log batch error"""
        if self.logger_instance:
            self.logger_instance.log_batch_error(config, execution_id, load_run_id, load_id, error_msg, batch_info)

    def _log_batch_no_data(
        self, config: ResourceConfig, execution_id: str, load_run_id: str, load_id: str, batch_info
    ) -> None:
        """Log batch with no data"""
        if self.logger_instance:
            self.logger_instance.log_batch_no_data(config, execution_id, load_run_id, load_id, batch_info)
