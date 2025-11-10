# File Loading Framework - Loading Orchestrator
# Orchestrates file loading from raw layer to bronze tables

import logging
import os
import re
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from ingen_fab.python_libs.common.flat_file_ingestion_utils import (
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

logger = logging.getLogger(__name__)


# Context classes for cleaner method signatures
class BatchContext:
    """Groups batch-level processing context to reduce parameter count"""
    def __init__(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        target_utils,
        file_loader: 'FileLoader',
        config_logger: logging.LoggerAdapter,
    ):
        self.config = config
        self.execution_id = execution_id
        self.load_run_id = load_run_id
        self.target_utils = target_utils
        self.file_loader = file_loader
        self.config_logger = config_logger


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
    - Target table writes (lakehouse only)
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
        logger_instance: FileLoadingLogger,
        log_lakehouse: lakehouse_utils,
        max_concurrency: int = 4,
    ):
        """
        Initialize orchestrator.

        Args:
            spark: Spark session
            logger_instance: FileLoadingLogger instance for state tracking (required)
            log_lakehouse: lakehouse_utils instance for log tables (log_resource_extract_batch, etc.)
            max_concurrency: Maximum number of parallel workers per group
        """
        self.spark = spark
        self.logger_instance = logger_instance
        self.log_lakehouse = log_lakehouse
        self.max_concurrency = max_concurrency

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
        load_run_id = self.logger_instance.log_config_execution_start(config, execution_id)

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
                self.logger_instance.log_config_execution_completion(config, execution_id, load_run_id, result)
                return result

            # Process batches one at a time
            total_batches = len(discovered_batches)

            # Create batch context to simplify method signatures
            batch_context = BatchContext(
                config=config,
                execution_id=execution_id,
                load_run_id=load_run_id,
                target_utils=target_utils,
                file_loader=file_loader,
                config_logger=config_logger,
            )

            all_metrics, batches_processed, batches_failed = self._process_batches(
                discovered_batches, batch_context, total_batches
            )

            # Update result with batch counts
            result.batches_processed = batches_processed
            result.batches_failed = batches_failed

            # Finalize result with aggregated metrics and status
            self._finalize_result(result, all_metrics, config, start_time, config_logger)

            # Log config execution completion
            self.logger_instance.log_config_execution_completion(config, execution_id, load_run_id, result)

        except (FileReadError, WriteError) as e:
            # Known loading errors - already have context
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            config_logger.exception(f"Loading failed: {e}")
            self.logger_instance.log_config_execution_error(config, execution_id, load_run_id, str(e), result)

        except Exception as e:
            # Unexpected error
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            config_logger.exception(f"Unexpected error in loading: {e}")
            self.logger_instance.log_config_execution_error(config, execution_id, load_run_id, str(e), result)

        return result

    def _process_batches(
        self,
        batches: List[BatchInfo],
        ctx: BatchContext,
        total_batches: int,
    ) -> Tuple[List[ProcessingMetrics], int, int]:
        """
        Process all batches one at a time (streaming) and return aggregated metrics.

        Args:
            batches: List of BatchInfo objects to process
            ctx: BatchContext with config, execution_id, load_run_id, target_utils, file_loader, config_logger
            total_batches: Total number of batches to process

        Returns:
            Tuple of (all_metrics, batches_processed, batches_failed)
        """
        all_metrics = []
        batches_processed = 0
        batches_failed = 0

        for batch_index, batch_info in enumerate(batches, start=1):
            try:
                # Step 1: Mark batch as loading in extraction log
                if batch_info.extract_batch_id:
                    self._update_load_state(
                        batch_info.extract_batch_id,
                        ctx.config.source_name,
                        ctx.config.resource_name,
                        'loading'
                    )

                # Step 2: Mark batch as processing in load log
                ctx.file_loader._start_batch(batch_info, ctx.execution_id, ctx.load_run_id)

                # Step 3: Read file into DataFrame
                df, read_metrics = ctx.file_loader.reader.read_batch(batch_info)

                # Step 4: Add ingestion metadata to DataFrame
                load_id = batch_info.batch_id
                df = self._add_ingestion_metadata(df, load_id, batch_info, ctx.config)

                # Step 5: Validate data (check for empty DataFrame)
                if df.count() == 0:
                    ctx.config_logger.warning(f"Loading batch {batch_index}/{total_batches}: 0 records - EMPTY (skipped)")
                    self.logger_instance.log_batch_no_data(ctx.config, ctx.execution_id, ctx.load_run_id, load_id, batch_info)
                    continue

                # Step 6: Write DataFrame to target table
                batch_start = time.time()
                write_metrics = self._write_to_target(df, ctx.config, ctx.target_utils, ctx.config_logger)

                # Step 7: Combine read and write metrics
                combined_metrics = self._combine_metrics(read_metrics, write_metrics)

                # Step 8: Mark batch as completed in load log
                ctx.file_loader._complete_batch(batch_info, combined_metrics)

                # Step 9: Mark batch as completed in extraction log
                if batch_info.extract_batch_id:
                    self._update_load_state(
                        batch_info.extract_batch_id,
                        ctx.config.source_name,
                        ctx.config.resource_name,
                        'completed'
                    )

                # Step 10: Log progress and track successful batch
                batch_duration = time.time() - batch_start
                records_formatted = f"{combined_metrics.records_processed:,}"
                ctx.config_logger.info(
                    f"Loading batch {batch_index}/{total_batches}: {records_formatted} records ({batch_duration:.2f}s)"
                )

                if combined_metrics.records_processed > 0:
                    all_metrics.append(combined_metrics)
                    batches_processed += 1

            except (WriteError, Exception) as error:
                # Update extraction log: mark as failed
                if batch_info.extract_batch_id:
                    self._update_load_state(
                        batch_info.extract_batch_id,
                        ctx.config.source_name,
                        ctx.config.resource_name,
                        'failed'
                    )

                # Handle batch error - mark as failed and move to quarantine
                batches_failed += 1
                ctx.file_loader._fail_batch(batch_info, error)
                self._handle_batch_error(error, batch_info, ctx.config_logger)

                # Stop processing if any batch fails
                raise

        return all_metrics, batches_processed, batches_failed

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
            total_duration_ms=read_metrics.read_duration_ms + write_metrics.write_duration_ms,
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
        config_logger: logging.LoggerAdapter,
    ) -> None:
        """
        Handle batch processing error with appropriate logging.

        Args:
            error: The exception that occurred
            batch_info: Information about the failed batch
            config_logger: Configured logger adapter
        """
        load_id = batch_info.batch_id
        error_msg = str(error)

        # Log with appropriate level based on error type
        if isinstance(error, WriteError):
            config_logger.exception(f"Batch {load_id} write failed: {error_msg}")
        else:
            config_logger.exception(f"Batch {load_id} failed: {error_msg}")

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
        """Get lakehouse utilities for target operations"""
        # Currently only lakehouse targets are supported
        # (warehouse support could be added in the future)
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
            if not self.log_lakehouse.check_if_table_exists("log_resource_extract_batch"):
                logger.warning("Table 'log_resource_extract_batch' not found, no batches to load")
                return []

            # Query for completed extractions ready for loading (using load_state)
            # This is the work queue pattern (like dlt's load_storage folders)
            extraction_df = (
                self.log_lakehouse.read_table("log_resource_extract_batch")
                .filter(col("source_name") == config.source_name)
                .filter(col("resource_name") == config.resource_name)
                .filter(col("status") == "completed")
                .filter(col("load_state") == "pending")
                .orderBy(col("completed_at").asc())
            )
            extraction_rows = extraction_df.collect()

            if not extraction_rows:
                return []

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
            self.log_lakehouse.update_table(
                table_name="log_resource_extract_batch",
                condition=(
                    (col("extract_batch_id") == extract_batch_id) &
                    (col("source_name") == source_name) &
                    (col("resource_name") == resource_name)
                ),
                set_values={"load_state": lit(new_state)}
            )
            logger.debug(f"Updated extraction {extract_batch_id} to load_state='{new_state}'")
        except Exception as e:
            logger.warning(f"Could not update load_state for {extract_batch_id}: {e}")

    def _get_table_row_count(
        self,
        target_utils,
        table_name: str,
        schema_name: Optional[str],
        config_logger: logging.LoggerAdapter,
    ) -> int:
        """
        Get row count for a table, returning 0 if table doesn't exist.

        Args:
            target_utils: Lakehouse utilities
            table_name: Name of the table
            schema_name: Optional schema name
            config_logger: Configured logger adapter

        Returns:
            Row count (0 if table doesn't exist)
        """
        try:
            table_df = target_utils.read_table(table_name, schema_name=schema_name)
            return table_df.count()
        except Exception as e:
            config_logger.debug(f"Table does not exist or cannot be read: {e}")
            return 0

    def _write_merge(
        self,
        df: DataFrame,
        config: ResourceConfig,
        target_utils,
        config_logger: logging.LoggerAdapter,
    ) -> ProcessingMetrics:
        """
        Write DataFrame using merge (upsert) mode.

        Args:
            df: Source DataFrame to merge
            config: Resource configuration
            target_utils: Lakehouse utilities
            config_logger: Configured logger adapter

        Returns:
            ProcessingMetrics with merge results
        """
        metrics = ProcessingMetrics()

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

        return metrics

    def _write_overwrite_or_append(
        self,
        df: DataFrame,
        config: ResourceConfig,
        target_utils,
        config_logger: logging.LoggerAdapter,
    ) -> ProcessingMetrics:
        """
        Write DataFrame using overwrite or append mode.

        Args:
            df: Source DataFrame to write
            config: Resource configuration
            target_utils: Lakehouse utilities
            config_logger: Configured logger adapter

        Returns:
            ProcessingMetrics with write results
        """
        metrics = ProcessingMetrics()

        # Get before count
        metrics.target_row_count_before = self._get_table_row_count(
            target_utils, config.target_table, config.target_schema, config_logger
        )
        config_logger.debug(f"Target table row count before write: {metrics.target_row_count_before}")

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
        metrics.target_row_count_after = self._get_table_row_count(
            target_utils, config.target_table, config.target_schema, config_logger
        )
        config_logger.debug(f"Target table row count after write: {metrics.target_row_count_after}")

        # Calculate inserted/deleted
        if config.write_mode == WriteMode.OVERWRITE:
            metrics.records_inserted = metrics.target_row_count_after
            metrics.records_deleted = metrics.target_row_count_before
        elif config.write_mode == WriteMode.APPEND:
            metrics.records_inserted = (
                metrics.target_row_count_after - metrics.target_row_count_before
            )

        return metrics

    def _write_to_target(
        self,
        df: DataFrame,
        config: ResourceConfig,
        target_utils,
        config_logger: logging.LoggerAdapter,
    ) -> ProcessingMetrics:
        """
        Write DataFrame to target table using configured write mode.

        Delegates to specialized methods based on write mode:
        - Merge: _write_merge()
        - Overwrite/Append: _write_overwrite_or_append()

        Args:
            df: Source DataFrame to write
            config: Resource configuration
            target_utils: Lakehouse utilities
            config_logger: Configured logger adapter

        Returns:
            ProcessingMetrics with write results and timing
        """
        write_start = time.time()
        metrics = ProcessingMetrics()  # Initialize to ensure it's always defined for exception handler

        try:
            # Delegate to specialized write method based on mode
            if config.write_mode.lower() == WriteMode.MERGE:
                metrics = self._write_merge(df, config, target_utils, config_logger)
            else:
                metrics = self._write_overwrite_or_append(df, config, target_utils, config_logger)

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
        config: ResourceConfig,
    ) -> DataFrame:
        """Add ingestion metadata columns to DataFrame"""
        result_df = df

        # Add filename metadata columns FIRST (extracted from path - business columns like file_date)
        if batch_info.file_paths:
            file_path = batch_info.file_paths[0]
            result_df = self._add_filename_metadata_columns(result_df, file_path, config)

        # Add soft delete column (if enabled)
        if config.soft_delete_enabled:
            result_df = result_df.withColumn("_raw_is_deleted", lit(False))

        # Add filename
        if batch_info.file_paths:
            filename = os.path.basename(batch_info.file_paths[0])
            result_df = result_df.withColumn("_raw_filename", lit(filename))

        # Add load_id
        result_df = result_df.withColumn("_raw_load_id", lit(load_id))

        # Add timestamps (last)
        result_df = result_df.withColumn("_raw_created_at", current_timestamp()) \
                             .withColumn("_raw_updated_at", current_timestamp())

        return result_df

    def _add_filename_metadata_columns(
        self,
        df: DataFrame,
        file_path: str,
        config: ResourceConfig
    ) -> DataFrame:
        """
        Extract metadata from file path and add as DataFrame columns.

        Args:
            df: Source DataFrame
            file_path: Full file path to extract metadata from
            config: Resource configuration with extraction_params

        Returns:
            DataFrame with added metadata columns
        """
        from pyspark.sql.functions import to_date, to_timestamp

        # Get metadata patterns from extraction_params
        if not config.extraction_params or not isinstance(config.extraction_params, dict):
            return df

        metadata_patterns = config.extraction_params.get("filename_metadata", [])
        if not metadata_patterns:
            return df

        # Extract each metadata field and add as column
        for pattern in metadata_patterns:
            field_name = pattern["name"]
            regex = pattern["regex"]
            field_type = pattern.get("type", "string")
            date_format = pattern.get("format", "yyyyMMdd")

            try:
                # Extract value using Python regex
                match = re.search(regex, file_path)

                if match:
                    groups = match.groups()
                    if groups:
                        # Single group or concatenate multiple groups
                        if len(groups) == 1:
                            value = groups[0]
                        else:
                            value = "".join(groups)

                        # Add column with appropriate type conversion
                        if field_type == "date":
                            df = df.withColumn(field_name, to_date(lit(value), date_format))
                        elif field_type == "timestamp":
                            df = df.withColumn(field_name, to_timestamp(lit(value), date_format))
                        elif field_type == "int":
                            df = df.withColumn(field_name, lit(int(value)))
                        elif field_type == "long":
                            df = df.withColumn(field_name, lit(int(value)).cast("long"))
                        elif field_type == "double":
                            df = df.withColumn(field_name, lit(float(value)))
                        elif field_type == "boolean":
                            df = df.withColumn(field_name, lit(value.lower() in ["true", "1", "yes"]))
                        else:  # string (default)
                            df = df.withColumn(field_name, lit(value))

                        logger.debug(f"Added metadata column: {field_name}='{value}' ({field_type})")
                    else:
                        # Regex matched but no capture groups - add NULL
                        df = df.withColumn(field_name, lit(None))
                else:
                    # Regex didn't match - add NULL
                    df = df.withColumn(field_name, lit(None))
                    logger.debug(f"Metadata field {field_name} not found in path: {file_path}")

            except Exception as e:
                # Error during extraction/conversion - add NULL and log warning
                df = df.withColumn(field_name, lit(None))
                logger.warning(f"Failed to extract metadata field {field_name}: {e}")

        return df

