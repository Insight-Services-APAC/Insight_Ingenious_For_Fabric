# File Loading Framework - Orchestrator
# Orchestrates file loading across multiple resources with parallel execution

import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

from ingen_fab.python_libs.common.flat_file_ingestion_utils import (
    ArchivePathResolver,
    ErrorHandlingUtils,
    ProcessingMetricsUtils,
)
from ingen_fab.python_libs.pyspark.ingestion.config import (
    FileSystemLoadingParams,
    ResourceConfig,
)
from ingen_fab.python_libs.pyspark.ingestion.constants import (
    DatastoreType,
    ExecutionStatus,
    WriteMode,
)
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    ArchiveError,
    ArchiveNetworkError,
    ArchivePermissionError,
    ArchiveResult,
    ArchiveStorageError,
    DuplicateFilesError,
    ErrorContext,
    FileDiscoveryError,
    FileReadError,
    PartialArchiveFailure,
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


def retry_on_transient_error(
    operation_func,
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    transient_exceptions=(ArchiveNetworkError,),
):
    """
    Retry an operation on transient errors with exponential backoff.

    Args:
        operation_func: Function to execute
        max_attempts: Maximum number of attempts
        backoff_factor: Multiplier for backoff delay
        transient_exceptions: Tuple of exception types to retry

    Returns:
        Result of operation_func()

    Raises:
        Last exception if all attempts fail
    """
    last_exception = None
    delay = 1.0  # Initial delay in seconds

    for attempt in range(1, max_attempts + 1):
        try:
            return operation_func()
        except transient_exceptions as e:
            last_exception = e
            if attempt < max_attempts:
                logger.debug(f"Attempt {attempt} failed, retrying in {delay}s: {e}")
                time.sleep(delay)
                delay *= backoff_factor
            else:
                logger.warning(f"All {max_attempts} attempts failed")
        except Exception as e:
            # Non-transient error - don't retry
            raise

    # Should never reach here, but just in case
    if last_exception:
        raise last_exception


class FileLoadingOrchestrator:
    """
    Orchestrates file loading across multiple resources.

    Handles:
    - Execution grouping and sequencing
    - Parallel processing within groups
    - Target table writes (lakehouse or warehouse)
    - State tracking and logging
    - Error handling and retry logic
    - Metrics aggregation
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

        # Log config execution start
        self._log_config_execution_start(config, execution_id)

        try:
            config_logger.info(f"Processing: {config.resource_name}")
            config_logger.info(f"Source: {config.source_file_path}")
            config_logger.info(f"Target: {config.target_schema_name}.{config.target_table_name}")
            config_logger.info(f"Format: {config.source_file_format}")
            config_logger.info(f"Write mode: {config.write_mode}")

            # Create FileLoader instance
            file_loader = FileLoader(spark=self.spark, config=config)

            # Discover and read files
            batches = file_loader.discover_and_read_files()

            if not batches:
                config_logger.info("No files discovered")
                result.status = ExecutionStatus.NO_DATA
                self._log_config_execution_completion(config, execution_id, result)
                return result

            # Process batches
            all_metrics, batches_processed, batches_failed = self._process_batches(
                batches, config, execution_id, target_utils, file_loader, config_logger
            )

            # Update result with batch counts
            result.batches_processed = batches_processed
            result.batches_failed = batches_failed

            # Finalize result with aggregated metrics and status
            self._finalize_result(result, all_metrics, config, start_time, config_logger)

            # Log config execution completion
            self._log_config_execution_completion(config, execution_id, result)

        except DuplicateFilesError as e:
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            config_logger.exception(f"Duplicate files detected: {e}")
            self._log_config_execution_error(config, execution_id, str(e), result)

        except (FileDiscoveryError, FileReadError, WriteError) as e:
            # Known ingestion errors - already have context
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            config_logger.exception(f"Resource processing failed: {e}")
            self._log_config_execution_error(config, execution_id, str(e), result)

        except Exception as e:
            # Unexpected error
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            config_logger.exception(f"Unexpected error in resource processing: {e}")
            self._log_config_execution_error(config, execution_id, str(e), result)

        return result

    def _process_batches(
        self,
        batches: List[Tuple[BatchInfo, DataFrame, ProcessingMetrics]],
        config: ResourceConfig,
        execution_id: str,
        target_utils,
        file_loader: FileLoader,
        config_logger: logging.LoggerAdapter,
    ) -> Tuple[List[ProcessingMetrics], int, int]:
        """
        Process all batches and return aggregated metrics.

        Args:
            batches: List of (BatchInfo, DataFrame, ProcessingMetrics) tuples
            config: ResourceConfig for this resource
            execution_id: Unique execution identifier
            target_utils: Lakehouse or warehouse utilities
            file_loader: FileLoader instance for archiving
            config_logger: Configured logger adapter

        Returns:
            Tuple of (all_metrics, batches_processed, batches_failed)
        """
        all_metrics = []
        batches_processed = 0
        batches_failed = 0

        for batch_info, df, read_metrics in batches:
            try:
                # Process single batch
                combined_metrics = self._process_single_batch(
                    batch_info, df, read_metrics, config, execution_id,
                    target_utils, file_loader, config_logger
                )

                # Track successful batch
                if combined_metrics.records_processed > 0:
                    all_metrics.append(combined_metrics)
                    batches_processed += 1

            except (WriteError, Exception) as error:
                # Handle batch error
                batches_failed += 1
                self._handle_batch_error(error, batch_info, config, execution_id, config_logger)

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
        target_utils,
        file_loader: FileLoader,
        config_logger: logging.LoggerAdapter,
    ) -> ProcessingMetrics:
        """
        Process a single batch - cleaner and more testable.

        Args:
            batch_info: Information about this batch
            df: DataFrame to process
            read_metrics: Metrics from file reading
            config: Resource configuration
            execution_id: Unique execution identifier
            target_utils: Lakehouse or warehouse utilities
            file_loader: FileLoader instance for archiving
            config_logger: Configured logger adapter

        Returns:
            Combined ProcessingMetrics for this batch
        """
        batch_start = time.time()
        load_id = batch_info.batch_id

        # Log batch start
        self._log_batch_start(config, execution_id, load_id, batch_info)

        # Add ingestion metadata
        df = self._add_ingestion_metadata(df, load_id, batch_info)

        # Validate data
        if df.count() == 0:
            config_logger.warning(f"Batch {load_id}: no data")
            return ProcessingMetrics()

        # Write to target
        write_metrics = self._write_to_target(df, config, target_utils, config_logger)

        # Combine metrics
        combined_metrics = self._combine_metrics(read_metrics, write_metrics)

        # Log batch completion
        self._log_batch_completion(config, execution_id, load_id, combined_metrics, batch_info)

        # Archive files if configured
        archive_result = self._archive_batch_files(
            batch_info, config, combined_metrics, file_loader, config_logger
        )

        # Check for archive failures
        if archive_result.has_failures:
            config_logger.warning(
                f"Archive completed with {archive_result.failed} failure(s): "
                f"{archive_result.summary()}"
            )

        batch_duration = time.time() - batch_start
        config_logger.info(
            f"Batch {load_id}: {combined_metrics.records_processed} records "
            f"in {batch_duration:.2f}s"
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
        config_logger: logging.LoggerAdapter,
    ) -> None:
        """
        Handle batch processing error with appropriate logging.

        Args:
            error: The exception that occurred
            batch_info: Information about the failed batch
            config: Resource configuration
            execution_id: Unique execution identifier
            config_logger: Configured logger adapter
        """
        load_id = batch_info.batch_id
        error_msg = str(error)

        # Log with appropriate level based on error type
        if isinstance(error, WriteError):
            config_logger.exception(f"Batch {load_id} write failed: {error_msg}")
        else:
            config_logger.exception(f"Batch {load_id} failed: {error_msg}")

        # Log batch error to state tracking
        self._log_batch_error(config, execution_id, load_id, error_msg, batch_info)

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
        if config.target_datastore_type.lower() == DatastoreType.LAKEHOUSE:
            return lakehouse_utils(
                target_workspace_name=config.target_workspace_name,
                target_lakehouse_name=config.target_datastore_name,
                spark=self.spark,
            )
        elif config.target_datastore_type.lower() == DatastoreType.WAREHOUSE:
            if warehouse_utils is None:
                raise ImportError("warehouse_utils not available")
            return warehouse_utils(
                target_workspace_name=config.target_workspace_name,
                target_warehouse_name=config.target_datastore_name,
                spark=self.spark,
            )
        else:
            raise ValueError(f"Unknown target_datastore_type: {config.target_datastore_type}")

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
                    table_name=config.target_table_name,
                    merge_keys=config.merge_keys,
                    schema_name=config.target_schema_name,
                    immutable_columns=["_raw_created_at"],
                    enable_schema_evolution=config.enable_schema_evolution,
                    partition_by=config.partition_columns,
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
                    existing_df = target_utils.read_table(config.target_table_name, schema_name=config.target_schema_name)
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
                    table_name=config.target_table_name,
                    schema_name=config.target_schema_name,
                    mode=config.write_mode,
                    options=write_options,
                    partition_by=config.partition_columns,
                )

                # Get after count
                try:
                    result_df = target_utils.read_table(config.target_table_name, schema_name=config.target_schema_name)
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
                message=f"Failed to write to target table {config.target_table_name}",
                context=ErrorContext(
                    resource_name=config.resource_name,
                    operation="write_to_target",
                    additional_info={
                        "table": f"{config.target_schema_name}.{config.target_table_name}",
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

    def _archive_batch_files(
        self,
        batch_info,
        config: ResourceConfig,
        metrics: ProcessingMetrics,
        file_loader: FileLoader,
        config_logger: logging.LoggerAdapter,
    ) -> ArchiveResult:
        """
        Archive files after successful processing.

        Returns:
            ArchiveResult with success/failure tracking
        """
        result = ArchiveResult()

        # Check if archiving is enabled
        loading_params = FileSystemLoadingParams.from_dict(config.loading_params)
        if not loading_params.enable_archive or not loading_params.archive_path:
            return result

        # Ensure completed_at is set
        if not metrics.completed_at:
            config_logger.warning("Cannot archive: completed_at timestamp not set")
            return result

        # Get source lakehouse utils from file_loader
        source_lakehouse = file_loader.lakehouse

        # Determine if cross-lakehouse archiving is configured
        is_cross_lakehouse = (
            loading_params.archive_workspace_name is not None
            or loading_params.archive_lakehouse_name is not None
        )

        # Create archive destination lakehouse utils if cross-lakehouse
        archive_lakehouse = None
        if is_cross_lakehouse:
            archive_workspace = (
                loading_params.archive_workspace_name
                or config.target_workspace_name
            )
            archive_lakehouse_name = (
                loading_params.archive_lakehouse_name
                or config.target_datastore_name
            )

            archive_lakehouse = lakehouse_utils(
                target_workspace_name=archive_workspace,
                target_lakehouse_name=archive_lakehouse_name,
                spark=self.spark,
            )

            config_logger.info(
                f"Cross-lakehouse archive to: {archive_workspace}/{archive_lakehouse_name}"
            )

        # Archive each file in the batch
        for file_path in batch_info.file_paths:
            try:
                # Resolve archive path using template
                archive_path = ArchivePathResolver.resolve(
                    template=loading_params.archive_path,
                    batch_info=batch_info,
                    config=config,
                    process_timestamp=metrics.completed_at,
                )

                # Extract relative path (remove lakehouse URI prefix)
                source_relative = file_path.replace(source_lakehouse.lakehouse_files_uri(), "")

                if is_cross_lakehouse and archive_lakehouse:
                    # Cross-lakehouse archiving
                    success = self._archive_file_cross_lakehouse(
                        source_lakehouse=source_lakehouse,
                        archive_lakehouse=archive_lakehouse,
                        source_relative=source_relative,
                        archive_path=archive_path,
                        config_logger=config_logger,
                        result=result,
                    )
                else:
                    # Same lakehouse archiving
                    success = self._archive_file_same_lakehouse(
                        source_lakehouse=source_lakehouse,
                        source_relative=source_relative,
                        archive_path=archive_path,
                        loading_params=loading_params,
                        config=config,
                        config_logger=config_logger,
                        result=result,
                    )

                if success:
                    result.add_success()
                    config_logger.debug(f"Archived: {source_relative} → {archive_path}")

            except ArchiveError as e:
                # Already tracked in result
                config_logger.debug(f"Archive error already tracked: {e}")
            except Exception as e:
                # Unexpected error
                result.add_failure(
                    file_path=file_path,
                    error_message=str(e),
                    error_type=type(e).__name__,
                )
                config_logger.warning(f"Archive error for {file_path}: {e}")

        # Log summary
        config_logger.info(result.summary())

        return result

    def _archive_file_cross_lakehouse(
        self,
        source_lakehouse,
        archive_lakehouse,
        source_relative: str,
        archive_path: str,
        config_logger: logging.LoggerAdapter,
        result: ArchiveResult,
    ) -> bool:
        """
        Archive a file to a different lakehouse.

        Returns:
            True if successful, False otherwise
        """
        source_full_path = f"{source_lakehouse.lakehouse_files_uri()}{source_relative.lstrip('/')}"
        dest_full_path = f"{archive_lakehouse.lakehouse_files_uri()}{archive_path.lstrip('/')}"

        try:
            from notebookutils import mssparkutils

            # Retry on transient errors
            def archive_operation():
                mv_result = mssparkutils.fs.mv(source_full_path, dest_full_path, True)
                success = mv_result is True or mv_result == "true"
                if not success:
                    raise ArchiveError(f"Move operation returned: {mv_result}")
                return success

            success = retry_on_transient_error(archive_operation, max_attempts=3)

            if success:
                config_logger.info(
                    f"Archived (cross-lakehouse): {source_relative} → "
                    f"{archive_lakehouse.target_store_name}/{archive_path}"
                )
                return True

        except PermissionError as e:
            result.add_failure(
                file_path=source_relative,
                error_message=str(e),
                error_type="ArchivePermissionError",
            )
            config_logger.warning(f"Permission denied for cross-lakehouse archive: {source_relative}")
        except (ConnectionError, TimeoutError) as e:
            result.add_failure(
                file_path=source_relative,
                error_message=str(e),
                error_type="ArchiveNetworkError",
                is_transient=True,
            )
            config_logger.warning(f"Network error during cross-lakehouse archive: {source_relative}")
        except Exception as e:
            result.add_failure(
                file_path=source_relative,
                error_message=str(e),
                error_type=type(e).__name__,
            )
            config_logger.warning(f"Cross-lakehouse archive error for {source_relative}: {e}")

        return False

    def _archive_file_same_lakehouse(
        self,
        source_lakehouse,
        source_relative: str,
        archive_path: str,
        loading_params: FileSystemLoadingParams,
        config: ResourceConfig,
        config_logger: logging.LoggerAdapter,
        result: ArchiveResult,
    ) -> bool:
        """
        Archive a file within the same lakehouse.

        Returns:
            True if successful, False otherwise
        """
        try:
            # Retry on transient errors
            def archive_operation():
                success = source_lakehouse.move_file(source_relative, archive_path)
                if not success:
                    raise ArchiveError("Move operation failed")
                return success

            success = retry_on_transient_error(archive_operation, max_attempts=3)

            if success:
                config_logger.info(f"Archived: {source_relative} → {archive_path}")

                # Clean up empty directories if configured
                if loading_params.cleanup_empty_dirs and config.source_file_path:
                    try:
                        base_path = config.source_file_path.rstrip('/')
                        source_lakehouse.cleanup_empty_directories_recursive(
                            file_path=source_relative,
                            base_path=base_path
                        )
                    except Exception as cleanup_error:
                        # Log but don't fail on cleanup errors
                        config_logger.debug(f"Directory cleanup warning: {cleanup_error}")

                return True

        except PermissionError as e:
            result.add_failure(
                file_path=source_relative,
                error_message=str(e),
                error_type="ArchivePermissionError",
            )
            config_logger.warning(f"Permission denied for archive: {source_relative}")
        except OSError as e:
            # Disk full, etc.
            result.add_failure(
                file_path=source_relative,
                error_message=str(e),
                error_type="ArchiveStorageError",
            )
            config_logger.warning(f"Storage error during archive: {source_relative}")
        except Exception as e:
            result.add_failure(
                file_path=source_relative,
                error_message=str(e),
                error_type=type(e).__name__,
            )
            config_logger.warning(f"Archive error for {source_relative}: {e}")

        return False

    # ========================================================================
    # LOGGING METHODS (delegate to FileLoadingLogger if provided)
    # ========================================================================

    def _log_config_execution_start(self, config: ResourceConfig, execution_id: str) -> None:
        """Log config execution start"""
        if self.logger_instance:
            self.logger_instance.log_config_execution_start(config, execution_id)

    def _log_config_execution_completion(
        self, config: ResourceConfig, execution_id: str, result: ResourceExecutionResult
    ) -> None:
        """Log config execution completion"""
        if self.logger_instance:
            self.logger_instance.log_config_execution_completion(config, execution_id, result)

    def _log_config_execution_error(
        self, config: ResourceConfig, execution_id: str, error_msg: str, result: ResourceExecutionResult
    ) -> None:
        """Log config execution error"""
        if self.logger_instance:
            self.logger_instance.log_config_execution_error(config, execution_id, error_msg, result)

    def _log_batch_start(
        self, config: ResourceConfig, execution_id: str, load_id: str, batch_info
    ) -> None:
        """Log batch start"""
        if self.logger_instance:
            self.logger_instance.log_batch_start(config, execution_id, load_id, batch_info)

    def _log_batch_completion(
        self, config: ResourceConfig, execution_id: str, load_id: str, metrics: ProcessingMetrics, batch_info
    ) -> None:
        """Log batch completion"""
        if self.logger_instance:
            self.logger_instance.log_batch_completion(config, execution_id, load_id, metrics, batch_info)

    def _log_batch_error(
        self, config: ResourceConfig, execution_id: str, load_id: str, error_msg: str, batch_info
    ) -> None:
        """Log batch error"""
        if self.logger_instance:
            self.logger_instance.log_batch_error(config, execution_id, load_id, error_msg, batch_info)
