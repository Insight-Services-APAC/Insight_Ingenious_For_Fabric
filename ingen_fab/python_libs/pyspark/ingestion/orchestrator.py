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
    ErrorHandlingUtils,
    ProcessingMetricsUtils,
)
from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.constants import (
    ExecutionStatus,
    WriteMode,
)
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    DuplicateFilesError,
    ErrorContext,
    ExtractionError,
    FileDiscoveryError,
    FileReadError,
    WriteError,
)
from ingen_fab.python_libs.pyspark.ingestion.extractors.filesystem_extractor import (
    FileSystemExtractor,
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


class FileLoadingOrchestrator:
    """
    Orchestrates data ingestion across multiple resources.

    Unified ingestion pattern:
    1. Extraction: External source (inbound files, API, database) → Raw layer
    2. Loading: Raw layer → Bronze tables

    Handles:
    - Extraction step (filesystem, API, database extractors)
    - Loading step (raw → bronze)
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
            # Consolidate metadata into single line
            target_full = f"{config.target_schema}.{config.target_table}" if config.target_schema else config.target_table
            config_logger.info(
                f"Source: {config.source_config.source_type} → Target: {target_full} "
                f"({config.file_format}, {config.write_mode})"
            )

            extraction_result = None
            batches_from_extraction = []

            try:
                extractor = self._get_extractor(config, config_logger)
                extraction_result = extractor.extract()

                # Check for validation failure (expected condition, not an exception)
                if extraction_result.error_message:
                    result.status = ExecutionStatus.FAILED
                    result.error_message = extraction_result.error_message
                    config_logger.error(f"Extraction validation failed: {extraction_result.error_message}")
                    self._log_config_execution_error(config, execution_id, extraction_result.error_message, result)
                    return result

                # Simplified extraction summary
                if extraction_result.promoted_count == 0 and extraction_result.failed_count == 0:
                    config_logger.info(f"No files found in {config.raw_file_path}")
                    result.status = ExecutionStatus.NO_DATA
                    self._log_config_execution_completion(config, execution_id, result)
                    return result

                # If extraction had failures, warn but continue
                if extraction_result.failed_count > 0:
                    config_logger.warning(
                        f"Extraction had {extraction_result.failed_count} failures, "
                        f"continuing with {extraction_result.promoted_count} successful"
                    )

                # Convert extraction results to batches (dlt pattern)
                if extraction_result.promoted_count > 0:
                    batches_from_extraction = self._convert_extraction_results_to_batches(
                        extraction_result, config
                    )

            except NotImplementedError as e:
                # Extractor not implemented yet - log and will use discovery fallback
                config_logger.warning(f"Extraction skipped: {e}")
                config_logger.info("Will use file discovery from raw layer")

            except ExtractionError as e:
                # Extraction failed completely
                result.status = ExecutionStatus.FAILED
                result.error_message = str(e)
                config_logger.exception(f"Extraction failed: {e}")
                self._log_config_execution_error(config, execution_id, str(e), result)
                return result


            # Create FileLoader instance
            file_loader = FileLoader(spark=self.spark, config=config)

            # Use extraction results if available (dlt pattern), otherwise discover
            if batches_from_extraction:
                discovered_batches = batches_from_extraction
            else:
                # Fallback: discover files in raw layer (when extraction skipped/not implemented)
                discovered_batches = file_loader.discover_files()

            if not discovered_batches:
                config_logger.info("No files to process")
                result.status = ExecutionStatus.NO_DATA
                self._log_config_execution_completion(config, execution_id, result)
                return result

            # Process batches one at a time (streaming)
            total_batches = len(discovered_batches)
            all_metrics, batches_processed, batches_failed = self._process_batches(
                discovered_batches, config, execution_id, target_utils, file_loader, config_logger, total_batches
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

        except (ExtractionError, FileDiscoveryError, FileReadError, WriteError) as e:
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
        batches: List[BatchInfo],
        config: ResourceConfig,
        execution_id: str,
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
            target_utils: Lakehouse or warehouse utilities
            file_loader: FileLoader instance for reading and archiving
            config_logger: Configured logger adapter

        Returns:
            Tuple of (all_metrics, batches_processed, batches_failed)
        """
        all_metrics = []
        batches_processed = 0
        batches_failed = 0

        for batch_index, batch_info in enumerate(batches, start=1):
            try:
                # Start batch lifecycle (mark as processing in log)
                file_loader._start_batch(batch_info)

                # Read batch (one at a time - streaming)
                df, read_metrics = file_loader.reader.read_batch(batch_info)

                # Process single batch (write and archive)
                combined_metrics = self._process_single_batch(
                    batch_info, df, read_metrics, config, execution_id,
                    target_utils, file_loader, config_logger, batch_index, total_batches
                )

                # Complete batch lifecycle (mark as completed and move to archive)
                file_loader._complete_batch(batch_info, combined_metrics)

                # Track successful batch
                if combined_metrics.records_processed > 0:
                    all_metrics.append(combined_metrics)
                    batches_processed += 1

            except (WriteError, Exception) as error:
                # Handle batch error - mark as failed and move to quarantine
                batches_failed += 1
                file_loader._fail_batch(batch_info, error)
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
            config_logger.warning(f"Loading batch {batch_index}/{total_batches}: 0 records - EMPTY (skipped)")
            # Log empty batch so it's not reprocessed on next run
            self._log_batch_no_data(config, execution_id, load_id, batch_info)
            return ProcessingMetrics()

        # Write to target
        write_metrics = self._write_to_target(df, config, target_utils, config_logger)

        # Combine metrics
        combined_metrics = self._combine_metrics(read_metrics, write_metrics)

        # Log batch completion
        self._log_batch_completion(config, execution_id, load_id, combined_metrics, batch_info)

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
        # Infer target type from target_lakehouse field
        # (could also add explicit target_type field if needed)
        return lakehouse_utils(
            target_workspace_name=config.target_workspace,
            target_lakehouse_name=config.target_lakehouse,
            spark=self.spark,
        )

    def _get_extractor(self, config: ResourceConfig, logger_instance=None):
        """
        Get appropriate extractor based on source type.

        Args:
            config: ResourceConfig with source_type
            logger_instance: Optional context-aware logger (ConfigLoggerAdapter)

        Returns:
            Extractor instance (FileSystemExtractor, APIExtractor, DatabaseExtractor, etc.)

        Raises:
            NotImplementedError: If source_type extractor is not implemented yet
        """
        source_type = config.source_config.source_type

        if source_type == "filesystem":
            return FileSystemExtractor(
                resource_config=config,
                spark=self.spark,
                logger_instance=logger_instance,
            )
        elif source_type == "api":
            # TODO: Implement APIExtractor
            raise NotImplementedError(
                f"API extractor not yet implemented. "
                f"Create APIExtractor in extractors/ directory."
            )
        elif source_type == "database":
            # TODO: Implement DatabaseExtractor
            raise NotImplementedError(
                f"Database extractor not yet implemented. "
                f"Create DatabaseExtractor in extractors/ directory."
            )
        else:
            raise ValueError(
                f"Unknown source_type: {source_type}. "
                f"Supported types: filesystem, api, database"
            )

    def _convert_extraction_results_to_batches(
        self,
        extraction_result,
        config: ResourceConfig,
    ) -> List[BatchInfo]:
        """
        Convert extraction results (promoted file paths) into BatchInfo objects.

        This implements the dlt pattern: extraction yields data that loading consumes directly,
        without re-discovering what was just extracted.

        Args:
            extraction_result: PromotionResult from extractor
            config: ResourceConfig for context

        Returns:
            List of BatchInfo objects ready for loading
        """
        import uuid
        from datetime import datetime

        batches = []

        # Get lakehouse utils for the source lakehouse
        source_workspace = config.source_config.connection_params.get("workspace_name")
        source_lakehouse = config.source_config.connection_params.get("lakehouse_name")

        lakehouse = lakehouse_utils(
            target_workspace_name=source_workspace,
            target_lakehouse_name=source_lakehouse,
            spark=self.spark,
        )

        for promoted_path in extraction_result.promoted_files:
            # promoted_path is already a full path (could be file or folder)
            # Build ABFSS URI if needed
            if not promoted_path.startswith("abfss://"):
                # Relative path - build full URI
                full_path = f"{lakehouse.lakehouse_files_uri()}{promoted_path.lstrip('/')}"
            else:
                # Already absolute
                full_path = promoted_path

            # Create BatchInfo
            batch_info = BatchInfo(
                batch_id=str(uuid.uuid4()),
                file_paths=[full_path],
                size_bytes=0,  # Size not critical for loading
                modified_time=datetime.now(),  # Use current time
            )

            # Add folder name if this looks like a folder path
            if promoted_path.endswith("/"):
                import os
                folder_name = os.path.basename(promoted_path.rstrip("/"))
                batch_info.folder_name = folder_name

            batches.append(batch_info)

        return batches

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

    def _log_batch_no_data(
        self, config: ResourceConfig, execution_id: str, load_id: str, batch_info
    ) -> None:
        """Log batch with no data"""
        if self.logger_instance:
            self.logger_instance.log_batch_no_data(config, execution_id, load_id, batch_info)
