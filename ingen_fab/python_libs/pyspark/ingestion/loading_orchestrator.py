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
from urllib.parse import urlparse

from pyspark.sql import SparkSession

from ingen_fab.python_libs.common.flat_file_ingestion_utils import (
    ProcessingMetricsUtils,
)
from ingen_fab.python_libs.pyspark.ingestion.config import MetadataColumns, ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    DataQualityRejectionError,
    ErrorContext,
    FileReadError,
    SchemaValidationError,
    WriteError,
)
from ingen_fab.python_libs.pyspark.ingestion.loader import FileLoader
from ingen_fab.python_libs.pyspark.ingestion.loading_logger import LoadingLogger
from ingen_fab.python_libs.pyspark.ingestion.logging_utils import ConfigLoggerAdapter
from ingen_fab.python_libs.pyspark.ingestion.results import (
    BatchInfo,
    ProcessingMetrics,
    ResourceExecutionResult,
)

logger = logging.getLogger(__name__)


# Context classes for cleaner method signatures
class BatchContext:
    """Groups batch-level processing context to reduce parameter count"""
    def __init__(
        self,
        config: ResourceConfig,
        execution_id: str,
        load_run_id: str,
        file_loader: 'FileLoader',
        config_logger: logging.LoggerAdapter,
    ):
        self.config = config
        self.execution_id = execution_id
        self.load_run_id = load_run_id
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

    File Management (Replay-Friendly Pattern):
    - Successful loads: Files STAY in extract_path (no archiving!)
    - Failed loads: Files STAY in extract_path for manual intervention
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
        logger_instance: LoadingLogger,
        max_concurrency: int = 4,
        metadata_columns: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize orchestrator.

        Args:
            spark: Spark session
            logger_instance: LoadingLogger instance for state tracking (required)
            max_concurrency: Maximum number of parallel workers per group
            metadata_columns: Override metadata column names (optional), e.g.:
                {
                    "_raw_created_load_id": "batch_id",
                    "_raw_updated_load_id": "last_batch_id",
                    "_raw_file_path": "source_file",
                    "_raw_created_at": "created_ts",
                    "_raw_updated_at": "modified_ts"
                }
        """
        self.spark = spark
        self.logger_instance = logger_instance
        self.max_concurrency = max_concurrency

        # Parse metadata column overrides (supports 0, 1, or many overrides)
        self.metadata_columns = MetadataColumns.from_dict(metadata_columns)

        # Configure logging
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s | %(levelname)-8s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                force=True
            )

    def _discover_batches(self, config: ResourceConfig, config_logger: logging.LoggerAdapter) -> List[BatchInfo]:
        """
        Discover batches to process from extraction logs (orchestrator owns this logic).

        Queries LoadingLogger for raw data, converts to BatchInfo objects,
        and validates business rules.

        Args:
            config: Resource configuration
            config_logger: Logger with resource context

        Returns:
            List of BatchInfo objects ready to process
        """
        # Query logger for raw data (data access layer)
        df = self.logger_instance.query_pending_extraction_batches(config)
        rows = df.collect()

        if not rows:
            return []

        # Convert to domain objects (orchestrator's job)
        batches = []
        for row in rows:
            # Validation logic (business rule)
            if not row.extract_file_paths or len(row.extract_file_paths) == 0:
                raise ValueError(
                    f"Empty extract_file_paths in batch table for extract_batch_id: {row.extract_batch_id}"
                )

            # Validate all paths are ABFSS URLs
            for path in row.extract_file_paths:
                if not path.startswith("abfss://"):
                    raise ValueError(
                        f"Invalid path in extract_file_paths: {path}. "
                        f"Expected full ABFSS URL (e.g., abfss://workspace@onelake.../Files/raw/...)"
                    )

            # Create BatchInfo
            batch_info = BatchInfo(
                batch_id=str(uuid.uuid4()),
                extract_batch_id=row.extract_batch_id,
                file_paths=list(row.extract_file_paths),
                destination_path=row.extract_file_paths[0],  # First path for compatibility
                size_bytes=row.file_size_bytes or 0,
                modified_time=row.completed_at,
            )

            # Add folder name if this is a folder path (business logic)
            if row.extract_file_paths[0].endswith("/"):
                folder_name = os.path.basename(row.extract_file_paths[0].rstrip("/"))
                batch_info.folder_name = folder_name

            batches.append(batch_info)

        config_logger.info(f"Discovered {len(batches)} batches from extraction logs")
        return batches

    def _recover_stale_batches(self, config: ResourceConfig, config_logger: logging.LoggerAdapter) -> None:
        """
        Recover batches stuck in processing state (crash recovery).

        Orchestrator owns business logic: WHEN to check, WHAT threshold, HOW to handle results.
        LoadingLogger provides data access.

        Args:
            config: Resource configuration
            config_logger: Logger with resource context
        """
        # Business decision: 1 hour threshold
        stale_df = self.logger_instance.query_stale_batches(config, threshold_hours=1)
        stale_rows = stale_df.collect()

        if not stale_rows:
            logger.debug("No stale batches found")
            return

        # Business logic: log warning and recover each batch
        config_logger.warning(f"Found {len(stale_rows)} stale batches, recovering...")

        for row in stale_rows:
            # Call logger to write recovery record (data access)
            self.logger_instance.log_batch_recovery(config, row)
            batch_id = row.load_batch_id if hasattr(row, 'load_batch_id') else row.load_id
            config_logger.info(f"Recovered batch {batch_id}")

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
            "rejected_batches": 0,
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

                # Sum all rejections across resources
                results["rejected_batches"] += result.batches_rejected

        logger.info(
            f"Execution complete: {results['successful']} successful, "
            f"{results['failed']} failed, {results['no_data']} no data, "
            f"{results['rejected_batches']} rejected batches"
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
        rejected = sum(1 for r in results if r.batches_rejected > 0)

        logger.info(f"Execution group {group_num} completed in {duration:.1f}s")
        logger.info(
            f"{len(results)} resource(s) - {completed} completed, {failed} failed, {no_data} no data, {rejected} with rejections"
        )

        # Sort results: successful/no-data first, then failed
        successful_results = [r for r in results if r.status != ExecutionStatus.FAILED]
        failed_results = [r for r in results if r.status == ExecutionStatus.FAILED]
        sorted_results = successful_results + failed_results

        # Log all results in sorted order
        for result in sorted_results:
            icon = "✓" if result.status == ExecutionStatus.COMPLETED else "✗" if result.status == ExecutionStatus.FAILED else "○"
            batch_summary = f"{result.batches_processed} batches"
            if result.batches_rejected > 0:
                batch_summary += f", {result.batches_rejected} rejected"

            if result.status == ExecutionStatus.FAILED and result.error_message:
                # Failed - log summary at INFO level (ERROR already logged when it happened)
                logger.info(f"{icon} {result.resource_name} (failed) - {result.error_message}")
            else:
                # Successful or no data - log status
                logger.info(
                    f"{icon} {result.resource_name} ({result.status}) - {batch_summary}"
                )

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

        # Log resource execution start and capture load_run_id
        load_run_id = self.logger_instance.log_resource_execution_start(config, execution_id)

        try:
            config_logger.info(f"Loading: {config.resource_name}")
            # Consolidate metadata into single line
            target_full = f"{config.target_schema}.{config.target_table}" if config.target_schema else config.target_table
            config_logger.info(
                f"Extract: {config.extract_path} → Target: {target_full} "
                f"({config.extract_file_format}, {config.target_write_mode})"
            )

            # Create FileLoader instance
            file_loader = FileLoader(
                spark=self.spark,
                config=config,
                metadata_columns=self.metadata_columns,
            )

            # Step 1: Crash recovery - recover stale batches before discovering new ones
            self._recover_stale_batches(config, config_logger)

            # Step 2: Discover batches from extraction logs (dlt-style work queue)
            discovered_batches = self._discover_batches(config, config_logger)

            if not discovered_batches:
                config_logger.info("No files to process")
                result.status = ExecutionStatus.NO_DATA
                self.logger_instance.log_resource_execution_completion(config, execution_id, load_run_id, result)
                return result

            # Create batch context to simplify method signatures
            batch_context = BatchContext(
                config=config,
                execution_id=execution_id,
                load_run_id=load_run_id,
                file_loader=file_loader,
                config_logger=config_logger,
            )

            all_metrics, batches_processed = self._process_batches(
                discovered_batches, batch_context
            )

            # Update result with batch counts
            result.batches_processed = batches_processed

            # Finalize result with aggregated metrics and status
            self._finalize_result(result, all_metrics, config, start_time, config_logger)

            # Log resource execution completion
            self.logger_instance.log_resource_execution_completion(config, execution_id, load_run_id, result)

        except DataQualityRejectionError as e:
            # Expected data quality rejection - already logged cleanly at INFO level
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            result.batches_rejected = 1  # Rejection caused the exception
            # NO stack trace, NO ERROR log (already logged at INFO in batch handler)
            self.logger_instance.log_resource_execution_error(config, execution_id, load_run_id, str(e), result)

        except SchemaValidationError as e:
            # Schema validation error - user config issue, already logged cleanly
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            # Log helpful message without stack trace
            config_logger.info(f"Schema validation failed - check schema_columns configuration")
            if e.context and e.context.additional_info.get("schema_error"):
                config_logger.info(f"Details: {e.context.additional_info['schema_error']}")
            self.logger_instance.log_resource_execution_error(config, execution_id, load_run_id, str(e), result)

        except (FileReadError, WriteError) as e:
            # Unexpected system errors - log with stack trace
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            config_logger.exception(f"Loading failed: {e}")
            self.logger_instance.log_resource_execution_error(config, execution_id, load_run_id, str(e), result)

        except Exception as e:
            # Unexpected error
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            config_logger.exception(f"Unexpected error in loading: {e}")
            self.logger_instance.log_resource_execution_error(config, execution_id, load_run_id, str(e), result)

        return result

    def _process_batches(
        self,
        batches: List[BatchInfo],
        ctx: BatchContext,
    ) -> Tuple[List[ProcessingMetrics], int]:
        """
        Process all batches sequentially and return aggregated metrics.

        Args:
            batches: List of BatchInfo objects to process
            ctx: BatchContext with config, execution_id, load_run_id, file_loader, config_logger

        Returns:
            Tuple of (all_metrics, batches_processed)
        """
        all_metrics = []
        batches_processed = 0
        total_batches = len(batches)

        for batch_index, batch_info in enumerate(batches, start=1):
            try:
                # Log batch start to database
                self.logger_instance.log_batch_start(
                    ctx.config,
                    ctx.execution_id,
                    ctx.load_run_id,
                    batch_info.batch_id,
                    batch_info
                )

                # Execute all workflow steps for this batch
                batch_start = time.time()
                combined_metrics = self._process_single_batch(batch_info, ctx)
                batch_duration = time.time() - batch_start

                # Log batch completion with detailed metrics
                completion_message = self._format_batch_completion_message(
                    batch_index, total_batches, combined_metrics, batch_duration
                )
                ctx.config_logger.info(completion_message)

                # Log completion to database
                self.logger_instance.log_batch_completion(
                    ctx.config,
                    ctx.execution_id,
                    ctx.load_run_id,
                    batch_info.batch_id,
                    combined_metrics,
                    batch_info
                )

                # Update extraction log (PENDING → COMPLETED)
                self.logger_instance.update_extraction_batch_load_state(
                    batch_info.extract_batch_id,
                    ctx.config.source_name,
                    ctx.config.resource_name,
                    ExecutionStatus.COMPLETED
                )

                # Track successful batch
                all_metrics.append(combined_metrics)
                batches_processed += 1

            except DataQualityRejectionError as error:
                # Data quality rejection - fail fast to maintain data continuity
                # File moved to error location, extraction log updated to 'failed'

                # Extract rejection reason from exception
                if error.context and error.context.additional_info:
                    rejection_reason = error.context.additional_info.get("rejection_reason") or str(error)
                else:
                    rejection_reason = str(error)

                # Log rejection to database
                self.logger_instance.log_batch_rejected(
                    ctx.config,
                    ctx.execution_id,
                    ctx.load_run_id,
                    batch_info.batch_id,
                    rejection_reason,
                    batch_info
                )

                # Stop processing entire resource to prevent data continuity issues
                raise

            except SchemaValidationError as error:
                # Schema validation error - user config issue
                # File stays in landing, extraction log stays 'pending' for retry
                error_message = str(error)
                self.logger_instance.log_batch_error(
                    ctx.config,
                    ctx.execution_id,
                    ctx.load_run_id,
                    batch_info.batch_id,
                    error_message,
                    batch_info
                )
                raise

            except (WriteError, Exception) as error:
                error_message = str(error)
                self.logger_instance.log_batch_error(
                    ctx.config,
                    ctx.execution_id,
                    ctx.load_run_id,
                    batch_info.batch_id,
                    error_message,
                    batch_info
                )
                self._handle_batch_error(error, batch_info, ctx.config_logger)
                
                # Stop processing if any batch fails
                raise

        return all_metrics, batches_processed

    def _format_batch_completion_message(
        self,
        batch_index: int,
        total_batches: int,
        metrics: ProcessingMetrics,
        duration: float,
    ) -> str:
        """
        Format batch completion log message with metrics breakdown.

        Args:
            batch_index: Current batch number (1-indexed)
            total_batches: Total number of batches
            metrics: Processing metrics for the batch
            duration: Duration in seconds

        Returns:
            Formatted message string: "Loaded batch X/Y: N records (ins/upd/del) in Xs"
        """
        records_formatted = f"{metrics.records_processed:,}"

        # Build operation breakdown (ins/upd/del)
        ops = []
        if metrics.records_inserted > 0:
            ops.append(f"{metrics.records_inserted:,} ins")
        if metrics.records_updated > 0:
            ops.append(f"{metrics.records_updated:,} upd")
        if metrics.records_deleted > 0:
            ops.append(f"{metrics.records_deleted:,} del")

        ops_text = f" ({', '.join(ops)})" if ops else ""
        return f"Loaded batch {batch_index}/{total_batches}: {records_formatted} records{ops_text} in {duration:.2f}s"

    def _process_single_batch(
        self,
        batch_info: BatchInfo,
        ctx: BatchContext,
    ) -> ProcessingMetrics:
        """
        Execute all workflow steps for processing a single batch.

        Workflow:
        1. Load files to staging table (no validation)
        2. Load staging table to target with validation and write (loader adds metadata)

        Args:
            batch_info: Information about the batch to process
            ctx: BatchContext with config, execution_id, load_run_id, file_loader, config_logger

        Returns:
            ProcessingMetrics with batch processing results

        Raises:
            DataQualityRejectionError: If validation fails (corrupt records, duplicates, etc.)
            WriteError: If loading to staging or target table fails
            SchemaValidationError: If schema config is invalid (rare - safety net)
            FileReadError: If reading from staging table fails
        """

        # Step 1: Load files to staging table (no validation)
        raw_result = ctx.file_loader.load_files_to_stg_table(batch_info)

        if raw_result.status == "failed":
            error_message = raw_result.rejection_reason or "Unknown error"
            ctx.config_logger.error(f"Failed to load files to staging table: {error_message}")

            raise WriteError(
                message=f"Failed to load files to staging table: {error_message}",
                context=ErrorContext(
                    resource_name=ctx.config.resource_name,
                    source_name=ctx.config.source_name,
                    batch_id=batch_info.batch_id,
                    file_path=batch_info.file_paths[0] if batch_info.file_paths else "unknown",
                    operation="load_files_to_stg_table",
                ),
            )

        # Log Step 1 success
        raw_rows = raw_result.metrics.source_row_count
        ctx.config_logger.info(f"Step 1/2: Loaded {raw_rows:,} rows to staging table")

        # Step 2: Load from staging table to target with validation and write
        result = ctx.file_loader.load_stg_table_to_target(batch_info)

        # Check for rejection
        if result.status == "rejected":
            rejection_reason = result.rejection_reason
            file_name = os.path.basename(batch_info.file_paths[0]) if batch_info.file_paths else "unknown"

            ctx.config_logger.info(f"Step 2/2: Batch rejected: {file_name}")
            ctx.config_logger.info(f"Reason: {rejection_reason}")

            # Move rejected batch to error location (data file + control file if present)
            self._move_rejected_batch_to_error(batch_info, ctx)

            # Update extraction log (PENDING → FAILED) so it's not picked up again
            self.logger_instance.update_extraction_batch_load_state(
                batch_info.extract_batch_id,
                ctx.config.source_name,
                ctx.config.resource_name,
                ExecutionStatus.FAILED
            )

            raise DataQualityRejectionError(
                message=f"Data quality rejection: {rejection_reason}",
                context=ErrorContext(
                    resource_name=ctx.config.resource_name,
                    source_name=ctx.config.source_name,
                    batch_id=batch_info.batch_id,
                    file_path=batch_info.file_paths[0] if batch_info.file_paths else "unknown",
                    operation="load_stg_table_to_target",
                    additional_info={"rejection_reason": rejection_reason},
                ),
            )

        # Log Step 2 success
        metrics = result.metrics
        target_rows = metrics.target_row_count_after
        ctx.config_logger.info(f"Step 2/2: Wrote {target_rows:,} rows to target table")

        return metrics

    def _extract_lakehouse_relative_path(
        self,
        file_path: str,
        base_path: str,
    ) -> str:
        """
        Extract relative path from either ABFSS URL or relative path.

        Handles both:
        - ABFSS URLs: abfss://container@host/lakehouse.Lakehouse/Files/raw/pe/.../file.dat
        - Relative paths: Files/raw/pe/.../file.dat

        For ABFSS URLs, extracts the path after '.Lakehouse/' and then
        calculates the relative portion after base_path.

        Args:
            file_path: Full ABFSS URL or relative path
            base_path: Base path to strip (e.g., "Files/raw/pe/pe_prom_codes")

        Returns:
            Relative path after base_path (e.g., "ds=2025-11-17/file.dat")
            Falls back to filename only if base_path not found in path
        """
        # Parse the path to extract lakehouse-relative portion
        if file_path.startswith("abfss://"):
            # Parse ABFSS URL to extract path component
            parsed = urlparse(file_path)
            url_path = parsed.path  # e.g., "/lakehouse.Lakehouse/Files/raw/pe/.../file.dat"

            # Extract path after '.Lakehouse/' boundary
            lakehouse_boundary = ".Lakehouse/"
            if lakehouse_boundary in url_path:
                # Split on boundary and take everything after it
                lakehouse_relative = url_path.split(lakehouse_boundary, 1)[1]
            else:
                # Fallback: use entire path without leading slash
                lakehouse_relative = url_path.lstrip("/")
        else:
            # Already a relative path
            lakehouse_relative = file_path

        # Now calculate relative path after base_path
        # Normalize both paths (remove trailing/leading slashes for comparison)
        normalized_base = base_path.strip("/")
        normalized_full = lakehouse_relative.strip("/")

        # Try to find base_path in the full path
        if normalized_base in normalized_full:
            # Split on base path and take everything after it
            relative_path = normalized_full.split(normalized_base, 1)[1].lstrip("/")
        else:
            # Fallback: use just the filename (last component)
            # This handles cases where folder structure differs
            relative_path = os.path.basename(normalized_full)

        return relative_path

    def _move_rejected_batch_to_error(
        self,
        batch_info: BatchInfo,
        ctx: BatchContext,
    ) -> None:
        """
        Move rejected batch files to error location.

        Handles:
        - Data file movement from extract_path to extract_error_path
        - Control file movement (if control_file_pattern configured)

        Note:
            Extraction batch logs are NOT updated - they remain pointing to landing.
            Loading batch logs record the rejection with status='failed'.
            Control file movement failures are logged but don't cause exceptions.

        Args:
            batch_info: Information about the rejected batch
            ctx: Batch processing context
        """
        # Validate destination_path exists
        if not batch_info.destination_path:
            ctx.config_logger.warning("Cannot move rejected batch - destination_path is missing")
            return

        # Build error destination path (preserve folder structure)
        source_path = batch_info.destination_path
        relative_path = self._extract_lakehouse_relative_path(
            file_path=source_path,
            base_path=ctx.config.extract_path,
        )
        error_path = f"{ctx.config.extract_error_path.rstrip('/')}/{relative_path}"

        ctx.config_logger.info(f"Moving rejected file: {source_path} → {error_path}")

        # Move data file to error location
        data_moved = ctx.file_loader.source_lakehouse_utils.move_file(source_path, error_path)

        if data_moved:
            ctx.config_logger.info("Data file moved to error location successfully")

            # Move control file if configured (best-effort, don't fail batch if this fails)
            self._move_control_file_to_error(source_path, error_path, ctx)

        else:
            ctx.config_logger.warning("Failed to move data file to error location - file remains in landing")

    def _move_control_file_to_error(
        self,
        data_source_path: str,
        data_error_path: str,
        ctx: BatchContext,
    ) -> None:
        """
        Move control file to error location (best-effort).

        Derives control file paths from data file paths and attempts to move.
        Failures are logged but don't raise exceptions (graceful degradation).

        Args:
            data_source_path: Data file path in landing location
            data_error_path: Data file path in error location
            ctx: Batch processing context
        """
        # Check if control files are configured for this resource
        control_pattern = ctx.config.source_extraction_params.get("control_file_pattern")
        if not control_pattern:
            # No control file configuration - nothing to do
            return

        try:
            # Derive control file paths from data file paths
            control_source = self._derive_control_file_path(data_source_path, control_pattern)
            control_error = self._derive_control_file_path(data_error_path, control_pattern)

            if not control_source or not control_error:
                ctx.config_logger.debug("Could not derive control file paths - skipping control file move")
                return

            # Attempt to move control file
            control_moved = ctx.file_loader.source_lakehouse_utils.move_file(
                control_source, control_error
            )

            if control_moved:
                control_filename = os.path.basename(control_source)
                ctx.config_logger.debug(f"Control file moved: {control_filename}")
            else:
                # File might not exist or already moved - log at debug level
                ctx.config_logger.debug("Control file not found or could not be moved")

        except Exception as e:
            # Control file movement is best-effort - log and continue
            ctx.config_logger.debug(f"Could not move control file: {e}")

    def _derive_control_file_path(
        self,
        data_file_path: str,
        control_pattern: str,
    ) -> Optional[str]:
        """
        Derive control file path from data file path using control_file_pattern.

        Supports two pattern modes:
        - Per-file mode: "{basename}.ctl" → derives from data filename
          Example: sales_20251117.dat → sales_20251117.ctl
        - Per-folder mode: "control.txt" → fixed name in same directory
          Example: sales_20251117.dat → control.txt

        Args:
            data_file_path: Full path to data file
            control_pattern: Control file pattern from extraction params

        Returns:
            Full path to control file, or None if pattern is invalid
        """
        if not control_pattern or not data_file_path:
            return None

        # Extract directory and filename from data file path
        directory = os.path.dirname(data_file_path)
        filename = os.path.basename(data_file_path)

        # Determine control file name based on pattern mode
        if "{basename}" in control_pattern:
            # Per-file mode: Replace {basename} with data file basename (no extension)
            basename = os.path.splitext(filename)[0]
            control_filename = control_pattern.replace("{basename}", basename)
        else:
            # Per-folder mode: Use pattern as fixed filename
            control_filename = control_pattern

        # Build full control file path
        return f"{directory}/{control_filename}" if directory else control_filename

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
        # Determine final status based on batch outcomes
        if result.batches_failed > 0:
            # System failures = FAILED status
            result.status = ExecutionStatus.FAILED
        elif result.batches_processed > 0:
            # All batches succeeded (rejections don't cause failure) = COMPLETED status
            result.status = ExecutionStatus.COMPLETED
        else:
            # No batches discovered or all skipped = NO_DATA status
            result.status = ExecutionStatus.NO_DATA

        # Aggregate metrics if we have any
        if all_metrics:
            result.metrics = ProcessingMetricsUtils.merge_metrics(
                all_metrics, config.target_write_mode
            )

            end_time = time.time()
            result.metrics.total_duration_ms = int((end_time - start_time) * 1000)

            # Consolidated resource summary (detail is in batch logs)
            duration_s = result.metrics.total_duration_ms / 1000
            batch_text = f"{result.batches_processed} batch{'es' if result.batches_processed != 1 else ''}"
            config_logger.info(f"Completed: {batch_text} in {duration_s:.1f}s")
