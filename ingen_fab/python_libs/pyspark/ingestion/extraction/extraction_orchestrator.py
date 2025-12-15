# Extraction Framework - Orchestrator
# Orchestrates data extraction from external sources to raw layer

import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List

from pyspark.sql import SparkSession

from ingen_fab.python_libs.pyspark.ingestion.common.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.common.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.ingestion.common.exceptions import ExtractionError
from ingen_fab.python_libs.pyspark.ingestion.common.logging_utils import (
    ResourceContext,
    resource_context,
)
from ingen_fab.python_libs.pyspark.ingestion.common.results import (
    ResourceExtractionResult,
)
from ingen_fab.python_libs.pyspark.ingestion.extraction.extraction_logger import (
    ExtractionLogger,
)
from ingen_fab.python_libs.pyspark.ingestion.extraction.extractors.base_extractor import (
    BaseExtractor,
)

logger = logging.getLogger(__name__)

class ExtractionOrchestrator:
    """
    Orchestrates data extraction across multiple resources.

    Pattern: External source (inbound files) → Raw layer

    Handles:
    - Extraction from filesystem sources
    - Execution grouping and sequencing
    - Parallel processing within groups
    - State tracking via ExtractionLogger
    - Error handling
    """

    def __init__(
        self,
        spark: SparkSession,
        extraction_logger: ExtractionLogger,
        max_concurrency: int = 4,
    ):
        """
        Initialize orchestrator.

        Args:
            spark: Spark session
            extraction_logger: ExtractionLogger instance for state tracking (required)
            max_concurrency: Maximum number of parallel workers per group
        """
        self.spark = spark
        self.extraction_logger = extraction_logger
        self.max_concurrency = max_concurrency if max_concurrency is not None else 4

    def process_resources(
        self,
        configs: List[ResourceConfig],
        execution_id: str,
        is_retry: bool = False,
    ) -> Dict[str, Any]:
        """
        Process multiple resource configurations.

        Args:
            configs: List of ResourceConfig objects to process
            execution_id: Unique execution identifier for this run
            is_retry: If True, only process configs whose last extraction failed

        Returns:
            Dictionary with execution summary
        """
        # Filter to failed configs if retry mode
        if is_retry:
            failed_keys = self.extraction_logger.get_failed_resource_keys(configs)
            configs = [c for c in configs if (c.source_name, c.resource_name) in failed_keys]

        results = {
            "success": True,
            "execution_id": execution_id,
            "total_resources": len(configs),
            "results": [],
        }

        if not configs:
            logger.info("No resources to process")
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
            for result in group_results:
                # Convert to dictionary and append
                results["results"].append(result.to_dict())

        # Count by status and determine overall success
        success_count = sum(1 for r in results["results"] if r["status"] == "success")
        warning_count = sum(1 for r in results["results"] if r["status"] == "warning")
        error_count = sum(1 for r in results["results"] if r["status"] == "error")

        results["success"] = error_count == 0

        logger.info(
            f"Extraction complete: {success_count} successful, "
            f"{warning_count} warnings, "
            f"{error_count} failed"
        )

        return results

    def _process_group_parallel(
        self,
        configs: List[ResourceConfig],
        execution_id: str,
        group_num: int,
    ) -> List[ResourceExtractionResult]:
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
                        error_result = ResourceExtractionResult(
                            resource_name=config.resource_name,
                            source_name=config.source_name,
                            status=ExecutionStatus.ERROR,
                            batches_processed=0,
                            batches_failed=0,
                            error_message=f"Thread execution error: {str(e)}",
                        )
                        results.append(error_result)
                        logger.error(f"Failed: {config.resource_name} - {str(e)}")

        # Log summary
        end_time = time.time()
        duration = end_time - start_time

        success_count = sum(1 for r in results if r.status == ExecutionStatus.SUCCESS)
        warning_count = sum(1 for r in results if r.status == ExecutionStatus.WARNING)
        error_count = sum(1 for r in results if r.status == ExecutionStatus.ERROR)

        logger.info(f"Execution group {group_num} completed in {duration:.1f}s")
        logger.info(
            f"{len(results)} resource(s) - {success_count} success, {warning_count} warning, {error_count} error"
        )

        # Sort results: success/warning first, then errors
        successful_results = [r for r in results if r.status != ExecutionStatus.ERROR]
        error_results = [r for r in results if r.status == ExecutionStatus.ERROR]
        sorted_results = successful_results + error_results

        # Log all results in sorted order
        for result in sorted_results:
            icon = "✓" if result.status == ExecutionStatus.SUCCESS else "✗" if result.status == ExecutionStatus.ERROR else "⚠"
            batches = result.batches_processed
            batch_text = f"{batches} batches"

            # Use total_items_count (always files in raw layer)
            total_items = result.total_items_count
            file_text = f"{total_items} files"

            if result.status == ExecutionStatus.ERROR and result.error_message:
                # Error - log summary at INFO level (ERROR already logged when it happened)
                logger.info(
                    f"{icon} {result.resource_name} (error) - {result.error_message}"
                )
            elif result.status == ExecutionStatus.WARNING and result.error_message:
                # Warning - show warning message for context
                logger.info(
                    f"{icon} {result.resource_name} (warning) - {result.error_message}"
                )
            else:
                # Success - show metrics
                logger.info(
                    f"{icon} {result.resource_name} ({result.status}) - {batch_text}, {file_text}"
                )

        return results

    def process_single_resource(
        self,
        config: ResourceConfig,
        execution_id: str,
    ) -> ResourceExtractionResult:
        """
        Process a single resource configuration (extraction only).

        Args:
            config: ResourceConfig to process
            execution_id: Unique execution identifier

        Returns:
            ResourceExtractionResult with extraction outcome
        """
        # 1. Set logging context (automatically prefixes all log messages)
        token = resource_context.set(ResourceContext(
            source_name=config.source_name,
            resource_name=config.resource_name,
        ))

        # 2. Initialize result
        start_time = time.time()
        result = ResourceExtractionResult(
            resource_name=config.resource_name,
            source_name=config.source_name,
            status=ExecutionStatus.PENDING,
            started_at=datetime.now(),
        )

        # Log start
        extract_run_id = self.extraction_logger.log_extraction_config_start(config, execution_id)

        try:
            logger.info(f"Extracting: {config.resource_name}")
            logger.info(
                f"Source: {config.source_config.source_type} ({config.extract_file_format_params.file_format}) → {config.extract_path}"
            )

            # 4. Execute extraction loop
            extractor = BaseExtractor.create(config, self.extraction_logger)
            metrics = {"extracted": 0, "failed": 0, "warning": 0, "files": 0, "bytes": 0}
            all_batches = []  # Track batches to extract error messages

            for batch in extractor.extract():
                all_batches.append(batch)  # Track for status determination

                # Log batch
                self.extraction_logger.log_extraction_batch(
                    config=config,
                    execution_id=execution_id,
                    extract_run_id=extract_run_id,
                    extraction_id=batch.extraction_id,
                    source_path=batch.source_path,
                    extract_file_paths=batch.extract_file_paths,
                    status=batch.status,
                    file_count=batch.file_count,
                    file_size_bytes=batch.file_size_bytes,
                    duration_ms=batch.duration_ms,
                    error_message=batch.error_message,
                )

                # Update metrics (generic - no knowledge of duplicates, no_data, etc.)
                if batch.status == ExecutionStatus.SUCCESS:
                    metrics["extracted"] += 1
                    metrics["files"] += batch.file_count
                    metrics["bytes"] += batch.file_size_bytes
                elif batch.status == ExecutionStatus.WARNING:
                    metrics["warning"] += 1
                elif batch.status == ExecutionStatus.ERROR:
                    metrics["failed"] += 1
                    metrics["files"] += batch.file_count
                    metrics["bytes"] += batch.file_size_bytes

            # 5. Update result
            result.batches_processed = metrics["extracted"]
            result.batches_failed = metrics["failed"]
            result.total_items_count = metrics["files"]
            result.total_bytes = metrics["bytes"]
            result.completed_at = datetime.now()

            # 6. Determine final status and collect all unique messages
            if metrics["failed"] > 0:
                # Any failures = overall failure
                result.status = ExecutionStatus.ERROR
                # Collect all unique error messages
                error_messages = [b.error_message for b in all_batches if b.status == ExecutionStatus.ERROR and b.error_message]
                result.error_message = "; ".join(dict.fromkeys(error_messages)) if error_messages else "Unknown error"
            elif metrics["extracted"] > 0:
                # At least one success = overall success (warnings ignored)
                result.status = ExecutionStatus.SUCCESS
                result.error_message = None
            elif metrics["warning"] > 0:
                # Only warnings, no successes
                result.status = ExecutionStatus.WARNING
                # Collect all unique warning messages
                warning_messages = [b.error_message for b in all_batches if b.status == ExecutionStatus.WARNING and b.error_message]
                result.error_message = "; ".join(dict.fromkeys(warning_messages)) if warning_messages else "Warnings occurred"
            else:
                # No batches yielded (shouldn't happen with new design, but safe default)
                result.status = ExecutionStatus.SUCCESS
                result.error_message = None

            # 7. Log summary
            duration_s = time.time() - start_time
            self._log_completion_summary(result, metrics, duration_s)

        except ExtractionError as e:
            result.status = ExecutionStatus.ERROR
            result.error_message = str(e)
            result.completed_at = datetime.now()
            logger.error(f"Extraction failed: {e}")

        except Exception as e:
            result.status = ExecutionStatus.ERROR
            result.error_message = str(e)
            result.completed_at = datetime.now()
            logger.exception(f"Unexpected error: {e}")

        finally:
            # 8. Persist final state (guaranteed execution)
            self.extraction_logger.log_extraction_config_end(
                config, execution_id, extract_run_id, result.status, result.error_message
            )
            # Reset context
            resource_context.reset(token)

        return result

    def _log_completion_summary(self, result, metrics, duration_s):
        """Log the final status summary."""
        match result.status:
            case ExecutionStatus.SUCCESS:
                if metrics['extracted'] > 0:
                    batch_text = f"{metrics['extracted']} batch{'es' if metrics['extracted'] != 1 else ''}"
                    file_text = f"{metrics['files']} file{'s' if metrics['files'] != 1 else ''}"
                    logger.info(f"Success: {batch_text}, {file_text} in {duration_s:.1f}s")
                else:
                    logger.info("Success: No data to extract (allowed)")
            case ExecutionStatus.WARNING:
                logger.warning(f"Warning: {result.error_message}")
            case ExecutionStatus.ERROR:
                logger.error(f"Error: {result.error_message}")


