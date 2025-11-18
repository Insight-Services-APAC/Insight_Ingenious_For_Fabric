# Extraction Framework - Orchestrator
# Orchestrates data extraction from external sources to raw layer

import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List

from pyspark.sql import SparkSession

from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig, FileSystemExtractionParams
from ingen_fab.python_libs.pyspark.ingestion.constants import ExecutionStatus, NoDataHandling
from ingen_fab.python_libs.pyspark.ingestion.exceptions import ExtractionError
from ingen_fab.python_libs.pyspark.ingestion.extractors.filesystem_extractor import (
    FileSystemExtractor,
)
from ingen_fab.python_libs.pyspark.ingestion.extraction_logger import ExtractionLogger
from ingen_fab.python_libs.pyspark.ingestion.logging_utils import ConfigLoggerAdapter
from ingen_fab.python_libs.pyspark.ingestion.results import ResourceExtractionResult

logger = logging.getLogger(__name__)

class ExtractionOrchestrator:
    """
    Orchestrates data extraction across multiple resources.

    Pattern: External source (inbound files, API, database) → Raw layer

    Handles:
    - Extraction from filesystem, API, database sources
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
        self.max_concurrency = max_concurrency

        # Configure logging
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s | %(levelname)-8s | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
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
            "duplicates": 0,
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
                if result.status == ExecutionStatus.FAILED:
                    results["failed"] += 1
                else:
                    results["successful"] += 1
                    # Also count specific states for detail
                    if result.status == ExecutionStatus.DUPLICATE:
                        results["duplicates"] += 1
                    elif result.status == ExecutionStatus.NO_DATA:
                        results["no_data"] += 1

        logger.info(
            f"Extraction complete: {results['successful']} successful, "
            f"{results['failed']} failed, {results['no_data']} no data, "
            f"{results['duplicates']} duplicates"
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

        # Sort results: successful/no-data first, then failed
        successful_results = [r for r in results if r.status != ExecutionStatus.FAILED]
        failed_results = [r for r in results if r.status == ExecutionStatus.FAILED]
        sorted_results = successful_results + failed_results

        # Log all results in sorted order
        for result in sorted_results:
            icon = "✓" if result.status == ExecutionStatus.COMPLETED else "✗" if result.status == ExecutionStatus.FAILED else "○"
            batches = result.batches_processed
            batch_text = f"{batches} batches"

            # Use total_items_count (always files in raw layer)
            total_items = result.total_items_count
            file_text = f"{total_items} files"

            if result.status == ExecutionStatus.FAILED and result.error_message:
                # Failed - log summary at INFO level (ERROR already logged when it happened)
                logger.info(
                    f"{icon} {result.resource_name} (failed) - {result.error_message}"
                )
            else:
                # Successful or no data - log status
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
        # 1. Setup logger and context
        config_logger = ConfigLoggerAdapter(logger, {
            'source_name': config.source_name,
            'config_name': config.resource_name,
        })

        # 2. Initialize result
        start_time = time.time()
        result = ResourceExtractionResult(
            resource_name=config.resource_name,
            status=ExecutionStatus.PENDING,
            started_at=datetime.now(),
        )

        # Extract no_data_handling policy
        params = config.source_extraction_params
        if isinstance(params, dict):
            no_data_handling = params.get("no_data_handling", "allow")
        else:
            no_data_handling = getattr(params, 'no_data_handling', 'allow')

        # 3. Log start
        extract_run_id = self.extraction_logger.log_extraction_config_start(config, execution_id)

        try:
            config_logger.info(f"Extracting: {config.resource_name}")
            config_logger.info(
                f"Source: {config.source_config.source_type} ({config.extract_file_format}) → {config.extract_path}"
            )

            # 4. Execute extraction loop
            extractor = self._get_extractor(config, config_logger)
            metrics = {"extracted": 0, "failed": 0, "duplicate": 0, "no_data": 0, "files": 0, "bytes": 0}

            for batch in extractor.extract():
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
                    promoted_count=batch.promoted_count,
                    failed_count=batch.failed_count,
                    duplicate_count=batch.duplicate_count,
                    duration_ms=batch.duration_ms,
                    error_message=batch.error_message,
                )

                # Update metrics
                if batch.status == ExecutionStatus.COMPLETED:
                    metrics["extracted"] += 1
                    metrics["files"] += batch.file_count
                    metrics["bytes"] += batch.file_size_bytes
                elif batch.status == ExecutionStatus.DUPLICATE:
                    metrics["duplicate"] += 1
                elif batch.status == ExecutionStatus.NO_DATA:
                    metrics["no_data"] += 1
                else:
                    metrics["failed"] += 1
                    metrics["files"] += batch.file_count
                    metrics["bytes"] += batch.file_size_bytes

            # 5. Update result
            result.batches_processed = metrics["extracted"]
            result.batches_failed = metrics["failed"]
            result.total_items_count = metrics["files"]
            result.total_bytes = metrics["bytes"]
            result.completed_at = datetime.now()

            # 6. Determine final status
            if metrics["failed"] > 0:
                result.status = ExecutionStatus.FAILED
                result.error_message = f"{metrics['failed']} batch(es) failed"
            elif metrics["extracted"] > 0:
                result.status = ExecutionStatus.COMPLETED
            elif no_data_handling == NoDataHandling.ALLOW:
                result.status = ExecutionStatus.SKIPPED
            elif no_data_handling == NoDataHandling.FAIL:
                result.status = ExecutionStatus.FAILED
                result.error_message = "No data found (no_data_handling=fail)"
            elif metrics["duplicate"] > 0:
                result.status = ExecutionStatus.DUPLICATE
            else:
                result.status = ExecutionStatus.NO_DATA

            # 7. Log summary
            duration_s = time.time() - start_time
            self._log_completion_summary(config_logger, result, metrics, duration_s)

        except ExtractionError as e:
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.now()
            config_logger.error(f"Extraction failed: {e}")

        except Exception as e:
            result.status = ExecutionStatus.FAILED
            result.error_message = str(e)
            result.completed_at = datetime.now()
            config_logger.exception(f"Unexpected error: {e}")

        finally:
            # 8. Persist final state (guaranteed execution)
            self.extraction_logger.log_extraction_config_end(
                config, execution_id, extract_run_id, result.status, result.error_message
            )

        return result

    def _log_completion_summary(self, config_logger, result, metrics, duration_s):
        """Log the final status summary."""
        match result.status:
            case ExecutionStatus.COMPLETED:
                batch_text = f"{metrics['extracted']} batch{'es' if metrics['extracted'] != 1 else ''}"
                file_text = f"{metrics['files']} file{'s' if metrics['files'] != 1 else ''}"
                config_logger.info(f"Completed: {batch_text}, {file_text} in {duration_s:.1f}s")
            case ExecutionStatus.FAILED:
                config_logger.error(f"Extraction failed: {result.error_message}")
            case ExecutionStatus.DUPLICATE:
                config_logger.info(f"All files were duplicates ({metrics['duplicate']})")
            case ExecutionStatus.NO_DATA:
                config_logger.info(f"No data found for extraction")
            case ExecutionStatus.SKIPPED:
                pass  # Already logged by extractor

    def _get_extractor(self, config: ResourceConfig, logger_instance=None):
        """
        Get appropriate extractor based on source type.

        Args:
            config: ResourceConfig with source_type
            logger_instance: Optional context-aware logger

        Returns:
            Extractor instance (FileSystemExtractor, APIExtractor, DatabaseExtractor)

        Raises:
            NotImplementedError: If source_type extractor is not implemented
        """
        source_type = config.source_config.source_type

        if source_type == "filesystem":
            return FileSystemExtractor(
                resource_config=config,
                extraction_logger=self.extraction_logger,
                logger_instance=logger_instance,
            )
        elif source_type == "api":
            raise NotImplementedError(
                f"API extractor not yet implemented. "
                f"Create APIExtractor in extractors/ directory."
            )
        elif source_type == "database":
            raise NotImplementedError(
                f"Database extractor not yet implemented. "
                f"Create DatabaseExtractor in extractors/ directory."
            )
        else:
            raise ValueError(
                f"Unknown source_type: {source_type}. "
                f"Supported types: filesystem, api, database"
            )

