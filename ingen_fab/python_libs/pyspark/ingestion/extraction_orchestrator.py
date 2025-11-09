# Extraction Framework - Orchestrator
# Orchestrates data extraction from external sources to raw layer

import logging
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List

from pyspark.sql import SparkSession

from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.ingestion.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.ingestion.exceptions import (
    DuplicateFilesError,
    ExtractionError,
)
from ingen_fab.python_libs.pyspark.ingestion.extractors.filesystem_extractor import (
    FileSystemExtractor,
)
from ingen_fab.python_libs.pyspark.ingestion.extraction_logging import (
    ExtractionLogger,
    ExtractionResult,
)
from ingen_fab.python_libs.pyspark.ingestion.logging_utils import ConfigLoggerAdapter

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
        extraction_logger: ExtractionLogger = None,
        max_concurrency: int = 4,
    ):
        """
        Initialize orchestrator.

        Args:
            spark: Spark session
            extraction_logger: ExtractionLogger instance for state tracking
            max_concurrency: Maximum number of parallel workers per group
        """
        self.spark = spark
        self.extraction_logger = extraction_logger
        self.max_concurrency = max_concurrency

        # Configure logging
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(levelname)s - %(message)s',
                force=True
            )

        # Suppress verbose Azure SDK and adlfs logging (must be after basicConfig)
        logging.getLogger("azure").setLevel(logging.WARNING)
        logging.getLogger("azure.core").setLevel(logging.WARNING)
        logging.getLogger("azure.core.pipeline").setLevel(logging.WARNING)
        logging.getLogger("azure.core.pipeline.policies").setLevel(logging.WARNING)
        logging.getLogger("azure.identity").setLevel(logging.WARNING)
        logging.getLogger("azure.identity._credentials").setLevel(logging.WARNING)
        logging.getLogger("adlfs").setLevel(logging.WARNING)
        logging.getLogger("adlfs.spec").setLevel(logging.WARNING)
        logging.getLogger("fsspec").setLevel(logging.WARNING)
        logging.getLogger("fsspec.spec").setLevel(logging.WARNING)
        logging.getLogger("adlfs.utils").setLevel(logging.WARNING)

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
                if result["status"] == ExecutionStatus.COMPLETED:
                    results["successful"] += 1
                elif result["status"] == ExecutionStatus.FAILED:
                    results["failed"] += 1
                else:
                    results["no_data"] += 1

        logger.info(
            f"Extraction complete: {results['successful']} successful, "
            f"{results['failed']} failed, {results['no_data']} no data"
        )

        return results

    def _process_group_parallel(
        self,
        configs: List[ResourceConfig],
        execution_id: str,
        group_num: int,
    ) -> List[Dict[str, Any]]:
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
                        error_result = {
                            "resource_name": config.resource_name,
                            "status": ExecutionStatus.FAILED,
                            "files_extracted": 0,
                            "files_failed": 0,
                            "error_message": f"Thread execution error: {str(e)}",
                        }
                        results.append(error_result)
                        logger.error(f"Failed: {config.resource_name} - {str(e)}")

        # Log summary
        end_time = time.time()
        duration = end_time - start_time

        completed = sum(1 for r in results if r["status"] == ExecutionStatus.COMPLETED)
        failed = sum(1 for r in results if r["status"] == ExecutionStatus.FAILED)
        no_data = sum(1 for r in results if r["status"] == ExecutionStatus.NO_DATA)

        logger.info(f"Execution group {group_num} completed in {duration:.1f}s")
        logger.info(
            f"{len(results)} resource(s) - {completed} completed, {failed} failed, {no_data} no data"
        )

        # Log individual results with batch and file counts
        for result in results:
            icon = "✓" if result["status"] == ExecutionStatus.COMPLETED else "✗" if result["status"] == ExecutionStatus.FAILED else "○"
            batches = result.get('files_extracted', 0)
            batch_text = f"{batches} batch{'es' if batches != 1 else ''}"

            # Try to get total file count from result if available
            total_files = result.get('total_files_count', batches)
            file_text = f"{total_files} file{'s' if total_files != 1 else ''}"

            logger.info(
                f"{icon} {result['resource_name']} (completed) - {batch_text}, {file_text}"
            )
            if result.get("error_message"):
                logger.error(f"  Error: {result['error_message']}")

        return results

    def process_single_resource(
        self,
        config: ResourceConfig,
        execution_id: str,
    ) -> Dict[str, Any]:
        """
        Process a single resource configuration (extraction only).

        Args:
            config: ResourceConfig to process
            execution_id: Unique execution identifier

        Returns:
            Dictionary with extraction outcome
        """
        # Create logger adapter with resource context
        config_logger = ConfigLoggerAdapter(logger, {
            'source_name': config.source_name,
            'config_name': config.resource_name,
        })

        start_time = time.time()
        result = {
            "resource_name": config.resource_name,
            "status": ExecutionStatus.PENDING,
            "files_extracted": 0,
            "files_failed": 0,
            "total_files_count": 0,
        }

        # Log extraction start and capture extract_run_id
        extract_run_id = self._log_extraction_config_start(config, execution_id)

        try:
            config_logger.info(f"Extracting: {config.resource_name}")
            config_logger.info(
                f"Source: {config.source_config.source_type} → "
                f"Raw: {config.raw_landing_path} ({config.file_format})"
            )

            # Get extractor and run extraction
            extractor = self._get_extractor(config, config_logger)
            extraction_result = extractor.extract()  # FileSystemExtractionResult

            # Check for validation failure
            if extraction_result.error_message:
                result["status"] = ExecutionStatus.FAILED
                result["error_message"] = extraction_result.error_message
                config_logger.error(f"Extraction validation failed: {extraction_result.error_message}")

                extraction_metrics = ExtractionResult()
                self._log_extraction_config_error(config, execution_id, extract_run_id, extraction_result.error_message, extraction_metrics)
                return result

            # No files found
            if extraction_result.extracted_count == 0 and extraction_result.failed_count == 0:
                config_logger.info(f"No files found in {config.source_config.connection_params.get('inbound_path', 'source')}")
                result["status"] = ExecutionStatus.NO_DATA
                self._log_extraction_config_no_data(config, execution_id, extract_run_id)
                return result

            # Warn about failures
            if extraction_result.failed_count > 0:
                config_logger.warning(
                    f"Extraction had {extraction_result.failed_count} failures, "
                    f"{extraction_result.extracted_count} successful"
                )

            # Log each extracted batch
            if extraction_result.extracted_count > 0:
                for source_path, destination_path, duration_ms in extraction_result.extracted_files:
                    extraction_id_uuid = str(uuid.uuid4())
                    self._log_extraction_batch(
                        config=config,
                        execution_id=execution_id,
                        extract_run_id=extract_run_id,
                        extraction_id=extraction_id_uuid,
                        source_path=source_path,
                        destination_path=destination_path,
                        duration_ms=duration_ms,
                        status="completed",
                    )

            # Update result
            result["files_extracted"] = extraction_result.extracted_count
            result["files_failed"] = extraction_result.failed_count
            result["total_files_count"] = extraction_result.total_files_count
            result["status"] = ExecutionStatus.COMPLETED if extraction_result.failed_count == 0 else ExecutionStatus.FAILED

            # Calculate metrics
            end_time = time.time()
            duration_ms = int((end_time - start_time) * 1000)

            # Map FileSystemExtractionResult to generic ExtractionResult
            # Note: Duplicates are not logged separately - they're just not extracted
            extraction_metrics = ExtractionResult(
                batches_extracted=extraction_result.extracted_count,
                batches_failed=extraction_result.failed_count,
                items_count=extraction_result.total_files_count,  # Total files across all batches
                total_bytes=0,  # Could sum from extraction_result if available
                duration_ms=duration_ms,
            )

            # Log completion
            self._log_extraction_config_completion(config, execution_id, extract_run_id, extraction_metrics)

            # Consolidated completion message
            total_files = extraction_result.total_files_count if extraction_result.total_files_count > 0 else extraction_result.extracted_count
            batch_text = f"{result['files_extracted']} batch{'es' if result['files_extracted'] != 1 else ''}"
            file_text = f"{total_files} file{'s' if total_files != 1 else ''}"
            config_logger.info(f"Completed: {batch_text}, {file_text} in {duration_ms / 1000:.1f}s")

        except DuplicateFilesError as e:
            result["status"] = ExecutionStatus.FAILED
            result["error_message"] = str(e)
            config_logger.exception(f"Duplicate files detected: {e}")
            extraction_metrics = ExtractionResult()
            self._log_extraction_config_error(config, execution_id, extract_run_id, str(e), extraction_metrics)

        except ExtractionError as e:
            result["status"] = ExecutionStatus.FAILED
            result["error_message"] = str(e)
            config_logger.exception(f"Extraction failed: {e}")
            extraction_metrics = ExtractionResult()
            self._log_extraction_config_error(config, execution_id, extract_run_id, str(e), extraction_metrics)

        except Exception as e:
            result["status"] = ExecutionStatus.FAILED
            result["error_message"] = str(e)
            config_logger.exception(f"Unexpected error in extraction: {e}")
            extraction_metrics = ExtractionResult()
            self._log_extraction_config_error(config, execution_id, extract_run_id, str(e), extraction_metrics)

        return result

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
                spark=self.spark,
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

    # ========================================================================
    # LOGGING METHODS (delegate to ExtractionLogger if provided)
    # ========================================================================

    def _log_extraction_config_start(self, config: ResourceConfig, execution_id: str) -> str:
        """Log config extraction start and return extract_run_id"""
        if self.extraction_logger:
            return self.extraction_logger.log_extraction_config_start(config, execution_id)
        return str(uuid.uuid4())  # Generate ID even if no logger (for consistency)

    def _log_extraction_config_completion(
        self, config: ResourceConfig, execution_id: str, extract_run_id: str, result: ExtractionResult
    ) -> None:
        """Log config extraction completion"""
        if self.extraction_logger:
            self.extraction_logger.log_extraction_config_completion(config, execution_id, extract_run_id, result)

    def _log_extraction_config_error(
        self, config: ResourceConfig, execution_id: str, extract_run_id: str, error_msg: str, result: ExtractionResult
    ) -> None:
        """Log config extraction error"""
        if self.extraction_logger:
            self.extraction_logger.log_extraction_config_error(config, execution_id, extract_run_id, error_msg, result)

    def _log_extraction_config_no_data(self, config: ResourceConfig, execution_id: str, extract_run_id: str) -> None:
        """Log config extraction with no data"""
        if self.extraction_logger:
            self.extraction_logger.log_extraction_config_no_data(config, execution_id, extract_run_id)

    def _log_extraction_batch(
        self,
        config: ResourceConfig,
        execution_id: str,
        extract_run_id: str,
        extraction_id: str,
        destination_path: str,
        status: str,
        source_path: str = None,
        file_count: int = 1,
        duration_ms: int = None,
    ) -> None:
        """Log extraction batch"""
        if self.extraction_logger:
            self.extraction_logger.log_extraction_batch(
                config=config,
                execution_id=execution_id,
                extract_run_id=extract_run_id,
                extraction_id=extraction_id,
                status=status,
                destination_path=destination_path,
                source_path=source_path,
                file_count=file_count,
                duration_ms=duration_ms,
            )
