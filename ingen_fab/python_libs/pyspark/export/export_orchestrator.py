"""ExportOrchestrator for orchestrating data exports."""

from __future__ import annotations

import logging
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingen_fab.python_libs.pyspark.export.common.config import ExportConfig
from ingen_fab.python_libs.pyspark.export.common.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.export.common.results import ExportResult, ExportMetrics
from ingen_fab.python_libs.pyspark.export.export_logger import ExportLogger
from ingen_fab.python_libs.pyspark.export.exporters.base_exporter import BaseExporter
from ingen_fab.python_libs.pyspark.export.writers.file_writer import FileWriter

logger = logging.getLogger(__name__)


class ExportOrchestrator:
    """
    Orchestrates data exports from source tables to Lakehouse Files.

    Handles:
    - Execution group ordering (sequential between groups)
    - Parallel processing within groups
    - State tracking and logging
    - Error handling
    """

    def __init__(
        self,
        spark: SparkSession,
        export_logger: ExportLogger,
        max_concurrency: int = 4,
    ):
        """
        Initialize ExportOrchestrator.

        Args:
            spark: SparkSession instance
            export_logger: ExportLogger instance for state tracking
            max_concurrency: Maximum parallel exports within a group
        """
        self.spark = spark
        self.export_logger = export_logger
        self.max_concurrency = max_concurrency
        self.logger = logger

    def process_exports(
        self,
        configs: List[ExportConfig],
        execution_id: str = None,
        is_retry: bool = False,
        run_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Process multiple export configurations.

        Args:
            configs: List of ExportConfig objects
            execution_id: Unique execution identifier (generated if not provided)
            is_retry: If True, only process failed exports
            run_date: Date for period calculations ("YYYY-MM-DD"). Defaults to today.

        Returns:
            Dictionary with execution summary
        """
        execution_id = execution_id or str(uuid.uuid4())
        effective_run_date = datetime.strptime(run_date, "%Y-%m-%d").date() if run_date else date.today()

        # Filter to failed configs if retry mode
        if is_retry:
            failed_keys = self.export_logger.get_failed_export_keys(
                [(c.export_group_name, c.export_name) for c in configs]
            )
            configs = [c for c in configs if (c.export_group_name, c.export_name) in failed_keys]
            self.logger.info(f"Retry mode: processing {len(configs)} failed exports")

        results = {
            "success": True,
            "execution_id": execution_id,
            "total_exports": len(configs),
            "successful": 0,
            "failed": 0,
            "results": [],
        }

        if not configs:
            self.logger.info("No exports to process")
            return results

        self.logger.info(f"Starting export processing for {len(configs)} configurations")

        # Group by execution_group
        execution_groups = defaultdict(list)
        for config in configs:
            execution_groups[config.execution_group].append(config)

        # Process groups sequentially
        for group_num in sorted(execution_groups.keys()):
            group_configs = execution_groups[group_num]
            self.logger.info(
                f"Processing execution group {group_num} with {len(group_configs)} exports"
            )

            group_results = self._process_group_parallel(group_configs, execution_id, group_num, effective_run_date)

            for result in group_results:
                results["results"].append(result.to_dict())
                if result.status == ExecutionStatus.SUCCESS:
                    results["successful"] += 1
                else:
                    results["failed"] += 1

        # Determine overall success
        results["success"] = results["failed"] == 0

        self.logger.info(
            f"Export processing complete: {results['successful']} successful, "
            f"{results['failed']} failed out of {results['total_exports']}"
        )

        return results

    def _process_group_parallel(
        self,
        configs: List[ExportConfig],
        execution_id: str,
        group_num: int,
        run_date: date,
    ) -> List[ExportResult]:
        """Process a group of exports in parallel."""
        results = []

        if len(configs) == 1:
            # Single export - no need for threading
            result = self.process_single_export(configs[0], execution_id, run_date)
            results = [result]
        else:
            # Multiple exports - use thread pool
            max_workers = min(len(configs), self.max_concurrency)
            self.logger.info(f"Processing group {group_num} with {max_workers} workers")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_config = {
                    executor.submit(self.process_single_export, config, execution_id, run_date): config
                    for config in configs
                }

                for future in as_completed(future_to_config):
                    config = future_to_config[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        self.logger.exception(f"Unexpected error processing {config.export_name}: {e}")
                        error_result = ExportResult(
                            export_group_name=config.export_group_name,
                            export_name=config.export_name,
                            status=ExecutionStatus.ERROR,
                            error_message=str(e),
                        )
                        results.append(error_result)

        return results

    def process_single_export(
        self,
        config: ExportConfig,
        execution_id: str,
        run_date: date,
    ) -> ExportResult:
        """
        Process a single export configuration.

        Args:
            config: Export configuration
            execution_id: Master execution ID
            run_date: Date for period calculations

        Returns:
            ExportResult with status and metrics
        """
        start_time = time.time()
        export_run_id = str(uuid.uuid4())
        started_at = datetime.utcnow()

        result = ExportResult(
            export_group_name=config.export_group_name,
            export_name=config.export_name,
            status=ExecutionStatus.PENDING,
            started_at=started_at,
        )

        # Log start
        self.export_logger.log_export_start(config, execution_id, export_run_id)

        try:
            self.logger.info(f"Starting export: {config.export_name}")

            # 1. Get watermark for incremental exports
            watermark_value = None
            if config.extract_type == "incremental" and config.incremental_column:
                watermark_value = self.export_logger.get_watermark(
                    config.export_group_name,
                    config.export_name
                )
                if watermark_value:
                    self.logger.info(f"Incremental export with watermark: {watermark_value}")
                else:
                    self.logger.info("Incremental export: first run (no watermark)")

            # 2. Create exporter and read source data (with optional watermark filter)
            exporter = BaseExporter.create(config, self.spark)
            df = exporter.read_source(watermark_value=watermark_value)

            row_count = df.count()
            self.logger.info(f"Read {row_count:,} rows from source for {config.export_name}")

            # 2. Create file writer
            file_writer = FileWriter(config=config, run_date=run_date)

            # 3. Write to files
            write_result = file_writer.write(df, export_run_id)

            if not write_result.success:
                raise Exception(write_result.error_message)

            # 4. Calculate metrics and update result
            end_time = time.time()
            duration_ms = int((end_time - start_time) * 1000)

            result.status = ExecutionStatus.SUCCESS
            result.metrics = ExportMetrics(
                rows_exported=write_result.rows_written,
                files_created=len(write_result.file_paths),
                total_bytes=write_result.bytes_written,
                duration_ms=duration_ms,
            )
            result.file_paths = write_result.file_paths
            result.trigger_file_path = write_result.trigger_file_path
            result.completed_at = datetime.utcnow()

            self.logger.info(
                f"Export {config.export_name} completed: "
                f"{write_result.rows_written:,} rows to {len(write_result.file_paths)} file(s) "
                f"in {duration_ms/1000:.2f}s"
            )

            # Log completion
            self.export_logger.log_export_completion(config, execution_id, export_run_id, result)

            # 5. Update watermark for incremental exports (after successful write)
            if config.extract_type == "incremental" and config.incremental_column:
                self._update_export_watermark(df, config, export_run_id)

        except Exception as e:
            end_time = time.time()
            duration_ms = int((end_time - start_time) * 1000)

            result.status = ExecutionStatus.ERROR
            result.error_message = str(e)
            result.metrics = ExportMetrics(duration_ms=duration_ms)
            result.completed_at = datetime.utcnow()

            self.logger.exception(f"Export {config.export_name} failed: {e}")
            self.export_logger.log_export_error(config, execution_id, export_run_id, str(e), result)

        return result

    def _update_export_watermark(
        self,
        df: DataFrame,
        config: ExportConfig,
        export_run_id: str
    ):
        """
        Calculate max value from exported data and update watermark.

        Args:
            df: DataFrame that was exported
            config: Export configuration
            export_run_id: Export run ID for audit
        """
        if not config.incremental_column:
            return

        try:
            max_val = df.agg(F.max(F.col(config.incremental_column))).collect()[0][0]

            if max_val is not None:
                # Format to ISO string based on type
                if isinstance(max_val, datetime):
                    watermark_str = max_val.isoformat()
                elif isinstance(max_val, date):
                    watermark_str = max_val.isoformat()
                else:
                    watermark_str = str(max_val)

                self.export_logger.update_watermark(
                    config.export_group_name,
                    config.export_name,
                    config.incremental_column,
                    watermark_str,
                    export_run_id,
                )
                self.logger.info(f"Updated watermark to: {watermark_str}")
            else:
                self.logger.warning("No max value found for watermark update (empty result)")
        except Exception as e:
            self.logger.warning(f"Failed to update watermark: {e}")
