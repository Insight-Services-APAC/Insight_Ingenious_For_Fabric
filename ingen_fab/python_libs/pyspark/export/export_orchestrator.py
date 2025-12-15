"""ExportOrchestrator for orchestrating data exports."""

from __future__ import annotations

import logging
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingen_fab.python_libs.pyspark.export.common.config import ExportConfig
from ingen_fab.python_libs.pyspark.export.common.constants import ExecutionStatus
from ingen_fab.python_libs.pyspark.export.common.param_resolver import (
    Params,
    resolve_params,
)
from ingen_fab.python_libs.pyspark.export.common.results import (
    ExportMetrics,
    ExportResult,
)
from ingen_fab.python_libs.pyspark.export.export_logger import ExportLogger
from ingen_fab.python_libs.pyspark.export.exporters.base_exporter import BaseExporter
from ingen_fab.python_libs.pyspark.export.writers.export_file_writer import (
    ExportFileWriter,
)

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

    def _get_unique_target_lakehouses(
        self,
        configs: List[ExportConfig]
    ) -> Set[Tuple[str, str]]:
        """Get unique (workspace, lakehouse) pairs from configs."""
        return {
            (c.target_workspace, c.target_lakehouse)
            for c in configs
        }

    def _mount_lakehouses(
        self,
        targets: Set[Tuple[str, str]]
    ) -> Dict[Tuple[str, str], str]:
        """Mount target lakehouses and return mount paths.

        Args:
            targets: Set of (workspace, lakehouse) tuples to mount

        Returns:
            Dict mapping (workspace, lakehouse) -> mount_path
        """
        import notebookutils

        mount_paths = {}
        for workspace, lakehouse in targets:
            lakehouse_name = lakehouse
            if not lakehouse_name.endswith(".Lakehouse"):
                lakehouse_name = f"{lakehouse_name}.Lakehouse"

            mount_point = f"/mnt/export_{workspace}_{lakehouse}"
            abfss_url = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}"

            notebookutils.fs.mount(abfss_url, mount_point)
            mount_path = notebookutils.fs.getMountPath(mount_point)

            mount_paths[(workspace, lakehouse)] = mount_path
            self.logger.info(f"Mounted {lakehouse} at {mount_path}")

        return mount_paths

    def _unmount_lakehouses(
        self,
        mount_paths: Dict[Tuple[str, str], str]
    ) -> None:
        """Unmount all mounted lakehouses.

        Args:
            mount_paths: Dict mapping (workspace, lakehouse) -> mount_path
        """
        import notebookutils

        for (workspace, lakehouse), _ in mount_paths.items():
            mount_point = f"/mnt/export_{workspace}_{lakehouse}"
            try:
                notebookutils.fs.unmount(mount_point)
                self.logger.info(f"Unmounted {mount_point}")
            except Exception as e:
                self.logger.warning(f"Failed to unmount {mount_point}: {e}")

    def process_exports(
        self,
        configs: List[ExportConfig],
        execution_id: str = None,
        is_retry: bool = False,
        run_date: Optional[str] = None,
        timezone: str = "Australia/Sydney",
    ) -> Dict[str, Any]:
        """
        Process multiple export configurations.

        Args:
            configs: List of ExportConfig objects
            execution_id: Unique execution identifier (generated if not provided)
            is_retry: If True, only process failed exports
            run_date: Date/datetime string (ISO format). Defaults to today in timezone.
            timezone: Timezone for default run_date (e.g., "Australia/Sydney", "UTC")

        Returns:
            Dictionary with execution summary
        """
        execution_id = execution_id or str(uuid.uuid4())
        tz = ZoneInfo(timezone)
        if run_date:
            effective_run_date = datetime.fromisoformat(run_date)
        else:
            effective_run_date = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)

        # Store timezone for passing to file writer
        self._timezone = timezone

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

        # Mount unique target lakehouses once (before processing)
        unique_targets = self._get_unique_target_lakehouses(configs)
        mount_paths: Dict[Tuple[str, str], str] = {}

        try:
            mount_paths = self._mount_lakehouses(unique_targets)

            # Process groups sequentially
            for group_num in sorted(execution_groups.keys()):
                group_configs = execution_groups[group_num]
                self.logger.info(
                    f"Processing execution group {group_num} with {len(group_configs)} exports"
                )

                group_results = self._process_group_parallel(
                    group_configs, execution_id, group_num, effective_run_date, mount_paths
                )

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
        finally:
            # Always unmount lakehouses
            self._unmount_lakehouses(mount_paths)

        return results

    def _process_group_parallel(
        self,
        configs: List[ExportConfig],
        execution_id: str,
        group_num: int,
        run_date: datetime,
        mount_paths: Dict[Tuple[str, str], str],
    ) -> List[ExportResult]:
        """Process a group of exports in parallel.

        Args:
            configs: Export configurations in this group
            execution_id: Master execution ID
            group_num: Execution group number
            run_date: Datetime for filename patterns
            mount_paths: Dict mapping (workspace, lakehouse) -> mount_path
        """
        results = []

        if len(configs) == 1:
            # Single export - no need for threading
            config = configs[0]
            mount_path = mount_paths[(config.target_workspace, config.target_lakehouse)]
            result = self.process_single_export(config, execution_id, run_date, mount_path)
            results = [result]
        else:
            # Multiple exports - use thread pool
            max_workers = min(len(configs), self.max_concurrency)
            self.logger.info(f"Processing group {group_num} with {max_workers} workers")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_config = {
                    executor.submit(
                        self.process_single_export,
                        config,
                        execution_id,
                        run_date,
                        mount_paths[(config.target_workspace, config.target_lakehouse)]
                    ): config
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
        run_date: datetime,
        mount_path: str,
    ) -> ExportResult:
        """
        Process a single export configuration.

        Args:
            config: Export configuration
            execution_id: Master execution ID
            run_date: Datetime for filename patterns
            mount_path: Path to mounted target lakehouse

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

            # 1. Resolve period dates FIRST (needed for both filtering AND filenames)
            period_start_date, period_end_date = self._resolve_period_dates(config, run_date)

            # 2. Get watermark for incremental exports
            watermark_value = None
            if config.extract_type == "incremental" and config.incremental_column:
                watermark_value = self.export_logger.get_watermark(
                    config.export_group_name,
                    config.export_name
                )
                if watermark_value:
                    self.logger.info(f"Incremental export with watermark: {watermark_value}")
                elif config.incremental_initial_watermark:
                    watermark_value = config.incremental_initial_watermark
                    self.logger.info(f"Incremental export: using incremental_initial_watermark: {watermark_value}")
                else:
                    self.logger.info("Incremental export: first run (no watermark, no initial_watermark)")

            # Store extract parameters on result for logging
            result.watermark_value = watermark_value
            result.period_start_date = period_start_date
            result.period_end_date = period_end_date

            # 3. Create exporter and read source data (with filtering)
            exporter = BaseExporter.create(config, self.spark)
            df = exporter.read_source(
                run_date=run_date,
                watermark_value=watermark_value,
                period_start=period_start_date,
                period_end=period_end_date,
            )

            row_count = df.count()
            self.logger.info(f"Read {row_count:,} rows from source for {config.export_name}")

            # 4. Get output DataFrame (filter to source_columns only)
            output_df = self._get_output_dataframe(df, config)

            # 5. Create file writer
            file_writer = ExportFileWriter(
                config=config,
                mount_path=mount_path,
                run_date=run_date,
                period_start_date=period_start_date,
                period_end_date=period_end_date,
                timezone=self._timezone,
            )

            # 6. Write to files (using filtered output_df, not original df)
            # Pass row_count to avoid second DataFrame scan
            write_result = file_writer.write(output_df, export_run_id, row_count=row_count)

            if not write_result.success:
                raise Exception(write_result.error_message)

            # 7. Calculate metrics and update result
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

            # 8. Update watermark for incremental exports (using original df with incremental_column)
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

    def _resolve_period_dates(
        self,
        config: ExportConfig,
        run_date: datetime,
    ) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Resolve period dates from config query.

        Args:
            config: Export configuration
            run_date: Run datetime for query substitution

        Returns:
            Tuple of (period_start_date, period_end_date), both None if no query configured
        """
        if not config.period_date_query:
            return None, None

        # Import Fabric libraries for synapsesql connector
        import com.microsoft.spark.fabric  # noqa: F401
        import sempy.fabric as fabric
        from com.microsoft.spark.fabric.Constants import Constants

        # Execute query with run_date substitution using synapsesql
        # (same connector used for reading export data - works with both Lakehouse and Warehouse)
        query = resolve_params(config.period_date_query, Params(run_date=run_date))
        self.logger.info(f"Resolving period dates with query: {query}")

        try:
            source = config.source_config
            workspace_id = fabric.resolve_workspace_id(source.source_workspace)

            result_df = (
                self.spark.read
                .option(Constants.WorkspaceId, workspace_id)
                .option(Constants.DatabaseName, source.source_datastore)
                .synapsesql(query)
            )
            row = result_df.first()

            if row is None:
                raise ValueError(f"Period date query returned no results: {query}")

            start_val = row["start_date"]
            end_val = row["end_date"]

            # Validate and convert to datetime
            start_date = self._to_datetime(start_val, "start_date")
            end_date = self._to_datetime(end_val, "end_date")

            self.logger.info(f"Resolved period dates: {start_date} to {end_date}")
            return start_date, end_date

        except Exception as e:
            raise ValueError(f"Failed to resolve period dates: {e}") from e

    def _to_datetime(self, value, field_name: str) -> datetime:
        """Convert value to datetime, validating it's a date or datetime."""
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        raise ValueError(
            f"period_date_query must return date/datetime for '{field_name}', "
            f"got {type(value).__name__}: {value}. "
            f"Use CAST({field_name} AS DATE) in your query."
        )

    def _get_output_dataframe(self, df: DataFrame, config: ExportConfig) -> DataFrame:
        """
        Get DataFrame with only the columns that should be written to output file.

        If source_columns is specified, returns DataFrame with only those columns.
        Otherwise returns the original DataFrame unchanged.

        Args:
            df: Source DataFrame (may include auto-added incremental_column)
            config: Export configuration

        Returns:
            DataFrame with only user-specified columns, or original if no restriction
        """
        source_columns = config.source_config.source_columns

        if not source_columns:
            return df

        return df.select(*source_columns)

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
                self.logger.info("No max value found for watermark update (empty result)")
        except Exception as e:
            self.logger.warning(f"Failed to update watermark: {e}")
