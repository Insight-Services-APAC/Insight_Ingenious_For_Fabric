"""Exporter for SQL Warehouse tables using synapsesql connector."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from ingen_fab.python_libs.pyspark.export.common.config import ExportConfig
from ingen_fab.python_libs.pyspark.export.common.exceptions import SourceReadError
from ingen_fab.python_libs.pyspark.export.common.param_resolver import (
    Params,
    resolve_params,
)
from ingen_fab.python_libs.pyspark.export.exporters.base_exporter import BaseExporter


class WarehouseExporter(BaseExporter, source_type="warehouse"):
    """Exporter for SQL Warehouse tables using synapsesql connector."""

    def __init__(self, config: ExportConfig, spark: SparkSession):
        """
        Initialize WarehouseExporter.

        Args:
            config: Export configuration
            spark: SparkSession instance
        """
        self.config = config
        self.spark = spark
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_config(cls, config: ExportConfig, spark: SparkSession) -> "WarehouseExporter":
        """Create WarehouseExporter from config."""
        return cls(config=config, spark=spark)

    def read_source(
        self,
        run_date: Optional[datetime] = None,
        watermark_value: Optional[str] = None,
        period_start: Optional[datetime] = None,
        period_end: Optional[datetime] = None,
    ) -> DataFrame:
        """Read from SQL Warehouse using spark.read.synapsesql with Fabric Constants.

        For source_query: Resolves {placeholders} - user controls all filtering.
        For source_table: Builds query with auto-filtering based on config.

        Args:
            run_date: Logical date for the export run.
            watermark_value: Optional watermark for incremental exports.
            period_start: Optional start date for period-based exports.
            period_end: Optional end date for period-based exports.
        """
        try:
            # Import Fabric libraries for synapsesql connector
            import com.microsoft.spark.fabric  # noqa: F401
            import sempy.fabric as fabric
            from com.microsoft.spark.fabric.Constants import Constants

            source = self.config.source_config
            workspace_id = fabric.resolve_workspace_id(source.source_workspace)

            if source.source_query:
                # User query - just resolve placeholders
                query = resolve_params(
                    source.source_query,
                    Params(
                        run_date=run_date,
                        period_start_date=period_start,
                        period_end_date=period_end,
                        watermark=watermark_value,
                    ),
                )
                self.logger.info(f"Executing query for export: {self.config.export_name}")
            else:
                # Generated query with auto-filtering
                query = self._build_table_query(watermark_value, period_start, period_end)

            return (
                self.spark.read
                .option(Constants.WorkspaceId, workspace_id)
                .option(Constants.DatabaseName, source.source_datastore)
                .synapsesql(query)
            )
        except Exception as e:
            raise SourceReadError(
                f"Failed to read from warehouse source for export {self.config.export_name}: {e}"
            ) from e

    def _build_table_query(
        self,
        watermark_value: Optional[str],
        period_start: Optional[datetime],
        period_end: Optional[datetime],
    ) -> str:
        """Build SELECT query with auto-filtering for source_table mode."""
        source = self.config.source_config
        # Schema is always required for warehouse
        full_path = f"{source.source_datastore}.{source.source_schema}.{source.source_table}"
        columns = self._get_select_columns()
        query = f"SELECT {columns} FROM {full_path}"

        # Auto-filter based on config
        if watermark_value and self.config.incremental_column:
            query += f" WHERE {self.config.incremental_column} > '{watermark_value}'"
            self.logger.info(f"Incremental filter: {self.config.incremental_column} > '{watermark_value}'")
        elif period_start and period_end and self.config.period_filter_column:
            col = self.config.period_filter_column
            query += f" WHERE {col} BETWEEN '{period_start}' AND '{period_end}'"
            self.logger.info(f"Period filter: {col} BETWEEN '{period_start}' AND '{period_end}'")
        else:
            self.logger.info(f"Reading {full_path} for export: {self.config.export_name}")

        return query
