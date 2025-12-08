"""Exporter for SQL Warehouse tables using synapsesql connector."""

from __future__ import annotations

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from ingen_fab.python_libs.pyspark.export.common.config import ExportConfig
from ingen_fab.python_libs.pyspark.export.common.exceptions import SourceReadError
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

    def read_source(self, watermark_value: Optional[str] = None) -> DataFrame:
        """Read from SQL Warehouse using spark.read.synapsesql with Fabric Constants.

        Args:
            watermark_value: Optional watermark for incremental exports.
                            If provided, adds WHERE clause to filter rows.
        """
        try:
            # Import Fabric libraries for synapsesql connector
            import com.microsoft.spark.fabric  # noqa: F401
            from com.microsoft.spark.fabric.Constants import Constants
            import sempy.fabric as fabric

            source = self.config.source_config

            # Resolve workspace name to ID
            workspace_id = fabric.resolve_workspace_id(source.source_workspace)

            if source.source_query:
                # Custom SQL query
                query = source.source_query
                self.logger.info(
                    f"Executing custom query for export: {self.config.export_name}"
                )
            elif source.source_table:
                # Build SELECT query: SELECT * FROM warehouse.schema.table
                # Schema is always required for warehouse
                full_path = f"{source.source_datastore}.{source.source_schema}.{source.source_table}"
                query = f"SELECT * FROM {full_path}"
                self.logger.info(
                    f"Reading {full_path} for export: {self.config.export_name}"
                )
            else:
                raise SourceReadError(
                    f"No source_table or source_query defined for export: {self.config.export_name}"
                )

            # Add incremental filter if watermark provided
            if watermark_value and self.config.incremental_column:
                if source.source_query:
                    # Wrap custom query with filter
                    query = f"SELECT * FROM ({query}) AS src WHERE {self.config.incremental_column} > '{watermark_value}'"
                else:
                    # Add WHERE clause to generated query
                    query = f"{query} WHERE {self.config.incremental_column} > '{watermark_value}'"
                self.logger.info(
                    f"Incremental filter: {self.config.incremental_column} > '{watermark_value}'"
                )

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
