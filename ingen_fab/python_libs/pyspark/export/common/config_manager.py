"""ConfigExportManager for reading export configurations from Delta tables."""

from __future__ import annotations

import logging
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingen_fab.python_libs.pyspark.export.common.config import ExportConfig
from ingen_fab.python_libs.common.export_resource_config_schema import get_config_export_resource_schema
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)


class ConfigExportManager:
    """
    Manager for reading export configurations from Delta tables.

    Reads from config_export_resource table and converts rows to ExportConfig objects.
    """

    CONFIG_TABLE_NAME = "config_export_resource"

    def __init__(
        self,
        config_lakehouse: lakehouse_utils,
        spark: SparkSession = None,
        auto_create_table: bool = True,
    ):
        """
        Initialize ConfigExportManager.

        Args:
            config_lakehouse: lakehouse_utils instance for config table operations
            spark: SparkSession instance (uses config_lakehouse.spark if not provided)
            auto_create_table: If True, create config table if it doesn't exist
        """
        self.config_lakehouse = config_lakehouse
        self.spark = spark or config_lakehouse.spark
        self.logger = logger

        if auto_create_table:
            self._ensure_config_table_exists()

    def _ensure_config_table_exists(self):
        """Create config table if it doesn't exist."""
        if not self.config_lakehouse.check_if_table_exists(self.CONFIG_TABLE_NAME):
            self.logger.info(f"Creating config table: {self.CONFIG_TABLE_NAME}")
            empty_df = self.spark.createDataFrame([], get_config_export_resource_schema())
            self.config_lakehouse.write_to_table(
                df=empty_df,
                table_name=self.CONFIG_TABLE_NAME,
                mode="overwrite",
            )
            self.logger.info(f"Created config table: {self.CONFIG_TABLE_NAME}")

    def get_configs(
        self,
        export_group_name: Optional[str] = None,
        export_name: Optional[str] = None,
        execution_group: Optional[int] = None,
        active_only: bool = True,
    ) -> List[ExportConfig]:
        """
        Get export configurations from the config table.

        Args:
            export_group_name: Optional filter by export group name
            export_name: Optional filter by export name
            execution_group: Optional filter by execution group
            active_only: If True, only return active configurations

        Returns:
            List of ExportConfig objects
        """
        df = self.config_lakehouse.read_table(table_name=self.CONFIG_TABLE_NAME)

        # Apply filters
        if active_only:
            df = df.filter(F.col("is_active") == True)

        if export_group_name:
            df = df.filter(F.col("export_group_name") == export_group_name)

        if export_name:
            df = df.filter(F.col("export_name") == export_name)

        if execution_group is not None:
            df = df.filter(F.col("execution_group") == execution_group)

        # Order by execution_group for proper processing order
        df = df.orderBy("export_group_name", "execution_group", "export_name")

        # Convert to list of ExportConfig objects
        configs = []
        for row in df.collect():
            try:
                config = ExportConfig.from_row(row.asDict())
                configs.append(config)
            except Exception as e:
                self.logger.warning(
                    f"Failed to parse config for export {row.export_name}: {e}"
                )

        self.logger.info(f"Loaded {len(configs)} export configurations")
        return configs

    def save_config(self, config: ExportConfig, created_by: str = "system"):
        """
        Save or update an export configuration.

        Args:
            config: ExportConfig to save
            created_by: User/system that created the config
        """
        from datetime import datetime

        now = datetime.utcnow()

        # Build row data
        row_data = {
            "export_group_name": config.export_group_name,
            "export_name": config.export_name,
            "is_active": config.is_active,
            "execution_group": config.execution_group,
            "source_type": config.source_config.source_type,
            "source_workspace": config.source_config.source_workspace,
            "source_datastore": config.source_config.source_datastore,
            "source_schema": config.source_config.source_schema,
            "source_table": config.source_config.source_table,
            "source_query": config.source_config.source_query,
            "target_workspace": config.target_workspace,
            "target_lakehouse": config.target_lakehouse,
            "target_path": config.target_path,
            "target_filename_pattern": config.target_filename_pattern,
            "file_format": config.file_format_params.file_format,
            "compression": config.file_format_params.compression,
            "compressed_filename_pattern": config.compressed_filename_pattern,
            "format_options": config.file_format_params.format_options,
            "max_rows_per_file": config.max_rows_per_file,
            "partition_by": ",".join(config.partition_by) if config.partition_by else None,
            "trigger_file_enabled": config.trigger_file_enabled,
            "trigger_file_pattern": config.trigger_file_pattern,
            "description": config.description,
            "created_at": now,
            "updated_at": now,
            "created_by": created_by,
            "updated_by": created_by,
        }

        # Use lakehouse_utils merge_to_table for upsert
        source_df = self.spark.createDataFrame([row_data], get_config_export_resource_schema())

        self.config_lakehouse.merge_to_table(
            df=source_df,
            table_name=self.CONFIG_TABLE_NAME,
            merge_keys=["export_group_name", "export_name"],
            enable_schema_evolution=False,
        )

        self.logger.info(f"Saved export configuration: {config.export_group_name}/{config.export_name}")
