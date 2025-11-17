# Configuration Manager for Ingestion Resources
# Provides high-level API for reading/writing ResourceConfig objects from Delta tables

import logging
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from ingen_fab.python_libs.common.ingestion_resource_config_schema import (
    get_ingestion_resource_config_schema,
    resource_config_to_row,
    row_to_resource_config,
)
from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)


class ConfigIngestionManager:
    """
    Configuration manager for loading ResourceConfig objects from Delta tables.

    Reads from/writes to the 'config_ingestion_resource' table.

    Example:
        config_mgr = ConfigIngestionManager(lakehouse_utils_instance)

        # Get all active configs for source "pe"
        configs = config_mgr.get_configs(source_name="pe")

        # Get specific config
        config = config_mgr.get_configs(source_name="pe", resource_name="pe_prom_codes")[0]
    """

    def __init__(
        self,
        lakehouse_utils_instance: lakehouse_utils,
        auto_create_table: bool = True,
    ):
        """
        Initialize ConfigIngestionManager.

        Args:
            lakehouse_utils_instance: Lakehouse utilities for accessing config table
            auto_create_table: If True, create empty table if it doesn't exist
        """
        self.lakehouse = lakehouse_utils_instance
        self.spark = lakehouse_utils_instance.spark
        self.config_table_name = "config_ingestion_resource"

        if auto_create_table:
            self._ensure_config_table_exists()

    def _ensure_config_table_exists(self) -> None:
        """Create config table if it doesn't exist (empty table only)"""
        if self.lakehouse.check_if_table_exists(self.config_table_name):
            logger.debug(f"Table '{self.config_table_name}' already exists")
        else:
            logger.info(f"Creating table '{self.config_table_name}'")
            schema = get_ingestion_resource_config_schema()
            empty_df = self.spark.createDataFrame([], schema)
            self.lakehouse.write_to_table(
                df=empty_df,
                table_name=self.config_table_name,
                mode="overwrite",
            )

    def get_configs(
        self,
        source_name: str,
        resource_name: Optional[str] = None,
    ) -> List[ResourceConfig]:
        """
        Get active configs for a source with optional resource filtering.

        Args:
            source_name: Source name to filter by (required, e.g., "sales", "pe")
            resource_name: Optional resource name to filter to specific config (e.g., "load_daily_sales")

        Returns:
            List of active ResourceConfig objects matching filters, ordered by execution_group, resource_name

        Raises:
            ValueError: If source_name is None or empty string

        Examples:
            # Get all active configs for source "pe"
            configs = config_mgr.get_configs(source_name="pe")

            # Get specific active config within a source
            configs = config_mgr.get_configs(source_name="pe", resource_name="pe_prom_codes")
        """
        # Validate required parameter
        if not source_name:
            raise ValueError("source_name is required and cannot be None or empty")

        # Read table using lakehouse_utils
        df = self.lakehouse.read_table(table_name=self.config_table_name)

        # Filter to active configs only
        df = df.filter(col("active") == True)

        # Filter by source_name (required)
        df = df.filter(col("source_name") == source_name)

        # Optionally filter by resource_name
        if resource_name:
            df = df.filter(col("resource_name") == resource_name)

        # Order by execution_group and resource_name
        df = df.orderBy("execution_group", "resource_name")

        # Convert to ResourceConfig objects
        configs = [row_to_resource_config(row) for row in df.collect()]

        # Log result
        filter_parts = []
        if source_name:
            filter_parts.append(f"source_name={source_name}")
        if resource_name:
            filter_parts.append(f"resource_name={resource_name}")
        filter_desc = ", ".join(filter_parts) if filter_parts else "all active"

        logger.info(f"Loaded {len(configs)} configs ({filter_desc})")

        return configs

    def save_config(self, config: ResourceConfig, created_by: str = "system", updated_by: str = "system") -> None:
        """
        Save a single ResourceConfig to the Delta table (insert or update).

        Uses MERGE to handle both inserts and updates based on resource_name (PK).

        Args:
            config: ResourceConfig object to save
            created_by: Username for created_by field (default: "system")
            updated_by: Username for updated_by field (default: "system")

        Example:
            config_mgr.save_config(my_config, created_by="john.doe")
        """
        self.save_configs([config], created_by, updated_by)

    def save_configs(self, configs: List[ResourceConfig], created_by: str = "system", updated_by: str = "system") -> None:
        """
        Save multiple ResourceConfig objects to the Delta table (bulk insert or update).

        Uses MERGE to handle both inserts and updates based on resource_name (PK).

        Args:
            configs: List of ResourceConfig objects to save
            created_by: Username for created_by field (default: "system")
            updated_by: Username for updated_by field (default: "system")

        Example:
            config_mgr.save_configs([config1, config2, config3], created_by="john.doe")
        """
        if not configs:
            logger.warning("No configs to save")
            return

        # Convert configs to row dicts
        rows = [resource_config_to_row(config, created_by, updated_by) for config in configs]

        # Create DataFrame from rows
        schema = get_ingestion_resource_config_schema()
        df = self.spark.createDataFrame(rows, schema)

        logger.info(f"Saving {len(configs)} config(s) to {self.config_table_name}")

        # Use lakehouse_utils merge_to_table instead of raw SQL
        self.lakehouse.merge_to_table(
            df=df,
            table_name=self.config_table_name,
            merge_keys=["resource_name"],
            enable_schema_evolution=False,
        )

        logger.info(f"Successfully saved {len(configs)} config(s)")

    def get_config_summary(self) -> DataFrame:
        """
        Get a summary view of all configs in the table.
        Useful for debugging and inspection.

        Returns:
            DataFrame with key config fields
        """
        df = self.lakehouse.read_table(table_name=self.config_table_name)

        return df.select(
            "resource_name",
            "source_name",
            "source_type",
            "target_table",
            "execution_group",
            "active",
            "created_at",
            "updated_at"
        ).orderBy("execution_group", "resource_name")
