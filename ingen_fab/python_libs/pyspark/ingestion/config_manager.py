# File Loading Framework - Configuration Manager
# Provides high-level API for reading ResourceConfig objects from Delta tables

import logging
from typing import List, Optional

from pyspark.sql import DataFrame

from ingen_fab.python_libs.common.ingestion_resource_config_schema import (
    get_ingestion_resource_config_schema,
    row_to_resource_config,
)
from ingen_fab.python_libs.pyspark.ingestion.config import ResourceConfig
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Configuration manager for loading ResourceConfig objects from Delta tables.

    Designed for parameterized notebook execution where filters are passed as parameters.
    Read-only access - save functionality to be added later.

    Example:
        config_mgr = ConfigManager(lakehouse_utils_instance)

        # Get all active configs
        configs = config_mgr.get_configs()

        # Filter by source
        configs = config_mgr.get_configs(source_name="sales")

        # Filter by specific resource
        configs = config_mgr.get_configs(resource_name="load_daily_sales")

        # Combine filters (AND logic)
        configs = config_mgr.get_configs(source_name="sales", resource_name="load_daily_sales")
    """

    def __init__(
        self,
        lakehouse_utils_instance: lakehouse_utils,
        config_table_name: str = "config_ingestion_resource",
        auto_create_table: bool = True,
    ):
        """
        Initialize ConfigManager.

        Args:
            lakehouse_utils_instance: Lakehouse utilities for accessing config table
            config_table_name: Name of the config table (default: config_ingestion_resource)
            auto_create_table: If True, create empty table if it doesn't exist
        """
        self.lakehouse = lakehouse_utils_instance
        self.spark = lakehouse_utils_instance.spark
        self.config_table_name = config_table_name

        if auto_create_table:
            self._ensure_config_table_exists()

    def _ensure_config_table_exists(self) -> None:
        """Create config table if it doesn't exist (empty table only)"""
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.config_table_name}")
            logger.debug(f"Table '{self.config_table_name}' already exists")
        except Exception:
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
        source_name: Optional[str] = None,
        resource_name: Optional[str] = None,
        active_only: bool = True,
    ) -> List[ResourceConfig]:
        """
        Get configs with flexible filtering via parameters.

        All parameters are optional - if none provided, returns all active configs.
        Multiple filters are combined with AND logic.

        Args:
            source_name: Filter by source name (e.g., "sales")
            resource_name: Filter by resource name (e.g., "load_daily_sales")
            active_only: Only return configs where active=True (default: True)

        Returns:
            List of ResourceConfig objects matching filters, ordered by execution_group, resource_name

        Examples:
            # Get all active configs
            configs = config_mgr.get_configs()

            # Get all configs for source "sales"
            configs = config_mgr.get_configs(source_name="sales")

            # Get specific config
            configs = config_mgr.get_configs(resource_name="load_daily_sales")

            # Get specific resource within a source (both filters applied)
            configs = config_mgr.get_configs(source_name="sales", resource_name="load_daily_sales")
        """
        # Build WHERE clause dynamically based on provided filters
        conditions = []

        if active_only:
            conditions.append("active = TRUE")

        if source_name:
            conditions.append(f"source_name = '{source_name}'")

        if resource_name:
            conditions.append(f"resource_name = '{resource_name}'")

        # Build query
        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT *
            FROM {self.config_table_name}
            WHERE {where_clause}
            ORDER BY execution_group, resource_name
        """

        logger.debug(f"Executing query: {query}")
        df = self.spark.sql(query)

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

    def get_config_summary(self) -> DataFrame:
        """
        Get a summary view of all configs in the table.
        Useful for debugging and inspection.

        Returns:
            DataFrame with key config fields
        """
        return self.spark.sql(
            f"""
            SELECT
                config_id,
                resource_name,
                source_name,
                source_type,
                target_table_name,
                execution_group,
                active,
                created_at,
                updated_at
            FROM {self.config_table_name}
            ORDER BY execution_group, resource_name
        """
        )
