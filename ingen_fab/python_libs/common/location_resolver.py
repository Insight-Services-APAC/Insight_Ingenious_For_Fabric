# Location Resolver
# Unified location resolver for dynamic workspace/datastore resolution

from typing import Dict, Optional, Any
from dataclasses import dataclass
from enum import Enum

from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import (
    FlatFileIngestionConfig,
)


class LocationType(Enum):
    """Type of location being resolved"""

    SOURCE = "source"
    TARGET = "target"


@dataclass
class LocationConfig:
    """Resolved location configuration"""

    workspace_id: str
    datastore_id: str
    datastore_type: str  # 'lakehouse' or 'warehouse'
    file_root_path: Optional[str] = None  # e.g., 'Files', 'Tables'
    schema_name: Optional[str] = None  # Schema name
    table_name: Optional[str] = None  # Table name
    location_type: Optional[str] = None  # 'source' or 'target' for context


class LocationResolver:
    """Unified location resolver for both source and target scenarios"""

    def __init__(
        self,
        default_workspace_id: Optional[str] = None,
        default_datastore_id: Optional[str] = None,
        default_datastore_type: str = "lakehouse",
        default_file_root_path: str = "Files",
        default_schema_name: str = "default",
    ):
        """
        Initialize with default fallback values

        Args:
            default_workspace_id: Default workspace ID
            default_datastore_id: Default datastore ID
            default_datastore_type: Default datastore type ('lakehouse' or 'warehouse')
            default_file_root_path: Default root path ('Files', 'Tables', etc.)
            default_schema_name: Default schema name
        """
        self.default_workspace_id = default_workspace_id
        self.default_datastore_id = default_datastore_id
        self.default_datastore_type = default_datastore_type
        self.default_file_root_path = default_file_root_path
        self.default_schema_name = default_schema_name

        # Cache for lakehouse/warehouse utilities to avoid repeated initialization
        self._utils_cache: Dict[str, Any] = {}

    def resolve_location(
        self, config: FlatFileIngestionConfig, location_type: LocationType
    ) -> LocationConfig:
        """
        Resolve location configuration for source or target

        Args:
            config: Flat file ingestion configuration
            location_type: Whether resolving source or target location

        Returns:
            Resolved location configuration
        """
        if location_type == LocationType.SOURCE:
            return self._resolve_source_location(config)
        elif location_type == LocationType.TARGET:
            return self._resolve_target_location(config)
        else:
            raise ValueError(f"Unsupported location type: {location_type}")

    def _resolve_source_location(
        self, config: FlatFileIngestionConfig
    ) -> LocationConfig:
        """Resolve source location with intelligent defaults"""
        workspace_id = (
            config.source_workspace_id
            or config.target_workspace_id
            or self.default_workspace_id
        )

        datastore_id = (
            config.source_datastore_id
            or self.default_datastore_id
            or config.target_datastore_id
        )

        datastore_type = config.source_datastore_type or self.default_datastore_type

        file_root_path = config.source_file_root_path or self.default_file_root_path

        if not workspace_id or not datastore_id:
            raise ValueError(
                f"Could not resolve source location for config {config.config_id}: "
                f"workspace_id={workspace_id}, datastore_id={datastore_id}"
            )

        return LocationConfig(
            workspace_id=workspace_id,
            datastore_id=datastore_id,
            datastore_type=datastore_type.lower(),
            file_root_path=file_root_path,
            location_type="source",
        )

    def _resolve_target_location(
        self, config: FlatFileIngestionConfig
    ) -> LocationConfig:
        """Resolve target location with intelligent defaults"""
        workspace_id = config.target_workspace_id or self.default_workspace_id
        datastore_id = config.target_datastore_id or self.default_datastore_id
        datastore_type = config.target_datastore_type or self.default_datastore_type
        schema_name = config.target_schema_name or self.default_schema_name

        if not workspace_id or not datastore_id:
            raise ValueError(
                f"Could not resolve target location for config {config.config_id}: "
                f"workspace_id={workspace_id}, datastore_id={datastore_id}"
            )

        return LocationConfig(
            workspace_id=workspace_id,
            datastore_id=datastore_id,
            datastore_type=datastore_type.lower(),
            schema_name=schema_name,
            table_name=config.target_table_name,
            location_type="target",
        )

    def create_utils(
        self, location_config: LocationConfig, spark=None, connection=None
    ) -> Any:
        """
        Create appropriate utils instance for the location

        Args:
            location_config: Resolved location configuration
            spark: Spark session (required for lakehouse)
            connection: Database connection (required for warehouse)

        Returns:
            lakehouse_utils or warehouse_utils instance
        """
        cache_key = f"{location_config.workspace_id}_{location_config.datastore_id}_{location_config.datastore_type}"

        # Return cached instance if available
        if cache_key in self._utils_cache:
            return self._utils_cache[cache_key]

        if location_config.datastore_type == "lakehouse":
            #if spark is None:
            #    raise ValueError("Spark session is required for lakehouse operations")

            # Import here to avoid circular dependencies
            from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

            utils_instance = lakehouse_utils(
                target_workspace_id=location_config.workspace_id,
                target_lakehouse_id=location_config.datastore_id,
                spark=spark,
            )

        elif location_config.datastore_type == "warehouse":
            if connection is None:
                raise ValueError(
                    "Database connection is required for warehouse operations"
                )

            # Import here to avoid circular dependencies
            from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils

            utils_instance = warehouse_utils(
                target_workspace_id=location_config.workspace_id,
                target_warehouse_id=location_config.datastore_id,
                connection=connection,
            )

        else:
            raise ValueError(
                f"Unsupported datastore type: {location_config.datastore_type}"
            )

        # Cache the instance
        self._utils_cache[cache_key] = utils_instance
        return utils_instance

    def get_utils(
        self,
        config: FlatFileIngestionConfig,
        location_type: LocationType,
        spark=None,
        connection=None,
    ) -> Any:
        """
        Convenience method to resolve location and create utils in one call

        Args:
            config: Flat file ingestion configuration
            location_type: Whether resolving source or target location
            spark: Spark session (for lakehouse)
            connection: Database connection (for warehouse)

        Returns:
            Appropriate utils instance for the location
        """
        location_config = self.resolve_location(config, location_type)
        return self.create_utils(location_config, spark=spark, connection=connection)

    def resolve_full_source_path(self, config: FlatFileIngestionConfig) -> str:
        """
        Resolve the full source file path including root path

        Args:
            config: Flat file ingestion configuration

        Returns:
            Full source path with root path prefix
        """
        source_config = self.resolve_location(config, LocationType.SOURCE)

        # Handle absolute paths
        if config.source_file_path.startswith("/"):
            return config.source_file_path

        # Handle paths that already include the root
        if config.source_file_path.startswith(source_config.file_root_path):
            return config.source_file_path

        # Combine root path with relative path
        return f"{source_config.file_root_path}/{config.source_file_path}".replace(
            "//", "/"
        )

    def clear_cache(self):
        """Clear the utils cache - useful for testing or memory management"""
        self._utils_cache.clear()

    def get_cache_info(self) -> Dict[str, str]:
        """Get information about cached utils instances"""
        return {
            key: str(type(utils).__name__) for key, utils in self._utils_cache.items()
        }

    # Backward compatibility methods
    def get_source_utils(
        self, config: FlatFileIngestionConfig, spark=None, connection=None
    ) -> Any:
        """Legacy method for backward compatibility"""
        return self.get_utils(
            config, LocationType.SOURCE, spark=spark, connection=connection
        )

    def get_target_utils(
        self, config: FlatFileIngestionConfig, spark=None, connection=None
    ) -> Any:
        """Legacy method for backward compatibility"""
        return self.get_utils(
            config, LocationType.TARGET, spark=spark, connection=connection
        )

    def resolve_source_config(self, config: FlatFileIngestionConfig) -> LocationConfig:
        """Legacy method for backward compatibility"""
        return self.resolve_location(config, LocationType.SOURCE)

    def resolve_target_config(self, config: FlatFileIngestionConfig) -> LocationConfig:
        """Legacy method for backward compatibility"""
        return self.resolve_location(config, LocationType.TARGET)
