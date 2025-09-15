"""Location resolver for data profiling operations."""

from dataclasses import dataclass
from typing import Any, Dict, Optional

from ingen_fab.python_libs.interfaces.data_profiling_config_interface import (
    DataProfilingConfig,
)


@dataclass
class ProfileLocationConfig:
    """Resolved location configuration for profiling"""
    
    workspace_id: str
    datastore_id: str
    datastore_type: str  # 'lakehouse' or 'warehouse'
    schema_name: Optional[str] = None
    table_name: Optional[str] = None


class ProfileLocationResolver:
    """Location resolver for data profiling operations"""
    
    def __init__(
        self,
        default_workspace_id: Optional[str] = None,
        default_datastore_id: Optional[str] = None,
        default_datastore_type: str = "lakehouse",
        default_schema_name: str = "default",
    ):
        """
        Initialize with default fallback values
        
        Args:
            default_workspace_id: Default workspace ID (usually config workspace)
            default_datastore_id: Default datastore ID (usually config datastore)
            default_datastore_type: Default datastore type ('lakehouse' or 'warehouse')
            default_schema_name: Default schema name
        """
        self.default_workspace_id = default_workspace_id
        self.default_datastore_id = default_datastore_id
        self.default_datastore_type = default_datastore_type
        self.default_schema_name = default_schema_name
        
        # Cache for lakehouse/warehouse utilities to avoid repeated initialization
        self._utils_cache: Dict[str, Any] = {}
    
    def resolve_location(self, config: DataProfilingConfig) -> ProfileLocationConfig:
        """
        Resolve location configuration for the table to be profiled
        
        Args:
            config: Data profiling configuration
            
        Returns:
            Resolved location configuration
        """
        # Use explicit workspace/datastore if provided, otherwise use defaults
        workspace_id = config.target_workspace_id or self.default_workspace_id
        datastore_id = config.target_datastore_id or self.default_datastore_id
        datastore_type = config.target_datastore_type or self.default_datastore_type
        schema_name = config.schema_name or self.default_schema_name
        
        if not workspace_id or not datastore_id:
            raise ValueError(
                f"Could not resolve location for profiling config {config.config_id}: "
                f"workspace_id={workspace_id}, datastore_id={datastore_id}"
            )
        
        return ProfileLocationConfig(
            workspace_id=workspace_id,
            datastore_id=datastore_id,
            datastore_type=datastore_type.lower(),
            schema_name=schema_name,
            table_name=config.table_name,
        )
    
    def create_utils(
        self, location_config: ProfileLocationConfig, spark=None, connection=None
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
        config: DataProfilingConfig,
        spark=None,
        connection=None,
    ) -> Any:
        """
        Convenience method to resolve location and create utils in one call
        
        Args:
            config: Data profiling configuration
            spark: Spark session (for lakehouse)
            connection: Database connection (for warehouse)
            
        Returns:
            Appropriate utils instance for the location
        """
        location_config = self.resolve_location(config)
        return self.create_utils(location_config, spark=spark, connection=connection)
    
    def clear_cache(self):
        """Clear the utils cache - useful for testing or memory management"""
        self._utils_cache.clear()
    
    def get_cache_info(self) -> Dict[str, str]:
        """Get information about cached utils instances"""
        return {
            key: str(type(utils).__name__) for key, utils in self._utils_cache.items()
        }