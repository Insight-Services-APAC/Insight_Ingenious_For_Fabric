"""Configuration utilities for managing application settings.

This module provides utilities for accessing configuration values
through both dictionary and object-based interfaces.
"""

from dataclasses import dataclass
from typing import Any, Dict

# variableLibraryInjectionStart: var_lib

# All variables as a dictionary
configs_dict = {
    "fabric_environment": "local",
    "fabric_deployment_workspace_id": "3a4fc13c-f7c5-463e-a9de-57c4754699ff",
    "synapse_source_database_1": "test1",
    "config_workspace_id": "3a4fc13c-f7c5-463e-a9de-57c4754699ff",
    "synapse_source_sql_connection": "sansdaisyn-ondemand.sql.azuresynapse.net",
    "config_lakehouse_name": "config",
    "edw_warehouse_name": "edw",
    "config_lakehouse_id": "2629d4cc-685c-458a-866b-b4705dde71a7",
    "edw_workspace_id": "50fbcab0-7d56-46f7-90f6-80ceb00ac86d",
    "edw_warehouse_id": "s",
    "edw_lakehouse_id": "6adb67d6-c8eb-4612-9053-890cae3a55d7",
    "edw_lakehouse_name": "edw",
    "legacy_synapse_connection_name": "synapse_connection",
    "synapse_export_shortcut_path_in_onelake": "exports/",
}

@dataclass
class ConfigsObject:
    """Strongly-typed configuration object with all settings.
    
    Provides type-safe access to configuration values with IntelliSense support.
    """
    fabric_environment: str
    fabric_deployment_workspace_id: str
    synapse_source_database_1: str
    config_workspace_id: str
    synapse_source_sql_connection: str
    config_lakehouse_name: str
    edw_warehouse_name: str
    config_lakehouse_id: str
    edw_workspace_id: str
    edw_warehouse_id: str
    edw_lakehouse_id: str
    edw_lakehouse_name: str
    legacy_synapse_connection_name: str
    synapse_export_shortcut_path_in_onelake: str

    def get_attribute(self, attr_name: str) -> Any:
        """Get attribute value by string name with error handling.
        
        Args:
            attr_name: The name of the attribute to retrieve.
            
        Returns:
            The value of the specified attribute.
            
        Raises:
            AttributeError: If the attribute does not exist.
        """
        if hasattr(self, attr_name):
            return getattr(self, attr_name)
        else:
            raise AttributeError(f"ConfigsObject has no attribute '{attr_name}'")

configs_object: ConfigsObject = ConfigsObject(**configs_dict)
# variableLibraryInjectionEnd: var_lib

# Module-level configuration constants
fabric_environments_table_name = "fabric_environments"
fabric_environments_table_schema = "config"
fabric_environments_table = f"{fabric_environments_table_schema}.{fabric_environments_table_name}"

def get_configs_as_dict() -> Dict[str, Any]:
    """Get all configuration values as a dictionary.
    
    Returns:
        Dictionary containing all configuration key-value pairs.
    """
    return configs_dict

def get_configs_as_object() -> ConfigsObject:
    """Get all configuration values as a strongly-typed object.
    
    Returns:
        ConfigsObject instance with all configuration values accessible as attributes.
    """
    return configs_object

def _is_local_environment() -> bool:        
    """Check if the current environment is local.
    
    Returns:
        True if the environment is local, False otherwise.
    """
    print (configs_dict.get("fabric_environment", "development"))
    return configs_dict.get("fabric_environment", "development") == "local"

def get_config_value(key: str) -> Any:
    """Get a specific configuration value by key.
    
    Args:
        key: The configuration key to retrieve.
        
    Returns:
        The configuration value.
        
    Raises:
        KeyError: If the configuration key does not exist.
    """
    if key not in configs_dict:
        raise KeyError(f"Configuration key '{key}' not found")
    return configs_dict[key]

def has_config(key: str) -> bool:
    """Check if a configuration key exists.
    
    Args:
        key: The configuration key to check.
        
    Returns:
        True if the key exists, False otherwise.
    """
    return key in configs_dict
