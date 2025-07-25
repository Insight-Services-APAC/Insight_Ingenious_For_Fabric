
"""Configuration utilities for managing application settings.

This module provides utilities for accessing configuration values
through both dictionary and object-based interfaces.
"""

from dataclasses import dataclass
from typing import Any, Dict

# variableLibraryInjectionStart: var_lib

# All variables as a dictionary
configs_dict = {'fabric_environment': 'local', 'fabric_deployment_workspace_id': '#####', 'synapse_source_database_1': 'test1', 'config_workspace_id': '#####', 'synapse_source_sql_connection': 'sansdaisyn-ondemand.sql.azuresynapse.net', 'config_lakehouse_name': 'config', 'edw_warehouse_name': 'edw', 'config_lakehouse_id': '2629d4cc-685c-458a-866b-b4705dde71a7', 'edw_workspace_id': '###', 'edw_warehouse_id': '###', 'edw_lakehouse_id': '6adb67d6-c8eb-4612-9053-890cae3a55d7', 'edw_lakehouse_name': 'edw', 'legacy_synapse_connection_name': 'synapse_connection', 'synapse_export_shortcut_path_in_onelake': 'exports/', 'raw_workspace_id': 'local_raw_workspace', 'raw_datastore_id': 'local_raw_datastore', 'config_warehouse_id': 'local-config-warehouse-id', 'config_workspace_name': 'local_workspace'}
# All variables as an object
from dataclasses import dataclass
@dataclass
class ConfigsObject:
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
    raw_workspace_id: str 
    raw_datastore_id: str 
    config_warehouse_id: str 
    config_workspace_name: str 
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
