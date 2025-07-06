"""Configuration utilities for managing application settings.

This module provides utilities for accessing configuration values
through both dictionary and object-based interfaces.
"""

from dataclasses import dataclass
from typing import Any, Dict


# variableLibraryInjectionStart: var_lib
@dataclass(frozen=True)
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

    def get_attribute(self, attr_name: str) -> Any:
        if hasattr(self, attr_name):
            return getattr(self, attr_name)
        raise AttributeError(f"ConfigsObject has no attribute '{attr_name}'")

_configs_dict: Dict[str, Any] = {
    "fabric_environment": "development",
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
_configs_object: ConfigsObject = ConfigsObject(**_configs_dict)
# variableLibraryInjectionEnd: var_lib

_fabric_environments_table_name: str = "fabric_environments"
_fabric_environments_table_schema: str = "config"
_fabric_environments_table: str = f"{_fabric_environments_table_schema}.{_fabric_environments_table_name}"

def configs_as_dict() -> Dict[str, Any]:
    """Get all configuration values as a dictionary."""
    return _configs_dict

def configs_as_object() -> ConfigsObject:
    """Get all configuration values as a strongly-typed object."""
    return _configs_object

def get_config_value(key: str) -> Any:
    """Get a specific configuration value by key."""
    if key not in _configs_dict:
        raise KeyError(f"Configuration key '{key}' not found")
    return _configs_dict[key]

def has_config(key: str) -> bool:
    """Check if a configuration key exists."""
    return key in _configs_dict

def fabric_environments_table() -> str:
    """Get the full table name for fabric environments."""
    return _fabric_environments_table
