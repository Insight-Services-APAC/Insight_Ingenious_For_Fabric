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
    "fabric_deployment_workspace_id": "544530ea-a8c9-4464-8878-f666d2a8f418",
    "config_workspace_name": "metcash_demo",
    "config_workspace_id": "544530ea-a8c9-4464-8878-f666d2a8f418",
    "config_wh_workspace_id": "544530ea-a8c9-4464-8878-f666d2a8f418",
    "config_lakehouse_name": "config",
    "config_lakehouse_id": "514ebe8f-2bf9-4a31-88f7-13d84706431c",
    "config_wh_warehouse_name": "config_wh",
    "config_wh_warehouse_id": "51226772-4e8f-4034-9cd2-1afd020d2773",
    "sample_lakehouse_name": "sample",
    "sample_lakehouse_id": "REPLACE_WITH_SAMPLE_LAKEHOUSE_GUID",
    "sample_lh_workspace_id": "544530ea-a8c9-4464-8878-f666d2a8f418",
    "sample_lh_lakehouse_name": "sample_lh",
    "sample_lh_lakehouse_id": "f6f98b54-458f-4d9c-9f7d-d682394340cc",
    "sample_wh_warehouse_id": "REPLACE_WITH_SAMPLE_WAREHOUSE_GUID",
    "raw_lh_workspace_id": "544530ea-a8c9-4464-8878-f666d2a8f418",
    "raw_lh_lakehouse_name": "REPLACE_WITH_RAW_LAKEHOUSE_NAME",
    "raw_lh_lakehouse_id": "REPLACE_WITH_RAW_LAKEHOUSE_GUID",
    "raw_wh_workspace_id": "544530ea-a8c9-4464-8878-f666d2a8f418",
    "raw_wh_warehouse_id": "REPLACE_WITH_RAW_WAREHOUSE_GUID",
    "edw_workspace_id": "544530ea-a8c9-4464-8878-f666d2a8f418",
    "edw_lakehouse_name": "edw",
    "edw_lakehouse_id": "REPLACE_WITH_EDW_LAKEHOUSE_GUID",
    "edw_warehouse_name": "edw",
    "edw_warehouse_id": "REPLACE_WITH_EDW_WAREHOUSE_GUID",
    "local_spark_provider": "native",  # Options: "native" or "lakesail"
}
# All variables as an object


@dataclass
class ConfigsObject:
    fabric_environment: str
    fabric_deployment_workspace_id: str
    config_workspace_name: str
    config_workspace_id: str
    config_wh_workspace_id: str
    config_lakehouse_name: str
    config_lakehouse_id: str
    config_wh_warehouse_name: str
    config_wh_warehouse_id: str
    sample_lakehouse_name: str
    sample_lakehouse_id: str
    sample_lh_workspace_id: str
    sample_lh_lakehouse_name: str
    sample_lh_lakehouse_id: str
    sample_wh_warehouse_id: str
    raw_lh_workspace_id: str
    raw_lh_lakehouse_name: str
    raw_lh_lakehouse_id: str
    raw_wh_workspace_id: str
    raw_wh_warehouse_id: str
    edw_workspace_id: str
    edw_lakehouse_name: str
    edw_lakehouse_id: str
    edw_warehouse_name: str
    edw_warehouse_id: str
    local_spark_provider: str = "native"  # Options: "native" or "lakesail"


configs_object: ConfigsObject = ConfigsObject(**configs_dict)
# variableLibraryInjectionEnd: var_lib

# Module-level configuration constants
fabric_environments_table_name = "fabric_environments"
fabric_environments_table_schema = "config"
fabric_environments_table = (
    f"{fabric_environments_table_schema}.{fabric_environments_table_name}"
)


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
    print(configs_dict.get("fabric_environment", "development"))
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
