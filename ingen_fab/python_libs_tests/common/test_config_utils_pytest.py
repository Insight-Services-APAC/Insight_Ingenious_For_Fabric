from __future__ import annotations

import pytest

from ingen_fab.python_libs.common import config_utils


def test_config_utils_initialization():
    """Test that config_utils module constants are set correctly."""
    assert config_utils.fabric_environments_table_name == "fabric_environments"
    assert config_utils.fabric_environments_table_schema == "config"
    assert config_utils.fabric_environments_table == "config.fabric_environments"


def test_configs_dict_structure():
    """Test that configs_dict contains expected keys and values."""
    configs = config_utils.configs_dict

    assert isinstance(configs, dict)
    assert "fabric_environment" in configs
    assert "fabric_deployment_workspace_id" in configs
    assert "synapse_source_database_1" in configs
    assert "config_workspace_id" in configs
    assert "synapse_source_sql_connection" in configs
    assert "config_lakehouse_name" in configs
    assert "edw_warehouse_name" in configs

    # Test specific expected values
    assert configs["fabric_environment"] == "local"
    assert configs["config_lakehouse_name"] == "config"
    assert configs["edw_warehouse_name"] == "edw"
    assert configs["edw_lakehouse_name"] == "edw"
    assert configs["legacy_synapse_connection_name"] == "synapse_connection"


def test_configs_object_structure():
    """Test that configs_object is created correctly from configs_dict."""
    configs_obj = config_utils.configs_object

    assert hasattr(configs_obj, "fabric_environment")
    assert hasattr(configs_obj, "fabric_deployment_workspace_id")
    assert hasattr(configs_obj, "synapse_source_database_1")
    assert hasattr(configs_obj, "config_workspace_id")
    assert hasattr(configs_obj, "synapse_source_sql_connection")
    assert hasattr(configs_obj, "config_lakehouse_name")
    assert hasattr(configs_obj, "edw_warehouse_name")

    # Test values match configs_dict
    assert configs_obj.fabric_environment == config_utils.configs_dict["fabric_environment"]
    assert configs_obj.config_lakehouse_name == config_utils.configs_dict["config_lakehouse_name"]
    assert configs_obj.edw_warehouse_name == config_utils.configs_dict["edw_warehouse_name"]


def test_configs_object_get_attribute_success():
    """Test that get_attribute method returns correct values."""
    configs_obj = config_utils.configs_object

    # Test existing attributes
    assert configs_obj.get_attribute("fabric_environment") == "local"
    assert configs_obj.get_attribute("config_lakehouse_name") == "config"
    assert configs_obj.get_attribute("edw_warehouse_name") == "edw"
    assert configs_obj.get_attribute("legacy_synapse_connection_name") == "synapse_connection"


def test_configs_object_get_attribute_error():
    """Test that get_attribute method raises error for non-existent attributes."""
    configs_obj = config_utils.configs_object

    with pytest.raises(AttributeError, match="ConfigsObject has no attribute 'non_existent_attr'"):
        configs_obj.get_attribute("non_existent_attr")


def test_get_configs_as_dict():
    """Test that get_configs_as_dict returns the correct dictionary."""
    result = config_utils.get_configs_as_dict()

    assert isinstance(result, dict)
    assert result == config_utils.configs_dict
    assert "fabric_environment" in result
    assert "config_lakehouse_name" in result
    assert result["fabric_environment"] == "local"


def test_get_configs_as_object():
    """Test that get_configs_as_object returns the correct object."""
    result = config_utils.get_configs_as_object()

    assert result == config_utils.configs_object
    assert hasattr(result, "fabric_environment")
    assert hasattr(result, "config_lakehouse_name")
    assert result.fabric_environment == "local"
    assert result.config_lakehouse_name == "config"


def test_configs_object_dataclass_type():
    """Test that ConfigsObject is properly created as a dataclass."""
    from dataclasses import is_dataclass

    configs_obj = config_utils.configs_object
    assert is_dataclass(configs_obj)
    assert is_dataclass(type(configs_obj))


def test_configs_dict_immutability():
    """Test that the configs_dict reference is consistent."""
    dict1 = config_utils.configs_dict
    dict2 = config_utils.configs_dict

    assert dict1 is dict2  # Same reference
    assert dict1 == dict2  # Same content


def test_configs_object_immutability():
    """Test that the configs_object reference is consistent."""
    obj1 = config_utils.configs_object
    obj2 = config_utils.configs_object

    assert obj1 is obj2  # Same reference
    assert obj1 == obj2  # Same content


def test_all_required_config_keys_present():
    """Test that all required configuration keys are present."""
    required_keys = [
        "fabric_environment",
        "fabric_deployment_workspace_id",
        "synapse_source_database_1",
        "config_workspace_id",
        "synapse_source_sql_connection",
        "config_lakehouse_name",
        "edw_warehouse_name",
        "config_lakehouse_id",
        "edw_workspace_id",
        "edw_warehouse_id",
        "edw_lakehouse_id",
        "edw_lakehouse_name",
        "legacy_synapse_connection_name",
        "synapse_export_shortcut_path_in_onelake",
    ]

    configs = config_utils.configs_dict

    for key in required_keys:
        assert key in configs, f"Required key '{key}' is missing from configs_dict"


def test_configs_object_attributes_match_dict():
    """Test that all ConfigsObject attributes match the configs_dict keys."""
    configs_dict = config_utils.configs_dict
    configs_obj = config_utils.configs_object

    for key, value in configs_dict.items():
        assert hasattr(configs_obj, key), f"ConfigsObject missing attribute '{key}'"
        assert getattr(configs_obj, key) == value, f"ConfigsObject.{key} doesn't match configs_dict['{key}']"


def test_module_constants():
    """Test that module constants are properly set."""
    assert hasattr(config_utils, "fabric_environments_table_name")
    assert hasattr(config_utils, "fabric_environments_table_schema")
    assert hasattr(config_utils, "fabric_environments_table")


def test_fabric_environments_table_construction():
    """Test that fabric_environments_table is constructed correctly."""
    expected_table = f"{config_utils.fabric_environments_table_schema}.{config_utils.fabric_environments_table_name}"
    assert config_utils.fabric_environments_table == expected_table
    assert config_utils.fabric_environments_table == "config.fabric_environments"
