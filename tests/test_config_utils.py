import pathlib
import sys
import unittest
from datetime import datetime
from unittest import mock

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ingen_fab.python_libs.python import config_utils  # Move import to the top


def sample_config_dict():
    return {
        "fabric_environment": "dev",
        "config_workspace_id": "ws",
        "config_lakehouse_id": "lh",
        "edw_workspace_id": "edw_ws",
        "edw_warehouse_id": "wh",
        "edw_warehouse_name": "wh_name",
        "edw_lakehouse_id": "lh_id",
        "edw_lakehouse_name": "lh_name",
        "legacy_synapse_connection_name": "legacy",
        "synapse_export_shortcut_path_in_onelake": "/path",
        "full_reset": True,
        "update_date": datetime(2020, 1, 1),
    }


def test_get_attribute():
    cfg = config_utils.config_utils.FabricConfig(**sample_config_dict())
    assert cfg.get_attribute("fabric_environment") == "dev"
    with pytest.raises(AttributeError):
        cfg.get_attribute("missing")


def test_get_configs_as_object():
    cu = config_utils.config_utils("ws", "lh")
    with mock.patch.object(cu, "get_configs_as_dict", return_value=sample_config_dict()):
        obj = cu.get_configs_as_object("dev")
    assert isinstance(obj, config_utils.config_utils.FabricConfig)
    assert obj.edw_warehouse_id == "wh"


def test_merge_config_record_executes_query():
    cu = config_utils.config_utils("ws", "lh")
    cfg = config_utils.config_utils.FabricConfig(**sample_config_dict())
    with mock.patch("ingen_fab.python_libs.python.config_utils.mssparkutils.session.execute") as exec_mock:
        cu.merge_config_record(cfg)
        exec_mock.assert_called_once()
        called_query = exec_mock.call_args.kwargs.get("query")
        assert called_query and "MERGE INTO" in called_query


class TestConfigUtils(unittest.TestCase):
    def setUp(self):
        # Initialize the config_utils instance
        self.config_util = config_utils("test_workspace_id", "test_warehouse_id")

    def test_get_configs_as_dict(self):
        # Test if the dictionary is returned correctly
        configs_dict = self.config_util.get_configs_as_dict()
        self.assertIsInstance(configs_dict, dict)
        self.assertIn("fabric_environment", configs_dict)
        self.assertEqual(configs_dict["fabric_environment"], "development")

    def test_get_configs_as_object(self):
        # Test if the object is returned correctly
        configs_object = self.config_util.get_configs_as_object()
        self.assertIsInstance(configs_object, config_utils.ConfigsObject)
        self.assertEqual(configs_object.fabric_environment, "development")

    def test_get_attribute_valid(self):
        # Test retrieving a valid attribute
        configs_object = self.config_util.get_configs_as_object()
        value = configs_object.get_attribute("fabric_environment")
        self.assertEqual(value, "development")

    def test_get_attribute_invalid(self):
        # Test retrieving an invalid attribute
        configs_object = self.config_util.get_configs_as_object()
        with self.assertRaises(AttributeError):
            configs_object.get_attribute("non_existent_attribute")


if __name__ == "__main__":
    unittest.main()
