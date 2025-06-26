from datetime import datetime
from typing import List, Optional, Any
from dataclasses import dataclass, asdict
import pandas as pd
import notebookutils
# from . warehouse_utils import warehouse_utils  # Assuming warehouse_utils is in the same package


class config_utils:

    # variableLibraryInjectionStart: var_lib

    # All variables as a dictionary
    configs_dict = {'fabric_environment': 'development', 'fabric_deployment_workspace_id': '3a4fc13c-f7c5-463e-a9de-57c4754699ff', 'synapse_source_database_1': 'test1', 'config_workspace_id': '3a4fc13c-f7c5-463e-a9de-57c4754699ff', 'synapse_source_sql_connection': 'sansdaisyn-ondemand.sql.azuresynapse.net', 'config_lakehouse_name': 'config', 'edw_warehouse_name': 'edw', 'config_lakehouse_id': '2629d4cc-685c-458a-866b-b4705dde71a7', 'edw_workspace_id': '50fbcab0-7d56-46f7-90f6-80ceb00ac86d', 'edw_warehouse_id': 's', 'edw_lakehouse_id': '6adb67d6-c8eb-4612-9053-890cae3a55d7', 'edw_lakehouse_name': 'edw', 'legacy_synapse_connection_name': 'synapse_connection', 'synapse_export_shortcut_path_in_onelake': 'exports/'}
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

        def get_attribute(self, attr_name: str) -> Any:
            """Get attribute value by string name with error handling."""
            if hasattr(self, attr_name):
                return getattr(self, attr_name)
            else:
                raise AttributeError(f"FabricConfig has no attribute '{attr_name}'")

    
    configs_object: ConfigsObject = ConfigsObject(**configs_dict)
    # variableLibraryInjectionEnd: var_lib
    
    def __init__(self, config_workspace_id, config_warehouse_id):
        self.fabric_environments_table_name = f"fabric_environments"
        self.fabric_environments_table_schema = "config"
        self.fabric_environments_table = f"{self.fabric_environments_table_schema}.{self.fabric_environments_table_name}"
        self._configs: dict[str, Any] = {}
        self.warehouse_utils: warehouse_utils = warehouse_utils(config_workspace_id, config_warehouse_id)

    @staticmethod
    def config_schema() -> List[dict]:
        return [
            {"name": "fabric_environment", "type": str, "nullable": False},
            {"name": "config_workspace_id", "type": str, "nullable": False},
            {"name": "config_lakehouse_id", "type": str, "nullable": False},
            {"name": "edw_workspace_id", "type": str, "nullable": False},
            {"name": "edw_warehouse_id", "type": str, "nullable": False},
            {"name": "edw_warehouse_name", "type": str, "nullable": False},
            {"name": "edw_lakehouse_id", "type": str, "nullable": False},
            {"name": "edw_lakehouse_name", "type": str, "nullable": False},
            {"name": "legacy_synapse_connection_name", "type": str, "nullable": False},
            {
                "name": "synapse_export_shortcut_path_in_onelake",
                "type": str,
                "nullable": False,
            },
            {"name": "full_reset", "type": bool, "nullable": False},
            {"name": "update_date", "type": datetime, "nullable": False},
        ]

    def get_configs_as_dict(self):
        return config_utils.configs_dict 

    def get_configs_as_object(self):
        return self.configs_object
