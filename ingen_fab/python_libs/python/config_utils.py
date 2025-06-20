from datetime import datetime
from typing import List, Optional, Any
from dataclasses import dataclass, asdict
from notebookutils import mssparkutils  # type: ignore # noqa: F401


class config_utils:
    """Helper utilities for reading and updating configuration tables."""
    @dataclass
    class FabricConfig:
        fabric_environment: str
        config_workspace_id: str
        config_lakehouse_id: str
        edw_workspace_id: str
        edw_warehouse_id: str
        edw_warehouse_name: str
        edw_lakehouse_id: str
        edw_lakehouse_name: str
        legacy_synapse_connection_name: str
        synapse_export_shortcut_path_in_onelake: str
        full_reset: bool
        update_date: Optional[datetime] = None  # If you're tracking timestamps

        def get_attribute(self, attr_name: str) -> Any:
            """Get attribute value by string name with error handling."""
            if hasattr(self, attr_name):
                return getattr(self, attr_name)
            else:
                raise AttributeError(f"FabricConfig has no attribute '{attr_name}'")

    def __init__(self, config_workspace_id: str, config_lakehouse_id: str) -> None:
        """Create a new ``config_utils`` instance."""

        self.fabric_environments_table = (
            f"{config_workspace_id}.{config_lakehouse_id}.config_fabric_environments"
        )
        self._configs: dict[str, Any] = {}
    
    
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
            {"name": "synapse_export_shortcut_path_in_onelake", "type": str, "nullable": False},
            {"name": "full_reset", "type": bool, "nullable": False},
            {"name": "update_date", "type": datetime, "nullable": False}
        ]

    def get_configs_as_dict(self, fabric_environment: str):
        query = f"SELECT * FROM {self.fabric_environments_table} WHERE fabric_environment = '{fabric_environment}'"
        df = mssparkutils.session.query(query=query)
        configs = df.collect()
        return configs[0].asDict() if configs else None

    def get_configs_as_object(self, fabric_environment: str):
        config_dict = self.get_configs_as_dict(fabric_environment)
        if not config_dict:
            return None
        return config_utils.FabricConfig(**config_dict)

    def merge_config_record(self, config: 'config_utils.FabricConfig'):
        if config.update_date is None:
            config.update_date = datetime.now()

        new_config = asdict(config)
        columns = ", ".join(new_config.keys())
        values = ", ".join(
            [f"'{v}'" if isinstance(v, str) else str(v) for v in new_config.values()]
        )

        query = (
            f"MERGE INTO {self.fabric_environments_table} AS target "
            f"USING (SELECT {values}) AS source ({columns}) "
            f"ON target.fabric_environment = source.fabric_environment "
            f"WHEN MATCHED THEN UPDATE SET * "
            f"WHEN NOT MATCHED THEN INSERT *"
        )
        mssparkutils.session.execute(query=query)
        print('Merged fabric environments table')

