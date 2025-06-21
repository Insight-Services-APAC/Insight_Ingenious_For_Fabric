
from datetime import datetime
from typing import List, Optional, Any
from dataclasses import dataclass, asdict
import notebookutils 
from . import warehouse_utils  # Assuming warehouse_utils is in the same package


class config_utils:
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

    def __init__(self, config_workspace_id, config_warehouse_id):
        self.fabric_environments_table_name = f"fabric_environments"
        self.fabric_environments_table_schema = "config"
        self.fabric_environments_table = f"{self.fabric_environments_table_schema}.{self.fabric_environments_table_name}"
        self._configs: dict[str, Any] = {}
        self.warehouse_utils = warehouse_utils(config_workspace_id, config_warehouse_id)
    
    
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
        df = self.warehouse_utils.execute_query(self.warehouse_utils.get_connection(), query)
        if df is not None and not df.empty:
            return df.iloc[0].to_dict()
        return None

    def get_configs_as_object(self, fabric_environment: str):
        config_dict = self.get_configs_as_dict(fabric_environment)
        if not config_dict:
            return None
        return config_utils.FabricConfig(**config_dict)

    def merge_config_record(self, config: 'config_utils.FabricConfig'):
        # 1. Check if table exists
        table_exists = self.warehouse_utils.check_if_table_exists(
            table_name=self.fabric_environments_table_name,
            schema_name=self.fabric_environments_table_schema
        )

        # 2. If table doesn't exist, create it
        if not table_exists:
            # Build CREATE TABLE statement from schema
            field_map = {
                str: "VARCHAR(300)",
                bool: "BIT",
                datetime: "DATETIME2(6)",
            }
            cols_sql = []
            for col in self.config_schema():
                col_sql = f"{col['name']} {field_map[col['type']]}"
                if not col['nullable']:
                    col_sql += " NOT NULL"
                cols_sql.append(col_sql)
            # Primary key constraint for fabric_environment
            # cols_sql.append("CONSTRAINT PK_fabric_env PRIMARY KEY (fabric_environment)")
            # Check if schema exists
            self.warehouse_utils.create_schema_if_not_exists(
                schema_name=self.fabric_environments_table_schema
            )

            # Create table
            create_table_sql = (
                f"CREATE TABLE {self.fabric_environments_table} (\n    " +
                ",\n    ".join(cols_sql) +
                "\n);"
            )
            self.warehouse_utils.execute_query(conn, create_table_sql)
            print("Created table config.fabric_environments.")

        #Prepare update and insert logic 
        if config.update_date is None:
            config.update_date = datetime.now()

        new_config = asdict(config)

        # Prepare update values
        update_set = []
        for col, v in new_config.items():
            if col == "fabric_environment":
                continue  # Don't update the primary key
            if isinstance(v, str):
                update_set.append(f"{col} = '{v}'")
            elif isinstance(v, bool):
                update_set.append(f"{col} = {1 if v else 0}")
            elif isinstance(v, datetime):
                update_set.append(f"{col} = CAST('{v.strftime('%Y-%m-%d %H:%M:%S.%f')}' AS DATETIME2)")
            elif v is None:
                update_set.append(f"{col} = NULL")
            else:
                update_set.append(f"{col} = {v}")

        update_set_clause = ", ".join(update_set)
        # The primary key for lookup
        where_clause = f"fabric_environment = '{new_config['fabric_environment']}'"

        # UPDATE statement
        update_sql = (
            f"UPDATE {self.fabric_environments_table} SET {update_set_clause} "
            f"WHERE {where_clause};"
        )

        conn = self.warehouse_utils.get_connection()
        result = self.warehouse_utils.execute_query(conn, update_sql)

        # Check if any rows were updated (fabric warehouse_utils should return affected rows)
        rows_affected = getattr(result, 'rowcount', None)
        needs_insert = rows_affected == 0 if rows_affected is not None else True

        # If no row updated, do an INSERT
        if needs_insert:
            cols = ", ".join(new_config.keys())
            vals = []
            for v in new_config.values():
                if isinstance(v, str):
                    vals.append(f"'{v}'")
                elif isinstance(v, bool):
                    vals.append(f"{1 if v else 0}")
                elif isinstance(v, datetime):
                    vals.append(f"CAST('{v.strftime('%Y-%m-%d %H:%M:%S.%f')}' AS DATETIME2)")
                elif v is None:
                    vals.append("NULL")
                else:
                    vals.append(str(v))
            vals_clause = ", ".join(vals)
            insert_sql = (
                f"INSERT INTO {self.fabric_environments_table} ({cols}) VALUES ({vals_clause});"
            )
            self.warehouse_utils.execute_query(conn, insert_sql)
            print("Inserted fabric environments record")
        else:
            print("Updated fabric environments record")





