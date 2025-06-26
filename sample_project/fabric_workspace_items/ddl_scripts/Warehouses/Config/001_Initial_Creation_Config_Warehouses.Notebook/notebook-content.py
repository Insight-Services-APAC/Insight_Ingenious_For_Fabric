# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META }

# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************



# THESE SHOULD BE SET BY THE SCHEDULER 
fabric_environment = "development"  # fabric environment, e.g. development, test, production
config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
config_lakehouse_id = "f0121783-34cc-4e64-bfb5-6b44b1a7f04b"
target_lakehouse_config_prefix = ""
# ONLY USE THIS IN DEV
full_reset = False # ⚠️ Full reset -- DESTRUCTIVE - will drop all tables




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions

# CELL ********************




# Auto-generated library code from python_libs
# Files are ordered based on dependency analysis


# === sql_templates.py ===
from jinja2 import Template


class SQLTemplates:
    """Render SQL templates for different dialects."""

    TEMPLATES = {
        "check_table_exists": {
            "fabric": """
                            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_SCHEMA = '{{ schema_name }}' AND TABLE_NAME = '{{ table_name }}'
                        """,
            "sqlserver": """
                            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_SCHEMA = '{{ schema_name }}' AND TABLE_NAME = '{{ table_name }}'
                        """,
        },
        "drop_table": {
            "fabric": "DROP TABLE IF EXISTS {{ schema_name }}.{{ table_name }}",
            "sqlserver": "DROP TABLE IF EXISTS {{ schema_name }}.{{ table_name }}'",
        },
        "create_table_from_values": {
            "fabric": "SELECT * INTO {{ schema_name }}.{{ table_name }} FROM (VALUES {{ values_clause }}) AS v({{ column_names }})",
            "sqlserver": "SELECT * INTO {{ schema_name }}.{{ table_name }} FROM (VALUES {{ values_clause }}) AS v({{ column_names }})",
        },
        "insert_row": {
            "fabric": "INSERT INTO {{ schema_name }}.{{ table_name }} VALUES ({{ row_values }})",
            "sqlserver": "INSERT INTO {{ schema_name }}.{{ table_name }} VALUES ({{ row_values }})",
        },
        "list_tables": {
            "fabric": "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES{% if prefix %} WHERE TABLE_NAME LIKE '{{ prefix }}%'{% endif %}",
            "sqlserver": "SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS name FROM INFORMATION_SCHEMA.TABLES{% if prefix %} WHERE TABLE_NAME LIKE '{{ prefix }}%'{% endif %}",
        },
    }

    def __init__(self, dialect: str = "fabric"):
        self.dialect = dialect

    def render(self, template_name: str, **kwargs) -> str:
        template_str = self.TEMPLATES[template_name][self.dialect]
        return Template(template_str).render(**kwargs)


# === warehouse_utils.py ===
import logging
from typing import Optional

import notebookutils  # type: ignore # noqa: F401
import pandas as pd
import pyodbc


class warehouse_utils:
    """Utilities for interacting with Fabric or local SQL Server warehouses."""

    def __init__(
        self,
        target_workspace_id: Optional[str] = None,
        target_warehouse_id: Optional[str] = None,
        *,
        dialect: str = "fabric",
        connection_string: Optional[str] = None,
    ):
        self.target_workspace_id = target_workspace_id
        self.target_warehouse_id = target_warehouse_id
        self.dialect = dialect
        self.connection_string = connection_string
        self.sql = SQLTemplates(dialect)

    def get_connection(self):
        """Return a connection object depending on the configured dialect."""
        try:
            if self.dialect == "fabric":
                print("Connection to Fabric Warehouse")
                conn = notebookutils.data.connect_to_artifact(
                    self.target_warehouse_id, self.target_workspace_id
                )
                print(conn)
                return conn
            else:
                return pyodbc.connect(self.connection_string)  # type: ignore
        except Exception as e:
            logging.error(f"Failed to connect to warehouse: {e}")
            raise

    def execute_query(self, conn, query: str):
        """Execute a query and return results as a DataFrame when possible."""
        print(conn)
        try:
            logging.info(f"Executing query: {query}")
            if self.dialect == "fabric":
                print(query)
                result = conn.query(query)
                logging.info("Query executed successfully.")
                return result
            else:
                cursor = conn.cursor()
                cursor.execute(query)
                if cursor.description:
                    rows = cursor.fetchall()
                    columns = [d[0] for d in cursor.description]
                    df = pd.DataFrame.from_records(rows, columns=columns)
                else:
                    conn.commit()
                    df = None
                logging.info("Query executed successfully.")
            return df
        except Exception as e:
            logging.error(f"Error executing query: {query}. Error: {e}")
            raise

    def create_schema_if_not_exists(self, schema_name: str):
        """Create a schema if it does not already exist."""
        try:
            conn = self.get_connection()
            schema_check_sql = """
            SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME = '{{ schema_name }}'
            """
            schema_result = self.execute_query(conn, schema_check_sql)
            schema_exists = len(schema_result) > 0 if schema_result is not None else False

            # Create schema if it doesn't exist
            if not schema_exists:
                create_schema_sql = f"CREATE SCHEMA {schema_name};"
                self.execute_query(conn, create_schema_sql)
                print(f"Created schema '{schema_name}'.")
            
            logging.info(f"Schema {schema_name} created or already exists.")
        except Exception as e:
            logging.error(f"Error creating schema {schema_name}: {e}")
            raise
    
    def check_if_table_exists(self, table_name, schema_name: str = "dbo") -> bool:
        try:
            conn = self.get_connection()
            query = self.sql.render("check_table_exists", table_name=table_name, schema_name=schema_name)
            result = self.execute_query(conn, query)
            table_exists = len(result) > 0 if result is not None else False
            return table_exists
        except Exception as e:
            logging.error(f"Error checking if table {table_name} exists: {e}")
            return False
        
    def write_to_warehouse_table(
        self,
        df,
        table_name: str,
        schema_name: str = "dbo",
        mode: str = "overwrite",
        options: dict = None
    ):
        try:
            conn = self.get_connection()
            pandas_df = df

            # Handle different write modes
            if mode == "overwrite":
                # Drop table if exists
                drop_query = self.sql.render("drop_table", table_name=table_name, schema_name=schema_name)
                self.execute_query(conn, drop_query)
                
                # Create table from dataframe using SELECT INTO syntax
                values = []
                for _, row in pandas_df.iterrows():
                    row_values = ', '.join([f"'{v}'" if isinstance(v, str) else str(v) for v in row])
                    values.append(f"({row_values})")
                
                # Get column names from DataFrame
                column_names = ', '.join(pandas_df.columns)
                values_clause = ', '.join(values)
                
                create_query = self.sql.render(
                    "create_table_from_values",
                    table_name=table_name,
                    schema_name=schema_name,
                    column_names=column_names,
                    values_clause=values_clause,
                )
                self.execute_query(conn, create_query)
            
            elif mode == "append":
                # Insert data into existing table
                for _, row in pandas_df.iterrows():
                    row_values = ', '.join([
                        f"'{v}'" if isinstance(v, str) else str(v) for v in row
                    ])
                    insert_query = self.sql.render(
                        "insert_row",
                        table_name=table_name,
                        schema_name=schema_name,
                        row_values=row_values,
                    )
                    self.execute_query(conn, insert_query)
            
            elif mode == "error" or mode == "errorifexists":
                # Check if table exists
                if self.check_if_table_exists(table_name, schema_name=schema_name):
                    raise ValueError(f"Table {table_name} already exists")
                # Create table from dataframe using SELECT INTO syntax
                values = []
                for _, row in pandas_df.iterrows():
                    row_values = ', '.join([
                        f"'{v}'" if isinstance(v, str) else str(v) for v in row
                    ])
                    values.append(f"({row_values})")
                
                # Get column names from DataFrame
                column_names = ', '.join(pandas_df.columns)
                values_clause = ', '.join(values)
                
                create_query = self.sql.render(
                    "create_table_from_values",
                    table_name=table_name,
                    schema_name=schema_name,
                    column_names=column_names,
                    values_clause=values_clause,
                )
                self.execute_query(conn, create_query)
            
            elif mode == "ignore":
                # Only write if table doesn't exist
                if not self.check_if_table_exists(table_name, schema_name=schema_name):
                    values = []
                    for _, row in pandas_df.iterrows():
                        row_values = ', '.join([
                            f"'{v}'" if isinstance(v, str) else str(v) for v in row
                        ])
                        values.append(f"({row_values})")

                    # Get column names from DataFrame
                    column_names = ', '.join(pandas_df.columns)
                    values_clause = ', '.join(values)

                    create_query = self.sql.render(
                        "create_table_from_values",
                        table_name=table_name,
                        schema_name=schema_name,
                        column_names=column_names,
                        values_clause=values_clause,
                    )
                    self.execute_query(conn, create_query)
        except Exception as e:
            logging.error(f"Error writing to table {table_name} with mode {mode}: {e}")
            raise

    def drop_all_tables(self, table_prefix=None):
        try:
            conn = self.get_connection()
            query = self.sql.render("list_tables", prefix=table_prefix)
            tables = self.execute_query(conn, query)

            for table in tables or []:
                table_name = table["name"] if isinstance(table, dict) else table[0]
                try:
                    drop_query = self.sql.render("drop_table", table_name=table_name)
                    self.execute_query(conn, drop_query)
                    logging.info(f"✔ Dropped table: {table_name}")
                except Exception as e:
                    logging.error(f"⚠ Error dropping table {table_name}: {e}")

            logging.info("✅ All eligible tables have been dropped.")
        except Exception as e:
            logging.error(f"Error dropping tables with prefix {table_prefix}: {e}")
            raise

# === ddl_utils.py ===
# { "depends_on": "warehouse_utils" }

from datetime import datetime
from typing import Callable, Optional
import inspect
import hashlib
from dataclasses import dataclass, asdict
import notebookutils  # type: ignore # noqa: F401



class ddl_utils:
    """Run DDL scripts once and track execution in a warehouse table."""

    def __init__(self, target_workspace_id: str, target_warehouse_id: str) -> None:
        self.target_workspace_id = target_workspace_id
        self.target_warehouse_id = target_warehouse_id
        self.execution_log_table_schema = "log"
        self.execution_log_table_name = "ddl_script_executions"
        self.warehouse_utils = warehouse_utils(
            target_workspace_id=target_workspace_id,
            target_warehouse_id=target_warehouse_id,
        )
        self.initialise_ddl_script_executions_table()

    def execution_log_schema():
        pass

    def print_log(self):
        conn = self.warehouse_utils.get_connection()
        query = f"SELECT * FROM [{self.execution_log_table_schema}].[{self.execution_log_table_name}]"
        df = self.warehouse_utils.execute_query(conn=conn, query=query)
        display(df)

    def check_if_script_has_run(self, script_id) -> bool:
        conn = self.warehouse_utils.get_connection()
        query = f"""
        SELECT *
        FROM [{self.execution_log_table_schema}].[{self.execution_log_table_name}]
        WHERE script_id = '{script_id}'
        AND execution_status = 'success'
        """
        df = self.warehouse_utils.execute_query(conn=conn, query=query)

        if df:
            if len(df) == 0:
                # print("matched:", matched)
                return False
            else:
                return True
        else:
            return False

    def print_skipped_script_execution(self, guid, object_name):
        print(
            f"skipping {guid}:{object_name} as the script has already run on workspace_id:"
            f"{self.target_workspace_id} | warehouse_id {self.target_warehouse_id}"
        )

    def write_to_execution_log(self, object_guid, object_name, script_status):
        conn = self.warehouse_utils.get_connection()
        current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_query = f"""
        INSERT INTO [{self.execution_log_table_schema}].[{self.execution_log_table_name}]
        (script_id, script_name, execution_status, update_date)
        VALUES ('{object_guid}', '{object_name}', '{script_status}', '{current_timestamp}')
        """
        self.warehouse_utils.execute_query(conn=conn, query=insert_query)

    def run_once(self, work_fn: callable, object_name: str, guid: str):
        """
        Runs `work_fn()` exactly once, keyed by `guid`. If `guid` is None,
        it's computed by hashing the source code of `work_fn`.
        """
        # 1. Auto-derive GUID if not provided
        if guid is None:
            try:
                src = inspect.getsource(work_fn)
            except (OSError, TypeError):
                raise ValueError(
                    "work_fn must be a named function defined at top-level"
                )
            # compute SHA256 and take first 12 hex chars
            digest = hashlib.sha256(src.encode("utf-8")).hexdigest()
            guid = digest
            print(f"Derived guid={guid} from work_fn source")

        # 2. Check execution
        if not self.check_if_script_has_run(script_id=guid):
            try:
                work_fn()
                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Success"
                )
            except Exception as e:
                print(f"Error in work_fn for {guid}: {e}")
                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Failure"
                )
                raise
        else:
            self.print_skipped_script_execution(guid=guid, object_name=object_name)

    def initialise_ddl_script_executions_table(self):
        guid = "b8c83c87-36d2-46a8-9686-ced38363e169"
        object_name = "ddl_script_executions"
        conn = self.warehouse_utils.get_connection()
        table_exists = self.warehouse_utils.check_if_table_exists(
            table_name=self.execution_log_table_name,
            schema_name=self.execution_log_table_schema,
        )

        if not table_exists:
            self.warehouse_utils.create_schema_if_not_exists(
                schema_name=self.execution_log_table_schema
            )
            # Create the table
            create_table_query = f"""
            CREATE TABLE [{self.execution_log_table_schema}].[{self.execution_log_table_name}] (
            script_id VARCHAR(255) NOT NULL,
            script_name VARCHAR(255) NOT NULL,
            execution_status VARCHAR(50) NOT NULL,
            update_date DATETIME2(0) NOT NULL
            )
            """
            self.warehouse_utils.execute_query(conn=conn, query=create_table_query)
            self.write_to_execution_log(
                object_guid=guid, object_name=object_name, script_status="Success"
            )
        else:
            print(f"Skipping {object_name} as it already exists")


# === lakehouse_utils.py ===
import notebookutils  # type: ignore # noqa: F401\
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class lakehouse_utils:
    def __init__(self, target_workspace_id, target_warehouse_id):
        self.target_workspace_id = target_workspace_id
        self.target_warehouse_id = target_warehouse_id


# === config_utils.py ===
from datetime import datetime
from typing import List, Optional, Any
from dataclasses import dataclass, asdict
import pandas as pd
import notebookutils


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
            {
                "name": "synapse_export_shortcut_path_in_onelake",
                "type": str,
                "nullable": False,
            },
            {"name": "full_reset", "type": bool, "nullable": False},
            {"name": "update_date", "type": datetime, "nullable": False},
        ]

    def get_configs_as_dict(self, fabric_environment: str):
        query = f"SELECT * FROM {self.fabric_environments_table} WHERE fabric_environment = '{fabric_environment}'"
        df = self.warehouse_utils.execute_query(
            self.warehouse_utils.get_connection(), query
        )
        if df is not None and not df.empty:
            return df.iloc[0].to_dict()
        return None

    def get_configs_as_object(self, fabric_environment: str):
        config_dict = self.get_configs_as_dict(fabric_environment)
        if not config_dict:
            return None
        return config_utils.FabricConfig(**config_dict)

    def merge_config_record(self, config: "config_utils.FabricConfig"):
        # 1. Check if table exists
        table_exists = self.warehouse_utils.check_if_table_exists(
            table_name=self.fabric_environments_table_name,
            schema_name=self.fabric_environments_table_schema,
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
                if not col["nullable"]:
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
                f"CREATE TABLE {self.fabric_environments_table} (\n    "
                + ",\n    ".join(cols_sql)
                + "\n);"
            )
            self.warehouse_utils.execute_query(conn, create_table_sql)
            print("Created table config.fabric_environments.")

        # Prepare update and insert logic
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
                update_set.append(
                    f"{col} = CAST('{v.strftime('%Y-%m-%d %H:%M:%S.%f')}' AS DATETIME2)"
                )
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
        rows_affected = getattr(result, "rowcount", None)
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
                    vals.append(
                        f"CAST('{v.strftime('%Y-%m-%d %H:%M:%S.%f')}' AS DATETIME2)"
                    )
                elif v is None:
                    vals.append("NULL")
                else:
                    vals.append(str(v))
            vals_clause = ", ".join(vals)
            insert_sql = f"INSERT INTO {self.fabric_environments_table} ({cols}) VALUES ({vals_clause});"
            self.warehouse_utils.execute_query(conn, insert_sql)
            print("Inserted fabric environments record")
        else:
            print("Updated fabric environments record")

    def overwrite_configs(self, configs: dict):
        df = pd.DataFrame([configs])
        self.warehouse_utils.write_to_warehouse_table(
            df,
            self.fabric_environments_table_name,
            self.fabric_environments_table_schema,
            "overwrite",
        )






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Instantiate the Helper Classes

# CELL ********************



cu = config_utils(config_workspace_id,config_lakehouse_id)
configs = cu.get_configs_as_object(fabric_environment)
target_workspace_id = configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_workspace_id")
target_lakehouse_id = configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_lakehouse_id")
lu = lakehouse_utils(target_workspace_id, target_lakehouse_id)
du = ddl_utils(target_workspace_id, target_lakehouse_id)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 001_config_parquet_loads_create.sql

# CELL ********************

guid = "6937eb26c16c"
def work():
    sql = """


CREATE TABLE config.config_parquet_loads (
    cfg_target_lakehouse_workspace_id VARCHAR(300) NOT NULL,
    cfg_target_lakehouse_id VARCHAR(300) NOT NULL,
    target_partition_columns VARCHAR(300) NOT NULL,
    target_sort_columns VARCHAR(300) NOT NULL,
    target_replace_where VARCHAR(300) NOT NULL,
    cfg_source_lakehouse_workspace_id VARCHAR(300) NOT NULL,
    cfg_source_lakehouse_id VARCHAR(300) NOT NULL,
    cfg_source_file_path VARCHAR(300) NOT NULL,
    source_file_path VARCHAR(300) NOT NULL,
    source_file_name VARCHAR(300) NOT NULL,
    cfg_legacy_synapse_connection_name VARCHAR(300) NOT NULL,
    synapse_source_schema_name VARCHAR(300) NOT NULL,
    synapse_source_table_name VARCHAR(300) NOT NULL,
    synapse_partition_clause VARCHAR(1000) NOT NULL,
    execution_group INT NOT NULL,
    active_yn VARCHAR(1) NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"001_config_parquet_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 002_config_synapse_loads_create.sql

# CELL ********************

guid = "bc17803f3701"
def work():
    sql = """


CREATE TABLE config.config_synapse_extracts (
    cfg_legacy_synapse_connection_name VARCHAR(300) NOT NULL,
    source_schema_name VARCHAR(300) NOT NULL,
    source_table_name VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000) NOT NULL,
    execution_group INT NOT NULL,
    active_yn VARCHAR(1) NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"002_config_synapse_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 003_log_parquet_loads_create.sql

# CELL ********************

guid = "0a6390965eba"
def work():
    sql = """


CREATE TABLE log.log_parquet_loads (
    execution_id VARCHAR(300) NOT NULL,
    cfg_target_lakehouse_workspace_id VARCHAR(300) NOT NULL,
    cfg_target_lakehouse_id VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000),
    status VARCHAR(300) NOT NULL,
    error_messages VARCHAR(4000),
    start_date BIGINT NOT NULL,
    finish_date BIGINT NOT NULL,
    update_date BIGINT NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"003_log_parquet_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 004_log_synapse_loads_create.sql

# CELL ********************

guid = "755109da42d0"
def work():
    sql = """


CREATE TABLE log.log_synapse_extracts (
    execution_id VARCHAR(300) NOT NULL,
    cfg_synapse_connection_name VARCHAR(300) NOT NULL,
    source_schema_name VARCHAR(300) NOT NULL,
    source_table_name VARCHAR(300) NOT NULL,
    extract_file_name VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000) NOT NULL,
    status VARCHAR(300) NOT NULL,
    error_messages VARCHAR(4000),
    start_date BIGINT NOT NULL,
    finish_date BIGINT NOT NULL,
    update_date BIGINT NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"004_log_synapse_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 005_config_synapse_loads_insert.sql

# CELL ********************

guid = "7461d6d52cd5"
def work():
    sql = """


INSERT INTO config.config_synapse_extracts (
    cfg_legacy_synapse_connection_name,
    source_schema_name,
    source_table_name,
    partition_clause,
    execution_group,
    active_yn
)
VALUES
('legacy_synapse_connection_name', 'dbo', 'dim_customer', '', 1, 'Y'),
('legacy_synapse_connection_name', 'dbo', 'fact_transactions', 'where year = @year and month = @month', 1, 'Y');


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"005_config_synapse_loads_insert", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 006_config_parquet_loads_insert.sql

# CELL ********************

guid = "6f95f2886b11"
def work():
    sql = """


INSERT INTO config.config_parquet_loads (
    cfg_target_lakehouse_workspace_id,
    cfg_target_lakehouse_id,
    target_partition_columns,
    target_sort_columns,
    target_replace_where,
    cfg_source_lakehouse_workspace_id,
    cfg_source_lakehouse_id,
    cfg_source_file_path,
    source_file_path,
    source_file_name,
    cfg_legacy_synapse_connection_name,
    synapse_source_schema_name,
    synapse_source_table_name,
    synapse_partition_clause,
    execution_group,
    active_yn
)
VALUES
('edw_workspace_id', 'edw_lakehouse_id', '', '', '', 'edw_lakehouse_workspace_id', 'edw_lakehouse_id', 'synapse_export_shortcut_path_in_onelake', 'dbo_dim_customer', '', 'legacy_synapse_connection_name', 'dbo', 'dim_customer', '', 1, 'Y'),
('edw_workspace_id', 'edw_lakehouse_id', 'year, month', 'year, month', 'WHERE year = @year AND month = @month', 'edw_workspace_id', 'edw_lakehouse_id', 'synapse_export_shortcut_path_in_onelake', 'dbo_dim_customer', '', 'legacy_synapse_connection_name', 'dbo', 'fact_transactions', 'WHERE year = @year AND month = @month', 1, 'Y');


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"006_config_parquet_loads_insert", guid)







# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📇 Print the execution log

# CELL ********************



du.print_log() 




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✔️ If we make it to the end return a successful result

# CELL ********************



from notebookutils import mssparkutils
mssparkutils.notebook.exit("success")



