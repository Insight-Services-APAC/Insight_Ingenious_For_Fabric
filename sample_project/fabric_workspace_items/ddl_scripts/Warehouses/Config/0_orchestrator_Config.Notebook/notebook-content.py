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



#lakehouse_name = "LH"  # name of your Lakehouse
#config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
#config_lakehouse_id = "a29c9d15-c24e-4779-8344-0c7c237b3990"

# Target for DDL scripts
target_lakehouse_config_prefix = "config"

# Fabric Configurations 
fabric_environment: str = "development" 
config_lakehouse_name = "LH1"  # name of your Lakehouse
config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
config_lakehouse_id = "f0121783-34cc-4e64-bfb5-6b44b1a7f04b"


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


# === config_utils.py ===
from dataclasses import dataclass
from typing import Any


class config_utils:
    # variableLibraryInjectionStart: var_lib

# All variables as a dictionary
configs_dict = {'fabric_environment': 'development_jr', 'fabric_deployment_workspace_id': 'b3fbeaf7-ec67-4622-ba37-8d8bcb7e436a', 'synapse_source_database_1': 'test1', 'config_workspace_id': '#####', 'synapse_source_sql_connection': 'sansdaisyn-ondemand.sql.azuresynapse.net', 'config_lakehouse_name': 'config', 'edw_warehouse_name': 'edw', 'config_lakehouse_id': '2629d4cc-685c-458a-866b-b4705dde71a7', 'edw_workspace_id': '###', 'edw_warehouse_id': '###', 'edw_lakehouse_id': '6adb67d6-c8eb-4612-9053-890cae3a55d7', 'edw_lakehouse_name': 'edw', 'legacy_synapse_connection_name': 'synapse_connection', 'synapse_export_shortcut_path_in_onelake': 'exports/'}
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
configs_object: ConfigsObject = ConfigsObject(**configs_dict)
# variableLibraryInjectionEnd: var_lib

    def __init__(self):
        self.fabric_environments_table_name = "fabric_environments"
        self.fabric_environments_table_schema = "config"
        self.fabric_environments_table = f"{self.fabric_environments_table_schema}.{self.fabric_environments_table_name}"
        self._configs: dict[str, Any] = {}

    def get_configs_as_dict(self):
        return config_utils.configs_dict

    def get_configs_as_object(self):
        return self.configs_object


# === sql_templates.py ===
from jinja2 import Template, Environment, exceptions

def required_filter(value, var_name=""):
    """Jinja2 filter: raises an error if value is not provided or is falsy."""
    if value is None or (hasattr(value, '__len__') and len(value) == 0):
        raise exceptions.TemplateRuntimeError(
            f"Required parameter '{var_name or 'unknown'}' was not provided!"
        )
    return value


class SQLTemplates:
    """Render SQL templates for different dialects."""

    TEMPLATES = [{'dialect': 'fabric', 'file_name': 'check_schema_exists.sql.jinja', 'file_contents': "SELECT 1 \nFROM INFORMATION_SCHEMA.SCHEMATA\nWHERE SCHEMA_NAME = '{{ schema_name | required }}'\n", 'full_path': './fabric/check_schema_exists.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'check_table_exists.sql.jinja', 'file_contents': "SELECT\n    1\nFROM\n    INFORMATION_SCHEMA.TABLES\nWHERE\n    TABLE_SCHEMA = '{{ schema_name | required }}'\n    AND TABLE_NAME = '{{ table_name | required }}'\n", 'full_path': './fabric/check_table_exists.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'create_table_from_values.sql.jinja', 'file_contents': "SELECT\n    * INTO {{ schema_name | required('schema_name') }}.{{ table_name | required('table_name') }}\nFROM\n    (\n        VALUES\n            {{ values_clause | required('values_clause') }}\n    ) AS v(\n        {{ column_names | required('column_names') }}\n    )\n", 'full_path': './fabric/create_table_from_values.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'drop_table.sql.jinja', 'file_contents': 'DROP TABLE IF EXISTS {{ schema_name | required }}.{{ table_name | required }}\n', 'full_path': './fabric/drop_table.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'insert_row.sql.jinja', 'file_contents': 'INSERT INTO {{ schema_name | required }}.{{ table_name | required }} VALUES ({{ row_values | required }})\n', 'full_path': './fabric/insert_row.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'list_tables.sql.jinja', 'file_contents': "SELECT\n    TABLE_SCHEMA, \n    TABLE_NAME\nFROM\n    INFORMATION_SCHEMA.TABLES\n\n    {% if prefix %}\nWHERE\n    TABLE_NAME LIKE '{{ prefix }}%'\n{% endif %}\n", 'full_path': './fabric/list_tables.sql.jinja'}, {'dialect': 'sqlserver', 'file_name': 'check_schema_exists.sql.jinja', 'file_contents': "SELECT 1 \nFROM INFORMATION_SCHEMA.SCHEMATA\nWHERE LOWER(SCHEMA_NAME) = LOWER('{{ schema_name | required }}')\n", 'full_path': './sqlserver/check_schema_exists.sql.jinja'}, {'dialect': 'sqlserver', 'file_name': 'check_table_exists.sql.jinja', 'file_contents': "SELECT\n    1\nFROM\n    INFORMATION_SCHEMA.TABLES\nWHERE\n    TABLE_SCHEMA = '{{ schema_name | required }}'\n    AND TABLE_NAME = '{{ table_name | required }}'\n", 'full_path': './sqlserver/check_table_exists.sql.jinja'}, {'dialect': 'sqlserver', 'file_name': 'create_table_from_values.sql.jinja', 'file_contents': "SELECT\n    * INTO {{ schema_name | required('schema_name') }}.{{ table_name | required('table_name') }}\nFROM\n    (\n        VALUES\n            {{ values_clause | required('values_clause') }}\n    ) AS v(\n        {{ column_names | required('column_names') }}\n    )\n", 'full_path': './sqlserver/create_table_from_values.sql.jinja'}, {'dialect': 'sqlserver', 'file_name': 'drop_table.sql.jinja', 'file_contents': 'DROP TABLE IF EXISTS {{ schema_name | required }}.{{ table_name | required }}\n', 'full_path': './sqlserver/drop_table.sql.jinja'}, {'dialect': 'sqlserver', 'file_name': 'insert_row.sql.jinja', 'file_contents': 'INSERT INTO {{ schema_name | required }}.{{ table_name | required }} VALUES ({{ row_values | required }})\n', 'full_path': './sqlserver/insert_row.sql.jinja'}, {'dialect': 'sqlserver', 'file_name': 'list_tables.sql.jinja', 'file_contents': "SELECT\n    TABLE_SCHEMA, \n    TABLE_NAME\nFROM\n    INFORMATION_SCHEMA.TABLES\n\n    {% if prefix %}\nWHERE\n    TABLE_NAME LIKE '{{ prefix }}%'\n{% endif %}\n", 'full_path': './sqlserver/list_tables.sql.jinja'}]


    def __init__(self, dialect: str = "fabric"):
        self.dialect = dialect
        # Use a Jinja2 Environment to add custom filters
        self.env = Environment()
        # Register the 'required' filter
        self.env.filters["required"] = lambda value, var_name="": required_filter(value, var_name)

    def get_template(self, template_name: str, dialect: str) -> str:
        """Get the SQL template for the specified dialect."""
        template = next(
        (t['file_contents'] for t in self.TEMPLATES
            if t['file_name'] == f"{template_name}.sql.jinja" and t['dialect'] == dialect), None
        )
        if not template:
            raise FileNotFoundError(f"Template {template_name} for dialect {dialect} not found.")
        return template

    def render(self, template_name: str, **kwargs) -> str:
        """Render a SQL template with the given parameters."""
        template_str = self.get_template(template_name, self.dialect)
        # Pass parameter names for error messages
        template = self.env.from_string(template_str)
        # Use kwargs for variable names
        params_with_names = {k: v for k, v in kwargs.items()}
        return template.render(**params_with_names)

# === warehouse_utils.py ===
import logging
import os
from typing import Optional

import notebookutils  # type: ignore # noqa: F401
import pandas as pd
import pyodbc  # type: ignore # noqa: F401
from sqlparse import format

from ingen_fab.fabric_api.utils import FabricApiUtils
from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface

    SQLTemplates,  # Assuming this is a custom module for SQL templates
)

logger = logging.getLogger(__name__)

class warehouse_utils(DataStoreInterface):
    """Utilities for interacting with Fabric or local SQL Server warehouses."""

    def __init__(
        self,
        target_workspace_id: Optional[str] = None,
        target_warehouse_id: Optional[str] = None,
        *,
        dialect: str = "fabric",
        connection_string: Optional[str] = None,
    ):
        self._target_workspace_id = target_workspace_id
        self._target_warehouse_id = target_warehouse_id
        self.dialect = dialect
        self.connection_string = connection_string
        self.sql = SQLTemplates(dialect)

    @property
    def target_workspace_id(self) -> str:
        """Get the target workspace ID."""
        if self._target_workspace_id is None:
            raise ValueError("target_workspace_id is not set")
        return self._target_workspace_id

    @property
    def target_store_id(self) -> str:
        """Get the target warehouse ID."""
        if self._target_warehouse_id is None:
            raise ValueError("target_warehouse_id is not set")
        return self._target_warehouse_id

    def get_connection(self):
        """Return a connection object depending on the configured dialect."""
        try:
            if self.dialect == "fabric":
                logger.debug("Connection to Fabric Warehouse")
                conn = notebookutils.data.connect_to_artifact(
                    self.target_warehouse_id, self.target_workspace_id
                )
                logger.debug(f"Connection established: {conn}")
                return conn
            else:
                logger.debug("Connection to SQL Server Warehouse")
                return pyodbc.connect(self.connection_string)  # type: ignore
        except Exception as e:
            logger.error(f"Failed to connect to warehouse: {e}")
            raise
    
    def _connect_to_local_sql_server():
        try:            
            password = os.getenv('SQL_SERVER_PASSWORD', 'default_password')
            connection_string = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;UID=sa;" + f"PWD={password};TrustServerCertificate=yes;"
            conn = pyodbc.connect(connection_string)
            logger.debug("Connected to local SQL Server instance.")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to local SQL Server instance: {e}")
            return None
    
    def execute_query(self, conn, query: str):
        """Execute a query and return results as a DataFrame when possible."""
        logger.debug(conn)
        try:
            logging.info(f"Executing query: {query}")
            if self.dialect == "fabric":               
                result = conn.query(query)
                logging.debug("Query executed successfully.")
                return result
            else:
                cursor = conn.cursor()
                logger.debug(f"Executing query: {query}")
                cursor.execute(query)
                if cursor.description:
                    rows = cursor.fetchall()
                    columns = [d[0] for d in cursor.description]
                    df = pd.DataFrame.from_records(rows, columns=columns)
                else:
                    conn.commit()
                    df = None
                logging.debug("Query executed successfully.")
            return df
        except Exception as e:
            
            #pretty print query 
            formatted_query = format(query, reindent=True, keyword_case='upper')
            logger.info(f"Executing query:\n{formatted_query}")
            logging.error(f"Error executing query: {query}. Error: {e}")
            
            raise

    def create_schema_if_not_exists(self, schema_name: str):
        """Create a schema if it does not already exist."""
        try:
            conn = self.get_connection()
            query = self.sql.render("check_schema_exists", schema_name=schema_name)
            result = self.execute_query(conn, query)
            schema_exists = len(result) > 0 if result is not None else False

            # Create schema if it doesn't exist
            if not schema_exists:
                create_schema_sql = f"CREATE SCHEMA {schema_name};"
                self.execute_query(conn, create_schema_sql)
                logging.info(f"Created schema '{schema_name}'.")
            
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
        
    def write_to_table(
        self,
        df,
        table_name: str,
        schema_name: str = "dbo",
        mode: str = "overwrite",
        options: dict[str, str] | None = None
    ) -> None:
        """Write a DataFrame to a warehouse table."""
        # Call the existing method for backward compatibility
        self.write_to_warehouse_table(df, table_name, schema_name, mode, options or {})

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

    def drop_all_tables(self, schema_name: str | None = None, table_prefix: str | None = None) -> None:
        try:
            conn = self.get_connection()
            query = self.sql.render("list_tables", prefix=table_prefix)
            tables = self.execute_query(conn, query)  # tables is a pandas DataFrame

            # You can use .itertuples() for efficient row access
            for row in tables.itertuples(index=False):
                # Adjust attribute names to match DataFrame columns
                schema_name = getattr(row, 'table_schema', None) or getattr(row, 'TABLE_SCHEMA', None)
                table_name = getattr(row, 'table_name', None) or getattr(row, 'TABLE_NAME', None)

                if not schema_name or not table_name:
                    logging.warning(f"Skipping row with missing schema/table: {row}")
                    continue

                try:
                    drop_query = self.sql.render("drop_table", schema_name=schema_name, table_name=table_name)
                    self.execute_query(conn, drop_query)
                    logging.info(f"✔ Dropped table: {schema_name}.{table_name}")
                except Exception as e:
                    logging.error(f"⚠ Error dropping table {schema_name}.{table_name}: {e}")

            logging.info("✅ All eligible tables have been dropped.")
        except Exception as e:
            logging.error(f"Error dropping tables with prefix {table_prefix}: {e}")

    # --- DataStoreInterface required methods ---
    def get_table_schema(self, table_name: str, schema_name: str | None = None) -> dict[str, object]:
        """Implements DataStoreInterface: Get the schema/column definitions for a table."""
        conn = self.get_connection()
        query = self.sql.render("get_table_schema", table_name=table_name, schema_name=schema_name or "dbo")
        result = self.execute_query(conn, query)
        if result is not None:
            return dict(zip(result.columns, result.values[0])) if not result.empty else {}
        return {}

    def read_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        filters: dict[str, object] | None = None,
    ) -> object:
        """Implements DataStoreInterface: Read data from a table, optionally filtering columns, rows, or limiting results."""
        conn = self.get_connection()
        query = self.sql.render(
            "read_table",
            table_name=table_name,
            schema_name=schema_name or "dbo",
            columns=columns,
            limit=limit,
            filters=filters,
        )
        return self.execute_query(conn, query)

    def delete_from_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        filters: dict[str, object] | None = None,
    ) -> int:
        """Implements DataStoreInterface: Delete rows from a table matching filters."""
        conn = self.get_connection()
        query = self.sql.render(
            "delete_from_table",
            table_name=table_name,
            schema_name=schema_name or "dbo",
            filters=filters,
        )
        result = self.execute_query(conn, query)
        # Return number of rows deleted if possible, else -1
        return getattr(result, 'rowcount', -1) if result is not None else -1

    def rename_table(
        self,
        old_table_name: str,
        new_table_name: str,
        schema_name: str | None = None,
    ) -> None:
        """Implements DataStoreInterface: Rename a table."""
        conn = self.get_connection()
        query = self.sql.render(
            "rename_table",
            old_table_name=old_table_name,
            new_table_name=new_table_name,
            schema_name=schema_name or "dbo",
        )
        self.execute_query(conn, query)

    def create_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        schema: dict[str, object] | None = None,
        options: dict[str, object] | None = None,
    ) -> None:
        """Implements DataStoreInterface: Create a new table with a given schema."""
        conn = self.get_connection()
        query = self.sql.render(
            "create_table",
            table_name=table_name,
            schema_name=schema_name or "dbo",
            schema=schema,
            options=options,
        )
        self.execute_query(conn, query)

    def drop_table(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> None:
        """Implements DataStoreInterface: Drop a single table."""
        conn = self.get_connection()
        query = self.sql.render(
            "drop_table",
            table_name=table_name,
            schema_name=schema_name or "dbo",
        )
        self.execute_query(conn, query)

    def list_tables(self) -> list[str]:
        """Implements DataStoreInterface: List all tables in the warehouse."""
        conn = self.get_connection()
        query = self.sql.render("list_tables")
        result = self.execute_query(conn, query)
        if result is not None and not result.empty:
            return result['table_name'].tolist() if 'table_name' in result.columns else result.iloc[:, 0].tolist()
        return []

    def list_schemas(self) -> list[str]:
        """Implements DataStoreInterface: List all schemas/namespaces in the warehouse."""
        conn = self.get_connection()
        query = self.sql.render("list_schemas")
        result = self.execute_query(conn, query)
        if result is not None and not result.empty:
            return result['schema_name'].tolist() if 'schema_name' in result.columns else result.iloc[:, 0].tolist()
        return []

    def get_table_row_count(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> int:
        """Implements DataStoreInterface: Get the number of rows in a table."""
        conn = self.get_connection()
        query = self.sql.render(
            "get_table_row_count",
            table_name=table_name,
            schema_name=schema_name or "dbo",
        )
        result = self.execute_query(conn, query)
        if result is not None and not result.empty:
            return int(result.iloc[0, 0])
        return 0

    def get_table_metadata(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> dict[str, object]:
        """Implements DataStoreInterface: Get metadata for a table (creation time, size, etc.)."""
        conn = self.get_connection()
        query = self.sql.render(
            "get_table_metadata",
            table_name=table_name,
            schema_name=schema_name or "dbo",
        )
        result = self.execute_query(conn, query)
        if result is not None and not result.empty:
            return result.iloc[0].to_dict()
        return {}

    def vacuum_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        retention_hours: int = 168,
    ) -> None:
        """Implements DataStoreInterface: Perform cleanup/compaction on a table (no-op for SQL warehouses)."""
        # Not applicable for SQL warehouses, but required by interface
        pass

    # --- End DataStoreInterface required methods ---

    # The following methods/properties are not part of DataStoreInterface but are kept for compatibility or utility:
    # - create_schema_if_not_exists
    # - write_to_warehouse_table
    # - _connect_to_local_sql_server

# === ddl_utils.py ===
# { "depends_on": "warehouse_utils" }

import hashlib
import inspect
from datetime import datetime

import notebookutils  # type: ignore # noqa: F401



class ddl_utils:
    """Run DDL scripts once and track execution in a warehouse table."""

    def __init__(self, target_workspace_id: str, target_warehouse_id: str) -> None:
        super().__init__()
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
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class lakehouse_utils:
    def __init__(self, target_workspace_id, target_warehouse_id):
        self.target_workspace_id = target_workspace_id
        self.target_warehouse_id = target_warehouse_id






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Instantiate the Helper Classes

# CELL ********************



cu = config_utils(config_workspace_id,config_lakehouse_id)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Drop all Tables in Target LH

# CELL ********************



if full_reset == True:
    from ipywidgets import ToggleButtons, VBox, Label, Button
    from IPython.display import display as pdisplay
    from IPython.display import clear_output

    
    configs = cu.get_configs_as_object(fabric_environment)

    target_workspace_id = configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_workspace_id")
    target_lakehouse_id =  configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_lakehouse_id")
    
    # Build the widget
    prompt = Label("❗ This will drop all tables — do you want to proceed?")
    yesno = ToggleButtons(options=["No","Yes"], description="Confirm Destructive Action:")
    go = Button(description="Submit", button_style="warning")

    out = VBox([prompt, yesno, go])
    pdisplay(out)

    # Define what happens on click
    def on_click(b):
        clear_output()  # hide the widget after click
        if yesno.value == "Yes":
            print("Dropping tables…")
            lu = lakehouse_utils(target_workspace_id, target_lakehouse_id)
            lu.drop_all_tables()
        else:
            print("Operation cancelled.")

    go.on_click(on_click)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run the lakehouse DDL Notebooks

# CELL ********************



# Import required libraries
from notebookutils import mssparkutils
import sys
from datetime import datetime

# Initialize variables
workspace_id = mssparkutils.runtime.context.get("currentWorkspaceId")
success_count = 0
failed_notebook = None
start_time = datetime.now()

# Define execution function
def execute_notebook(notebook_name, index, total, timeout_seconds=3600):
    """Execute a single notebook and handle success/failure."""
    global success_count
    
    try:
        
        print(f"{'='*60}")
        print(f"Executing notebook {index}/{total}:{notebook_name}")
        print(f"{'='*60}")
        params = {
            "fabric_environment": fabric_environment,
            "config_workspace_id": config_workspace_id,
            "config_lakehouse_id": config_lakehouse_id,
            "target_lakehouse_config_prefix": target_lakehouse_config_prefix,
            'useRootDefaultLakehouse': True
        }
        # Run the notebook
        result = mssparkutils.notebook.run(
            notebook_name,
            timeout_seconds,
            params
        )
        
        if (result == 'success'):
            success_count += 1
        else: 
            raise Exception({"result": result}) 

        print(f"✓ Successfully executed: {notebook_name}")
        print(f"Exit value: {result}")
        return True
        
    except Exception as e:
        print(f"✗ Failed to execute: {notebook_name}")
        print(f"Error: {str(e)}")
        
        # Stop execution on failure
        error_msg = f"Orchestration stopped due to failure in notebook: {notebook_name}. Error: {str(e)}"
        mssparkutils.notebook.exit(error_msg)
        return False

print(f"Starting orchestration for Config lakehouse")
print(f"Start time: {start_time}")
print(f"Workspace ID: {workspace_id}")
print(f"Total notebooks to execute: 2")
print("="*60)
execute_notebook("001_Initial_Creation_Config_Warehouses", 1, 2)
execute_notebook("002_Parquet_Load_Update_Config_Warehouses", 2, 2)

# Final Summary
end_time = datetime.now()
duration = end_time - start_time

print(f"{'='*60}")
print(f"Orchestration Complete!")
print(f"{'='*60}")
print(f"End time: {end_time}")
print(f"Duration: {duration}")
print(f"Total notebooks: 2")
print(f"Successfully executed: {success_count}")
print(f"Failed: 2 - {success_count}")

if success_count == 2:
    print("✓ All notebooks executed successfully!")
    mssparkutils.notebook.exit("success")
else:
    print(f"✗ Orchestration completed with failures")
    mssparkutils.notebook.exit(f"Orchestration completed with {success_count}/2 successful executions")



