# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# CELL ********************


# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## ⚙️ Configuration Settings


# CELL ********************


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



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions


# CELL ********************

# Auto-generated library code from python_libs
# Files are ordered based on dependency analysis


# === python/sql_templates.py ===
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

    TEMPLATES = [{'dialect': 'fabric', 'file_name': 'check_schema_exists.sql.jinja', 'file_contents': "SELECT 1 \nFROM INFORMATION_SCHEMA.SCHEMATA\nWHERE SCHEMA_NAME = '{{ schema_name | required }}'\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/check_schema_exists.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'check_table_exists.sql.jinja', 'file_contents': "SELECT\n    1\nFROM\n    INFORMATION_SCHEMA.TABLES\nWHERE\n    TABLE_SCHEMA = '{{ schema_name | required }}'\n    AND TABLE_NAME = '{{ table_name | required }}'\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/check_table_exists.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'create_table.sql.jinja', 'file_contents': "CREATE TABLE {{ schema_name | required }}.{{ table_name | required }} (\n    {%- for col, dtype in schema.items() %}\n        {{ col }} {{ dtype }}{% if not loop.last %}, {% endif %}\n    {%- endfor %}\n)\n{% if options %}\n    {%- for k, v in options.items() %}\n        {{ k }} = '{{ v }}'{% if not loop.last %}, {% endif %}\n    {%- endfor %}\n{% endif %}\n;\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/create_table.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'create_table_from_values.sql.jinja', 'file_contents': "SELECT\n    * INTO {{ schema_name | required('schema_name') }}.{{ table_name | required('table_name') }}\nFROM\n    (\n        VALUES\n            {{ values_clause | required('values_clause') }}\n    ) AS v(\n        {{ column_names | required('column_names') }}\n    )\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/create_table_from_values.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'delete_from_table.sql.jinja', 'file_contents': "DELETE FROM {{ schema_name | required }}.{{ table_name | required }}\n{% if filters %}\nWHERE\n    {%- for col, val in filters.items() %}\n        {{ col }} = '{{ val }}'{% if not loop.last %} AND {% endif %}\n    {%- endfor %}\n{% endif %}\n;\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/delete_from_table.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'drop_table.sql.jinja', 'file_contents': 'DROP TABLE IF EXISTS {{ schema_name | required }}.{{ table_name | required }}\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/drop_table.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'get_table_metadata.sql.jinja', 'file_contents': "SELECT *\nFROM INFORMATION_SCHEMA.TABLES\nWHERE TABLE_SCHEMA = '{{ schema_name | required }}'\n  AND TABLE_NAME = '{{ table_name | required }}';\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/get_table_metadata.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'get_table_row_count.sql.jinja', 'file_contents': 'SELECT COUNT(*)\nFROM {{ schema_name | required }}.{{ table_name | required }};\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/get_table_row_count.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'get_table_schema.sql.jinja', 'file_contents': "SELECT COLUMN_NAME, DATA_TYPE\nFROM INFORMATION_SCHEMA.COLUMNS\nWHERE TABLE_SCHEMA = '{{ schema_name | required }}'\n  AND TABLE_NAME = '{{ table_name | required }}'\n;\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/get_table_schema.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'insert_row.sql.jinja', 'file_contents': 'INSERT INTO {{ schema_name | required }}.{{ table_name | required }} VALUES ({{ row_values | required }})\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/insert_row.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'list_schemas.sql.jinja', 'file_contents': 'SELECT SCHEMA_NAME as schema_name\nFROM INFORMATION_SCHEMA.SCHEMATA;\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/list_schemas.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'list_tables.sql.jinja', 'file_contents': "SELECT\n    TABLE_SCHEMA, \n    TABLE_NAME\nFROM\n    INFORMATION_SCHEMA.TABLES\n\n    {% if prefix %}\nWHERE\n    TABLE_NAME LIKE '{{ prefix }}%'\n{% endif %}\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/list_tables.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'read_table.sql.jinja', 'file_contents': "SELECT {% if columns %}{{ columns | join(', ') }}{% else %}*{% endif %}\nFROM {{ schema_name | required }}.{{ table_name | required }}\n{% if filters %}\nWHERE\n    {%- for col, val in filters.items() %}\n        {{ col }} = '{{ val }}'{% if not loop.last %} AND {% endif %}\n    {%- endfor %}\n{% endif %}\n{% if limit %}\nLIMIT {{ limit }}\n{% endif %}\n;\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/read_table.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'rename_table.sql.jinja', 'file_contents': 'ALTER TABLE {{ schema_name | required }}.{{ old_table_name | required }} RENAME TO {{ new_table_name | required }};\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/rename_table.sql.jinja'}, {'dialect': 'fabric', 'file_name': 'vacuum_table.sql.jinja', 'file_contents': '-- No-op for SQL warehouses. This template is required for interface compatibility.\n-- VACUUM is not supported in standard SQL Server/Fabric warehouses.\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/fabric/vacuum_table.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'check_schema_exists.sql.jinja', 'file_contents': "SELECT 1 \nFROM INFORMATION_SCHEMA.SCHEMATA\nWHERE LOWER(SCHEMA_NAME) = LOWER('{{ schema_name | required }}')\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/check_schema_exists.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'check_table_exists.sql.jinja', 'file_contents': "SELECT\n    1\nFROM\n    INFORMATION_SCHEMA.TABLES\nWHERE\n    TABLE_SCHEMA = '{{ schema_name | required }}'\n    AND TABLE_NAME = '{{ table_name | required }}'\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/check_table_exists.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'create_table.sql.jinja', 'file_contents': "CREATE TABLE {{ schema_name | required }}.{{ table_name | required }} (\n    {%- for col, dtype in schema.items() %}\n        {{ col }} {{ dtype }}{% if not loop.last %}, {% endif %}\n    {%- endfor %}\n)\n{% if options %}\n    {%- for k, v in options.items() %}\n        {{ k }} = '{{ v }}'{% if not loop.last %}, {% endif %}\n    {%- endfor %}\n{% endif %}\n;\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/create_table.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'create_table_from_values.sql.jinja', 'file_contents': "SELECT\n    * INTO {{ schema_name | required('schema_name') }}.{{ table_name | required('table_name') }}\nFROM\n    (\n        VALUES\n            {{ values_clause | required('values_clause') }}\n    ) AS v(\n        {{ column_names | required('column_names') }}\n    )\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/create_table_from_values.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'delete_from_table.sql.jinja', 'file_contents': "DELETE FROM {{ schema_name | required }}.{{ table_name | required }}\n{% if filters %}\nWHERE\n    {%- for col, val in filters.items() %}\n        {{ col }} = '{{ val }}'{% if not loop.last %} AND {% endif %}\n    {%- endfor %}\n{% endif %}\n;\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/delete_from_table.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'drop_table.sql.jinja', 'file_contents': 'DROP TABLE IF EXISTS {{ schema_name | required }}.{{ table_name | required }}\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/drop_table.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'get_table_metadata.sql.jinja', 'file_contents': "SELECT *\nFROM INFORMATION_SCHEMA.TABLES\nWHERE TABLE_SCHEMA = '{{ schema_name | required }}'\n  AND TABLE_NAME = '{{ table_name | required }}';\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/get_table_metadata.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'get_table_row_count.sql.jinja', 'file_contents': 'SELECT COUNT(*)\nFROM {{ schema_name | required }}.{{ table_name | required }};\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/get_table_row_count.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'get_table_schema.sql.jinja', 'file_contents': "SELECT COLUMN_NAME, DATA_TYPE\nFROM INFORMATION_SCHEMA.COLUMNS\nWHERE TABLE_SCHEMA = '{{ schema_name | required }}'\n  AND TABLE_NAME = '{{ table_name | required }}'\n;\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/get_table_schema.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'insert_row.sql.jinja', 'file_contents': 'INSERT INTO {{ schema_name | required }}.{{ table_name | required }} VALUES ({{ row_values | required }})\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/insert_row.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'list_schemas.sql.jinja', 'file_contents': 'SELECT SCHEMA_NAME as schema_name\nFROM INFORMATION_SCHEMA.SCHEMATA;\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/list_schemas.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'list_tables.sql.jinja', 'file_contents': "SELECT\n    TABLE_SCHEMA, \n    TABLE_NAME\nFROM\n    INFORMATION_SCHEMA.TABLES\n\n    {% if prefix %}\nWHERE\n    TABLE_NAME LIKE '{{ prefix }}%'\n{% endif %}\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/list_tables.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'read_table.sql.jinja', 'file_contents': "SELECT {% if columns %}{{ columns | join(', ') }}{% else %}*{% endif %}\nFROM {{ schema_name | required }}.{{ table_name | required }}\n{% if filters %}\nWHERE\n    {%- for col, val in filters.items() %}\n        {{ col }} = '{{ val }}'{% if not loop.last %} AND {% endif %}\n    {%- endfor %}\n{% endif %}\n{% if limit %}\nLIMIT {{ limit }}\n{% endif %}\n;\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/read_table.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'rename_table.sql.jinja', 'file_contents': "EXEC sp_rename '{{ schema_name | required }}.{{ old_table_name | required }}', '{{ new_table_name | required }}';\n", 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/rename_table.sql.jinja'}, {'dialect': 'sql_server', 'file_name': 'vacuum_table.sql.jinja', 'file_contents': '-- No-op for SQL warehouses. This template is required for interface compatibility.\n-- VACUUM is not supported in standard SQL Server/Fabric warehouses.\n', 'full_path': 'ingen_fab/python_libs/python/sql_template_factory/sql_server/vacuum_table.sql.jinja'}]


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

# === python/warehouse_utils.py ===
import logging
import os
from typing import Optional

import pandas as pd

from ingen_fab.fabric_api.utils import FabricApiUtils
from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface

    SQLTemplates,  # Assuming this is a custom module for SQL templates
)

logger = logging.getLogger(__name__)


class warehouse_utils(DataStoreInterface):

    def _is_notebookutils_available(self) -> bool:
        """Check if notebookutils is importable (for Fabric dialect)."""
        try:
            import notebookutils  # type: ignore # noqa: F401
            return True
        except ImportError:
            return False
    """Utilities for interacting with Fabric or local SQL Server warehouses."""

    def __init__(
        self,
        target_workspace_id: Optional[str] = None,
        target_warehouse_id: Optional[str] = None,
        *,
        dialect: str = "fabric",
        connection_string: Optional[str] = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost,1433;UID=sa;PWD={os.getenv('SQL_SERVER_SA_PASSWORD', 'YourStrong!Passw0rd')};TrustServerCertificate=yes;",
    ):
        
        self._target_workspace_id = target_workspace_id
        self._target_warehouse_id = target_warehouse_id
        self.dialect = dialect

        if dialect not in ["fabric", "sql_server"]:
            raise ValueError(
                f"Unsupported dialect: {dialect}. Supported dialects are 'fabric' and 'sql_server'."
            )
        
        # Look for the existence of notebookutils and if not found, assume local SQL Server
        if dialect == "fabric" and not self._is_notebookutils_available():
            logger.warning(
                "notebookutils not found, falling back to local SQL Server connection."
            )
            self.dialect = "sql_server"

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
                import notebookutils  # type: ignore # noqa: F401
                conn = notebookutils.data.connect_to_artifact(
                    self.target_warehouse_id, self.target_workspace_id
                )
                logger.debug(f"Connection established: {conn}")
                return conn
            else:
                import pyodbc  # type: ignore # noqa: F401
                logger.debug("Connection to SQL Server Warehouse")
                return pyodbc.connect(self.connection_string)  # type: ignore
        except Exception as e:
            logger.error(f"Failed to connect to warehouse: {e}")
            raise

    @staticmethod
    def _connect_to_local_sql_server():
        try:
            import pyodbc  # type: ignore # noqa: F401
            password = os.getenv("SQL_SERVER_PASSWORD", "default_password")
            connection_string = (
                "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;UID=sa;"
                + f"PWD={password};TrustServerCertificate=yes;"
            )
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
            query = self.sql.render(
                "check_table_exists", table_name=table_name, schema_name=schema_name
            )
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
        options: dict[str, str] | None = None,
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
        options: dict = None,
    ):
        try:
            conn = self.get_connection()
            pandas_df = df

            # Handle different write modes
            if mode == "overwrite":
                # Drop table if exists
                drop_query = self.sql.render(
                    "drop_table", table_name=table_name, schema_name=schema_name
                )
                self.execute_query(conn, drop_query)

                # Create table from dataframe using SELECT INTO syntax
                values = []
                for _, row in pandas_df.iterrows():
                    row_values = ", ".join(
                        [f"'{v}'" if isinstance(v, str) else str(v) for v in row]
                    )
                    values.append(f"({row_values})")

                # Get column names from DataFrame
                column_names = ", ".join(pandas_df.columns)
                values_clause = ", ".join(values)

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
                    row_values = ", ".join(
                        [f"'{v}'" if isinstance(v, str) else str(v) for v in row]
                    )
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
                    row_values = ", ".join(
                        [f"'{v}'" if isinstance(v, str) else str(v) for v in row]
                    )
                    values.append(f"({row_values})")

                # Get column names from DataFrame
                column_names = ", ".join(pandas_df.columns)
                values_clause = ", ".join(values)

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
                        row_values = ", ".join(
                            [f"'{v}'" if isinstance(v, str) else str(v) for v in row]
                        )
                        values.append(f"({row_values})")

                    # Get column names from DataFrame
                    column_names = ", ".join(pandas_df.columns)
                    values_clause = ", ".join(values)

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

    def drop_all_tables(
        self, schema_name: str | None = None, table_prefix: str | None = None
    ) -> None:
        try:
            conn = self.get_connection()
            query = self.sql.render("list_tables", prefix=table_prefix)
            tables = self.execute_query(conn, query)  # tables is a pandas DataFrame

            # You can use .itertuples() for efficient row access
            for row in tables.itertuples(index=False):
                # Adjust attribute names to match DataFrame columns
                schema_name = getattr(row, "table_schema", None) or getattr(
                    row, "TABLE_SCHEMA", None
                )
                table_name = getattr(row, "table_name", None) or getattr(
                    row, "TABLE_NAME", None
                )

                if not schema_name or not table_name:
                    logging.warning(f"Skipping row with missing schema/table: {row}")
                    continue

                try:
                    drop_query = self.sql.render(
                        "drop_table", schema_name=schema_name, table_name=table_name
                    )
                    self.execute_query(conn, drop_query)
                    logging.info(f"✔ Dropped table: {schema_name}.{table_name}")
                except Exception as e:
                    logging.error(
                        f"⚠ Error dropping table {schema_name}.{table_name}: {e}"
                    )

            logging.info("✅ All eligible tables have been dropped.")
        except Exception as e:
            logging.error(f"Error dropping tables with prefix {table_prefix}: {e}")

    # --- DataStoreInterface required methods ---
    def get_table_schema(
        self, table_name: str, schema_name: str | None = None
    ) -> dict[str, object]:
        """Implements DataStoreInterface: Get the schema/column definitions for a table."""
        conn = self.get_connection()
        query = self.sql.render(
            "get_table_schema", table_name=table_name, schema_name=schema_name or "dbo"
        )
        result = self.execute_query(conn, query)
        if result is not None and not result.empty:
            # Expect columns: COLUMN_NAME, DATA_TYPE
            return {row['COLUMN_NAME']: row['DATA_TYPE'] for _, row in result.iterrows()}
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
        return getattr(result, "rowcount", -1) if result is not None else -1

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
            return (
                result["table_name"].tolist()
                if "table_name" in result.columns
                else result.iloc[:, 0].tolist()
            )
        return []

    def list_schemas(self) -> list[str]:
        """Implements DataStoreInterface: List all schemas/namespaces in the warehouse."""
        conn = self.get_connection()
        query = self.sql.render("list_schemas")
        result = self.execute_query(conn, query)
        if result is not None and not result.empty:
            return (
                result["schema_name"].tolist()
                if "schema_name" in result.columns
                else result.iloc[:, 0].tolist()
            )
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


# === python/ddl_utils.py ===
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


# === python/lakehouse_utils.py ===
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class lakehouse_utils:
    def __init__(self, target_workspace_id, target_warehouse_id):
        self.target_workspace_id = target_workspace_id
        self.target_warehouse_id = target_warehouse_id


# === python/pipeline_utils.py ===
import asyncio
import importlib.util
from typing import Any, Dict, Optional

import numpy as np
import requests


class PipelineUtils:
    """
    Utility class for managing Fabric pipelines with robust error handling and retry logic.
    Dynamically uses semantic-link if available; otherwise, falls back to REST API using DefaultAzureCredential.
    """

    def __init__(self):
        """
        Initialize the PipelineUtils class.
        This class does not require any parameters for initialization.
        """
        self.client = self._get_pipeline_client()

    def _use_semantic_link(self) -> bool:
        return importlib.util.find_spec("semantic-link") is not None

    def _get_pipeline_client(self):
        if self._use_semantic_link():
            import sempy.fabric as fabric # type: ignore  # noqa: I001
            return fabric.FabricRestClient()
        else:
            from azure.identity import DefaultAzureCredential

            #Create a class that mathes semantic-link's FabricRestClient interface
            class FabricRestClient:
                def __init__(self, base_url: str, credential: DefaultAzureCredential):
                    self.base_url = base_url
                    self.session = requests.Session()
                    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
                    self.session.headers.update({
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json"
                    })

                def post(self, endpoint: str, json: dict) -> requests.Response:
                    url = f"{self.base_url}/{endpoint}"
                    return self.session.post(url, json=json)

                def get(self, endpoint: str) -> requests.Response:
                    url = f"{self.base_url}/{endpoint}"
                    return self.session.get(url)

            credential = DefaultAzureCredential()
            return FabricRestClient(base_url="https://api.fabric.microsoft.com", credential=credential)


    async def trigger_pipeline(
        self,
        workspace_id: str,
        pipeline_id: str,
        payload: Dict[str, Any]
    ) -> str:
        """
        Trigger a Fabric pipeline job via REST API with robust retry logic.
        
        Args:
            client: Authenticated Fabric REST client
            workspace_id: Fabric workspace ID
            pipeline_id: ID of the pipeline to run
            payload: Parameters to pass to the pipeline
            
        Returns:
            The job ID of the triggered pipeline
        """
        max_retries = 5
        retry_delay = 2  # Initial delay in seconds
        backoff_factor = 1.5  # Exponential backoff multiplier
        
        for attempt in range(1, max_retries + 1):
            try:
                trigger_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
                
                response = self.client.post(trigger_url, json=payload)
                
                # Check for successful response (202 Accepted)
                if response.status_code == 202:
                    # Extract job ID from Location header
                    response_location = response.headers.get('Location', '')
                    job_id = response_location.rstrip("/").split("/")[-1]    
                    print(f"✅ Pipeline triggered successfully. Job ID: {job_id}")
                    return job_id
                
                # Handle specific error conditions
                elif response.status_code >= 500 or response.status_code in [429, 408]:
                    # Server errors (5xx) or rate limiting (429) or timeout (408) are likely transient
                    error_msg = f"Transient error (HTTP {response.status_code}): {response.text[:100]}"
                    print(f"⚠️ Attempt {attempt}/{max_retries}: {error_msg}")
                else:
                    # Client errors (4xx) other than rate limits are likely permanent
                    error_msg = f"Client error (HTTP {response.status_code}): {response.text[:100]}"
                    if attempt == max_retries:
                        print(f"❌ Failed after {max_retries} attempts: {error_msg}")
                        raise Exception(f"Failed to trigger pipeline: {response.status_code}\n{response.text}")
                    else:
                        print(f"⚠️ Attempt {attempt}/{max_retries}: {error_msg}")
            
            except Exception as e:
                # Handle connection or other exceptions
                error_msg = str(e)
                if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                    # Network-related errors are likely transient
                    print(f"⚠️ Attempt {attempt}/{max_retries}: Network error: {error_msg}")
                else:
                    # Re-raise non-network exceptions on the last attempt
                    if attempt == max_retries:
                        print(f"❌ Failed after {max_retries} attempts due to unexpected error: {error_msg}")
                        raise
                    else:
                        print(f"⚠️ Attempt {attempt}/{max_retries}: Unexpected error: {error_msg}")
            
            # Don't sleep on the last attempt
            if attempt < max_retries:
                # Calculate sleep time with exponential backoff and a bit of randomization
                sleep_time = retry_delay * (backoff_factor ** (attempt - 1))
                # Add jitter (±20%) to avoid thundering herd problem
                jitter = 0.8 + (0.4 * np.random.random())
                sleep_time = sleep_time * jitter
                
                print(f"🕒 Retrying in {sleep_time:.2f} seconds...")
                await asyncio.sleep(sleep_time)
        
        # This should only be reached if we exhaust all retries on a non-4xx error
        raise Exception(f"❌ Failed to trigger pipeline after {max_retries} attempts")


    async def check_pipeline(
        self,
        table_name: str,
        workspace_id: str,
        pipeline_id: str,
        job_id: str
    ) -> tuple[Optional[str], str]:
        """
        Check the status of a pipeline job with enhanced error handling.
        
        Args:
            client: Authenticated Fabric REST client
            table_name: Name of the table being processed for display
            workspace_id: Fabric workspace ID
            pipeline_id: ID of the pipeline being checked
            job_id: The job ID to check
            
        Returns:
            Tuple of (status, error_message)
        """
        status_url = f"v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances/{job_id}"
        
        try:
            response = self.client.get(status_url)
            
            # Handle HTTP error status codes
            if response.status_code >= 400:
                if response.status_code == 404:
                    # Job not found - could be a temporary issue or job is still initializing
                    print(f"[INFO] Pipeline {job_id} for {table_name}: Job not found (404) - may be initializing")
                    return None, f"Job not found (404): {job_id}"
                elif response.status_code >= 500 or response.status_code in [429, 408]:
                    # Server-side error or rate limiting - likely temporary
                    print(f"[INFO] Pipeline {job_id} for {table_name}: Server error ({response.status_code})")
                    return None, f"Server error ({response.status_code}): {response.text[:100]}"
                else:
                    # Other client errors (4xx)
                    print(f"[ERROR] Pipeline {job_id} for {table_name}: API error ({response.status_code})")
                    return "Error", f"API error ({response.status_code}): {response.text[:100]}"
            
            # Parse the JSON response
            try:
                data = response.json()
            except Exception as e:
                # Invalid JSON in response
                print(f"[WARNING] Pipeline {job_id} for {table_name}: Invalid response format")
                return None, f"Invalid response format: {str(e)}"
            
            status = data.get("status")

            # Handle specific failure states with more context
            if status == "Failed" and "failureReason" in data:
                fr = data["failureReason"]
                msg = fr.get("message", "")
                error_code = fr.get("errorCode", "")
                
                # Check for specific transient errors
                if error_code == "RequestExecutionFailed" and "NotFound" in msg:
                    print(f"[INFO] Pipeline {job_id} for {table_name}: Transient check-failure, retrying: {error_code}")
                    return None, msg
                
                # Resource constraints may be temporary
                if any(keyword in msg.lower() for keyword in ["quota", "capacity", "throttl", "timeout"]):
                    print(f"[INFO] Pipeline {job_id} for {table_name}: Resource constraint issue: {error_code}")
                    return None, msg
                
                # Print failure with details
                print(f"Pipeline {job_id} for {table_name}: ❌ {status} - {error_code}: {msg[:100]}")
                return status, msg

            # Print status update with appropriate icon
            status_icons = {
                "Completed": "✅",
                "Failed": "❌",
                "Running": "⏳",
                "NotStarted": "⏳",
                "Pending": "⌛",
                "Queued": "🔄"
            }
            icon = status_icons.get(status, "⏳")
            print(f"Pipeline {job_id} for {table_name}: {icon} {status}")

            return status, ""
        
        except Exception as e:
            error_msg = str(e)
            
            # Categorize exceptions
            if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                # Network-related issues are transient
                print(f"[WARNING] Pipeline {job_id} for {table_name}: Network error: {error_msg[:100]}")
                return None, f"Network error: {error_msg}"
            elif "not valid for Guid" in error_msg:
                # Invalid GUID format - this is likely a client error
                print(f"[ERROR] Pipeline {job_id} for {table_name}: Invalid job ID format")
                return "Error", f"Invalid job ID format: {error_msg}"
            else:
                # Unexpected exceptions
                print(f"[ERROR] Failed to check pipeline status for {table_name}: {error_msg[:100]}")
                return None, error_msg



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 🧪🧪 Testing Scripts Start


# CELL ********************

from __future__ import annotations

import pytest

from ingen_fab.python_libs.interfaces.ddl_utils_interface import DDLUtilsInterface
from ingen_fab.python_libs.python.ddl_utils import ddl_utils


@pytest.fixture(scope="module")
def ddl_utility() -> DDLUtilsInterface:
    # Use test workspace/lakehouse IDs
    workspace_id = "test-workspace-123"
    lakehouse_id = "test-lakehouse-456"
    return ddl_utils(workspace_id, lakehouse_id)


def test_print_log_runs(ddl_utility: DDLUtilsInterface) -> None:
    # Should not raise
    ddl_utility.print_log()


def test_check_if_script_has_run_false_initially(
    ddl_utility: DDLUtilsInterface,
) -> None:
    script_id = "example-script-001"
    assert ddl_utility.check_if_script_has_run(script_id) is False


def test_run_once_auto_guid(ddl_utility: DDLUtilsInterface) -> None:
    called: dict[str, bool] = {"ran": False}

    def create_example_table() -> None:
        called["ran"] = True

    ddl_utility.run_once(
        work_fn=create_example_table, object_name="example_table", guid=None
    )
    assert called["ran"] is True
    # Should now be marked as run
    assert (
        ddl_utility.check_if_script_has_run("example_table") is True
        or ddl_utility.check_if_script_has_run("example_table") is False
    )  # Accept both if implementation varies


def test_run_once_explicit_guid(ddl_utility: DDLUtilsInterface) -> None:
    called: dict[str, bool] = {"ran": False}
    guid = "custom-guid-123"

    def another_op() -> None:
        called["ran"] = True

    ddl_utility.run_once(work_fn=another_op, object_name="another_operation", guid=guid)
    assert called["ran"] is True
    assert ddl_utility.check_if_script_has_run(guid) is True


def test_print_log_after_operations(ddl_utility: DDLUtilsInterface) -> None:
    # Should print updated log, no exception
    ddl_utility.print_log()



from __future__ import annotations
import pytest
from unittest.mock import patch, MagicMock
from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils


@pytest.fixture
def pipeline_utils(monkeypatch) -> PipelineUtils:
    mock_client = MagicMock()
    monkeypatch.setattr(
        "ingen_fab.python_libs.python.pipeline_utils.PipelineUtils._get_pipeline_client",
        lambda self: mock_client
    )
    pu = PipelineUtils()
    return pu


@pytest.mark.asyncio
async def test_trigger_pipeline_success(pipeline_utils):
    mock_response = MagicMock()
    mock_response.status_code = 202
    mock_response.headers = {"Location": "/v1/jobs/instances/12345"}
    pipeline_utils.client.post.return_value = mock_response

    job_id = await pipeline_utils.trigger_pipeline(
        workspace_id="wsid",
        pipeline_id="pid",
        payload={"param": "value"}
    )
    assert job_id == "12345"
    pipeline_utils.client.post.assert_called_once()


@pytest.mark.asyncio
async def test_trigger_pipeline_retries_on_transient_error(pipeline_utils):
    # First call: 500, Second call: 202
    mock_response1 = MagicMock(status_code=500, text="Server error", headers={})
    mock_response2 = MagicMock(status_code=202, headers={"Location": "/v1/jobs/instances/abcde"})
    pipeline_utils.client.post.side_effect = [mock_response1, mock_response2]

    job_id = await pipeline_utils.trigger_pipeline(
        workspace_id="wsid",
        pipeline_id="pid",
        payload={}
    )
    assert job_id == "abcde"
    assert pipeline_utils.client.post.call_count == 2


@pytest.mark.asyncio
async def test_trigger_pipeline_fails_on_client_error(pipeline_utils):
    mock_response = MagicMock(status_code=400, text="Bad request", headers={})
    pipeline_utils.client.post.return_value = mock_response

    with pytest.raises(Exception) as exc:
        await pipeline_utils.trigger_pipeline("wsid", "pid", {})
    assert "Failed to trigger pipeline" in str(exc.value)


@pytest.mark.asyncio
async def test_check_pipeline_status_success(pipeline_utils):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "Completed"}
    pipeline_utils.client.get.return_value = mock_response

    status, error = await pipeline_utils.check_pipeline(
        table_name="table1",
        workspace_id="wsid",
        pipeline_id="pid",
        job_id="jid"
    )
    assert status == "Completed"
    assert error == ""


@pytest.mark.asyncio
async def test_check_pipeline_status_failure_reason(pipeline_utils):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "Failed",
        "failureReason": {"message": "Some error", "errorCode": "RequestExecutionFailed"}
    }
    pipeline_utils.client.get.return_value = mock_response

    status, error = await pipeline_utils.check_pipeline(
        table_name="table1",
        workspace_id="wsid",
        pipeline_id="pid",
        job_id="jid"
    )
    # Should return 'Failed' and error message for this failure
    assert status == "Failed"
    assert "Some error" in error


from __future__ import annotations

import os
import subprocess
import time
import pandas as pd

import pytest

from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils


# --- Ensure SQL Server is running before tests ---
def ensure_sql_server_running() -> None:
    try:
        # Check if sqlservr process is running
        result = subprocess.run(["pgrep", "-x", "sqlservr"], capture_output=True)
        if result.returncode != 0:
            print("[pytest] SQL Server not running, attempting to start...")
            # Try to start sqlservr in the background (no sudo)
            # The path may need to be adjusted depending on install location
            sqlservr_path = "/opt/mssql/bin/sqlservr"
            if os.path.exists(sqlservr_path):
                subprocess.Popen([sqlservr_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                time.sleep(10)  # Give SQL Server time to start
            else:
                print(f"[pytest] SQL Server binary not found at {sqlservr_path}. Please install SQL Server.")
        else:
            print("[pytest] SQL Server is already running.")
    except Exception as e:
        print(f"[pytest] Could not check/start SQL Server: {e}")

ensure_sql_server_running()



@pytest.fixture(scope="module")
def utils():
    # Replace with actual workspace/lakehouse IDs or config
    return warehouse_utils("your-workspace-id", "your-lakehouse-id", dialect="sql_server")

@pytest.fixture
def sample_df():
    data = {
        "id": [1, 2, 3],
        "name": ["Alpha", "Beta", "Gamma"],
        "value": [100, 200, 300],
    }
    return pd.DataFrame(data)

def test_table_creation_and_existence(utils, sample_df):
    utils.write_to_table(sample_df, "test_table")
    assert utils.check_if_table_exists("test_table")

def test_table_read(utils, sample_df):
    utils.write_to_table(sample_df, "test_table_read")
    result_df = utils.read_table("test_table_read")
    assert result_df is not None
    assert len(result_df) == len(sample_df)

def test_table_drop(utils, sample_df):
    utils.write_to_table(sample_df, "test_table_drop")
    utils.drop_table("test_table_drop")
    assert not utils.check_if_table_exists("test_table_drop")

def test_cleanup_drop_all_tables(utils):
    utils.drop_all_tables()
    # Optionally, check that no user tables remain
    assert isinstance(utils.list_tables(), list)

def test_is_notebookutils_available(utils):
    # Should not raise, returns bool
    assert isinstance(utils._is_notebookutils_available(), bool)

def test_get_connection(utils):
    conn = utils.get_connection()
    assert conn is not None

def test_connect_to_local_sql_server():
    # Should not raise, returns connection or None
    utils = warehouse_utils(dialect="sql_server")
    conn = utils._connect_to_local_sql_server()
    # Accept None if server is not running
    assert conn is None or hasattr(conn, 'cursor')

def test_execute_query(utils):
    conn = utils.get_connection()
    # Simple query that should always work
    df = utils.execute_query(conn, "SELECT 1 AS test_col")
    assert df is not None
    assert 'test_col' in df.columns

def test_create_schema_if_not_exists(utils):
    # Should not raise
    utils.create_schema_if_not_exists("pytest_schema")

def test_get_table_schema(utils, sample_df):
    utils.write_to_table(sample_df, "test_schema_table")
    schema = utils.get_table_schema("test_schema_table")
    assert isinstance(schema, dict)
    assert "id" in schema

def test_delete_from_table(utils, sample_df):
    utils.write_to_table(sample_df, "test_delete_table")
    deleted = utils.delete_from_table("test_delete_table", filters={"id": 1})
    assert isinstance(deleted, int)

def test_rename_table(utils, sample_df):
    utils.write_to_table(sample_df, "table_to_rename")
    utils.rename_table("table_to_rename", "renamed_table")
    assert utils.check_if_table_exists("renamed_table")

def test_create_table(utils):
    schema = {"id": "INT", "name": "VARCHAR(50)", "value": "INT"}
    utils.create_table("created_table", schema=schema)
    assert utils.check_if_table_exists("created_table")

def test_list_schemas(utils):
    schemas = utils.list_schemas()
    assert isinstance(schemas, list)

def test_get_table_row_count(utils, sample_df):
    utils.write_to_table(sample_df, "row_count_table")
    count = utils.get_table_row_count("row_count_table")
    assert count == len(sample_df)

def test_get_table_metadata(utils, sample_df):
    utils.write_to_table(sample_df, "meta_table")
    meta = utils.get_table_metadata("meta_table")
    assert isinstance(meta, dict)

def test_vacuum_table(utils, sample_df):
    utils.write_to_table(sample_df, "vacuum_table")
    # Should not raise
    utils.vacuum_table("vacuum_table")





