import logging
import os
from typing import Optional

import notebookutils  # type: ignore # noqa: F401
import pandas as pd
import pyodbc  # type: ignore # noqa: F401
from sqlparse import format

from ingen_fab.fabric_api.utils import FabricApiUtils
from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface

from .sql_templates import (
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
            # pretty print query
            formatted_query = format(query, reindent=True, keyword_case="upper")
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
        if result is not None:
            return (
                dict(zip(result.columns, result.values[0])) if not result.empty else {}
            )
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
