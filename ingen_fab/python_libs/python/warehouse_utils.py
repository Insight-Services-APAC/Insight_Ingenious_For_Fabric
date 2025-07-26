import logging
import os
import platform
from datetime import datetime, date
from typing import Any, Optional

import pandas as pd

from ingen_fab.python_libs.common.config_utils import get_configs_as_object
from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface
from ingen_fab.python_libs.python.notebook_utils_abstraction import NotebookUtilsFactory
from ingen_fab.python_libs.python.sql_templates import (
    SQLTemplates,  # Assuming this is a custom module for SQL templates
)

logger = logging.getLogger(__name__)


class warehouse_utils(DataStoreInterface):
    def _is_notebookutils_available(
        self, notebookutils: Optional[Any] = None, mssparkutils: Optional[Any] = None
    ) -> bool:
        """Check if notebookutils is importable (for Fabric dialect)."""
        return NotebookUtilsFactory.create_instance(
            notebookutils=notebookutils
        ).is_available()

    """Utilities for interacting with Fabric or local SQL Server warehouses."""

    def __init__(
        self,
        target_workspace_id: Optional[str] = None,
        target_warehouse_id: Optional[str] = None,
        *,
        dialect: str = "fabric",
        connection_string: Optional[str] = None,
        notebookutils: Optional[Any] = None,
    ):
        self._target_workspace_id = target_workspace_id
        self._target_warehouse_id = target_warehouse_id
        self.dialect = dialect

        if dialect not in ["fabric", "sql_server", "mysql"]:
            raise ValueError(
                f"Unsupported dialect: {dialect}. Supported dialects are 'fabric', 'sql_server', and 'mysql'."
            )

        # Initialize notebook utils abstraction
        self.notebook_utils = NotebookUtilsFactory.get_instance(
            notebookutils=notebookutils
        )

        # Look for the existence of notebookutils and if not found, assume local database
        # Check if we're using LocalNotebookUtils (fallback) instead of real Fabric notebookutils
        if (
            dialect == "fabric"
            and type(self.notebook_utils).__name__ == "LocalNotebookUtils"
        ):
            # Detect ARM architecture and choose appropriate local database
            machine_arch = platform.machine().lower()
            is_arm = machine_arch in ["arm64", "aarch64", "arm"]

            if is_arm:
                logger.warning(
                    f"notebookutils not found on ARM architecture ({machine_arch}), falling back to local MySQL connection."
                )
                self.dialect = "mysql"
            else:
                logger.warning(
                    f"notebookutils not found on {machine_arch} architecture, falling back to local SQL Server connection."
                )
                self.dialect = "sql_server"

        # Set default connection string based on final dialect if not provided
        if connection_string is None:
            if self.dialect == "sql_server":
                self.connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost,1433;UID=sa;PWD={os.getenv('SQL_SERVER_PASSWORD', 'YourStrong!Passw0rd')};TrustServerCertificate=yes;"
            elif self.dialect == "mysql":
                self.connection_string = f"host=localhost,port=3306,user=root,password={os.getenv('MYSQL_PASSWORD', 'password')},database=local"
            else:
                self.connection_string = ""
        else:
            self.connection_string = connection_string

        self.sql = SQLTemplates(self.dialect)

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
        if get_configs_as_object().fabric_environment == "local":
            # Use PostgreSQL for local development
            conn = self._connect_to_local_postgresql()
        else:
            conn = self.notebook_utils.connect_to_artifact(
                self._target_warehouse_id, self._target_workspace_id
            )
        return conn

    def execute_query(
        self, conn=None, query: str = None, params: tuple | list | None = None
    ):
        """Execute a query and return results as a DataFrame when possible."""
        if not conn:
            conn = self.get_connection()
        # Check if the connection is of type pyodbc.Connection
        try:
            logging.debug(f"Executing query: {query}")
            if params:
                logging.debug(f"Query parameters: {params}")

            if hasattr(conn, "query"):
                # For Fabric connections that have a query method
                if params:
                    result = conn.query(query, params)
                else:
                    result = conn.query(query)
                logging.debug("Query executed successfully.")
                return result
            else:
                cursor = conn.cursor()
                logger.debug(f"Executing query: {query}")

                # Execute with or without parameters
                if params:
                    cursor.execute(query, params)
                else:
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
            if params:
                logging.error(f"Query parameters: {params}")
            logging.error("Connection details: %s", conn)
            raise

    def create_local_database(self) -> None:
        """Create a database called 'local' if it does not already exist."""
        try:
            conn = self.get_connection()
            self._create_local_database_with_connection(conn)

        except Exception as e:
            logging.error(f"Error creating database 'local': {e}")
            raise

    def _create_local_database_with_connection(self, conn) -> None:
        """Create a database called 'local' using the provided connection."""
        try:
            # Check if database exists
            check_db_sql = """
            SELECT 1 
            FROM sys.databases 
            WHERE name = 'local'
            """
            result = self.execute_query(conn, check_db_sql)
            db_exists = len(result) > 0 if result is not None else False

            # Create database if it doesn't exist
            if not db_exists:
                # CREATE DATABASE must be executed outside of a transaction
                cursor = conn.cursor()

                # Set autocommit to True to avoid transaction issues
                old_autocommit = conn.autocommit
                conn.autocommit = True

                try:
                    create_db_sql = "CREATE DATABASE [local];"
                    logger.debug(f"Executing CREATE DATABASE: {create_db_sql}")
                    cursor.execute(create_db_sql)
                    logging.info("Created database 'local'.")
                finally:
                    # Restore original autocommit setting
                    conn.autocommit = old_autocommit
                    cursor.close()
            else:
                logging.info("Database 'local' already exists.")

        except Exception as e:
            logging.error(f"Error creating database 'local': {e}")
            logging.exception("Stack trace:", exc_info=True)
            raise

    def _connect_to_local_sql_server(self):
        """Connect to local SQL Server using pyodbc, or return mock connection for testing."""
        try:
            import pyodbc

            logger.debug(
                f"Connecting to local SQL Server with connection string: {self.connection_string}"
            )

            # First connect without database specified to create the database
            conn = pyodbc.connect(self.connection_string)
            logger.debug("Successfully connected to local SQL Server")

            # Create local database if it doesn't exist
            self._create_local_database_with_connection(conn)

            # Check if database is already in connection string
            if (
                "DATABASE=" not in self.connection_string.upper()
                and "INITIAL CATALOG=" not in self.connection_string.upper()
            ):
                # Add local database to connection string and reconnect
                local_connection_string = (
                    self.connection_string.rstrip(";") + ";DATABASE=local;"
                )
                logger.debug(
                    f"Reconnecting with local database: {local_connection_string}"
                )
                conn.close()
                conn = pyodbc.connect(local_connection_string)
                logger.debug(
                    f"Successfully connected to local database: {local_connection_string}"
                )
            else:
                logger.debug(
                    f"Local database already specified in connection string, using existing connection: {self.connection_string}"
                )

            return conn
        except ImportError:
            logger.error("pyodbc not available, using mock connection for testing")

    def _connect_to_local_mysql(self):
        """Connect to local MySQL using mysql-connector-python, or return mock connection for testing."""
        try:
            import mysql.connector

            # Parse connection string to mysql.connector format
            connection_params = {}
            for param in self.connection_string.split(","):
                if "=" in param:
                    key, value = param.split("=", 1)
                    connection_params[key.strip()] = value.strip()

            logger.debug(f"Connecting to local MySQL with params: {connection_params}")

            # First connect without database specified to create the database if needed
            conn_params_no_db = connection_params.copy()
            if "database" in conn_params_no_db:
                del conn_params_no_db["database"]

            conn = mysql.connector.connect(**conn_params_no_db)
            logger.debug("Successfully connected to local MySQL")

            # Create local database if it doesn't exist
            self._create_local_mysql_database_with_connection(conn)

            # Reconnect with database specified
            conn.close()
            conn = mysql.connector.connect(**connection_params)
            logger.debug(
                f"Successfully connected to local MySQL database: {connection_params.get('database', 'local')}"
            )

            return conn
        except ImportError:
            logger.error(
                "mysql-connector-python not available, using mock connection for testing"
            )

    def _connect_to_local_postgresql(self):
        """Connect to local PostgreSQL using psycopg2, or return mock connection for testing."""
        try:
            import psycopg2
            from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

            # Default PostgreSQL connection parameters for local development
            connection_params = {
                'host': os.getenv('POSTGRES_HOST', 'localhost'),
                'port': int(os.getenv('POSTGRES_PORT', '5432')),
                'user': os.getenv('POSTGRES_USER', 'postgres'),
                'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
                'database': 'postgres'  # Connect to postgres db first to create target db
            }

            logger.debug(f"Connecting to local PostgreSQL with params: {connection_params}")

            # First connect to postgres database to create the target database if needed
            conn = psycopg2.connect(**connection_params)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            logger.debug("Successfully connected to local PostgreSQL")

            # Create local database if it doesn't exist
            self._create_local_postgresql_database_with_connection(conn)

            # Close and reconnect to the target database
            conn.close()
            connection_params['database'] = os.getenv('POSTGRES_DATABASE', 'local')
            conn = psycopg2.connect(**connection_params)
            logger.debug(f"Successfully connected to local PostgreSQL database: {connection_params['database']}")

            return conn
        except ImportError:
            logger.error("psycopg2 not available, using mock connection for testing")
            return None
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            logger.error("Make sure PostgreSQL is running and connection parameters are correct")
            return None

    def _create_local_postgresql_database_with_connection(self, conn) -> None:
        """Create a database called 'local' using the provided PostgreSQL connection."""
        try:
            target_db = os.getenv('POSTGRES_DATABASE', 'local')
            
            # Check if database exists
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            db_exists = cursor.fetchone() is not None

            # Create database if it doesn't exist
            if not db_exists:
                # Database names cannot be parameterized in PostgreSQL
                create_db_sql = f'CREATE DATABASE "{target_db}"'
                logger.debug(f"Executing CREATE DATABASE: {create_db_sql}")
                cursor.execute(create_db_sql)
                logging.info(f"Created database '{target_db}'.")
            else:
                logging.info(f"Database '{target_db}' already exists.")

            cursor.close()
        except Exception as e:
            logging.error(f"Error creating PostgreSQL database 'local': {e}")
            logging.exception("Stack trace:", exc_info=True)
            raise

    def _create_local_mysql_database_with_connection(self, conn) -> None:
        """Create a database called 'local' using the provided MySQL connection."""
        try:
            # Check if database exists
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES LIKE 'local'")
            db_exists = len(cursor.fetchall()) > 0

            # Create database if it doesn't exist
            if not db_exists:
                create_db_sql = "CREATE DATABASE `local`"
                logger.debug(f"Executing CREATE DATABASE: {create_db_sql}")
                cursor.execute(create_db_sql)
                logging.info("Created database 'local'.")
            else:
                logging.info("Database 'local' already exists.")

            cursor.close()
        except Exception as e:
            logging.error(f"Error creating MySQL database 'local': {e}")
            logging.exception("Stack trace:", exc_info=True)
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
                        [
                            "NULL"
                            if pd.isna(v) or v is None
                            else f"'{v}'"
                            if isinstance(v, (str, datetime, date))
                            else str(v)
                            for v in row
                        ]
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
                        [
                            "NULL"
                            if pd.isna(v) or v is None
                            else f"'{v}'"
                            if isinstance(v, (str, datetime, date))
                            else str(v)
                            for v in row
                        ]
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
                        [
                            "NULL"
                            if pd.isna(v) or v is None
                            else f"'{v}'"
                            if isinstance(v, (str, datetime, date))
                            else str(v)
                            for v in row
                        ]
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
            return {
                row["COLUMN_NAME"]: row["DATA_TYPE"] for _, row in result.iterrows()
            }
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

    # File system methods (required by DataStoreInterface but not typical for warehouses)

    def read_file(
        self,
        file_path: str,
        file_format: str,
        options: dict[str, Any] | None = None,
    ) -> Any:
        """Implements DataStoreInterface: Read a file (not applicable for warehouses)."""
        raise NotImplementedError(
            "File operations not supported for warehouse utilities"
        )

    def write_file(
        self,
        df: Any,
        file_path: str,
        file_format: str,
        options: dict[str, Any] | None = None,
    ) -> None:
        """Implements DataStoreInterface: Write a file (not applicable for warehouses)."""
        raise NotImplementedError(
            "File operations not supported for warehouse utilities"
        )

    def file_exists(self, file_path: str) -> bool:
        """Implements DataStoreInterface: Check if a file exists (not applicable for warehouses)."""
        raise NotImplementedError(
            "File operations not supported for warehouse utilities"
        )

    def list_files(
        self,
        directory_path: str,
        pattern: str | None = None,
        recursive: bool = False,
    ) -> list[str]:
        """Implements DataStoreInterface: List files in a directory (not applicable for warehouses)."""
        raise NotImplementedError(
            "File operations not supported for warehouse utilities"
        )

    def get_file_info(self, file_path: str) -> dict[str, Any]:
        """Implements DataStoreInterface: Get file information (not applicable for warehouses)."""
        raise NotImplementedError(
            "File operations not supported for warehouse utilities"
        )

    # --- End DataStoreInterface required methods ---

    # Additional utility methods
