import logging
import os
import platform
import sys
import time
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

        if dialect not in ["fabric", "sql_server", "mysql", "postgres"]:
            raise ValueError(
                f"Unsupported dialect: {dialect}. Supported dialects are 'fabric', 'sql_server', 'mysql', and 'postgres'."
            )

        # Initialize notebook utils abstraction
        self.notebook_utils = NotebookUtilsFactory.get_instance(
            notebookutils=notebookutils
        )

        # Use fabric_environment from config_utils to determine local database dialect
        # This ensures consistency with sql_translator and other parts of the codebase
        if (
            dialect == "fabric"
            and type(self.notebook_utils).__name__ == "LocalNotebookUtils"
        ):
            # Get the fabric_environment to determine which local database to use
            try:
                config = get_configs_as_object()
                fabric_env = getattr(config, 'fabric_environment', 'production')
                
                if fabric_env == 'local':
                    # Use PostgreSQL for local development (matching sql_translator)
                    logger.info(
                        "Running in local environment, using PostgreSQL for database operations."
                    )
                    self.dialect = "postgres"
                else:
                    # Non-local environments without notebookutils shouldn't happen
                    logger.warning(
                        f"Environment '{fabric_env}' detected but notebookutils not available. "
                        "This configuration is not supported."
                    )
                    self.dialect = "sql_server"
            except Exception as e:
                # Fallback to architecture-based detection if config fails
                logger.warning(f"Could not get fabric_environment from config: {e}")
                machine_arch = platform.machine().lower()
                is_arm = machine_arch in ["arm64", "aarch64", "arm"]

                if is_arm:
                    logger.warning(
                        f"Falling back to MySQL for ARM architecture ({machine_arch})."
                    )
                    self.dialect = "mysql"
                else:
                    logger.warning(
                        f"Falling back to SQL Server for {machine_arch} architecture."
                    )
                    self.dialect = "sql_server"

        # Set default connection string based on final dialect if not provided
        if connection_string is None:
            if self.dialect == "sql_server":
                self.connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=localhost,1433;UID=sa;PWD={os.getenv('SQL_SERVER_PASSWORD', 'YourStrong!Passw0rd')};TrustServerCertificate=yes;"
            elif self.dialect == "mysql":
                self.connection_string = f"host=localhost,port=3306,user=root,password={os.getenv('MYSQL_PASSWORD', 'password')},database=local"
            elif self.dialect == "postgres":
                self.connection_string = f"host=localhost,port=5432,user=postgres,password={os.getenv('POSTGRES_PASSWORD', 'postgres')},database=local"
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
        self, conn=None, query: str = None, params: tuple | list | None = None, max_retries: int = 3
    ):
        """Execute a query and return results as a DataFrame when possible.
        
        Args:
            conn: Database connection. If None, gets a new connection.
            query: SQL query to execute
            params: Query parameters
            max_retries: Maximum number of retry attempts for transient failures
        """
        if not conn:
            conn = self.get_connection()
            
        # Translate SQL if we're using PostgreSQL in local mode
        if get_configs_as_object().fabric_environment == "local" and query:
            from ingen_fab.python_libs.python.sql_translator import get_sql_translator
            try:
                translator = get_sql_translator()
                query = translator.translate_sql(query)
                logging.debug(f"Translated SQL for PostgreSQL: {query}")
            except Exception as e:
                logging.warning(f"SQL translation failed: {e}")
                logging.warning(f"Using original SQL: {query}")
        
        # Retry logic with exponential backoff
        for attempt in range(max_retries + 1):
            try:
                logging.debug(f"Executing query (attempt {attempt + 1}/{max_retries + 1}): {query}")
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
                error_str = str(e).lower()
                
                # Check if this is a retryable error
                retryable_errors = [
                    'connection reset',
                    'connection lost',
                    'timeout',
                    'connection refused',
                    'deadlock',
                    'lock wait timeout',
                    'connection closed',
                    'broken pipe',
                    'network error',
                    'temporary failure'
                ]
                
                is_retryable = any(err in error_str for err in retryable_errors)
                
                if attempt < max_retries and is_retryable:
                    # Calculate exponential backoff delay (0.5s, 1s, 2s)
                    delay = 0.5 * (2 ** attempt)
                    logging.warning(f"Retryable error on attempt {attempt + 1}: {e}")
                    logging.warning(f"Retrying in {delay:.1f} seconds...")
                    time.sleep(delay)
                    
                    # Try to get a fresh connection for retry
                    try:
                        conn = self.get_connection()
                    except Exception as conn_error:
                        logging.error(f"Failed to get new connection for retry: {conn_error}")
                        
                    continue
                else:
                    # Non-retryable error or max retries exceeded
                    if attempt == max_retries:
                        logging.error(f"Max retries ({max_retries}) exceeded for query: {query}")
                    else:
                        logging.error(f"Non-retryable error for query: {query}")
                        
                    logging.error(f"Error executing query: {query}. Error: {e}")
                    if params:
                        logging.error(f"Query parameters: {params}")
                    logging.error("Connection details: %s", conn)
                    error_message = f"Error executing query: {query}. Error: {e}"
                    print(error_message, file=sys.stderr)
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
            logger.error("psycopg2 not available. Please install it with: uv pip install psycopg2-binary")
            return None
        except psycopg2.OperationalError as e:
            error_msg = str(e)
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            
            # Provide helpful messages based on the error
            if "could not connect to server" in error_msg or "Connection refused" in error_msg:
                error_message = "\n" + "=" * 70 + "\n"
                error_message += "PostgreSQL Connection Error - Server Not Available\n"
                error_message += "=" * 70 + "\n"
                error_message += "\nPostgreSQL server is not running or not accessible.\n"
                error_message += "\nTo fix this issue, try one of the following:\n"
                error_message += "\n1. Start PostgreSQL (if already installed):\n"
                error_message += "   - Ubuntu/Debian: sudo service postgresql start\n"
                error_message += "   - macOS (Homebrew): brew services start postgresql\n"
                error_message += "   - macOS (Postgres.app): Open Postgres.app\n"
                error_message += "   - Windows: net start postgresql-x64-15\n"
                
                error_message += "\n2. Install PostgreSQL (if not installed):\n"
                error_message += "   - Ubuntu/Debian: sudo apt-get install postgresql\n"
                error_message += "   - macOS: brew install postgresql\n"
                error_message += "   - Windows: Download from https://www.postgresql.org/download/windows/\n"
                
                error_message += "\n3. Check if PostgreSQL is running on a different port:\n"
                error_message += f"   Current port: {connection_params.get('port', 5432)}\n"
                error_message += "   Set custom port: export POSTGRES_PORT=<your_port>\n"
                error_message += "=" * 70 + "\n"
                
                # Log and print for visibility
                logger.error(error_message)
                print(error_message, file=sys.stderr)
                
            elif "password authentication failed" in error_msg or "no password supplied" in error_msg:
                error_message = "\n" + "=" * 70 + "\n"
                error_message += "PostgreSQL Authentication Error\n"
                error_message += "=" * 70 + "\n"
                error_message += "\nFailed to authenticate with PostgreSQL server.\n"
                error_message += "\nTo fix this issue:\n"
                error_message += "\n1. Set the PostgreSQL password for the 'postgres' user:\n"
                error_message += "   sudo -u postgres psql -c \"ALTER USER postgres PASSWORD 'password';\"\n"
                
                error_message += "\n2. Configure environment variables:\n"
                error_message += "   export POSTGRES_USER=postgres\n"
                error_message += "   export POSTGRES_PASSWORD=password\n"
                error_message += "   export POSTGRES_DATABASE=local\n"
                
                error_message += "\n3. Or use a different user with proper credentials:\n"
                error_message += "   export POSTGRES_USER=<your_username>\n"
                error_message += "   export POSTGRES_PASSWORD=<your_password>\n"
                
                error_message += "\nCurrent connection parameters:\n"
                error_message += f"   Host: {connection_params.get('host', 'localhost')}\n"
                error_message += f"   Port: {connection_params.get('port', 5432)}\n"
                error_message += f"   User: {connection_params.get('user', 'postgres')}\n"
                error_message += f"   Database: {connection_params.get('database', 'postgres')}\n"
                error_message += "=" * 70 + "\n"
                
                # Log and print for visibility
                logger.error(error_message)
                print(error_message, file=sys.stderr)
                
            elif "database" in error_msg and "does not exist" in error_msg:
                error_message = "\n" + "=" * 70 + "\n"
                error_message += "PostgreSQL Database Error\n"
                error_message += "=" * 70 + "\n"
                error_message += f"\nThe database '{connection_params.get('database')}' does not exist.\n"
                error_message += "\nTo create it, run:\n"
                error_message += f"   sudo -u postgres createdb {connection_params.get('database')}\n"
                error_message += "=" * 70 + "\n"
                
                # Log and print for visibility
                logger.error(error_message)
                print(error_message, file=sys.stderr)
            else:
                error_message = "\n" + "=" * 70 + "\n"
                error_message += "PostgreSQL Connection Error\n"
                error_message += "=" * 70 + "\n"
                error_message += "\nCheck your PostgreSQL configuration and ensure:\n"
                error_message += "1. PostgreSQL service is running\n"
                error_message += "2. Connection parameters are correct\n"
                error_message += "3. PostgreSQL is configured to accept connections\n"
                error_message += "\nFor more details, check PostgreSQL logs:\n"
                error_message += "   - Ubuntu/Debian: /var/log/postgresql/\n"
                error_message += "   - macOS: /usr/local/var/log/\n"
                error_message += "   - Windows: Check Event Viewer\n"
                error_message += "=" * 70 + "\n"
                
                # Log and print for visibility
                logger.error(error_message)
                print(error_message, file=sys.stderr)
                
            return None
        except Exception as e:
            logger.error(f"Unexpected error connecting to PostgreSQL: {e}")
            logger.error("\nFor local development with ingen_fab, PostgreSQL is required.")
            logger.error("Please ensure PostgreSQL is properly installed and configured.")
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

    def create_schema_if_not_exists(self, schema_name: str, max_retries: int = 3):
        """Create a schema if it does not already exist."""
        for attempt in range(max_retries + 1):
            try:
                conn = self.get_connection()
                query = self.sql.render("check_schema_exists", schema_name=schema_name)
                result = self.execute_query(conn, query)
                schema_exists = len(result) > 0 if result is not None else False

                # Create schema if it doesn't exist
                if not schema_exists:
                    create_schema_query = self.sql.render("create_schema", schema_name=schema_name)
                    self.execute_query(conn, create_schema_query)
                    logging.info(f"Created schema '{schema_name}'.")

                logging.info(f"Schema {schema_name} created or already exists.")
                return  # Success, exit the retry loop
                
            except Exception as e:
                error_str = str(e).lower()
                
                # Check if this is a schema already exists error (race condition)
                schema_exists_errors = [
                    'already exists',
                    'duplicate key',
                    'already been used',
                    'schema with name',
                    'cannot create schema'
                ]
                
                is_schema_exists_error = any(err in error_str for err in schema_exists_errors)
                
                if is_schema_exists_error:
                    # Schema was created by another process - this is actually success
                    logging.info(f"Schema '{schema_name}' already exists (created by another process).")
                    return
                    
                elif attempt < max_retries:
                    # Other error, retry with exponential backoff
                    delay = 0.5 * (2 ** attempt)
                    logging.warning(f"Error creating schema '{schema_name}' on attempt {attempt + 1}: {e}")
                    logging.warning(f"Retrying in {delay:.1f} seconds...")
                    time.sleep(delay)
                    continue
                else:
                    # Max retries exceeded
                    logging.error(f"Error creating schema {schema_name} after {max_retries} retries: {e}")
                    print(f"Error creating schema for ddl log table {schema_name}: {e}", file=sys.stderr)
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
