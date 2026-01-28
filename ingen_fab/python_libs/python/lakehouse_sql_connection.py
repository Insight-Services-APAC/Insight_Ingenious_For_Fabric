"""
Fabric Lakehouse SQL Endpoint Connection
Provides PyODBC connection to Lakehouse SQL endpoints in Microsoft Fabric
"""

from __future__ import annotations

from typing import Optional

import pyodbc


class FabricLakehouseConnection:
    """
    Fabric Lakehouse SQL Endpoint connection using PyODBC.
    Similar to FabricWarehouseConnection but connects to Lakehouse SQL endpoints.
    """

    def __init__(
        self,
        target_lakehouse_id: str,
        target_workspace_id: str,
        connection_string: str,
        token_bytes: bytes,
    ):
        """
        Initialize Fabric Lakehouse SQL endpoint connection.

        Args:
            target_lakehouse_id: Target lakehouse ID
            target_workspace_id: Target workspace ID
            connection_string: PyODBC connection string
            token_bytes: Encoded authentication token bytes
        """
        self.target_lakehouse_id = target_lakehouse_id
        self.target_workspace_id = target_workspace_id
        self._target_lakehouse_id = target_lakehouse_id  # For compatibility
        self._target_workspace_id = target_workspace_id  # For compatibility
        self.connection_string = connection_string
        self.token_bytes = token_bytes

        # SQL Server connection attribute for AAD token
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        self.attrs_before = {SQL_COPT_SS_ACCESS_TOKEN: token_bytes}

        # Check if we can actually connect
        self.can_execute = self._test_connection()

    def _test_connection(self) -> bool:
        """Test if connection is available."""
        try:
            conn = pyodbc.connect(
                self.connection_string, autocommit=False, attrs_before=self.attrs_before
            )
            conn.close()
            return True
        except Exception as e:
            print(f"⚠️ Lakehouse SQL endpoint connection test failed: {str(e)}")
            return False

    def get_connection(self) -> Optional[pyodbc.Connection]:
        """Get a database connection to the lakehouse SQL endpoint."""
        if not self.can_execute:
            return None
        try:
            return pyodbc.connect(
                self.connection_string, autocommit=False, attrs_before=self.attrs_before
            )
        except Exception as e:
            print(f"⚠️ Connection failed: {str(e)}")
            return None

    def check_if_table_exists(
        self, table_name: str, schema_name: str = "dbo"
    ) -> bool:
        """
        Check if a table exists in the lakehouse SQL endpoint.

        Args:
            table_name: Name of the table
            schema_name: Schema name (default: dbo)

        Returns:
            bool: True if table exists, False otherwise
        """
        query = f"""
        SELECT COUNT(*) as count
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'
        """

        if self.can_execute:
            try:
                result = self.execute_query(query)
                if result and len(result) > 0:
                    count = result[0][0]
                    exists = count > 0
                    print(
                        f"🔍 Table {schema_name}.{table_name}: {'EXISTS' if exists else 'NOT FOUND'}"
                    )
                    return exists
            except Exception as e:
                print(f"⚠️ Error checking table existence: {str(e)}")

        print(
            f"🔍 CHECK TABLE EXISTS: {schema_name}.{table_name} (assuming NOT EXISTS)"
        )
        return False

    def create_schema_if_not_exists(
        self, schema_name: str, max_retries: int = 3
    ) -> bool:
        """
        Create schema if it doesn't exist.
        Note: Lakehouse SQL endpoints may have limitations on schema creation.

        Args:
            schema_name: Name of the schema to create
            max_retries: Maximum number of retry attempts

        Returns:
            bool: True if successful, False otherwise
        """
        query = f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema_name}')
        BEGIN
            EXEC('CREATE SCHEMA [{schema_name}]')
        END
        """
        print(f"\n📁 CREATE SCHEMA: {schema_name}")
        print("DDL to execute against lakehouse SQL endpoint:")
        print(f"```sql\n{query}\n```")

        if self.can_execute:
            return self.execute_ddl(query, f"Create schema {schema_name}", max_retries)
        else:
            print("⚠️ Execution disabled - DDL displayed only")
            return False

    @property
    def target_store_id(self) -> str:
        """Return target lakehouse ID for compatibility."""
        return self._target_lakehouse_id

    def execute_query(
        self,
        query: str,
        conn: Optional[pyodbc.Connection] = None,
        params: Optional[list] = None,
        max_retries: int = 3,
    ) -> list:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query to execute
            conn: Optional existing connection to use
            params: Optional query parameters
            max_retries: Maximum number of retry attempts

        Returns:
            list: Query results as list of tuples
        """
        if not self.can_execute:
            print("⚠️ Query execution disabled")
            return []

        connection = conn or self.get_connection()
        if not connection:
            return []

        for attempt in range(max_retries):
            try:
                cursor = connection.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                # Fetch results for SELECT queries
                if query.strip().upper().startswith("SELECT"):
                    results = cursor.fetchall()
                    cursor.close()
                    if not conn:  # Only close if we created the connection
                        connection.close()
                    return results
                else:
                    cursor.close()
                    if not conn:
                        connection.close()
                    return []

            except Exception as e:
                print(f"⚠️ Query execution attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    print(f"❌ Query failed after {max_retries} attempts")
                    if not conn:
                        connection.close()
                    return []
                else:
                    print(
                        f"🔄 Retrying query execution (attempt {attempt + 2}/{max_retries})..."
                    )
        return []

    def execute_ddl(
        self, ddl_query: str, operation_name: str = "DDL Operation", max_retries: int = 3
    ) -> bool:
        """
        Execute DDL commands with proper transaction handling.

        Args:
            ddl_query: DDL SQL statement to execute
            operation_name: Description of the operation
            max_retries: Maximum number of retry attempts

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.can_execute:
            print(f"⚠️ DDL execution disabled for: {operation_name}")
            return False

        connection = self.get_connection()
        if not connection:
            print(f"❌ Cannot establish connection for: {operation_name}")
            return False

        for attempt in range(max_retries):
            try:
                cursor = connection.cursor()
                cursor.execute(ddl_query)
                connection.commit()
                cursor.close()
                connection.close()
                print(f"✅ {operation_name} executed successfully")
                return True

            except pyodbc.Error as ex:
                sqlstate = ex.args
                print(f"❌ {operation_name} failed (attempt {attempt + 1}): {sqlstate}")
                connection.rollback()

                if attempt == max_retries - 1:
                    print(f"❌ {operation_name} failed after {max_retries} attempts")
                    connection.close()
                    return False
                else:
                    print(
                        f"🔄 Retrying {operation_name} (attempt {attempt + 2}/{max_retries})..."
                    )
            except Exception as e:
                print(
                    f"❌ Unexpected error in {operation_name} (attempt {attempt + 1}): {str(e)}"
                )
                connection.rollback()
                if attempt == max_retries - 1:
                    connection.close()
                    return False
        return False
