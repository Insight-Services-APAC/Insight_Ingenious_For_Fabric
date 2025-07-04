"""
Standard interface for data store utilities.

This module defines the common interface that both lakehouse_utils and warehouse_utils
should implement to ensure consistent behavior across different storage systems.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DataStoreInterface(ABC):
    """Abstract interface for data store utilities."""

    @property
    @abstractmethod
    def target_workspace_id(self) -> str:
        """Get the target workspace ID."""
        pass

    @property
    @abstractmethod
    def target_store_id(self) -> str:
        """Get the target store ID (lakehouse_id or warehouse_id)."""
        pass

    @abstractmethod
    def check_if_table_exists(
        self, table_name: str, schema_name: str | None = None
    ) -> bool:
        """
        Check if a table exists in the data store.

        Args:
            table_name: Name of the table to check
            schema_name: Schema name (optional, mainly for SQL databases)

        Returns:
            True if table exists, False otherwise
        """
        pass

    @abstractmethod
    def write_to_table(
        self,
        df: Any,
        table_name: str,
        schema_name: str | None = None,
        mode: str = "overwrite",
        options: dict[str, Any] | None = None,
    ) -> None:
        """
        Write a DataFrame to a table in the data store.

        Args:
            df: DataFrame to write (Spark DataFrame or Pandas DataFrame)
            table_name: Name of the target table
            schema_name: Schema name (optional, mainly for SQL databases)
            mode: Write mode ("overwrite", "append", "error", "ignore")
            options: Additional options for writing
        """
        pass

    @abstractmethod
    def drop_all_tables(
        self, schema_name: str | None = None, table_prefix: str | None = None
    ) -> None:
        """
        Drop all tables in the data store.

        Args:
            schema_name: Schema to limit drops to (optional, mainly for SQL databases)
            table_prefix: Prefix to filter tables by (optional)
        """
        pass

    @abstractmethod
    def get_connection(self) -> Any:
        """
        Get a connection object for the data store.

        Returns:
            Connection object (type varies by implementation)
        """
        pass

    @abstractmethod
    def list_tables(self) -> list[str]:
        """
        List all tables in the data store.

        Returns:
            List of table names
        """
        pass

    @abstractmethod
    def execute_query(self, query: str):
        """
        Execute a SQL query on the data store.

        Args:
            query: SQL query to execute

        Returns:
            Query result (type varies by implementation)
        """
        pass

    @abstractmethod
    def get_table_schema(
        self, table_name: str, schema_name: str | None = None
    ) -> dict[str, Any]:
        """
        Get the schema/column definitions for a table.

        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)

        Returns:
            Dictionary describing the table schema
        """
        pass

    @abstractmethod
    def read_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        filters: dict[str, Any] | None = None,
    ) -> Any:
        """
        Read data from a table, optionally filtering columns, rows, or limiting results.

        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            columns: List of columns to select (optional)
            limit: Maximum number of rows to return (optional)
            filters: Dictionary of filters to apply (optional)

        Returns:
            DataFrame or result set (type varies by implementation)
        """
        pass

    @abstractmethod
    def delete_from_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> int:
        """
        Delete rows from a table matching filters.

        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            filters: Dictionary of filters to apply (optional)

        Returns:
            Number of rows deleted
        """
        pass

    @abstractmethod
    def rename_table(
        self,
        old_table_name: str,
        new_table_name: str,
        schema_name: str | None = None,
    ) -> None:
        """
        Rename a table.

        Args:
            old_table_name: Current table name
            new_table_name: New table name
            schema_name: Schema name (optional)
        """
        pass

    @abstractmethod
    def create_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        schema: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
    ) -> None:
        """
        Create a new table with a given schema.

        Args:
            table_name: Name of the new table
            schema_name: Schema name (optional)
            schema: Dictionary describing the table schema (optional)
            options: Additional options for table creation (optional)
        """
        pass

    @abstractmethod
    def drop_table(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> None:
        """
        Drop a single table.

        Args:
            table_name: Name of the table to drop
            schema_name: Schema name (optional)
        """
        pass

    @abstractmethod
    def list_schemas(self) -> list[str]:
        """
        List all schemas/namespaces in the data store.

        Returns:
            List of schema names
        """
        pass

    @abstractmethod
    def get_table_row_count(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> int:
        """
        Get the number of rows in a table.

        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)

        Returns:
            Number of rows in the table
        """
        pass

    @abstractmethod
    def get_table_metadata(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Get metadata for a table (creation time, size, etc.).

        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)

        Returns:
            Dictionary of table metadata
        """
        pass

    @abstractmethod
    def vacuum_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        retention_hours: int = 168,
    ) -> None:
        """
        Perform cleanup/compaction on a table (for systems like Delta Lake).

        Args:
            table_name: Name of the table
            schema_name: Schema name (optional)
            retention_hours: Retention period in hours (default: 168)
        """
        pass

    @abstractmethod
    def optimise_table(self, table_name: str) -> None:
        """
        !!! LAKEHOUSE ONLY !!!
        Perform optimise on a table to reduce number of parquet files.
        """
        pass
