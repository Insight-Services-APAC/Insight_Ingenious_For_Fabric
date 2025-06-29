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
    def check_if_table_exists(self, table_name: str, schema_name: str | None = None) -> bool:
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
    def drop_all_tables(self, schema_name: str | None = None, table_prefix: str | None = None) -> None:
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
