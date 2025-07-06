from __future__ import annotations

from abc import ABC
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface


class MockDataStoreImplementation(DataStoreInterface):
    """Concrete implementation of DataStoreInterface for testing."""
    
    def __init__(self, workspace_id: str = "test-workspace", store_id: str = "test-store"):
        self._workspace_id = workspace_id
        self._store_id = store_id
        self._tables = {}
        self._schemas = ["default", "test_schema"]
        self._connection = MagicMock()

    @property
    def target_workspace_id(self) -> str:
        return self._workspace_id

    @property
    def target_store_id(self) -> str:
        return self._store_id

    def check_if_table_exists(self, table_name: str, schema_name: str | None = None) -> bool:
        key = f"{schema_name}.{table_name}" if schema_name else table_name
        return key in self._tables

    def write_to_table(
        self,
        df: Any,
        table_name: str,
        schema_name: str | None = None,
        mode: str = "overwrite",
        options: dict[str, Any] | None = None,
    ) -> None:
        key = f"{schema_name}.{table_name}" if schema_name else table_name
        self._tables[key] = {"df": df, "mode": mode, "options": options}

    def drop_all_tables(self, schema_name: str | None = None, table_prefix: str | None = None) -> None:
        if schema_name:
            keys_to_remove = [k for k in self._tables.keys() if k.startswith(f"{schema_name}.")]
        else:
            keys_to_remove = list(self._tables.keys())
        
        if table_prefix:
            keys_to_remove = [k for k in keys_to_remove if table_prefix in k]
        
        for key in keys_to_remove:
            del self._tables[key]

    def get_connection(self) -> Any:
        return self._connection

    def list_tables(self) -> list[str]:
        return list(self._tables.keys())

    def execute_query(self, query: str):
        return f"Executed: {query}"

    def get_table_schema(self, table_name: str, schema_name: str | None = None) -> dict[str, Any]:
        key = f"{schema_name}.{table_name}" if schema_name else table_name
        if key in self._tables:
            return {"column1": "string", "column2": "integer"}
        return {}

    def read_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        filters: dict[str, Any] | None = None,
    ) -> Any:
        key = f"{schema_name}.{table_name}" if schema_name else table_name
        if key in self._tables:
            return self._tables[key]["df"]
        return None

    def delete_from_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> int:
        key = f"{schema_name}.{table_name}" if schema_name else table_name
        if key in self._tables:
            return 5  # Mock return value
        return 0

    def rename_table(
        self,
        old_table_name: str,
        new_table_name: str,
        schema_name: str | None = None,
    ) -> None:
        old_key = f"{schema_name}.{old_table_name}" if schema_name else old_table_name
        new_key = f"{schema_name}.{new_table_name}" if schema_name else new_table_name
        if old_key in self._tables:
            self._tables[new_key] = self._tables.pop(old_key)

    def create_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        schema: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
    ) -> None:
        key = f"{schema_name}.{table_name}" if schema_name else table_name
        self._tables[key] = {"schema": schema, "options": options}

    def drop_table(self, table_name: str, schema_name: str | None = None) -> None:
        key = f"{schema_name}.{table_name}" if schema_name else table_name
        if key in self._tables:
            del self._tables[key]

    def list_schemas(self) -> list[str]:
        return self._schemas

    def get_table_row_count(self, table_name: str, schema_name: str | None = None) -> int:
        key = f"{schema_name}.{table_name}" if schema_name else table_name
        if key in self._tables:
            return 100  # Mock return value
        return 0

    def get_table_metadata(self, table_name: str, schema_name: str | None = None) -> dict[str, Any]:
        key = f"{schema_name}.{table_name}" if schema_name else table_name
        if key in self._tables:
            return {"created_at": "2023-01-01", "size": "1MB"}
        return {}

    def vacuum_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        retention_hours: int = 168,
    ) -> None:
        pass  # Mock implementation - no-op


@pytest.fixture
def data_store():
    """Create a mock data store implementation for testing."""
    return MockDataStoreImplementation()


def test_data_store_interface_is_abstract():
    """Test that DataStoreInterface is an abstract base class."""
    assert issubclass(DataStoreInterface, ABC)
    
    # Test that we cannot instantiate the abstract class directly
    with pytest.raises(TypeError):
        DataStoreInterface()


def test_mock_implementation_properties(data_store):
    """Test that the mock implementation provides correct properties."""
    assert data_store.target_workspace_id == "test-workspace"
    assert data_store.target_store_id == "test-store"


def test_table_existence_operations(data_store):
    """Test table existence checking operations."""
    # Initially no tables exist
    assert not data_store.check_if_table_exists("test_table")
    assert not data_store.check_if_table_exists("test_table", "test_schema")
    
    # Create a table
    data_store.write_to_table({"data": "test"}, "test_table")
    assert data_store.check_if_table_exists("test_table")
    
    # Create a table with schema
    data_store.write_to_table({"data": "test"}, "schema_table", "test_schema")
    assert data_store.check_if_table_exists("schema_table", "test_schema")
    assert not data_store.check_if_table_exists("schema_table")  # Without schema


def test_write_to_table_operations(data_store):
    """Test table writing operations."""
    test_df = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    
    # Write without schema
    data_store.write_to_table(test_df, "test_table")
    assert data_store.check_if_table_exists("test_table")
    
    # Write with schema and options
    options = {"format": "delta", "partitionBy": "col1"}
    data_store.write_to_table(test_df, "test_table2", "test_schema", "append", options)
    assert data_store.check_if_table_exists("test_table2", "test_schema")


def test_drop_operations(data_store):
    """Test table dropping operations."""
    # Create some tables
    data_store.write_to_table({"data": "test"}, "table1")
    data_store.write_to_table({"data": "test"}, "table2")
    data_store.write_to_table({"data": "test"}, "schema_table", "test_schema")
    
    # Drop single table
    data_store.drop_table("table1")
    assert not data_store.check_if_table_exists("table1")
    assert data_store.check_if_table_exists("table2")
    
    # Drop table with schema
    data_store.drop_table("schema_table", "test_schema")
    assert not data_store.check_if_table_exists("schema_table", "test_schema")
    
    # Drop all tables
    data_store.drop_all_tables()
    assert len(data_store.list_tables()) == 0


def test_connection_operations(data_store):
    """Test connection-related operations."""
    conn = data_store.get_connection()
    assert conn is not None
    
    # Test query execution
    result = data_store.execute_query("SELECT * FROM test_table")
    assert result == "Executed: SELECT * FROM test_table"


def test_list_operations(data_store):
    """Test listing operations."""
    # Test list tables
    tables = data_store.list_tables()
    assert isinstance(tables, list)
    
    # Test list schemas
    schemas = data_store.list_schemas()
    assert isinstance(schemas, list)
    assert "default" in schemas
    assert "test_schema" in schemas


def test_table_schema_operations(data_store):
    """Test table schema operations."""
    # Create table and get schema
    data_store.create_table("test_table", schema={"col1": "string", "col2": "int"})
    schema = data_store.get_table_schema("test_table")
    assert isinstance(schema, dict)
    assert "column1" in schema
    assert "column2" in schema


def test_table_read_operations(data_store):
    """Test table reading operations."""
    test_df = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    
    # Write and read table
    data_store.write_to_table(test_df, "read_table")
    result = data_store.read_table("read_table")
    assert result == test_df
    
    # Test with schema
    data_store.write_to_table(test_df, "read_table2", "test_schema")
    result = data_store.read_table("read_table2", "test_schema")
    assert result == test_df
    
    # Test reading non-existent table
    result = data_store.read_table("non_existent")
    assert result is None


def test_table_modification_operations(data_store):
    """Test table modification operations."""
    test_df = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    
    # Create table and delete from it
    data_store.write_to_table(test_df, "delete_table")
    deleted_count = data_store.delete_from_table("delete_table", filters={"col1": 1})
    assert deleted_count == 5  # Mock return value
    
    # Test rename table
    data_store.write_to_table(test_df, "old_name")
    data_store.rename_table("old_name", "new_name")
    assert not data_store.check_if_table_exists("old_name")
    assert data_store.check_if_table_exists("new_name")


def test_table_metadata_operations(data_store):
    """Test table metadata operations."""
    data_store.write_to_table({"data": "test"}, "meta_table")
    
    # Get row count
    count = data_store.get_table_row_count("meta_table")
    assert count == 100  # Mock return value
    
    # Get metadata
    metadata = data_store.get_table_metadata("meta_table")
    assert isinstance(metadata, dict)
    assert "created_at" in metadata
    assert "size" in metadata


def test_table_vacuum_operations(data_store):
    """Test table vacuum operations."""
    data_store.write_to_table({"data": "test"}, "vacuum_table")
    
    # Test vacuum with default retention
    data_store.vacuum_table("vacuum_table")
    
    # Test vacuum with custom retention
    data_store.vacuum_table("vacuum_table", retention_hours=72)
    
    # Test vacuum with schema
    data_store.write_to_table({"data": "test"}, "vacuum_table2", "test_schema")
    data_store.vacuum_table("vacuum_table2", "test_schema", 48)


def test_create_table_operations(data_store):
    """Test table creation operations."""
    # Create table with schema
    schema = {"id": "int", "name": "string", "value": "double"}
    data_store.create_table("created_table", schema=schema)
    assert data_store.check_if_table_exists("created_table")
    
    # Create table with schema and options
    options = {"format": "delta", "location": "/tmp/test"}
    data_store.create_table("created_table2", "test_schema", schema, options)
    assert data_store.check_if_table_exists("created_table2", "test_schema")


def test_drop_all_tables_with_filters(data_store):
    """Test dropping all tables with schema and prefix filters."""
    # Create tables in different schemas
    data_store.write_to_table({"data": "test"}, "prefix_table1")
    data_store.write_to_table({"data": "test"}, "prefix_table2")
    data_store.write_to_table({"data": "test"}, "other_table")
    data_store.write_to_table({"data": "test"}, "schema_table", "test_schema")
    
    # Drop tables with prefix
    data_store.drop_all_tables(table_prefix="prefix_")
    assert not data_store.check_if_table_exists("prefix_table1")
    assert not data_store.check_if_table_exists("prefix_table2")
    assert data_store.check_if_table_exists("other_table")
    assert data_store.check_if_table_exists("schema_table", "test_schema")


def test_interface_method_signatures():
    """Test that the interface defines the expected method signatures."""
    # Get all abstract methods
    abstract_methods = DataStoreInterface.__abstractmethods__
    
    expected_methods = {
        'target_workspace_id', 'target_store_id', 'check_if_table_exists',
        'write_to_table', 'drop_all_tables', 'get_connection', 'list_tables',
        'execute_query', 'get_table_schema', 'read_table', 'delete_from_table',
        'rename_table', 'create_table', 'drop_table', 'list_schemas',
        'get_table_row_count', 'get_table_metadata', 'vacuum_table'
    }
    
    assert abstract_methods == expected_methods