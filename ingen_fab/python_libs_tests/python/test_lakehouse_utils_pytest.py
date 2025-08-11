from __future__ import annotations

import os
import shutil
import tempfile

import pandas as pd
import pytest

from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils


@pytest.fixture(scope="module")
def temp_dir():
    """Create a temporary directory for test tables."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="module")
def lakehouse_util(temp_dir):
    """Create a lakehouse utils instance for testing."""
    # Set up environment to use local file system
    os.environ["FABRIC_ENVIRONMENT"] = "local"

    # Create test workspace and lakehouse directories
    workspace_id = "test-workspace"
    lakehouse_id = "test-lakehouse"

    # Create the Tables directory structure
    tables_dir = os.path.join(temp_dir, lakehouse_id, "Tables")
    os.makedirs(tables_dir, exist_ok=True)

    # Mock the lakehouse_tables_uri method to use our temp directory
    util = lakehouse_utils(workspace_id, lakehouse_id)
    original_uri = util.lakehouse_tables_uri  # noqa: F841
    util.lakehouse_tables_uri = lambda: f"file://{tables_dir}/"

    yield util


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pd.DataFrame(
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [100, 200, 300]}
    )


def test_target_workspace_id(lakehouse_util):
    """Test that target_workspace_id property returns correct value."""
    assert lakehouse_util.target_workspace_id == "test-workspace"


def test_target_store_id(lakehouse_util):
    """Test that target_store_id property returns correct value."""
    assert lakehouse_util.target_store_id == "test-lakehouse"


def test_get_connection(lakehouse_util):
    """Test that get_connection returns placeholder message."""
    connection = lakehouse_util.get_connection
    assert "Placeholder for Spark session" in connection


def test_lakehouse_tables_uri(lakehouse_util):
    """Test that lakehouse_tables_uri returns correct URI format."""
    uri = lakehouse_util.lakehouse_tables_uri()
    assert uri.startswith("file://")
    assert uri.endswith("/")


def test_check_if_table_exists_false(lakehouse_util):
    """Test checking for non-existent table."""
    exists = lakehouse_util.check_if_table_exists("nonexistent_table")
    assert exists is False


def test_write_to_table_and_check_exists(lakehouse_util, sample_df):
    """Test writing to table and checking existence."""
    table_name = "test_table"

    # Write the table
    lakehouse_util.write_to_table(sample_df, table_name)

    # Check if table exists
    exists = lakehouse_util.check_if_table_exists(table_name)
    assert exists is True


def test_read_table(lakehouse_util, sample_df):
    """Test reading a table."""
    table_name = "read_test_table"

    # Write the table first
    lakehouse_util.write_to_table(sample_df, table_name)

    # Read the table
    result_df = lakehouse_util.read_table(table_name)

    # Check that data matches
    assert len(result_df) == len(sample_df)
    assert list(result_df.columns) == list(sample_df.columns)


def test_list_tables(lakehouse_util, sample_df):
    """Test listing tables."""
    table_name = "list_test_table"

    # Write a table
    lakehouse_util.write_to_table(sample_df, table_name)

    # List tables
    tables = lakehouse_util.list_tables()

    # Check that our table is in the list
    assert isinstance(tables, list)
    assert table_name in tables


def test_drop_table(lakehouse_util, sample_df):
    """Test dropping a table."""
    table_name = "drop_test_table"

    # Write the table first
    lakehouse_util.write_to_table(sample_df, table_name)

    # Verify it exists
    assert lakehouse_util.check_if_table_exists(table_name) is True

    # Drop the table
    lakehouse_util.drop_table(table_name)

    # Verify it no longer exists
    assert lakehouse_util.check_if_table_exists(table_name) is False


def test_write_modes(lakehouse_util, sample_df):
    """Test different write modes."""
    table_name = "mode_test_table"

    # Write initial data
    lakehouse_util.write_to_table(sample_df, table_name, mode="overwrite")

    # Read and verify
    result_df = lakehouse_util.read_table(table_name)
    assert len(result_df) == 3

    # Append more data
    additional_df = pd.DataFrame(
        {"id": [4, 5], "name": ["Dave", "Eve"], "value": [400, 500]}
    )

    lakehouse_util.write_to_table(additional_df, table_name, mode="append")

    # Read and verify total count
    result_df = lakehouse_util.read_table(table_name)
    assert len(result_df) == 5


def test_table_with_schema(lakehouse_util, sample_df):
    """Test table operations with schema parameter."""
    table_name = "schema_test_table"
    schema_name = "test_schema"

    # Write with schema (should work the same as without for delta tables)
    lakehouse_util.write_to_table(sample_df, table_name, schema_name=schema_name)

    # Check existence with schema
    exists = lakehouse_util.check_if_table_exists(table_name, schema_name)
    assert exists is True

    # Read with schema
    result_df = lakehouse_util.read_table(table_name, schema_name)
    assert len(result_df) == len(sample_df)
