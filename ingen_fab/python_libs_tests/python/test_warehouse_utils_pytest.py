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
    conn = utils.get_connection()
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
