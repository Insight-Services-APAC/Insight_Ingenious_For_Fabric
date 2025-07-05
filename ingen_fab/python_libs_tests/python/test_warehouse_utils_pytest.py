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
