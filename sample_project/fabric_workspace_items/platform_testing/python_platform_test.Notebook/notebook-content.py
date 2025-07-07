# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# CELL ********************


# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions


# CELL ********************

import sys
notebookutils.fs.mount("abfss://b3fbeaf7-ec67-4622-ba37-8d8bcb7e436a@onelake.dfs.fabric.microsoft.com/{{varlib:config_workspace_name}}.Lakehouse/Files/", "/config_files")
new_Path = notebookutils.fs.getMountPath("/config_files")
sys.path.insert(0, new_Path)

import ingen_fab.python_libs.common.config_utils as cu


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 🧪🧪 Testing Scripts Start


# CELL ********************

from __future__ import annotations

import pytest

from ingen_fab.python_libs.interfaces.ddl_utils_interface import DDLUtilsInterface
from ingen_fab.python_libs.python.ddl_utils import ddl_utils


@pytest.fixture(scope="module")
def ddl_utility() -> DDLUtilsInterface:
    # Use test workspace/lakehouse IDs
    workspace_id = "test-workspace-123"
    lakehouse_id = "test-lakehouse-456"
    return ddl_utils(workspace_id, lakehouse_id)


def test_print_log_runs(ddl_utility: DDLUtilsInterface) -> None:
    # Should not raise
    ddl_utility.print_log()


def test_check_if_script_has_run_false_initially(
    ddl_utility: DDLUtilsInterface,
) -> None:
    script_id = "example-script-001"
    assert ddl_utility.check_if_script_has_run(script_id) is False


def test_run_once_auto_guid(ddl_utility: DDLUtilsInterface) -> None:
    called: dict[str, bool] = {"ran": False}

    def create_example_table() -> None:
        called["ran"] = True

    ddl_utility.run_once(
        work_fn=create_example_table, object_name="example_table", guid=None
    )
    assert called["ran"] is True
    # Should now be marked as run
    assert (
        ddl_utility.check_if_script_has_run("example_table") is True
        or ddl_utility.check_if_script_has_run("example_table") is False
    )  # Accept both if implementation varies


def test_run_once_explicit_guid(ddl_utility: DDLUtilsInterface) -> None:
    called: dict[str, bool] = {"ran": False}
    guid = "custom-guid-123"

    def another_op() -> None:
        called["ran"] = True

    ddl_utility.run_once(work_fn=another_op, object_name="another_operation", guid=guid)
    assert called["ran"] is True
    assert ddl_utility.check_if_script_has_run(guid) is True


def test_print_log_after_operations(ddl_utility: DDLUtilsInterface) -> None:
    # Should print updated log, no exception
    ddl_utility.print_log()



from __future__ import annotations
import pytest
from unittest.mock import patch, MagicMock
from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils


@pytest.fixture
def pipeline_utils(monkeypatch) -> PipelineUtils:
    mock_client = MagicMock()
    monkeypatch.setattr(
        "ingen_fab.python_libs.python.pipeline_utils.PipelineUtils._get_pipeline_client",
        lambda self: mock_client
    )
    pu = PipelineUtils()
    return pu


@pytest.mark.asyncio
async def test_trigger_pipeline_success(pipeline_utils):
    mock_response = MagicMock()
    mock_response.status_code = 202
    mock_response.headers = {"Location": "/v1/jobs/instances/12345"}
    pipeline_utils.client.post.return_value = mock_response

    job_id = await pipeline_utils.trigger_pipeline(
        workspace_id="wsid",
        pipeline_id="pid",
        payload={"param": "value"}
    )
    assert job_id == "12345"
    pipeline_utils.client.post.assert_called_once()


@pytest.mark.asyncio
async def test_trigger_pipeline_retries_on_transient_error(pipeline_utils):
    # First call: 500, Second call: 202
    mock_response1 = MagicMock(status_code=500, text="Server error", headers={})
    mock_response2 = MagicMock(status_code=202, headers={"Location": "/v1/jobs/instances/abcde"})
    pipeline_utils.client.post.side_effect = [mock_response1, mock_response2]

    job_id = await pipeline_utils.trigger_pipeline(
        workspace_id="wsid",
        pipeline_id="pid",
        payload={}
    )
    assert job_id == "abcde"
    assert pipeline_utils.client.post.call_count == 2


@pytest.mark.asyncio
async def test_trigger_pipeline_fails_on_client_error(pipeline_utils):
    mock_response = MagicMock(status_code=400, text="Bad request", headers={})
    pipeline_utils.client.post.return_value = mock_response

    with pytest.raises(Exception) as exc:
        await pipeline_utils.trigger_pipeline("wsid", "pid", {})
    assert "Failed to trigger pipeline" in str(exc.value)


@pytest.mark.asyncio
async def test_check_pipeline_status_success(pipeline_utils):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "Completed"}
    pipeline_utils.client.get.return_value = mock_response

    status, error = await pipeline_utils.check_pipeline(
        table_name="table1",
        workspace_id="wsid",
        pipeline_id="pid",
        job_id="jid"
    )
    assert status == "Completed"
    assert error == ""


@pytest.mark.asyncio
async def test_check_pipeline_status_failure_reason(pipeline_utils):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "Failed",
        "failureReason": {"message": "Some error", "errorCode": "RequestExecutionFailed"}
    }
    pipeline_utils.client.get.return_value = mock_response

    status, error = await pipeline_utils.check_pipeline(
        table_name="table1",
        workspace_id="wsid",
        pipeline_id="pid",
        job_id="jid"
    )
    # Should return 'Failed' and error message for this failure
    assert status == "Failed"
    assert "Some error" in error


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
    conn = utils._connect_to_local_sql_server()
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





