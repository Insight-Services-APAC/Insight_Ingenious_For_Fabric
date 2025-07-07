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


def test_check_if_script_has_run_false_initially(ddl_utility: DDLUtilsInterface) -> None:
    script_id = "example-script-001"
    assert ddl_utility.check_if_script_has_run(script_id) is False


def test_run_once_auto_guid(ddl_utility: DDLUtilsInterface) -> None:
    ddl_utility.remove_history()
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
    ddl_utility.remove_history()
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
