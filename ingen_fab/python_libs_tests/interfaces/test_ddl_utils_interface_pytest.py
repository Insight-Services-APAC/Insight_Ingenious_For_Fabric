from __future__ import annotations

from typing import Callable
from unittest.mock import Mock

import pytest

from ingen_fab.python_libs.interfaces.ddl_utils_interface import DDLUtilsInterface


class MockDDLUtils:
    """Mock implementation of DDLUtilsInterface for testing."""

    def __init__(self, target_workspace_id: str, target_datastore_id: str) -> None:
        self.target_workspace_id = target_workspace_id
        self.target_datastore_id = target_datastore_id
        self.execution_log = []
        self.initialized = False

    @staticmethod
    def execution_log_schema():
        """Mock schema for execution log table."""
        return {
            "object_guid": "STRING",
            "object_name": "STRING",
            "script_status": "STRING",
            "execution_datetime": "TIMESTAMP",
        }

    def print_log(self) -> None:
        """Mock print log implementation."""
        print(f"Execution log has {len(self.execution_log)} entries")

    def check_if_script_has_run(self, script_id: str) -> bool:
        """Mock check if script has run."""
        return any(
            entry["object_guid"] == script_id and entry["script_status"] == "success"
            for entry in self.execution_log
        )

    def print_skipped_script_execution(self, guid: str, object_name: str) -> None:
        """Mock print skipped execution."""
        print(f"Script {guid} for {object_name} was skipped")

    def write_to_execution_log(
        self, object_guid: str, object_name: str, script_status: str
    ) -> None:
        """Mock write to execution log."""
        self.execution_log.append(
            {
                "object_guid": object_guid,
                "object_name": object_name,
                "script_status": script_status,
                "execution_datetime": "2024-01-01T00:00:00Z",
            }
        )

    def run_once(
        self, work_fn: Callable[[], None], object_name: str, guid: str | None = None
    ) -> None:
        """Mock run once implementation."""
        if guid is None:
            guid = f"auto_generated_{object_name}"

        if self.check_if_script_has_run(guid):
            self.print_skipped_script_execution(guid, object_name)
            return

        try:
            work_fn()
            self.write_to_execution_log(guid, object_name, "success")
        except Exception as e:
            self.write_to_execution_log(guid, object_name, "failure")
            raise e

    def initialise_ddl_script_executions_table(self) -> None:
        """Mock initialize execution log table."""
        self.initialized = True


class TestDDLUtilsInterface:
    """Test the DDLUtilsInterface protocol."""

    def test_interface_exists(self):
        """Test that the DDLUtilsInterface exists."""
        assert DDLUtilsInterface is not None

    def test_interface_is_protocol(self):
        """Test that DDLUtilsInterface is a Protocol."""

        # Check if it's a Protocol (this is a basic check)
        assert hasattr(DDLUtilsInterface, "__protocol__") or "Protocol" in str(
            DDLUtilsInterface
        )

    def test_interface_has_required_methods(self):
        """Test that the interface defines all required methods."""
        # Check that all required methods are defined
        required_methods = [
            "__init__",
            "execution_log_schema",
            "print_log",
            "check_if_script_has_run",
            "print_skipped_script_execution",
            "write_to_execution_log",
            "run_once",
            "initialise_ddl_script_executions_table",
        ]

        for method in required_methods:
            assert hasattr(DDLUtilsInterface, method), (
                f"Method {method} not found in interface"
            )

    def test_mock_implementation_follows_interface(self):
        """Test that our mock implementation follows the interface."""
        # This tests that our mock class implements all required methods
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")

        # Test that all interface methods are implemented
        assert hasattr(mock_ddl, "print_log")
        assert hasattr(mock_ddl, "check_if_script_has_run")
        assert hasattr(mock_ddl, "print_skipped_script_execution")
        assert hasattr(mock_ddl, "write_to_execution_log")
        assert hasattr(mock_ddl, "run_once")
        assert hasattr(mock_ddl, "initialise_ddl_script_executions_table")
        assert hasattr(mock_ddl, "execution_log_schema")


class TestMockDDLUtils:
    """Test the mock DDL utils implementation."""

    def test_init(self):
        """Test initialization of mock DDL utils."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        assert mock_ddl.target_workspace_id == "test-workspace"
        assert mock_ddl.target_datastore_id == "test-datastore"
        assert mock_ddl.execution_log == []
        assert mock_ddl.initialized is False

    def test_execution_log_schema(self):
        """Test execution log schema method."""
        schema = MockDDLUtils.execution_log_schema()
        assert isinstance(schema, dict)
        assert "object_guid" in schema
        assert "object_name" in schema
        assert "script_status" in schema
        assert "execution_datetime" in schema

    def test_print_log_empty(self):
        """Test print log with empty log."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        with (
            pytest.raises(SystemExit)
            if hasattr(pytest, "CaptureResult")
            else pytest.warns(None)
        ):
            pass
        # Just test that it doesn't throw an exception
        mock_ddl.print_log()

    def test_check_if_script_has_run_false(self):
        """Test checking for script that hasn't run."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        result = mock_ddl.check_if_script_has_run("test-guid")
        assert result is False

    def test_check_if_script_has_run_true(self):
        """Test checking for script that has run successfully."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        mock_ddl.write_to_execution_log("test-guid", "test-object", "success")
        result = mock_ddl.check_if_script_has_run("test-guid")
        assert result is True

    def test_check_if_script_has_run_failed(self):
        """Test checking for script that has failed."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        mock_ddl.write_to_execution_log("test-guid", "test-object", "failure")
        result = mock_ddl.check_if_script_has_run("test-guid")
        assert result is False

    def test_print_skipped_script_execution(self):
        """Test printing skipped script execution."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        # Should not throw an exception
        mock_ddl.print_skipped_script_execution("test-guid", "test-object")

    def test_write_to_execution_log(self):
        """Test writing to execution log."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        mock_ddl.write_to_execution_log("test-guid", "test-object", "success")

        assert len(mock_ddl.execution_log) == 1
        entry = mock_ddl.execution_log[0]
        assert entry["object_guid"] == "test-guid"
        assert entry["object_name"] == "test-object"
        assert entry["script_status"] == "success"
        assert "execution_datetime" in entry

    def test_run_once_first_execution(self):
        """Test run_once with first execution."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        mock_work_fn = Mock()

        mock_ddl.run_once(mock_work_fn, "test-object", "test-guid")

        mock_work_fn.assert_called_once()
        assert len(mock_ddl.execution_log) == 1
        assert mock_ddl.execution_log[0]["script_status"] == "success"

    def test_run_once_skipped_execution(self):
        """Test run_once with skipped execution."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        mock_work_fn = Mock()

        # Run once to mark as executed
        mock_ddl.run_once(mock_work_fn, "test-object", "test-guid")

        # Run again - should be skipped
        mock_work_fn.reset_mock()
        mock_ddl.run_once(mock_work_fn, "test-object", "test-guid")

        mock_work_fn.assert_not_called()
        assert len(mock_ddl.execution_log) == 1  # Still only one entry

    def test_run_once_auto_generated_guid(self):
        """Test run_once with auto-generated GUID."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        mock_work_fn = Mock()

        mock_ddl.run_once(mock_work_fn, "test-object")

        mock_work_fn.assert_called_once()
        assert len(mock_ddl.execution_log) == 1
        assert mock_ddl.execution_log[0]["object_guid"] == "auto_generated_test-object"

    def test_run_once_with_exception(self):
        """Test run_once when work function throws exception."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        mock_work_fn = Mock(side_effect=RuntimeError("Test error"))

        with pytest.raises(RuntimeError, match="Test error"):
            mock_ddl.run_once(mock_work_fn, "test-object", "test-guid")

        mock_work_fn.assert_called_once()
        assert len(mock_ddl.execution_log) == 1
        assert mock_ddl.execution_log[0]["script_status"] == "failure"

    def test_initialise_ddl_script_executions_table(self):
        """Test initializing DDL script executions table."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        assert mock_ddl.initialized is False

        mock_ddl.initialise_ddl_script_executions_table()

        assert mock_ddl.initialized is True

    def test_print_log_with_entries(self):
        """Test print log with entries."""
        mock_ddl = MockDDLUtils("test-workspace", "test-datastore")
        mock_ddl.write_to_execution_log("guid1", "object1", "success")
        mock_ddl.write_to_execution_log("guid2", "object2", "failure")

        # Should not throw an exception
        mock_ddl.print_log()
        assert len(mock_ddl.execution_log) == 2
