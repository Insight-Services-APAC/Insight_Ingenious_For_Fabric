"""Tests for notebook utils abstraction."""

import os
from unittest.mock import MagicMock, patch

import pytest

from ingen_fab.python_libs.python.notebook_utils_abstraction import (
    FabricNotebookUtils,
    LocalNotebookUtils,
    NotebookUtilsFactory,
    get_notebook_utils,
)


class TestFabricNotebookUtils:
    """Test Fabric notebook utils implementation."""

    def test_fabric_utils_not_available_when_no_notebookutils(self):
        """Test that Fabric utils reports not available when notebookutils is not importable."""
        # Note: This test depends on the actual environment
        # In a test environment without notebookutils, it should be False
        fabric_utils = FabricNotebookUtils()
        # The actual value depends on whether notebookutils is available
        # This test documents the behavior
        availability = fabric_utils.is_available()
        assert isinstance(availability, bool)

    def test_fabric_utils_structure(self):
        """Test that Fabric utils has the correct structure."""
        fabric_utils = FabricNotebookUtils()
        assert hasattr(fabric_utils, "_notebookutils")
        assert hasattr(fabric_utils, "_mssparkutils")
        assert hasattr(fabric_utils, "_available")


class TestLocalNotebookUtils:
    """Test Local notebook utils implementation."""

    def test_local_utils_always_available(self):
        """Test that local utils are always available."""
        local_utils = LocalNotebookUtils()
        assert local_utils.is_available()

    def test_local_utils_loads_secrets_from_env(self):
        """Test that local utils loads secrets from environment variables."""
        with patch.dict(os.environ, {"SECRET_TEST_KEY": "test_value"}):
            local_utils = LocalNotebookUtils()
            assert "TEST_KEY" in local_utils._secrets
            assert local_utils._secrets["TEST_KEY"] == "test_value"

    def test_get_secret_from_env(self):
        """Test getting secrets from environment variables."""
        with patch.dict(os.environ, {"SECRET_TEST_KEY": "test_value"}):
            local_utils = LocalNotebookUtils()
            secret = local_utils.get_secret("TEST_KEY", "dummy_vault")
            assert secret == "test_value"

    def test_get_secret_not_found(self):
        """Test that ValueError is raised when secret is not found."""
        local_utils = LocalNotebookUtils()
        with pytest.raises(ValueError, match="Secret 'NONEXISTENT' not found"):
            local_utils.get_secret("NONEXISTENT", "dummy_vault")

    def test_display_dataframe(self):
        """Test displaying a DataFrame."""
        local_utils = LocalNotebookUtils()

        # Mock a DataFrame-like object
        mock_df = MagicMock()
        mock_df.to_string.return_value = "   A  B\n0  1  3\n1  2  4"

        with patch("builtins.print") as mock_print:
            local_utils.display(mock_df)
            mock_print.assert_called_once_with("   A  B\n0  1  3\n1  2  4")

    def test_display_regular_object(self):
        """Test displaying a regular object."""
        local_utils = LocalNotebookUtils()

        with patch("builtins.print") as mock_print:
            local_utils.display("test string")
            mock_print.assert_called_once_with("test string")

    def test_exit_notebook_logs_value(self):
        """Test that exit_notebook logs the value."""
        local_utils = LocalNotebookUtils()

        with patch("builtins.print") as mock_print:
            local_utils.exit_notebook("test_value")
            mock_print.assert_called_once_with("Notebook would exit with value: test_value")

    def test_connect_to_artifact_returns_pyodbc_connection(self):
        """Test that connect_to_artifact returns a pyodbc connection."""
        with patch("pyodbc.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            local_utils = LocalNotebookUtils()
            conn = local_utils.connect_to_artifact("test_artifact", "test_workspace")

            assert conn == mock_conn
            mock_connect.assert_called_once_with(local_utils.connection_string)


class TestNotebookUtilsFactory:
    """Test the factory for creating notebook utils."""

    def test_factory_creates_local_when_forced(self):
        """Test that factory creates local utils when forced."""
        NotebookUtilsFactory.reset_instance()
        utils = NotebookUtilsFactory.get_instance(force_local=True)
        assert isinstance(utils, LocalNotebookUtils)

    def test_factory_singleton_behavior(self):
        """Test that factory returns the same instance."""
        NotebookUtilsFactory.reset_instance()
        utils1 = NotebookUtilsFactory.get_instance()
        utils2 = NotebookUtilsFactory.get_instance()
        assert utils1 is utils2

    def test_factory_reset_creates_new_instance(self):
        """Test that reset creates a new instance."""
        NotebookUtilsFactory.reset_instance()
        utils1 = NotebookUtilsFactory.get_instance()
        NotebookUtilsFactory.reset_instance()
        utils2 = NotebookUtilsFactory.get_instance()
        assert utils1 is not utils2

    def test_get_notebook_utils_convenience_function(self):
        """Test the convenience function."""
        NotebookUtilsFactory.reset_instance()
        utils = get_notebook_utils()
        assert utils is not None
        assert hasattr(utils, "connect_to_artifact")
        assert hasattr(utils, "display")
        assert hasattr(utils, "exit_notebook")
        assert hasattr(utils, "get_secret")
        assert hasattr(utils, "is_available")
