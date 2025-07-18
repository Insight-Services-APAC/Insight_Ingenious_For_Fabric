from __future__ import annotations

import os
import tempfile
import pytest
from unittest.mock import Mock, patch

from ingen_fab.python_libs.python.notebook_utils_abstraction import (
    NotebookUtilsInterface,
    FabricNotebookUtils,
    LocalNotebookUtils,
    NotebookUtilsFactory
)


@pytest.fixture(autouse=True)
def reset_factory():
    """Reset the factory singleton before each test."""
    NotebookUtilsFactory.reset_instance()
    yield
    NotebookUtilsFactory.reset_instance()


@pytest.fixture
def mock_notebookutils():
    """Create a mock notebookutils object."""
    mock_utils = Mock()
    mock_utils.data.connect_to_artifact.return_value = Mock()
    return mock_utils


class TestFabricNotebookUtils:
    """Test the FabricNotebookUtils implementation."""
    
    def test_init_with_notebookutils(self, mock_notebookutils):
        """Test initialization with notebookutils."""
        with patch('ingen_fab.python_libs.python.notebook_utils_abstraction.get_configs_as_object') as mock_config:
            mock_config.return_value.fabric_environment = "development"
            
            utils = FabricNotebookUtils(mock_notebookutils)
            assert utils._notebookutils == mock_notebookutils
            assert utils.is_available() is True
    
    def test_init_local_environment(self, mock_notebookutils):
        """Test initialization in local environment."""
        with patch('ingen_fab.python_libs.python.notebook_utils_abstraction.get_configs_as_object') as mock_config:
            mock_config.return_value.fabric_environment = "local"
            
            utils = FabricNotebookUtils(mock_notebookutils)
            assert utils.is_available() is False
    
    def test_connect_to_artifact_available(self, mock_notebookutils):
        """Test connecting to artifact when available."""
        with patch('ingen_fab.python_libs.python.notebook_utils_abstraction.get_configs_as_object') as mock_config:
            mock_config.return_value.fabric_environment = "development"
            
            utils = FabricNotebookUtils(mock_notebookutils)
            result = utils.connect_to_artifact("test-artifact", "test-workspace")
            
            mock_notebookutils.data.connect_to_artifact.assert_called_once_with("test-artifact", "test-workspace")
            assert result is not None
    
    def test_connect_to_artifact_unavailable(self, mock_notebookutils):
        """Test connecting to artifact when unavailable."""
        with patch('ingen_fab.python_libs.python.notebook_utils_abstraction.get_configs_as_object') as mock_config:
            mock_config.return_value.fabric_environment = "local"
            
            utils = FabricNotebookUtils(mock_notebookutils)
            
            with pytest.raises(RuntimeError, match="notebookutils not available"):
                utils.connect_to_artifact("test-artifact", "test-workspace")
    
    def test_display_available(self, mock_notebookutils):
        """Test display when available."""
        with patch('ingen_fab.python_libs.python.notebook_utils_abstraction.get_configs_as_object') as mock_config:
            mock_config.return_value.fabric_environment = "development"
            
            utils = FabricNotebookUtils(mock_notebookutils)
            
            with patch.object(utils, '_fabric_display') as mock_display:
                utils.display("test object")
                mock_display.assert_called_once_with("test object")
    
    def test_display_unavailable(self, mock_notebookutils):
        """Test display when unavailable falls back to print."""
        with patch('ingen_fab.python_libs.python.notebook_utils_abstraction.get_configs_as_object') as mock_config:
            mock_config.return_value.fabric_environment = "local"
            
            utils = FabricNotebookUtils(mock_notebookutils)
            
            with patch('builtins.print') as mock_print:
                utils.display("test object")
                mock_print.assert_called_once_with("test object")
    
    def test_exit_notebook_unavailable(self, mock_notebookutils):
        """Test exit notebook when unavailable."""
        with patch('ingen_fab.python_libs.python.notebook_utils_abstraction.get_configs_as_object') as mock_config:
            mock_config.return_value.fabric_environment = "local"
            
            utils = FabricNotebookUtils(mock_notebookutils)
            # Should not raise exception
            utils.exit_notebook("test value")
    
    def test_get_secret_unavailable(self, mock_notebookutils):
        """Test get secret when unavailable."""
        with patch('ingen_fab.python_libs.python.notebook_utils_abstraction.get_configs_as_object') as mock_config:
            mock_config.return_value.fabric_environment = "local"
            
            utils = FabricNotebookUtils(mock_notebookutils)
            
            with pytest.raises(RuntimeError, match="notebookutils not available"):
                utils.get_secret("test-secret", "test-vault")


class TestLocalNotebookUtils:
    """Test the LocalNotebookUtils implementation."""
    
    def test_init_default_connection(self):
        """Test initialization with default connection string."""
        utils = LocalNotebookUtils()
        assert utils.connection_string is not None
        assert "localhost,1433" in utils.connection_string
    
    def test_init_custom_connection(self):
        """Test initialization with custom connection string."""
        custom_conn = "custom connection string"
        utils = LocalNotebookUtils(custom_conn)
        assert utils.connection_string == custom_conn
    
    def test_get_default_connection_string(self):
        """Test default connection string generation."""
        utils = LocalNotebookUtils()
        conn_str = utils._get_default_connection_string()
        assert "DRIVER={ODBC Driver 18 for SQL Server}" in conn_str
        assert "SERVER=localhost,1433" in conn_str
        assert "UID=sa" in conn_str
        assert "TrustServerCertificate=yes" in conn_str
    
    def test_get_default_connection_string_with_env_password(self):
        """Test default connection string with environment password."""
        with patch.dict(os.environ, {"SQL_SERVER_PASSWORD": "test_password"}):
            utils = LocalNotebookUtils()
            conn_str = utils._get_default_connection_string()
            assert "PWD=test_password" in conn_str
    
    def test_load_local_secrets(self):
        """Test loading secrets from environment variables."""
        with patch.dict(os.environ, {
            "SECRET_TEST_SECRET": "test_value",
            "SECRET_ANOTHER_SECRET": "another_value",
            "NOT_A_SECRET": "ignored"
        }):
            utils = LocalNotebookUtils()
            secrets = utils._load_local_secrets()
            assert secrets["TEST_SECRET"] == "test_value"
            assert secrets["ANOTHER_SECRET"] == "another_value"
            assert "NOT_A_SECRET" not in secrets
    
    def test_connect_to_artifact_success(self):
        """Test successful connection to artifact."""
        with patch('builtins.__import__') as mock_import:
            mock_pyodbc = Mock()
            mock_connection = Mock()
            mock_pyodbc.connect.return_value = mock_connection
            
            def import_side_effect(name, *args, **kwargs):
                if name == 'pyodbc':
                    return mock_pyodbc
                return __import__(name, *args, **kwargs)
            
            mock_import.side_effect = import_side_effect
            
            with patch.dict(os.environ, {"SQL_SERVER_PASSWORD": "test_password"}):
                utils = LocalNotebookUtils()
                result = utils.connect_to_artifact("test-artifact", "test-workspace")
                
                assert result == mock_connection
                mock_pyodbc.connect.assert_called_once()
    
    def test_connect_to_artifact_fallback_password(self):
        """Test connection with fallback password."""
        with patch('builtins.__import__') as mock_import:
            mock_pyodbc = Mock()
            mock_connection = Mock()
            mock_pyodbc.connect.return_value = mock_connection
            
            def import_side_effect(name, *args, **kwargs):
                if name == 'pyodbc':
                    return mock_pyodbc
                return __import__(name, *args, **kwargs)
            
            mock_import.side_effect = import_side_effect
            
            with patch.dict(os.environ, {"SQL_SERVER_PASSWORD": "fallback_password"}):
                utils = LocalNotebookUtils()
                result = utils.connect_to_artifact("test-artifact", "test-workspace")
                
                assert result == mock_connection
    
    def test_connect_to_artifact_default_password(self):
        """Test connection with default password."""
        with patch('builtins.__import__') as mock_import:
            mock_pyodbc = Mock()
            mock_connection = Mock()
            mock_pyodbc.connect.return_value = mock_connection
            
            def import_side_effect(name, *args, **kwargs):
                if name == 'pyodbc':
                    return mock_pyodbc
                return __import__(name, *args, **kwargs)
            
            mock_import.side_effect = import_side_effect
            
            # Clear any password env vars
            with patch.dict(os.environ, {}, clear=True):
                utils = LocalNotebookUtils()
                result = utils.connect_to_artifact("test-artifact", "test-workspace")
                
                assert result == mock_connection
            # Check that default password was used
            call_args = mock_pyodbc.connect.call_args[0][0]
            assert "PWD=YourStrong!Passw0rd" in call_args
    
    def test_connect_to_artifact_no_pyodbc(self):
        """Test connection when pyodbc is not available."""
        with patch.dict('sys.modules', {'pyodbc': None}):
            utils = LocalNotebookUtils()
            with pytest.raises(RuntimeError, match="pyodbc not available"):
                utils.connect_to_artifact("test-artifact", "test-workspace")
    
    def test_display_with_to_string(self):
        """Test display with object that has to_string method."""
        mock_obj = Mock()
        mock_obj.to_string.return_value = "string representation"
        
        utils = LocalNotebookUtils()
        with patch('builtins.print') as mock_print:
            utils.display(mock_obj)
            mock_print.assert_called_once_with("string representation")
    
    def test_display_without_to_string(self):
        """Test display with object that doesn't have to_string method."""
        test_obj = "test object"
        
        utils = LocalNotebookUtils()
        with patch('builtins.print') as mock_print:
            utils.display(test_obj)
            mock_print.assert_called_once_with(test_obj)
    
    def test_exit_notebook(self):
        """Test exit notebook functionality."""
        utils = LocalNotebookUtils()
        with patch('builtins.print') as mock_print:
            utils.exit_notebook("test value")
            mock_print.assert_called_once_with("Notebook would exit with value: test value")
    
    def test_exit_notebook_no_value(self):
        """Test exit notebook without value."""
        utils = LocalNotebookUtils()
        # Should not raise exception
        utils.exit_notebook()
    
    def test_get_secret_from_loaded_secrets(self):
        """Test getting secret from loaded secrets."""
        with patch.dict(os.environ, {"SECRET_TEST_SECRET": "test_value"}):
            utils = LocalNotebookUtils()
            secret = utils.get_secret("TEST_SECRET", "test-vault")
            assert secret == "test_value"
    
    def test_get_secret_from_env_with_prefix(self):
        """Test getting secret from environment with prefix."""
        with patch.dict(os.environ, {"SECRET_TEST_SECRET": "test_value"}):
            utils = LocalNotebookUtils()
            secret = utils.get_secret("test_secret", "test-vault")
            assert secret == "test_value"
    
    def test_get_secret_not_found(self):
        """Test getting non-existent secret."""
        utils = LocalNotebookUtils()
        with pytest.raises(ValueError, match="Secret 'nonexistent' not found"):
            utils.get_secret("nonexistent", "test-vault")
    
    def test_is_available(self):
        """Test is_available always returns True."""
        utils = LocalNotebookUtils()
        assert utils.is_available() is True


class TestNotebookUtilsFactory:
    """Test the NotebookUtilsFactory."""
    
    def test_get_instance_singleton(self):
        """Test that get_instance returns singleton."""
        instance1 = NotebookUtilsFactory.get_instance()
        instance2 = NotebookUtilsFactory.get_instance()
        assert instance1 is instance2
    
    def test_create_instance_with_notebookutils(self, mock_notebookutils):
        """Test creating instance with notebookutils."""
        instance = NotebookUtilsFactory.create_instance(notebookutils=mock_notebookutils)
        assert isinstance(instance, FabricNotebookUtils)
    
    def test_create_instance_force_local(self, mock_notebookutils):
        """Test creating local instance when forced."""
        instance = NotebookUtilsFactory.create_instance(notebookutils=mock_notebookutils, force_local=True)
        assert isinstance(instance, LocalNotebookUtils)
    
    def test_create_instance_fallback_to_local(self):
        """Test fallback to local when no notebookutils provided."""
        instance = NotebookUtilsFactory.create_instance()
        assert isinstance(instance, LocalNotebookUtils)
    
    def test_reset_instance(self):
        """Test resetting the singleton instance."""
        instance1 = NotebookUtilsFactory.get_instance()
        NotebookUtilsFactory.reset_instance()
        instance2 = NotebookUtilsFactory.get_instance()
        assert instance1 is not instance2