from __future__ import annotations

import os
import pytest
from unittest.mock import Mock, patch

from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
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
    mock_utils.lakehouse.get.return_value = Mock()
    return mock_utils


@pytest.fixture
def mock_mssparkutils():
    """Create a mock mssparkutils object."""
    mock_utils = Mock()
    mock_utils.credentials.getSecret.return_value = "test_secret_value"
    mock_utils.notebook.exit.return_value = None
    return mock_utils


class TestFabricNotebookUtils:
    """Test the FabricNotebookUtils implementation."""
    
    def test_init_with_utils(self, mock_notebookutils, mock_mssparkutils):
        """Test initialization with both utils."""
        utils = FabricNotebookUtils(mock_notebookutils, mock_mssparkutils)
        assert utils._notebookutils == mock_notebookutils
        assert utils._mssparkutils == mock_mssparkutils
        assert utils.is_available() is True
    
    def test_init_without_utils(self):
        """Test initialization without utils."""
        utils = FabricNotebookUtils()
        assert utils.is_available() is False
    
    def test_connect_to_artifact_available(self, mock_notebookutils, mock_mssparkutils):
        """Test connecting to artifact when available."""
        utils = FabricNotebookUtils(mock_notebookutils, mock_mssparkutils)
        result = utils.connect_to_artifact("test-artifact", "test-workspace")
        
        mock_notebookutils.lakehouse.get.assert_called_once_with("test-artifact")
        assert result is not None
    
    def test_connect_to_artifact_unavailable(self):
        """Test connecting to artifact when unavailable."""
        utils = FabricNotebookUtils()
        
        with pytest.raises(RuntimeError, match="notebookutils not available"):
            utils.connect_to_artifact("test-artifact", "test-workspace")
    
    def test_display_unavailable(self):
        """Test display when unavailable falls back to print."""
        utils = FabricNotebookUtils()
        
        with patch('builtins.print') as mock_print:
            utils.display("test object")
            mock_print.assert_called_once_with("test object")
    
    def test_exit_notebook_available(self, mock_notebookutils, mock_mssparkutils):
        """Test exit notebook when available."""
        utils = FabricNotebookUtils(mock_notebookutils, mock_mssparkutils)
        utils.exit_notebook("test value")
        
        mock_mssparkutils.notebook.exit.assert_called_once_with("test value")
    
    def test_exit_notebook_unavailable(self):
        """Test exit notebook when unavailable."""
        utils = FabricNotebookUtils()
        # Should not raise exception
        utils.exit_notebook("test value")
    
    def test_get_secret_available(self, mock_notebookutils, mock_mssparkutils):
        """Test get secret when available."""
        utils = FabricNotebookUtils(mock_notebookutils, mock_mssparkutils)
        result = utils.get_secret("test-secret", "test-vault")
        
        mock_mssparkutils.credentials.getSecret.assert_called_once_with("test-vault", "test-secret")
        assert result == "test_secret_value"
    
    def test_get_secret_unavailable(self):
        """Test get secret when unavailable."""
        utils = FabricNotebookUtils()
        
        with pytest.raises(RuntimeError, match="mssparkutils not available"):
            utils.get_secret("test-secret", "test-vault")


class TestLocalNotebookUtils:
    """Test the LocalNotebookUtils implementation."""
    
    def test_init_default(self):
        """Test initialization with defaults."""
        utils = LocalNotebookUtils()
        assert utils.spark_session_name == "spark"
        assert utils.spark_session is None
        assert utils._secrets == {}
    
    def test_init_custom_spark_session(self):
        """Test initialization with custom spark session name."""
        utils = LocalNotebookUtils(spark_session_name="custom_spark")
        assert utils.spark_session_name == "custom_spark"
    
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
    
    def test_get_spark_session_none(self):
        """Test getting spark session when none exists."""
        utils = LocalNotebookUtils()
        session = utils._get_spark_session()
        assert session is None
    
    def test_get_spark_session_existing(self):
        """Test getting existing spark session."""
        mock_session = Mock()
        with patch('builtins.globals', return_value={"spark": mock_session}):
            utils = LocalNotebookUtils()
            session = utils._get_spark_session()
            assert session == mock_session
    
    def test_connect_to_artifact_no_spark(self):
        """Test connect to artifact without spark session."""
        utils = LocalNotebookUtils()
        
        with pytest.raises(RuntimeError, match="Spark session not available"):
            utils.connect_to_artifact("test-artifact", "test-workspace")
    
    def test_display_with_spark_dataframe(self):
        """Test display with spark DataFrame."""
        mock_df = Mock()
        mock_df.show = Mock()
        
        utils = LocalNotebookUtils()
        utils.display(mock_df)
        
        mock_df.show.assert_called_once()
    
    def test_display_with_pandas_dataframe(self):
        """Test display with pandas DataFrame."""
        mock_df = Mock()
        mock_df.head = Mock(return_value="dataframe_head")
        # Make sure it doesn't have count attribute (pandas DataFrames don't have count method like Spark)
        del mock_df.count
        
        utils = LocalNotebookUtils()
        with patch('builtins.print') as mock_print:
            utils.display(mock_df)
            mock_print.assert_called_once_with("dataframe_head")
    
    def test_display_with_regular_object(self):
        """Test display with regular object."""
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
    
    def test_create_lakehouse_path(self):
        """Test creating lakehouse path."""
        utils = LocalNotebookUtils()
        path = utils._create_lakehouse_path("test-lakehouse")
        assert "/tmp/test-lakehouse" in str(path)
    
    def test_create_lakehouse_path_existing(self):
        """Test creating lakehouse path when it already exists."""
        utils = LocalNotebookUtils()
        with patch('pathlib.Path.mkdir') as mock_mkdir:
            mock_mkdir.side_effect = FileExistsError()
            # Should not raise exception
            path = utils._create_lakehouse_path("test-lakehouse")
            assert "/tmp/test-lakehouse" in str(path)


class TestNotebookUtilsFactory:
    """Test the NotebookUtilsFactory."""
    
    def test_get_instance_singleton(self):
        """Test that get_instance returns singleton."""
        instance1 = NotebookUtilsFactory.get_instance()
        instance2 = NotebookUtilsFactory.get_instance()
        assert instance1 is instance2
    
    def test_create_instance_force_local(self, mock_notebookutils, mock_mssparkutils):
        """Test creating local instance when forced."""
        instance = NotebookUtilsFactory.create_instance(
            notebookutils=mock_notebookutils, 
            mssparkutils=mock_mssparkutils,
            force_local=True
        )
        assert isinstance(instance, LocalNotebookUtils)
    
    def test_create_instance_fallback_to_local(self):
        """Test fallback to local when no utils provided."""
        instance = NotebookUtilsFactory.create_instance()
        assert isinstance(instance, LocalNotebookUtils)
    
    def test_create_instance_mssparkutils_only(self, mock_mssparkutils):
        """Test creating instance with only mssparkutils."""
        instance = NotebookUtilsFactory.create_instance(mssparkutils=mock_mssparkutils)
        assert isinstance(instance, LocalNotebookUtils)  # Falls back to local
    
    def test_reset_instance(self):
        """Test resetting the singleton instance."""
        instance1 = NotebookUtilsFactory.get_instance()
        NotebookUtilsFactory.reset_instance()
        instance2 = NotebookUtilsFactory.get_instance()
        assert instance1 is not instance2


class TestMssparkutilsAbstraction:
    """Test the mssparkutils abstraction classes."""
    
    def test_mssparkutils_notebook_exit(self):
        """Test mssparkutils notebook exit functionality."""
        from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsInterface
        
        # Test that the inner classes exist
        assert hasattr(NotebookUtilsInterface, 'mssparkutils')
        assert hasattr(NotebookUtilsInterface.mssparkutils, 'notebook')
        
        # These are abstract classes, so we can't instantiate them directly
        # but we can verify they exist and have the expected structure