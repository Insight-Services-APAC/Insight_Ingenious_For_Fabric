"""Tests for lakehouse_utils with different Spark providers."""

import os
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


class TestLakehouseUtilsProviders:
    """Test suite for lakehouse_utils with different Spark providers."""

    def test_lakehouse_utils_native_provider(self):
        """Test lakehouse_utils with native Spark provider."""
        with patch('ingen_fab.python_libs.common.config_utils.get_configs_as_object') as mock_config:
            # Configure mock to return local environment with native provider
            mock_config_obj = MagicMock()
            mock_config_obj.fabric_environment = "local"
            mock_config_obj.local_spark_provider = "native"
            mock_config.return_value = mock_config_obj
            
            # Create lakehouse_utils instance
            lh_utils = lakehouse_utils(
                target_workspace_id="test-workspace",
                target_lakehouse_id="test-lakehouse"
            )
            
            # Verify Spark session was created
            assert lh_utils.spark is not None
            assert isinstance(lh_utils.spark, SparkSession)
            assert lh_utils.spark_version == "local"
            
            # Verify it's a native session (no _client attribute)
            assert not hasattr(lh_utils.spark, "_client")
            
            # Clean up
            lh_utils.spark.stop()

    @pytest.mark.skipif(
        not os.system("python -c 'import pysail' 2>/dev/null") == 0,
        reason="pysail not installed"
    )
    def test_lakehouse_utils_lakesail_provider(self):
        """Test lakehouse_utils with lakesail Spark provider."""
        from ingen_fab.python_libs.common.spark_session_factory import SparkSessionFactory
        
        # Reset lakesail server
        SparkSessionFactory._lakesail_server = None
        
        with patch('ingen_fab.python_libs.common.config_utils.get_configs_as_object') as mock_config:
            # Configure mock to return local environment with lakesail provider
            mock_config_obj = MagicMock()
            mock_config_obj.fabric_environment = "local"
            mock_config_obj.local_spark_provider = "lakesail"
            mock_config.return_value = mock_config_obj
            
            # Create lakehouse_utils instance
            lh_utils = lakehouse_utils(
                target_workspace_id="test-workspace",
                target_lakehouse_id="test-lakehouse"
            )
            
            # Verify Spark session was created
            assert lh_utils.spark is not None
            assert isinstance(lh_utils.spark, SparkSession)
            assert lh_utils.spark_version == "local"
            
            # Verify it's a lakesail session (has _client attribute)
            assert hasattr(lh_utils.spark, "_client")
            
            # Clean up
            lh_utils.spark.stop()
            SparkSessionFactory._cleanup_lakesail_server()

    def test_lakehouse_utils_fabric_environment(self):
        """Test lakehouse_utils in Fabric environment (non-local)."""
        with patch('ingen_fab.python_libs.common.config_utils.get_configs_as_object') as mock_config:
            # Configure mock to return non-local environment
            mock_config_obj = MagicMock()
            mock_config_obj.fabric_environment = "development"
            mock_config.return_value = mock_config_obj
            
            # Mock the Spark session that would exist in Fabric
            mock_spark = MagicMock(spec=SparkSession)
            
            # Create lakehouse_utils instance with provided spark
            lh_utils = lakehouse_utils(
                target_workspace_id="test-workspace",
                target_lakehouse_id="test-lakehouse",
                spark=mock_spark
            )
            
            # Verify it uses the provided Spark session
            assert lh_utils.spark == mock_spark
            assert lh_utils.spark_version == "fabric"

    def test_lakehouse_utils_environment_variable_override(self):
        """Test that LOCAL_SPARK_PROVIDER env var overrides config."""
        with patch('ingen_fab.python_libs.common.config_utils.get_configs_as_object') as mock_config:
            # Configure mock to return local environment with lakesail provider
            mock_config_obj = MagicMock()
            mock_config_obj.fabric_environment = "local"
            mock_config_obj.local_spark_provider = "lakesail"
            mock_config.return_value = mock_config_obj
            
            # Set environment variable to override
            os.environ["LOCAL_SPARK_PROVIDER"] = "native"
            
            try:
                # Create lakehouse_utils instance
                lh_utils = lakehouse_utils(
                    target_workspace_id="test-workspace",
                    target_lakehouse_id="test-lakehouse"
                )
                
                # Should use native provider due to env var
                assert not hasattr(lh_utils.spark, "_client")
                
                # Clean up
                lh_utils.spark.stop()
            finally:
                # Clean up environment
                del os.environ["LOCAL_SPARK_PROVIDER"]

    def test_lakehouse_tables_uri_local(self):
        """Test lakehouse_tables_uri for local environment."""
        with patch('ingen_fab.python_libs.common.config_utils.get_configs_as_object') as mock_config:
            mock_config_obj = MagicMock()
            mock_config_obj.fabric_environment = "local"
            mock_config_obj.local_spark_provider = "native"
            mock_config.return_value = mock_config_obj
            
            lh_utils = lakehouse_utils(
                target_workspace_id="test-workspace",
                target_lakehouse_id="test-lakehouse"
            )
            
            uri = lh_utils.lakehouse_tables_uri()
            assert uri.startswith("file:///")
            assert "Tables/" in uri
            
            lh_utils.spark.stop()

    def test_lakehouse_files_uri_local(self):
        """Test lakehouse_files_uri for local environment."""
        with patch('ingen_fab.python_libs.common.config_utils.get_configs_as_object') as mock_config:
            mock_config_obj = MagicMock()
            mock_config_obj.fabric_environment = "local"
            mock_config_obj.local_spark_provider = "native"
            mock_config.return_value = mock_config_obj
            
            lh_utils = lakehouse_utils(
                target_workspace_id="test-workspace",
                target_lakehouse_id="test-lakehouse"
            )
            
            uri = lh_utils.lakehouse_files_uri()
            assert uri.startswith("file:///")
            assert "Files/" in uri
            
            lh_utils.spark.stop()