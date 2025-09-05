"""Tests for SparkSessionFactory with both native and lakesail providers."""

import os
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

from ingen_fab.python_libs.common.spark_session_factory import SparkSessionFactory


class TestSparkSessionFactory:
    """Test suite for SparkSessionFactory."""

    def test_create_native_session(self):
        """Test creating a native Spark session."""
        # Clean up any existing sessions
        try:
            existing = SparkSession.getActiveSession()
            if existing:
                existing.stop()
        except:
            pass

        spark = SparkSessionFactory.create_local_spark_session(provider="native")
        
        assert spark is not None
        assert isinstance(spark, SparkSession)
        
        # Test that Delta extensions are configured
        configs = spark.sparkContext.getConf().getAll()
        config_dict = dict(configs)
        
        assert "spark.sql.extensions" in config_dict
        assert "DeltaSparkSessionExtension" in config_dict["spark.sql.extensions"]
        
        spark.stop()

    def test_reuse_existing_native_session(self):
        """Test that existing native sessions are reused."""
        # Create first session
        spark1 = SparkSessionFactory.create_local_spark_session(provider="native")
        
        # Create second session - should reuse first
        spark2 = SparkSessionFactory.create_local_spark_session(provider="native")
        
        assert spark1 == spark2
        
        spark1.stop()

    @pytest.mark.skipif(
        not os.system("python -c 'import pysail' 2>/dev/null") == 0,
        reason="pysail not installed"
    )
    def test_create_lakesail_session(self):
        """Test creating a Lakesail Spark session."""
        # Clean up any existing sessions
        try:
            existing = SparkSession.getActiveSession()
            if existing:
                existing.stop()
        except:
            pass

        # Reset the server instance
        SparkSessionFactory._lakesail_server = None

        spark = SparkSessionFactory.create_local_spark_session(
            provider="lakesail",
            lakesail_port=50052  # Use different port to avoid conflicts
        )
        
        assert spark is not None
        assert isinstance(spark, SparkSession)
        
        # Test that it's a remote session
        assert hasattr(spark, "_client")
        
        # Clean up
        spark.stop()
        SparkSessionFactory._cleanup_lakesail_server()

    def test_lakesail_import_error(self):
        """Test that ImportError is raised when pysail is not available."""
        with patch.dict('sys.modules', {'pysail.spark': None}):
            with pytest.raises(ImportError, match="pysail is required"):
                SparkSessionFactory.create_local_spark_session(provider="lakesail")

    def test_invalid_provider(self):
        """Test that ValueError is raised for invalid provider."""
        with pytest.raises(ValueError, match="Unknown Spark provider"):
            SparkSessionFactory.create_local_spark_session(provider="invalid")

    def test_environment_variable_override(self):
        """Test that LOCAL_SPARK_PROVIDER environment variable overrides config."""
        # Clean up any existing sessions
        try:
            existing = SparkSession.getActiveSession()
            if existing:
                existing.stop()
        except:
            pass

        # Set environment variable
        os.environ["LOCAL_SPARK_PROVIDER"] = "native"
        
        # Even if we request lakesail, it should use native due to env var
        spark = SparkSessionFactory.create_local_spark_session(provider="lakesail")
        
        # Should be a native session (no _client attribute)
        assert not hasattr(spark, "_client")
        
        spark.stop()
        
        # Clean up environment
        del os.environ["LOCAL_SPARK_PROVIDER"]

    def test_get_or_create_with_config(self):
        """Test get_or_create_spark_session with config auto-detection."""
        with patch('ingen_fab.python_libs.common.config_utils.get_configs_as_object') as mock_config:
            mock_config_obj = MagicMock()
            mock_config_obj.local_spark_provider = "native"
            mock_config.return_value = mock_config_obj
            
            spark = SparkSessionFactory.get_or_create_spark_session()
            
            assert spark is not None
            assert isinstance(spark, SparkSession)
            
            spark.stop()

    @pytest.mark.skipif(
        not os.system("python -c 'import pysail' 2>/dev/null") == 0,
        reason="pysail not installed"
    )
    def test_lakesail_server_lifecycle(self):
        """Test Lakesail server lifecycle management."""
        # Reset server
        SparkSessionFactory._lakesail_server = None
        
        # Create first session - should start server
        spark1 = SparkSessionFactory.create_local_spark_session(
            provider="lakesail",
            lakesail_port=50053
        )
        
        assert SparkSessionFactory._lakesail_server is not None
        server1 = SparkSessionFactory._lakesail_server
        
        # Create second session - should reuse server
        spark2 = SparkSessionFactory.create_local_spark_session(
            provider="lakesail",
            lakesail_port=50053
        )
        
        assert SparkSessionFactory._lakesail_server == server1
        
        # Clean up
        spark1.stop()
        spark2.stop()
        SparkSessionFactory._cleanup_lakesail_server()
        
        assert SparkSessionFactory._lakesail_server is None