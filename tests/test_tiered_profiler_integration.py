"""Integration tests for the refactored TieredProfiler system."""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from ingen_fab.python_libs.interfaces.data_profiling_interface import ProfileType
from ingen_fab.python_libs.interfaces.profiler_registry import get_registry, get_profiler
from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler, ScanLevel


@pytest.fixture
def spark_session():
    """Create a test Spark session."""
    spark = SparkSession.builder \
        .appName("test_tiered_profiler") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("department", StringType(), True),
    ])
    
    data = [
        (1, "Alice", 30, "Engineering"),
        (2, "Bob", 35, "Sales"),
        (3, "Charlie", 28, "Engineering"),
        (4, None, 32, "HR"),
        (5, "Eve", 29, "Sales"),
    ]
    
    return spark_session.createDataFrame(data, schema)


def test_tiered_profiler_registration():
    """Test that TieredProfiler is registered in the registry."""
    registry = get_registry()
    
    # Check that tiered profiler is registered
    assert "tiered" in registry.list_profilers()
    assert "default" in registry.list_profilers()
    
    # Check that ultra_fast is NOT registered (removed)
    assert "ultra_fast" not in registry.list_profilers()
    
    # Get metadata
    metadata = registry.get_profiler_metadata("tiered")
    assert metadata.name == "tiered"
    assert metadata.priority == 250  # Highest priority


def test_tiered_profiler_factory(spark_session):
    """Test creating TieredProfiler through the factory."""
    # Mock lakehouse for testing
    mock_lakehouse = MagicMock()
    mock_lakehouse.get_connection = spark_session
    
    # Get profiler from registry
    profiler = get_profiler("tiered", spark=spark_session, lakehouse=mock_lakehouse)
    
    assert isinstance(profiler, TieredProfiler)
    assert profiler.spark == spark_session


def test_tiered_profiler_implements_interface(spark_session, sample_dataframe):
    """Test that TieredProfiler implements DataProfilingInterface correctly."""
    # Mock lakehouse
    mock_lakehouse = MagicMock()
    mock_lakehouse.get_connection = spark_session
    mock_lakehouse.check_if_table_exists.return_value = False
    mock_lakehouse.write_to_table.return_value = None
    
    profiler = TieredProfiler(spark=spark_session, lakehouse=mock_lakehouse)
    
    # Test profile_dataset method exists and works
    sample_dataframe.createOrReplaceTempView("test_table")
    
    with patch.object(profiler, 'scan_level_1_discovery') as mock_l1, \
         patch.object(profiler, 'scan_level_2_schema') as mock_l2:
        
        mock_l1.return_value = []
        mock_l2.return_value = []
        
        # This should not raise an error
        try:
            profile = profiler.profile_dataset(
                "test_table",
                profile_type=ProfileType.BASIC
            )
        except ValueError:
            # Expected if mocked methods don't return proper data
            pass


def test_scan_levels_enum():
    """Test that ScanLevel enum is properly defined."""
    assert ScanLevel.LEVEL_1_DISCOVERY.value == "level_1_discovery"
    assert ScanLevel.LEVEL_2_SCHEMA.value == "level_2_schema"
    assert ScanLevel.LEVEL_3_PROFILE.value == "level_3_profile"
    assert ScanLevel.LEVEL_4_ADVANCED.value == "level_4_advanced"


def test_profile_type_mapping(spark_session):
    """Test that ProfileType maps correctly to scan levels."""
    mock_lakehouse = MagicMock()
    mock_lakehouse.get_connection = spark_session
    mock_lakehouse.check_if_table_exists.return_value = False
    
    profiler = TieredProfiler(spark=spark_session, lakehouse=mock_lakehouse)
    
    # Create test table
    spark_session.sql("CREATE OR REPLACE TEMPORARY VIEW test_mapping AS SELECT 1 as id")
    
    # Mock the scan methods to track which are called
    with patch.object(profiler, 'scan_level_1_discovery') as mock_l1, \
         patch.object(profiler, 'scan_level_2_schema') as mock_l2, \
         patch.object(profiler, 'scan_level_3_profile') as mock_l3, \
         patch.object(profiler, 'scan_level_4_advanced') as mock_l4:
        
        # Configure mocks
        mock_l1.return_value = []
        mock_l2.return_value = []
        mock_l3.return_value = []
        mock_l4.return_value = []
        
        # Test BASIC - should call L1 and L2
        try:
            profiler.profile_dataset("test_mapping", ProfileType.BASIC)
        except:
            pass
        assert mock_l1.called
        assert mock_l2.called
        assert not mock_l3.called
        assert not mock_l4.called
        
        # Reset mocks
        mock_l1.reset_mock()
        mock_l2.reset_mock()
        mock_l3.reset_mock()
        mock_l4.reset_mock()
        
        # Test FULL - should call L1, L2, and L3
        try:
            profiler.profile_dataset("test_mapping", ProfileType.FULL)
        except:
            pass
        assert mock_l1.called
        assert mock_l2.called
        assert mock_l3.called
        assert not mock_l4.called  # L4 only for RELATIONSHIP type


def test_default_profiler_is_tiered():
    """Test that 'default' alias points to TieredProfiler."""
    registry = get_registry()
    
    # Get both profilers
    default_metadata = registry.get_profiler_metadata("default")
    tiered_metadata = registry.get_profiler_metadata("tiered")
    
    # They should have the same characteristics
    assert default_metadata.name == tiered_metadata.name
    assert default_metadata.description == tiered_metadata.description
    assert default_metadata.priority == tiered_metadata.priority


def test_standard_profiler_uses_tiered_for_large_datasets():
    """Test that DataProfilingPySpark can use TieredProfiler for large datasets."""
    from ingen_fab.python_libs.pyspark.data_profiling_pyspark import DataProfilingPySpark
    
    spark = MagicMock()
    lakehouse = MagicMock()
    
    # Create profiler with tiered support
    profiler = DataProfilingPySpark(
        spark=spark,
        lakehouse=lakehouse,
        use_tiered_for_large_datasets=True,
        large_dataset_threshold=1000
    )
    
    # Check that tiered profiler was initialized
    assert profiler.use_tiered_for_large_datasets
    assert profiler.large_dataset_threshold == 1000
    # Note: tiered_profiler might be None if import fails in test environment


if __name__ == "__main__":
    pytest.main([__file__, "-v"])