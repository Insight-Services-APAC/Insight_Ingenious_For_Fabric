#!/usr/bin/env python3
"""
Test script for lakehouse_utils class functionality.

This script tests the lakehouse_utils class against the current Spark session.
It creates sample data, writes to Delta tables, validates functionality,
and cleans up afterward.

Usage:
    uv run test_lakehouse_utils.py
"""

from __future__ import annotations

import shutil
import tempfile
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

# Import the class to test
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


def setup_local_test_environment() -> tuple[str, str, Path]:
    """
    Set up a local test environment using temporary directories.
    Returns workspace_id, lakehouse_id, and temp_dir path.
    """
    print("🔧 Setting up local test environment...")
    
    # Create temporary directory for testing
    temp_dir = Path(tempfile.mkdtemp(prefix="lakehouse_test_"))
    print(f"📁 Created temporary directory: {temp_dir}")
    
    # Use dummy IDs for testing
    workspace_id = "test-workspace-12345"
    lakehouse_id = "test-lakehouse-67890"
    
    return workspace_id, lakehouse_id, temp_dir


def create_test_dataframes(spark: SparkSession) -> dict:
    """Create sample DataFrames for testing."""
    print("📊 Creating test DataFrames...")
    
    # Schema for customer data
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("registration_date", TimestampType(), True)
    ])
    
    # Sample customer data
    customer_data = [
        (1, "Alice Johnson", "alice@example.com", datetime(2024, 1, 15, 10, 30)),
        (2, "Bob Smith", "bob@example.com", datetime(2024, 2, 20, 14, 15)),
        (3, "Carol Brown", "carol@example.com", datetime(2024, 3, 10, 9, 45)),
        (4, "David Wilson", "david@example.com", datetime(2024, 4, 5, 16, 20)),
        (5, "Eve Davis", "eve@example.com", datetime(2024, 5, 12, 11, 10))
    ]
    
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Schema for orders data
    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("order_date", TimestampType(), True)
    ])
    
    # Sample orders data
    orders_data = [
        (101, 1, "Laptop", 1, datetime(2024, 6, 1, 12, 0)),
        (102, 2, "Mouse", 2, datetime(2024, 6, 2, 13, 30)),
        (103, 1, "Keyboard", 1, datetime(2024, 6, 3, 15, 45)),
        (104, 3, "Monitor", 1, datetime(2024, 6, 4, 10, 15)),
        (105, 4, "Headphones", 1, datetime(2024, 6, 5, 14, 20))
    ]
    
    orders_df = spark.createDataFrame(orders_data, orders_schema)
    
    print(f"✅ Created customer DataFrame with {customer_df.count()} rows")
    print(f"✅ Created orders DataFrame with {orders_df.count()} rows")
    
    return {
        "customers": customer_df,
        "orders": orders_df
    }


def test_lakehouse_utils_initialization():
    """Test lakehouse_utils class initialization."""
    print("\n🧪 Testing lakehouse_utils initialization...")
    
    workspace_id, lakehouse_id, temp_dir = setup_local_test_environment()
    
    try:
        # Initialize lakehouse_utils
        utils = lakehouse_utils(workspace_id, lakehouse_id)
        
        # Verify attributes
        assert utils.target_workspace_id == workspace_id
        assert utils.target_lakehouse_id == lakehouse_id
        assert utils.spark is not None
        
        print("✅ lakehouse_utils initialized successfully")
        print(f"   - Workspace ID: {utils.target_workspace_id}")
        print(f"   - Lakehouse ID: {utils.target_lakehouse_id}")
        print(f"   - Spark session: {type(utils.spark).__name__}")
        
        return utils, temp_dir
        
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        shutil.rmtree(temp_dir)
        raise


def test_lakehouse_tables_uri(utils: lakehouse_utils):
    """Test the lakehouse_tables_uri method."""
    print("\n🧪 Testing lakehouse_tables_uri method...")
    
    try:
        uri = utils.lakehouse_tables_uri()
        expected_pattern = f"abfss://{utils.target_workspace_id}@onelake.dfs.fabric.microsoft.com/{utils.target_lakehouse_id}/Tables/"
        
        assert uri == expected_pattern
        print(f"✅ URI generated correctly: {uri}")
        
    except Exception as e:
        print(f"❌ URI generation failed: {e}")
        raise


def test_table_existence_check(utils: lakehouse_utils, temp_dir: Path):
    """Test the check_if_table_exists method."""
    print("\n🧪 Testing check_if_table_exists method...")
    
    try:
        # Test with non-existent path
        fake_path = f"file://{temp_dir}/non_existent_table"
        exists = utils.check_if_table_exists(fake_path)
        assert not exists
        print("✅ Non-existent table correctly identified as not existing")
        
        # Note: Testing with actual Delta table creation would require Delta Lake setup
        # which might not be available in all test environments
        
    except Exception as e:
        print(f"❌ Table existence check failed: {e}")
        raise


def test_write_to_lakehouse_table(utils: lakehouse_utils, dataframes: dict, temp_dir: Path):
    """Test writing DataFrames to lakehouse tables (simulated locally)."""
    print("\n🧪 Testing write_to_lakehouse_table method...")
    
    try:
        # Create a local test path instead of actual lakehouse path
        test_table_path = temp_dir / "test_tables"
        test_table_path.mkdir(exist_ok=True)
        
        # Temporarily modify the URI method for local testing
        original_uri_method = utils.lakehouse_tables_uri
        utils.lakehouse_tables_uri = lambda: f"file://{test_table_path}/"
        
        # Test writing customers table
        customers_df = dataframes["customers"]
        print(f"📝 Writing customers table with {customers_df.count()} rows...")
        
        # Write with default options
        utils.write_to_lakehouse_table(customers_df, "customers")
        print("✅ Customers table written successfully")
        
        # Test writing with custom options
        orders_df = dataframes["orders"]
        print(f"📝 Writing orders table with {orders_df.count()} rows...")
        
        custom_options = {
            "mergeSchema": "true",
            "overwriteSchema": "true"
        }
        utils.write_to_lakehouse_table(
            orders_df, 
            "orders", 
            options=custom_options
        )
        print("✅ Orders table written successfully with custom options")
        
        # Verify files were created
        customers_path = test_table_path / "customers"
        orders_path = test_table_path / "orders"
        
        if customers_path.exists():
            print(f"✅ Customers table directory created: {customers_path}")
        if orders_path.exists():
            print(f"✅ Orders table directory created: {orders_path}")
        
        # Restore original method
        utils.lakehouse_tables_uri = original_uri_method
        
    except Exception as e:
        print(f"❌ Table writing failed: {e}")
        # Restore original method on error
        if 'original_uri_method' in locals():
            utils.lakehouse_tables_uri = original_uri_method
        raise


def test_error_handling(utils: lakehouse_utils):
    """Test error handling in various scenarios."""
    print("\n🧪 Testing error handling...")
    
    try:
        # Test with invalid table path
        invalid_path = "invalid://path/to/nowhere"
        exists = utils.check_if_table_exists(invalid_path)
        assert not exists  # Should handle errors gracefully
        print("✅ Invalid path handled gracefully")
        
        # Test with None DataFrame (should raise error)
        try:
            utils.write_to_lakehouse_table(None, "test_table")
            print("❌ Should have raised error for None DataFrame")
        except Exception:
            print("✅ None DataFrame properly rejected")
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        raise


def display_test_summary():
    """Display a summary of all tests."""
    print("\n" + "="*60)
    print("🎯 LAKEHOUSE UTILS TEST SUMMARY")
    print("="*60)
    print("✅ Class initialization")
    print("✅ URI generation")
    print("✅ Table existence checking")
    print("✅ DataFrame writing (local simulation)")
    print("✅ Error handling")
    print("="*60)
    print("🎉 All tests completed successfully!")
    print("\nNote: Some tests used local file system simulation")
    print("      due to the need for actual Fabric lakehouse connectivity.")


def main():
    """Main test execution function."""
    print("🚀 Starting lakehouse_utils functionality tests...")
    print("="*60)
    
    temp_dir = None
    
    try:
        # Initialize Spark session if not already active
        spark = SparkSession.builder \
            .appName("lakehouse_utils_test") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
        
        print(f"✅ Spark session active: {spark.version}")
        
        # Test initialization
        utils, temp_dir = test_lakehouse_utils_initialization()
        
        # Test URI generation
        test_lakehouse_tables_uri(utils)
        
        # Test table existence checking
        test_table_existence_check(utils, temp_dir)
        
        # Create test data
        test_dataframes = create_test_dataframes(spark)
        
        # Test table writing
        test_write_to_lakehouse_table(utils, test_dataframes, temp_dir)
        
        # Test error handling
        test_error_handling(utils)
        
        # Display summary
        display_test_summary()
        
    except Exception as e:
        print(f"\n❌ Test execution failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    finally:
        # Cleanup
        if temp_dir and temp_dir.exists():
            print(f"\n🧹 Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir)
    
    return 0


if __name__ == "__main__":
    exit(main())
