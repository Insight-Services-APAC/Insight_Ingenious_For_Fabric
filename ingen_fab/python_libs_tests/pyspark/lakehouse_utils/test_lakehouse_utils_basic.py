#!/usr/bin/env python3
"""
Simplified test script for lakehouse_utils class functionality.

This script tests the lakehouse_utils class without requiring Delta Lake dependencies.
It focuses on testing the basic functionality and structure validation.

Usage:
    python test_lakehouse_utils_basic.py
"""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


def setup_test_environment() -> tuple[str, str, Path]:
    """Set up test environment."""
    print("🔧 Setting up test environment...")
    
    temp_dir = Path(tempfile.mkdtemp(prefix="lakehouse_test_"))
    print(f"📁 Created temporary directory: {temp_dir}")
    
    workspace_id = "test-workspace-12345"
    lakehouse_id = "test-lakehouse-67890"
    
    return workspace_id, lakehouse_id, temp_dir


def test_initialization():
    """Test basic initialization without Delta dependencies."""
    print("\n🧪 Testing lakehouse_utils initialization...")
    
    workspace_id, lakehouse_id, temp_dir = setup_test_environment()
    
    try:
        # Test with basic Spark session (no Delta configuration)
        utils = lakehouse_utils(workspace_id, lakehouse_id)
        
        # Verify basic attributes
        assert utils.target_workspace_id == workspace_id
        assert utils.target_lakehouse_id == lakehouse_id
        assert utils.spark is not None
        
        print("✅ lakehouse_utils initialized successfully")
        print(f"   - Workspace ID: {utils.target_workspace_id}")
        print(f"   - Lakehouse ID: {utils.target_lakehouse_id}")
        print(f"   - Spark session active: {utils.spark.version}")
        
        return utils, temp_dir
        
    except Exception as e:
        print(f"❌ Initialization failed: {e}")
        shutil.rmtree(temp_dir)
        raise


def test_uri_generation(utils: lakehouse_utils):
    """Test URI generation."""
    print("\n🧪 Testing URI generation...")
    
    try:
        uri = utils.lakehouse_tables_uri()
        expected = f"abfss://{utils.target_workspace_id}@onelake.dfs.fabric.microsoft.com/{utils.target_lakehouse_id}/Tables/"
        
        assert uri == expected
        print("✅ URI generated correctly:")
        print(f"   {uri}")
        
    except Exception as e:
        print(f"❌ URI generation failed: {e}")
        raise


def test_basic_dataframe_operations(utils: lakehouse_utils):
    """Test basic DataFrame operations without Delta."""
    print("\n🧪 Testing basic DataFrame operations...")
    
    try:
        # Create simple test data
        data = [("Alice", 25), ("Bob", 30), ("Carol", 35)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        
        df = utils.spark.createDataFrame(data, schema)
        row_count = df.count()
        
        print(f"✅ Created DataFrame with {row_count} rows")
        
        # Show schema and data
        print("✅ DataFrame schema:")
        df.printSchema()
        
        print("✅ DataFrame content:")
        df.show()
        
        return df
        
    except Exception as e:
        print(f"❌ DataFrame operations failed: {e}")
        raise


def test_table_existence_simulation(utils: lakehouse_utils, temp_dir: Path):
    """Test table existence checking with simulation."""
    print("\n🧪 Testing table existence checking (simulated)...")
    
    try:
        # Test with local file path (should return False due to Delta check)
        test_path = f"file://{temp_dir}/test_table"
        exists = utils.check_if_table_exists(test_path)
        
        # Should return False because it's not a Delta table
        assert not exists
        print("✅ Non-Delta path correctly identified as not existing")
        
        # Test with obviously invalid path
        invalid_path = "invalid://definitely/not/a/path"
        exists_invalid = utils.check_if_table_exists(invalid_path)
        
        assert not exists_invalid
        print("✅ Invalid path handled gracefully")
        
    except Exception as e:
        print(f"❌ Table existence check failed: {e}")
        raise


def test_write_method_structure(utils: lakehouse_utils, df):
    """Test the structure of write method without actually writing Delta."""
    print("\n🧪 Testing write method structure...")
    
    try:
        # Test that the method exists and has proper signature
        write_method = getattr(utils, 'write_to_lakehouse_table')
        assert callable(write_method)
        print("✅ write_to_lakehouse_table method exists and is callable")
        
        # Test URI construction for table path
        table_name = "test_table"
        expected_path = f"{utils.lakehouse_tables_uri()}{table_name}"
        print(f"✅ Table path would be: {expected_path}")
        
        # We can't actually test writing without Delta Lake properly configured
        print("⚠️  Actual write operation skipped (requires Delta Lake)")
        
    except Exception as e:
        print(f"❌ Write method test failed: {e}")
        raise


def test_error_handling(utils: lakehouse_utils):
    """Test error handling scenarios."""
    print("\n🧪 Testing error handling...")
    
    try:
        # Test table existence with None path
        try:
            utils.check_if_table_exists(None)
            print("❌ Should have raised error for None path")
        except Exception:
            print("✅ None path properly rejected")
        
        # Test with empty string
        exists = utils.check_if_table_exists("")
        assert not exists
        print("✅ Empty path handled gracefully")
        
    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        raise


def test_class_attributes(utils: lakehouse_utils):
    """Test class attributes and methods."""
    print("\n🧪 Testing class attributes and methods...")
    
    try:
        # Check all expected methods exist
        expected_methods = [
            'lakehouse_tables_uri',
            'check_if_table_exists', 
            'write_to_lakehouse_table',
            'drop_all_tables'
        ]
        
        for method_name in expected_methods:
            assert hasattr(utils, method_name), f"Missing method: {method_name}"
            assert callable(getattr(utils, method_name)), f"Method not callable: {method_name}"
            print(f"✅ Method {method_name} exists and is callable")
        
        # Check attributes
        assert hasattr(utils, 'target_workspace_id')
        assert hasattr(utils, 'target_lakehouse_id')
        assert hasattr(utils, 'spark')
        
        print("✅ All expected attributes present")
        
    except Exception as e:
        print(f"❌ Class attribute test failed: {e}")
        raise


def display_summary():
    """Display test summary."""
    print("\n" + "="*60)
    print("🎯 BASIC LAKEHOUSE UTILS TEST SUMMARY")
    print("="*60)
    print("✅ Class initialization")
    print("✅ Attribute verification")
    print("✅ Method existence and callability")
    print("✅ URI generation")
    print("✅ Basic DataFrame operations")
    print("✅ Table existence checking (simulated)")
    print("✅ Error handling")
    print("="*60)
    print("🎉 Basic structure tests completed successfully!")
    print("\nNote: Full Delta Lake functionality testing requires")
    print("      proper Delta Lake dependencies and configuration.")


def main():
    """Main test execution."""
    print("🚀 Starting basic lakehouse_utils tests...")
    print("="*60)
    
    temp_dir = None
    
    try:
        # Create basic Spark session (without Delta configuration)
        spark = SparkSession.builder \
            .appName("lakehouse_utils_basic_test") \
            .getOrCreate()
        
        print(f"✅ Spark session active: {spark.version}")
        
        # Test initialization
        utils, temp_dir = test_initialization()
        
        # Test class structure
        test_class_attributes(utils)
        
        # Test URI generation
        test_uri_generation(utils)
        
        # Test basic DataFrame operations
        test_df = test_basic_dataframe_operations(utils)
        
        # Test table existence checking
        test_table_existence_simulation(utils, temp_dir)
        
        # Test write method structure
        test_write_method_structure(utils, test_df)
        
        # Test error handling
        test_error_handling(utils)
        
        # Display summary
        display_summary()
        
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
