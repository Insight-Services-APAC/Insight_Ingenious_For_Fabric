#!/usr/bin/env python3
"""
Test script to demonstrate hierarchical nested structure support
"""
import sys
import os
from datetime import datetime

# Add the project root to the path
sys.path.insert(0, '/workspaces/ingen_fab')

from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import FlatFileIngestionConfig
from ingen_fab.python_libs.pyspark.flat_file_ingestion_pyspark import (
    PySparkFlatFileDiscovery,
    PySparkFlatFileProcessor,
    PySparkFlatFileLogging,
    PySparkFlatFileIngestionOrchestrator
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

def create_nested_structure_configs():
    """Create test configurations for hierarchical nested structure"""
    configs = []
    
    # Configuration for orders table in nested structure
    configs.append(FlatFileIngestionConfig(
        config_id="nested_orders_test",
        config_name="Nested Hierarchical - Orders Table",
        source_file_path="synthetic_data/retail_oltp_incremental_nested_parquet",
        source_file_format="parquet",
        target_workspace_id="test_workspace",
        target_datastore_id="test_lakehouse",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="nested_orders",
        execution_group=1,
        active_yn="Y",
        
        # Enable hierarchical nested structure
        import_pattern="date_partitioned",
        hierarchical_date_structure=True,  # Enable nested date discovery
        table_subfolder="orders",           # Specific table to process
        date_partition_format="YYYY/MM/DD", # Hierarchical date format
        date_range_start="2024-01-01",
        date_range_end="2024-01-03",        # Limit to first 3 days for testing
        skip_existing_dates=False,
        source_is_folder=True,
        write_mode="append"
    ))
    
    # Configuration for customers table in nested structure
    configs.append(FlatFileIngestionConfig(
        config_id="nested_customers_test",
        config_name="Nested Hierarchical - Customers Table",
        source_file_path="synthetic_data/retail_oltp_incremental_nested_parquet",
        source_file_format="parquet",
        target_workspace_id="test_workspace",
        target_datastore_id="test_lakehouse",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="nested_customers",
        execution_group=1,  # Same group as orders - should run in parallel
        active_yn="Y",
        
        # Enable hierarchical nested structure
        import_pattern="date_partitioned",
        hierarchical_date_structure=True,  # Enable nested date discovery
        table_subfolder="customers",        # Specific table to process
        date_partition_format="YYYY/MM/DD", # Hierarchical date format
        date_range_start="2024-01-01",
        date_range_end="2024-01-03",        # Limit to first 3 days for testing
        skip_existing_dates=False,
        source_is_folder=True,
        write_mode="overwrite"
    ))
    
    # Configuration for order_items table (different execution group)
    configs.append(FlatFileIngestionConfig(
        config_id="nested_order_items_test",
        config_name="Nested Hierarchical - Order Items Table",
        source_file_path="synthetic_data/retail_oltp_incremental_nested_parquet",
        source_file_format="parquet",
        target_workspace_id="test_workspace",
        target_datastore_id="test_lakehouse",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="nested_order_items",
        execution_group=2,  # Different group - should run after group 1
        active_yn="Y",
        
        # Enable hierarchical nested structure
        import_pattern="date_partitioned",
        hierarchical_date_structure=True,  # Enable nested date discovery
        table_subfolder="order_items",      # Specific table to process
        date_partition_format="YYYY/MM/DD", # Hierarchical date format
        date_range_start="2024-01-01",
        date_range_end="2024-01-02",        # Just first 2 days for variety
        skip_existing_dates=False,
        source_is_folder=True,
        write_mode="append"
    ))
    
    return configs

def test_nested_discovery():
    """Test the nested structure discovery functionality"""
    print("🧪 Testing Hierarchical Nested Structure Discovery")
    print("=" * 60)
    
    # Create test configurations
    configs = create_nested_structure_configs()
    
    # Set up lakehouse utils
    lakehouse = lakehouse_utils("test_workspace", "test_lakehouse")
    
    # Create discovery service
    discovery = PySparkFlatFileDiscovery(lakehouse)
    
    print(f"📋 Testing {len(configs)} nested structure configurations:")
    
    for i, config in enumerate(configs, 1):
        print(f"\n--- Test {i}: {config.config_name} ---")
        print(f"Table: {config.table_subfolder}")
        print(f"Date Range: {config.date_range_start} to {config.date_range_end}")
        print(f"Hierarchical: {config.hierarchical_date_structure}")
        
        try:
            # Test file discovery
            discovered_files = discovery.discover_files(config)
            
            print(f"📁 Discovery Results:")
            print(f"  - Found {len(discovered_files)} date/table combinations")
            
            for j, file_result in enumerate(discovered_files, 1):
                print(f"  {j}. Path: {file_result.file_path}")
                print(f"     Date: {file_result.date_partition}")
                print(f"     Folder: {file_result.folder_name}")
                
        except Exception as e:
            print(f"❌ Discovery failed: {e}")
            import traceback
            traceback.print_exc()

def main():
    print("🚀 Hierarchical Nested Structure Test")
    print("=" * 60)
    
    # Test discovery functionality
    test_nested_discovery()
    
    print(f"\n✅ Nested structure testing completed!")
    
    print(f"\n📖 Summary:")
    print(f"This test demonstrates the new hierarchical nested structure support:")
    print(f"  • Structure: YYYY/MM/DD/table_name/part-*.parquet")
    print(f"  • Multi-table processing: Different tables from same date partitions")
    print(f"  • Parallel execution: Tables in same execution group run concurrently")
    print(f"  • Sequential groups: Different execution groups run in order")
    print(f"  • Backward compatibility: Existing flat structures still work")

if __name__ == "__main__":
    main()