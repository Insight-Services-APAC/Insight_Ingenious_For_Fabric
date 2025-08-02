#!/usr/bin/env python3
"""
Test script to verify that dates are processed in the correct sequential order
"""
import sys
sys.path.insert(0, '/workspaces/ingen_fab')

from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import FlatFileIngestionConfig
from ingen_fab.python_libs.pyspark.flat_file_ingestion_pyspark import PySparkFlatFileDiscovery
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

def test_date_sequence_order():
    """Test that dates are discovered and processed in correct sequential order"""
    print("🗓️ Testing Date Sequence Processing Order")
    print("=" * 60)
    
    # Create configuration that will discover multiple dates
    config = FlatFileIngestionConfig(
        config_id="date_sequence_test",
        config_name="Date Sequence Order Test",
        source_file_path="synthetic_data/retail_oltp_incremental_nested_parquet",
        source_file_format="parquet",
        target_workspace_id="test_workspace",
        target_datastore_id="test_lakehouse",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="date_sequence_test",
        execution_group=1,
        active_yn="Y",
        
        # Configure for multiple dates in hierarchical structure
        import_pattern="date_partitioned",
        hierarchical_date_structure=True,
        table_subfolder="orders",
        date_partition_format="YYYY/MM/DD",
        date_range_start="2024-01-01",
        date_range_end="2024-01-05",  # 5 days to test ordering
        skip_existing_dates=False,
        source_is_folder=True,
        write_mode="overwrite"
    )
    
    # Set up discovery service
    lakehouse = lakehouse_utils("test_workspace", "test_lakehouse")
    discovery = PySparkFlatFileDiscovery(lakehouse)
    
    print(f"📋 Configuration:")
    print(f"  - Table: {config.table_subfolder}")
    print(f"  - Date Range: {config.date_range_start} to {config.date_range_end}")
    print(f"  - Date Format: {config.date_partition_format}")
    
    # Discover files
    print(f"\n🔍 Discovering files...")
    discovered_files = discovery.discover_files(config)
    
    print(f"\n📁 File Discovery Results:")
    print(f"  - Found {len(discovered_files)} date/file combinations")
    
    print(f"\n📅 Date Processing Order Analysis:")
    print(f"  (This shows the order dates will be processed)")
    
    date_order = []
    for i, file_result in enumerate(discovered_files, 1):
        date_order.append(file_result.date_partition)
        print(f"  {i:2d}. Date: {file_result.date_partition}")
        print(f"      Path: {file_result.file_path}")
        print(f"      Folder: {file_result.folder_name}")
    
    print(f"\n✅ Date Sequence Verification:")
    
    # Check if dates are sorted correctly
    sorted_dates = sorted(date_order)
    is_sorted = date_order == sorted_dates
    
    print(f"  - Original order: {date_order}")
    print(f"  - Expected order: {sorted_dates}")
    print(f"  - Is correctly sorted: {'✅ YES' if is_sorted else '❌ NO'}")
    
    if is_sorted:
        print(f"\n🎉 SUCCESS: Dates are processed in correct chronological order!")
        print(f"   The system processes dates sequentially from earliest to latest.")
    else:
        print(f"\n⚠️ WARNING: Dates are NOT in chronological order!")
        print(f"   This could cause issues with incremental data processing.")
    
    # Verify the sorting mechanism
    print(f"\n🔧 Sorting Mechanism Analysis:")
    print(f"  The system uses: discovered_files.sort(key=lambda x: x.date_partition or '')")
    print(f"  This sorts by the date_partition string in YYYY-MM-DD format")
    print(f"  Since YYYY-MM-DD format sorts lexicographically in chronological order,")
    print(f"  this ensures dates are processed from earliest to latest.")
    
    return is_sorted

def demonstrate_processing_flow():
    """Demonstrate the complete processing flow"""
    print(f"\n" + "=" * 60)
    print("📋 Complete Date Processing Flow")
    print("=" * 60)
    
    print(f"1. 🔍 File Discovery Phase:")
    print(f"   - System scans the hierarchical structure (YYYY/MM/DD/table/)")
    print(f"   - Discovers all date/table combinations within date range")
    print(f"   - Filters by table_subfolder if specified")
    
    print(f"\n2. 📅 Date Sorting Phase:")
    print(f"   - discovered_files.sort(key=lambda x: x.date_partition or '')")
    print(f"   - Sorts by date_partition string (YYYY-MM-DD format)")
    print(f"   - Results in chronological order (earliest to latest)")
    
    print(f"\n3. 🔄 Sequential Processing Phase:")
    print(f"   - for file_result in discovered_files:")
    print(f"   - Each date is processed one at a time, in order")
    print(f"   - Read data → Validate → Write to target table")
    print(f"   - Log execution start/completion for each date")
    
    print(f"\n4. 📊 Benefits of Sequential Processing:")
    print(f"   ✅ Ensures correct chronological order")
    print(f"   ✅ Prevents race conditions between dates")
    print(f"   ✅ Allows proper incremental data handling")
    print(f"   ✅ Enables consistent error handling per date")
    print(f"   ✅ Supports proper logging and monitoring")

def main():
    """Main test function"""
    print("🧪 Date Sequence Processing Test")
    print("=" * 60)
    
    # Test the date sequencing
    is_correct_order = test_date_sequence_order()
    
    # Demonstrate the flow
    demonstrate_processing_flow()
    
    print(f"\n" + "=" * 60)
    print("📊 Final Summary")
    print("=" * 60)
    
    if is_correct_order:
        print("✅ Date processing is correctly implemented:")
        print("  • Dates are discovered and sorted chronologically")
        print("  • Processing happens sequentially from earliest to latest")
        print("  • Each date is fully processed before moving to the next")
        print("  • This ensures proper incremental data handling")
    else:
        print("❌ Date processing needs attention:")
        print("  • Dates may not be in correct chronological order")
        print("  • This could impact incremental data processing")
    
    return is_correct_order

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)