# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark",
# META     "display_name": "PySpark (Synapse)"
# META   },
# META   "language_info": {
# META     "name": "python",
# META     "language_group": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************


# Default parameters
# Add default parameters here




# MARKDOWN ********************

# ## 『』Parameters

# PARAMETERS CELL ********************




# Default parameters
config_id = ""
execution_group = None
environment = "development"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 📄 Flat File Ingestion Notebook (Lakehouse)

# CELL ********************



# This notebook processes flat files (CSV, JSON, Parquet, Avro, XML) and loads them into lakehouse tables based on configuration metadata.
# Uses modularized components from python_libs for maintainable and reusable code.




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Load Python Libraries and Initialize Environment

# CELL ********************

import sys

# Check if running in Fabric environment
if "notebookutils" in sys.modules:
    import sys
    
    notebookutils.fs.mount("abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_lakehouse_name}}.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
    mount_path = notebookutils.fs.getMountPath("/config_files")  # type: ignore # noqa: F821
    
    run_mode = "fabric"
    sys.path.insert(0, mount_path)

    
    # PySpark environment - spark session should be available
    
else:
    print("NotebookUtils not available, assumed running in local mode.")
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )
    notebookutils = NotebookUtilsFactory.create_instance()
        
    spark = None
    
    mount_path = None
    run_mode = "local"

import traceback

def load_python_modules_from_path(base_path: str, relative_files: list[str], max_chars: int = 1_000_000_000):
    """
    Executes Python files from a Fabric-mounted file path using notebookutils.fs.head.
    
    Args:
        base_path (str): The root directory where modules are located.
        relative_files (list[str]): List of relative paths to Python files (from base_path).
        max_chars (int): Max characters to read from each file (default: 1,000,000).
    """
    success_files = []
    failed_files = []

    for relative_path in relative_files:
        full_path = f"file:{base_path}/{relative_path}"
        try:
            print(f"🔄 Loading: {full_path}")
            code = notebookutils.fs.head(full_path, max_chars)
            exec(code, globals())  # Use globals() to share context across modules
            success_files.append(relative_path)
        except Exception as e:
            failed_files.append(relative_path)
            print(f"❌ Error loading {relative_path}")
            print(f"   Error type: {type(e).__name__}")
            print(f"   Error message: {str(e)}")
            print(f"   Stack trace:")
            traceback.print_exc()

    print("\n✅ Successfully loaded:")
    for f in success_files:
        print(f" - {f}")

    if failed_files:
        print("\n⚠️ Failed to load:")
        for f in failed_files:
            print(f" - {f}")

def clear_module_cache(prefix: str):
    """Clear module cache for specified prefix"""
    for mod in list(sys.modules):
        if mod.startswith(prefix):
            print("deleting..." + mod)
            del sys.modules[mod]

# Clear the module cache only when running in Fabric environment
# When running locally, module caching conflicts can occur in parallel execution
if run_mode == "fabric":
    # Check if ingen_fab modules are present in cache (indicating they need clearing)
    ingen_fab_modules = [mod for mod in sys.modules.keys() if mod.startswith(('ingen_fab.python_libs', 'ingen_fab'))]
    
    if ingen_fab_modules:
        print(f"Found {len(ingen_fab_modules)} ingen_fab modules to clear from cache")
        clear_module_cache("ingen_fab.python_libs")
        clear_module_cache("ingen_fab")
        print("✓ Module cache cleared for ingen_fab libraries")
    else:
        print("ℹ No ingen_fab modules found in cache - already cleared or first load")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 🔧 Load Configuration and Initialize

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔧 Load Configuration and Initialize Utilities

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import get_configs_as_object, ConfigsObject
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.get_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py"
    ]
    load_python_modules_from_path(mount_path, files_to_load)


# Initialize configuration
configs: ConfigsObject = get_configs_as_object()



# Additional imports for flat file ingestion
import uuid
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any


# Load flat file ingestion components
if run_mode == "local":
    from ingen_fab.python_libs.interfaces.flat_file_ingestion_interface import FlatFileIngestionConfig
    from ingen_fab.python_libs.pyspark.flat_file_ingestion_pyspark import (
        PySparkFlatFileDiscovery,
        PySparkFlatFileProcessor,
        PySparkFlatFileLogging,
        PySparkFlatFileIngestionOrchestrator
    )
else:
    # Additional files for flat file ingestion modular components
    flat_file_ingestion_files = [
        "ingen_fab/python_libs/interfaces/flat_file_ingestion_interface.py",
        "ingen_fab/python_libs/common/flat_file_ingestion_utils.py",
        "ingen_fab/python_libs/pyspark/flat_file_ingestion_pyspark.py"
    ]
    load_python_modules_from_path(mount_path, flat_file_ingestion_files)


execution_id = str(uuid.uuid4())

print(f"Execution ID: {execution_id}")
print(f"Config ID: {config_id}")
print(f"Execution Group: {execution_group}")
print(f"Environment: {environment}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 📋 Load Configuration Data

# CELL ********************




# Initialize config lakehouse utilities
config_lakehouse = lakehouse_utils(
    target_workspace_id=configs.config_workspace_id,
    target_lakehouse_id=configs.config_lakehouse_id,
    spark=spark
)

# Initialize raw data lakehouse utilities for file access
raw_lakehouse = lakehouse_utils(
    target_workspace_id=configs.raw_lh_workspace_id,
    target_lakehouse_id=configs.raw_wh_lakehouse_id,
    spark=spark
)


# Load configuration

config_df = config_lakehouse.read_table("config_flat_file_ingestion").toPandas()





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 🐛 Debug Configuration Override

# CELL ********************



# Debug mode - override configurations with embedded test data
debug_mode = True  # Set to False to use normal database configurations

if debug_mode:
    import pandas as pd
    from datetime import datetime
    
    # Define debug configurations directly in the notebook
    debug_configs = [
        {
            "config_id": "debug_test_001",
            "config_name": "Debug Test - CSV File",
            "source_file_path": "test_data/sample.csv",
            "source_file_format": "csv",
            "target_workspace_id": configs.raw_lh_workspace_id,
            "target_datastore_id": configs.raw_wh_lakehouse_id,
            "target_datastore_type": "lakehouse",
            "target_schema_name": "debug",
            "target_table_name": "debug_test_table",
            "staging_table_name": None,
            "file_delimiter": ",",
            "has_header": True,
            "encoding": "utf-8",
            "date_format": "yyyy-MM-dd",
            "timestamp_format": "yyyy-MM-dd HH:mm:ss",
            "schema_inference": True,
            "custom_schema_json": None,
            "partition_columns": "",
            "sort_columns": "",
            "write_mode": "overwrite",
            "merge_keys": "",
            "data_validation_rules": None,
            "error_handling_strategy": "log",
            "execution_group": 1,
            "active_yn": "Y",
            "created_date": datetime.now().strftime("%Y-%m-%d"),
            "modified_date": None,
            "created_by": "debug_user",
            "modified_by": None,
            "quote_character": '"',
            "escape_character": '"',
            "multiline_values": True,
            "ignore_leading_whitespace": False,
            "ignore_trailing_whitespace": False,
            "null_value": "",
            "empty_value": "",
            "comment_character": None,
            "max_columns": 100,
            "max_chars_per_column": 50000,
            "import_pattern": "single_file",
            "date_partition_format": None,
            "table_relationship_group": None,
            "batch_import_enabled": False,
            "file_discovery_pattern": None,
            "import_sequence_order": 1,
            "date_range_start": None,
            "date_range_end": None,
            "skip_existing_dates": None,
            "source_is_folder": False
        }
    ]
    
    # Override config_df with debug configurations
    config_df = pd.DataFrame(debug_configs)
    print("🐛 DEBUG MODE ACTIVE - Using embedded test configurations")
    print(f"Debug configurations loaded: {len(config_df)} items")
    
    # Display debug configurations
    display(config_df[["config_id", "config_name", "source_file_path", "target_table_name"]])
else:
    print("📋 Using standard database configurations")


# Filter configurations
if config_id:
    config_df = config_df[config_df["config_id"] == config_id]
else:
    # If execution_group is not set or is empty, process all execution groups
    if execution_group and str(execution_group).strip():
        config_df = config_df[
            (config_df["execution_group"] == execution_group) & 
            (config_df["active_yn"] == "Y")
        ]
    else:
        config_df = config_df[config_df["active_yn"] == "Y"]

if config_df.empty:
    raise ValueError(f"No active configurations found for config_id: {config_id}, execution_group: {execution_group}")

print(f"Found {len(config_df)} configurations to process")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 🧪 Create Debug Test Data

# CELL ********************



# Create test data files for debug mode
if debug_mode:
    import os
    from pyspark.sql import Row
    
    # Create test data directory
    test_data_dir = "Files/test_data"
    
    # Create sample CSV data
    sample_data = [
        Row(id=1, name="Test User 1", email="test1@example.com", created_date="2024-01-01"),
        Row(id=2, name="Test User 2", email="test2@example.com", created_date="2024-01-02"),
        Row(id=3, name="Test User 3", email="test3@example.com", created_date="2024-01-03"),
    ]
    
    # Create DataFrame and write to test location
    test_df = spark.createDataFrame(sample_data)
    
    
    # For lakehouse, write to the raw lakehouse Files location
    raw_lakehouse.write_file(
        df=test_df,
        file_path=test_data_dir + "/sample.csv",
        file_format="csv",
        options={"header": True}
    )
    print(f"✅ Created test CSV file at: {test_data_dir}/sample.csv")
    
    # Also create a JSON test file
    json_data = [
        Row(product_id=101, product_name="Widget A", price=19.99, category="Electronics"),
        Row(product_id=102, product_name="Widget B", price=29.99, category="Electronics"),
        Row(product_id=103, product_name="Gadget X", price=39.99, category="Accessories"),
    ]
    json_df = spark.createDataFrame(json_data)
    raw_lakehouse.write_file(
        df=json_df,
        file_path=test_data_dir + "/products.json",
        file_format="json"
    )
    print(f"✅ Created test JSON file at: {test_data_dir}/products.json")
    
    
    # Display sample data
    print("\n📄 Sample test data:")
    test_df.show()
    
    # Update debug configurations to use multiple test files
    if len(debug_configs) == 1:
        # Add a second configuration for JSON file
        debug_configs.append({
            **debug_configs[0],  # Copy all fields from first config
            "config_id": "debug_test_002",
            "config_name": "Debug Test - JSON File",
            "source_file_path": "test_data/products.json",
            "source_file_format": "json",
            "target_table_name": "debug_products_table",
            "file_delimiter": None,
            "has_header": None,
        })
        # Re-create config_df with updated configurations
        config_df = pd.DataFrame(debug_configs)
        print(f"\n🔄 Updated debug configurations: {len(config_df)} items")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 🚀 Initialize Modular Services

# CELL ********************




# Initialize the modular flat file ingestion services for lakehouse
discovery_service = PySparkFlatFileDiscovery(raw_lakehouse)
processor_service = PySparkFlatFileProcessor(spark, raw_lakehouse)
logging_service = PySparkFlatFileLogging(config_lakehouse)

# Initialize the orchestrator with all services
orchestrator = PySparkFlatFileIngestionOrchestrator(
    discovery_service=discovery_service,
    processor_service=processor_service,
    logging_service=logging_service
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 📊 Process Configurations

# CELL ********************



# Convert pandas DataFrame rows to FlatFileIngestionConfig objects
configurations = []
for _, config_row in config_df.iterrows():
    config = FlatFileIngestionConfig.from_dict(config_row.to_dict())
    configurations.append(config)

# Process all configurations using the orchestrator
results = orchestrator.process_configurations(configurations, execution_id)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 📈 Execution Summary

# CELL ********************



# Print comprehensive summary
print("\n=== EXECUTION SUMMARY ===")
print(f"Execution ID: {results['execution_id']}")
print(f"Total configurations processed: {results['total_configurations']}")
print(f"Successful: {results['successful']}")
print(f"Failed: {results['failed']}")
print(f"No data found: {results['no_data_found']}")

# Display successful configurations
successful_configs = [r for r in results['configurations'] if r['status'] == 'completed']
if successful_configs:
    print("\nSuccessful configurations:")
    for result in successful_configs:
        metrics = result['metrics']
        duration_sec = metrics.total_duration_ms / 1000 if metrics.total_duration_ms > 0 else 0
        print(f"  - {result['config_name']}: {metrics.records_processed} records in {duration_sec:.2f}s")
        print(f"    Performance: {metrics.avg_rows_per_second:.0f} rows/sec")
        print(f"    Read time: {metrics.read_duration_ms}ms, Write time: {metrics.write_duration_ms}ms")
        print(f"    Row count reconciliation: {metrics.row_count_reconciliation_status}")

# Display failed configurations
failed_configs = [r for r in results['configurations'] if r['status'] == 'failed']
if failed_configs:
    print("\nFailed configurations:")
    for result in failed_configs:
        print(f"  - {result['config_name']}: {'; '.join(result['errors'])}")

# Display configurations with no data
no_data_configs = [r for r in results['configurations'] if r['status'] in ['no_data_found', 'no_data_processed']]
if no_data_configs:
    print("\nConfigurations with no data found:")
    for result in no_data_configs:
        metrics = result['metrics']
        print(f"  - {result['config_name']}: No source files discovered")
        print(f"    Read time: {metrics.read_duration_ms}ms")
        print(f"    Row count reconciliation: {metrics.row_count_reconciliation_status}")

print(f"\nExecution completed at: {datetime.now()}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 🔍 Debug Results Verification

# CELL ********************



# Verify debug results if in debug mode
if debug_mode and successful_configs:
    print("🔍 Verifying debug ingestion results...\n")
    
    for result in successful_configs:
        table_name = result['config'].target_table_name
        schema_name = result['config'].target_schema_name
        
        try:
            
            # Read the ingested table
            ingested_df = raw_lakehouse.read_table(f"{schema_name}.{table_name}")
            record_count = ingested_df.count()
            
            print(f"✅ Table {schema_name}.{table_name}:")
            print(f"   - Records: {record_count}")
            print(f"   - Columns: {', '.join(ingested_df.columns)}")
            print(f"   - Sample data:")
            ingested_df.show(5, truncate=False)
            
            
        except Exception as e:
            print(f"❌ Error verifying table {schema_name}.{table_name}: {str(e)}")
    
    print("\n🎯 Debug verification complete!")
    print("💡 To disable debug mode, set 'debug_mode = False' in the Debug Configuration Override cell")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✔️ Exit notebook with result

# CELL ********************


notebookutils.mssparkutils.notebook.exit("success")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

