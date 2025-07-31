






# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_python",
# META     "display_name": "Python (Synapse)"
# META   },
# META   "language_info": {
# META     "name": "python",
# META     "language_group": "synapse_python"
# META   }
# META }


# MARKDOWN ********************

# ## 『』Parameters

# PARAMETERS CELL ********************




# Default parameters
config_id = ""
execution_group = None
environment = "development"




# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📄 Flat File Ingestion Notebook (Warehouse)

# CELL ********************



# This notebook processes flat files (CSV, JSON, Parquet, Avro, XML) and loads them into warehouse tables based on configuration metadata.
# Uses modularized components from python_libs for maintainable and reusable code.




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_python"
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

    
    # Python environment - no spark session needed
    spark = None
    
else:
    print("NotebookUtils not available, assumed running in local mode.")
    from ingen_fab.python_libs.python.notebook_utils_abstraction import (
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
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔧 Load Configuration and Initialize

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_python"
# META }

# MARKDOWN ********************

# ## 🔧 Load Configuration and Initialize Utilities

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import get_configs_as_object, ConfigsObject
    from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils
    
    from ingen_fab.python_libs.python.ddl_utils import ddl_utils
    
    from ingen_fab.python_libs.python.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.get_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/python/lakehouse_utils.py",
        
        "ingen_fab/python_libs/python/ddl_utils.py",
        
        "ingen_fab/python_libs/python/notebook_utils_abstraction.py"
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

# Import modularized flat file ingestion components
from python_libs.interfaces.flat_file_ingestion_interface import FlatFileIngestionConfig


# Import Python/warehouse-specific flat file ingestion components
from python_libs.python.flat_file_ingestion_python import (
    PythonFlatFileDiscovery,
    PythonFlatFileProcessor,
    PythonFlatFileLogging,
    PythonFlatFileIngestionOrchestrator
)

# Import warehouse_utils for warehouse operations
if run_mode == "local":
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
else:
    files_to_load = ["ingen_fab/python_libs/python/warehouse_utils.py"]
    load_python_modules_from_path(mount_path, files_to_load)


execution_id = str(uuid.uuid4())

print(f"Execution ID: {execution_id}")
print(f"Config ID: {config_id}")
print(f"Execution Group: {execution_group}")
print(f"Environment: {environment}")




# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📋 Load Configuration Data

# CELL ********************




# Initialize config warehouse utilities
config_warehouse = warehouse_utils(
    target_workspace_id=configs.config_workspace_id,
    target_warehouse_id=configs.config_warehouse_id
)

# Initialize raw data warehouse utilities for file access
raw_warehouse = warehouse_utils(
    target_workspace_id=configs.raw_workspace_id,
    target_warehouse_id=configs.raw_datastore_id
)


# Load configuration

# Load configuration from warehouse table
config_query = "SELECT * FROM config_flat_file_ingestion"
config_df = config_warehouse.execute_query_to_dataframe(config_query)


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
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🚀 Initialize Modular Services

# CELL ********************




# Initialize the modular flat file ingestion services for warehouse
discovery_service = PythonFlatFileDiscovery(raw_warehouse)
processor_service = PythonFlatFileProcessor(raw_warehouse)
logging_service = PythonFlatFileLogging(config_warehouse)

# Initialize the orchestrator with all services
orchestrator = PythonFlatFileIngestionOrchestrator(
    discovery_service=discovery_service,
    processor_service=processor_service,
    logging_service=logging_service
)





# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Process Configurations

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
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📈 Execution Summary

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

# ## ✔️ Exit notebook with result

# CELL ********************


notebookutils.mssparkutils.notebook.exit("success")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_python"
# META }

