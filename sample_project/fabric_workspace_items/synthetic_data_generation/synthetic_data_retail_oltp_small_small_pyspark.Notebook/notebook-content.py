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
    
    notebookutils.fs.mount("abfss://metcash_demo@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
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

# ## 🗂️ Load Custom Python Libraries

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import *
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    from ingen_fab.python_libs.pyspark.synthetic_data_utils import PySparkSyntheticDataGenerator, PySparkDatasetBuilder
    notebookutils = NotebookUtilsFactory.create_instance() 
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        "ingen_fab/python_libs/pyspark/ddl_utils.py",
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py",
        "ingen_fab/python_libs/pyspark/synthetic_data_utils.py"
    ]

    load_python_modules_from_path(mount_path, files_to_load)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ⚙️ Configuration Settings

# CELL ********************


# Dataset Configuration
dataset_config = {"chunk_size": 1000000, "dataset_id": "retail_oltp_small", "dataset_name": "Retail OLTP - Small", "dataset_type": "transactional", "description": "Small retail transactional system with customers, orders, products", "domain": "retail", "relationships": "normalized", "schema_pattern": "oltp", "seed_value": None, "tables": ["customers", "products", "orders", "order_items"], "target_rows": 1000000}
generation_mode = "pyspark"
target_rows = 1000000
chunk_size = 1000000
seed_value = None
output_mode = "parquet"  # "table" or "parquet"

# variableLibraryInjectionStart: var_lib

# All variables as a dictionary
configs_dict = {'fabric_environment': 'development', 'fabric_deployment_workspace_id': '544530ea-a8c9-4464-8878-f666d2a8f418', 'config_workspace_name': 'metcash_demo', 'config_workspace_id': '544530ea-a8c9-4464-8878-f666d2a8f418', 'config_wh_workspace_id': '544530ea-a8c9-4464-8878-f666d2a8f418', 'config_lakehouse_name': 'config', 'config_lakehouse_id': '514ebe8f-2bf9-4a31-88f7-13d84706431c', 'config_wh_warehouse_name': 'config_wh', 'config_wh_warehouse_id': '51226772-4e8f-4034-9cd2-1afd020d2773', 'sample_lakehouse_name': 'sample', 'sample_lakehouse_id': 'REPLACE_WITH_SAMPLE_LAKEHOUSE_GUID', 'sample_wh_workspace_id': 'REPLACE_WITH_SAMPLE_WH_WORKSPACE_GUID', 'sample_wh_warehouse_name': 'sample_wh', 'sample_wh_warehouse_id': 'REPLACE_WITH_SAMPLE_WAREHOUSE_GUID', 'raw_workspace_id': '544530ea-a8c9-4464-8878-f666d2a8f418', 'raw_datastore_id': 'REPLACE_WITH_RAW_DATASTORE_GUID', 'edw_workspace_id': '544530ea-a8c9-4464-8878-f666d2a8f418', 'edw_lakehouse_name': 'edw', 'edw_lakehouse_id': 'REPLACE_WITH_EDW_LAKEHOUSE_GUID', 'edw_warehouse_name': 'edw', 'edw_warehouse_id': 'REPLACE_WITH_EDW_WAREHOUSE_GUID'}
# All variables as an object
from dataclasses import dataclass
@dataclass
class ConfigsObject:
    fabric_environment: str 
    fabric_deployment_workspace_id: str 
    config_workspace_name: str 
    config_workspace_id: str 
    config_wh_workspace_id: str 
    config_lakehouse_name: str 
    config_lakehouse_id: str 
    config_wh_warehouse_name: str 
    config_wh_warehouse_id: str 
    sample_lakehouse_name: str 
    sample_lakehouse_id: str 
    sample_wh_workspace_id: str 
    sample_wh_warehouse_name: str 
    sample_wh_warehouse_id: str 
    raw_workspace_id: str 
    raw_datastore_id: str 
    edw_workspace_id: str 
    edw_lakehouse_name: str 
    edw_lakehouse_id: str 
    edw_warehouse_name: str 
    edw_warehouse_id: str 
configs_object: ConfigsObject = ConfigsObject(**configs_dict)
# variableLibraryInjectionEnd: var_lib



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🆕 Initialize Components

# CELL ********************


# Get configurations
configs: ConfigsObject = get_configs_as_object()

# Initialize lakehouse utils first (this will create/get the Spark session)
target_lakehouse_id = get_config_value("config_lakehouse_id")
target_workspace_id = get_config_value("config_workspace_id")

lh_utils = lakehouse_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark
)

# Initialize synthetic data generator using Spark session from lakehouse_utils
generator = PySparkSyntheticDataGenerator(
    lakehouse_utils_instance=lh_utils,
    seed=seed_value
)

dataset_builder = PySparkDatasetBuilder(generator)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🎲 Generate Synthetic Data

# CELL ********************


print(f"🎲 Generating synthetic data for dataset: {dataset_config['dataset_id']}")
print(f"📊 Target rows: {target_rows:,}")
print(f"🔧 Generation mode: {generation_mode}")
print(f"🌱 Seed value: {seed_value}")


# Generate OLTP (Transactional) Dataset
print("📋 Generating OLTP dataset...")

# Calculate appropriate table sizes based on target rows
if target_rows <= 100000:
    # Small dataset
    customers_count = max(1000, target_rows // 50)
    products_count = max(100, target_rows // 100)
    orders_count = target_rows // 5
else:
    # Large dataset
    customers_count = max(10000, target_rows // 500)
    products_count = max(1000, target_rows // 1000)
    orders_count = target_rows // 10

print(f"👥 Customers: {customers_count:,}")
print(f"📦 Products: {products_count:,}")
print(f"🛒 Orders: {orders_count:,}")

# Generate tables
customers_df = generator.generate_customers_table(customers_count)
products_df = generator.generate_products_table(products_count)
orders_df = generator.generate_orders_table(orders_count, customers_count)

# Save data using lakehouse_utils
if output_mode == "parquet":
    print("💾 Saving to Parquet files...")
    # Create dataset directory structure
    dataset_dir = f"synthetic_data/{dataset_config['dataset_id']}"
    
    # Write to parquet files
    lh_utils.write_file(customers_df, f"{dataset_dir}/customers", "parquet", {"mode": "overwrite"})
    lh_utils.write_file(products_df, f"{dataset_dir}/products", "parquet", {"mode": "overwrite"})
    lh_utils.write_file(orders_df, f"{dataset_dir}/orders", "parquet", {"mode": "overwrite"})
    
    print(f"📁 Data saved to Files/{dataset_dir}/")
else:
    print("💾 Saving to Delta tables...")
    lh_utils.write_to_table(customers_df, "retail_oltp_small_customers", mode="overwrite")
    lh_utils.write_to_table(products_df, "retail_oltp_small_products", mode="overwrite")
    lh_utils.write_to_table(orders_df, "retail_oltp_small_orders", mode="overwrite")

print("✅ OLTP dataset generation completed!")





# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Data Quality Validation

# CELL ********************


print("🔍 Performing data quality validation...")

total_generated_rows = 0

if output_mode == "parquet":
    # Validate parquet files
    dataset_dir = f"synthetic_data/{dataset_config['dataset_id']}"
    print(f"📋 Checking parquet files in: Files/{dataset_dir}/")
    
    # List files in the dataset directory
    files = lh_utils.list_files(dataset_dir, pattern="*.parquet", recursive=True)
    
    # Get unique table names from file paths
    table_names = set()
    for file_path in files:
        # Extract table name from path (handle partitioned and non-partitioned)
        parts = file_path.split('/')
        if 'synthetic_data' in parts:
            idx = parts.index('synthetic_data')
            if idx + 2 < len(parts):
                table_name = parts[idx + 2]
                if not table_name.endswith('.parquet'):
                    table_names.add(table_name)
    
    for table_name in sorted(table_names):
        # Read the parquet file/directory
        df = lh_utils.read_file(f"{dataset_dir}/{table_name}", "parquet")
        row_count = df.count()
        total_generated_rows += row_count
        
        print(f"📊 {table_name}: {row_count:,} rows")
        
        # Show sample data
        print(f"📝 Sample data from {table_name}:")
        df.show(5, truncate=False)
else:
    # Get list of generated tables
    generated_tables = []
    for table in lh_utils.list_tables():
        if table.startswith("retail_oltp_small"):
            generated_tables.append(table)

    print(f"📋 Generated tables: {generated_tables}")

    # Validate row counts and basic statistics
    for table_name in generated_tables:
        df = lh_utils.read_table(table_name)
        row_count = df.count()
        total_generated_rows += row_count
        
        print(f"📊 {table_name}: {row_count:,} rows")
        
        # Show sample data
        print(f"📝 Sample data from {table_name}:")
        df.show(5, truncate=False)

print(f"🎯 Total rows generated: {total_generated_rows:,}")
print(f"🎯 Target rows: {target_rows:,}")

if total_generated_rows >= target_rows * 0.9:  # Allow 10% tolerance
    print("✅ Row count validation passed!")
else:
    print("⚠️ Row count validation warning: Generated rows below target")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📈 Performance Metrics

# CELL ********************


print("📈 Performance and storage metrics:")

# Calculate storage usage
total_size_bytes = 0

if output_mode == "parquet":
    # Calculate size for parquet files
    dataset_dir = f"synthetic_data/{dataset_config['dataset_id']}"
    files = lh_utils.list_files(dataset_dir, pattern="*.parquet", recursive=True)
    
    for file_path in files:
        try:
            file_info = lh_utils.get_file_info(file_path)
            if 'size' in file_info:
                total_size_bytes += file_info['size']
        except Exception as e:
            print(f"⚠️ Could not get size for {file_path}: {e}")
    
    print(f"💾 Total parquet files size: {total_size_bytes / (1024 * 1024):.2f} MB")
else:
    # Calculate size for Delta tables
    for table_name in generated_tables:
        try:
            # Get table details
            table_details = lh_utils.execute_query(f"DESCRIBE DETAIL {table_name}").collect()[0]
            size_bytes = table_details.sizeInBytes if hasattr(table_details, 'sizeInBytes') else 0
            total_size_bytes += size_bytes
            
            size_mb = size_bytes / (1024 * 1024)
            print(f"💾 {table_name}: {size_mb:.2f} MB")
        except Exception as e:
            print(f"⚠️ Could not get size for {table_name}: {e}")

total_size_mb = total_size_bytes / (1024 * 1024)
total_size_gb = total_size_mb / 1024

print(f"📊 Total storage used: {total_size_mb:.2f} MB ({total_size_gb:.2f} GB)")

# Calculate generation rate
if total_generated_rows > 0:
    print(f"⚡ Generation rate: {total_generated_rows / max(1, total_size_mb):.0f} rows/MB")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ✅ Completion Summary

# CELL ********************


print("🎉 Synthetic Data Generation Completed Successfully!")
print(f"📊 Dataset: {dataset_config['dataset_name']}")
print(f"🏷️ Dataset ID: {dataset_config['dataset_id']}")
print(f"📈 Schema Pattern: {dataset_config['schema_pattern']}")
print(f"💾 Output Mode: {output_mode.upper()}")
print(f"🎯 Rows Generated: {total_generated_rows:,}")
if output_mode == "parquet":
    print(f"📁 Location: Files/synthetic_data/{dataset_config['dataset_id']}/")
else:
    print(f"📋 Tables Created: {len(generated_tables)}")
print(f"💾 Total Storage: {total_size_gb:.2f} GB")

# Return success
notebookutils.exit_notebook("success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

