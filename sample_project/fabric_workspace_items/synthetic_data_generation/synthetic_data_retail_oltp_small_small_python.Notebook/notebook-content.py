# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "language_info": {
# META     "name": "python"
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
# META   "language_group": "python"
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
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🗂️ Load Custom Python Libraries

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import *
    from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.python.ddl_utils import ddl_utils
    from ingen_fab.python_libs.python.notebook_utils_abstraction import NotebookUtilsFactory
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
    from ingen_fab.python_libs.python.synthetic_data_utils import SyntheticDataGenerator, DatasetBuilder
    notebookutils = NotebookUtilsFactory.create_instance() 
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/python/lakehouse_utils.py",
        "ingen_fab/python_libs/python/ddl_utils.py",
        "ingen_fab/python_libs/python/notebook_utils_abstraction.py",
        "ingen_fab/python_libs/python/warehouse_utils.py",
        "ingen_fab/python_libs/python/synthetic_data_utils.py"
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
dataset_config = {"chunk_size": 10000, "dataset_id": "retail_oltp_small", "dataset_name": "Retail OLTP - Small", "dataset_type": "transactional", "description": "Small retail transactional system with customers, orders, products", "domain": "retail", "relationships": "normalized", "scale": "small", "schema_pattern": "oltp", "seed_value": None, "tables": ["customers", "products", "orders", "order_items"], "target_rows": 10000}
generation_mode = "python"
target_rows = 10000
chunk_size = 10000
seed_value = None
output_mode = "table"




# variableLibraryInjectionStart: var_lib
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

# Initialize target datastore configuration
target_datastore_config_prefix = "config"
# Initialize lakehouse utils first (this will create/get the Spark session)
target_lakehouse_id = get_config_value(f"{target_datastore_config_prefix}_lakehouse_id")
target_workspace_id = get_config_value(f"{target_datastore_config_prefix}_workspace_id")

lh_utils = lakehouse_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark
)

# Now initialize PySpark synthetic data generator with the lakehouse utils instance
generator = PySparkSyntheticDataGenerator(
    lakehouse_utils_instance=lh_utils,
    seed=seed_value
)
dataset_builder = PySparkDatasetBuilder(generator, lh_utils)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Dataset Information

# CELL ********************


# Display dataset information
dataset_info = {
    "Dataset ID": dataset_config.get("dataset_id", "custom"),
    "Dataset Name": dataset_config.get("dataset_name", "Custom Dataset"),
    "Domain": dataset_config.get("domain", "generic"),
    "Schema Pattern": dataset_config.get("schema_pattern", "custom"),
    "Description": dataset_config.get("description", "No description provided")
}

print("=" * 60)
print("SYNTHETIC DATA GENERATION - DATASET INFORMATION")
print("=" * 60)
for key, value in dataset_info.items():
    print(f"{key:20}: {value}")
print("=" * 60)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔄 Generate Synthetic Data

# CELL ********************


import time

# Track generation progress
generated_tables = {}
total_rows_generated = 0
execution_start_time = time.time()

# Get dataset configuration
dataset_id = dataset_config.get("dataset_id", "custom_dataset")
schema_pattern = dataset_config.get("schema_pattern", "oltp")

print(f"🚀 Starting synthetic data generation for: {dataset_id}")
print(f"📊 Schema pattern: {schema_pattern}")
print(f"🎯 Target environment: lakehouse")
print(f"⚙️ Generation mode: {generation_mode}")


# Python generation logic
if schema_pattern == "oltp":
    # Generate OLTP tables using Python
    if "customers" in dataset_config.get("tables", []):
        print("📋 Generating customers table...")
        customers_df = generator.generate_customers_table(
            dataset_config.get("customer_rows", 10000)
        )
        generated_tables["customers"] = customers_df
        total_rows_generated += len(customers_df)
        
    if "products" in dataset_config.get("tables", []):
        print("🏷️ Generating products table...")
        products_df = generator.generate_products_table(
            dataset_config.get("product_rows", 1000)
        )
        generated_tables["products"] = products_df
        total_rows_generated += len(products_df)
        
    if "orders" in dataset_config.get("tables", []) and "customers" in generated_tables:
        print("🛒 Generating orders table...")
        orders_df = generator.generate_orders_table(
            dataset_config.get("order_rows", 10000),
            generated_tables["customers"],
            generated_tables.get("products")
        )
        generated_tables["orders"] = orders_df
        total_rows_generated += len(orders_df)
        
    if "order_items" in dataset_config.get("tables", []) and "orders" in generated_tables:
        print("📦 Generating order items table...")
        order_items_df = generator.generate_order_items_table(
            generated_tables["orders"],
            generated_tables.get("products")
        )
        generated_tables["order_items"] = order_items_df
        total_rows_generated += len(order_items_df)

elif schema_pattern == "star_schema":
    # Generate star schema using dataset builder
    dataset_builder = DatasetBuilder(generator)
    dataset_tables = dataset_builder.build_retail_star_schema(scale_factor=1.0)
    
    for table_name, table_df in dataset_tables.items():
        generated_tables[table_name] = table_df
        total_rows_generated += len(table_df)
        print(f"✅ Generated {table_name}: {len(table_df):,} rows")



# Save generated tables

# Save to target environment tables
print("💾 Saving tables to target environment...")

for table_name, table_df in generated_tables.items():
    lh_utils.write_dataframe_to_table(table_df, table_name, mode="overwrite")
    print(f"✅ Saved {table_name} to lakehouse with {table_df.count() if hasattr(table_df, 'count') else len(table_df):,} rows")



execution_time = time.time() - execution_start_time
print(f"⏱️ Total execution time: {execution_time:.2f} seconds")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ✅ Generation Complete

# CELL ********************


# Display summary
print("=" * 60)
print("SYNTHETIC DATA GENERATION COMPLETE")
print("=" * 60)
print(f"Dataset: {dataset_config.get('dataset_name', 'Custom Dataset')}")
print(f"Generation Mode: {generation_mode}")

print(f"Tables Generated: {len(generated_tables)}")
print(f"Total Rows: {total_rows_generated:,}")
print(f"Execution Time: {execution_time:.2f} seconds")
print("=" * 60)

# Log completion
if 'notebookutils' in globals() and hasattr(notebookutils, 'log'):
    notebookutils.log(f"Synthetic data generation completed for {dataset_config.get('dataset_id')}")


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
# META   "language_group": "python"
# META }

