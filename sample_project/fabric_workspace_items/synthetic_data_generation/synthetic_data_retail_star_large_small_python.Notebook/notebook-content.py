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
dataset_config = {"chunk_size": 10000, "dataset_id": "retail_star_large", "dataset_name": "Retail Star Schema - Large", "dataset_type": "analytical", "description": "Large retail data warehouse with multiple fact tables", "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store", "dim_supplier"], "domain": "retail", "fact_tables": ["fact_sales", "fact_inventory"], "schema_pattern": "star_schema", "seed_value": None, "target_rows": 10000}
generation_mode = "python"
target_rows = 10000
seed_value = None

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

# Initialize synthetic data generator
generator = SyntheticDataGenerator(seed=seed_value)
dataset_builder = DatasetBuilder(generator)

# Initialize warehouse utils
target_warehouse_id = get_config_value("config_warehouse_id")
target_workspace_id = get_config_value("config_workspace_id")

wu = warehouse_utils(
    target_workspace_id=target_workspace_id,
    target_warehouse_id=target_warehouse_id
)



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


# Generate Star Schema (Analytical) Dataset
print("⭐ Generating Star Schema dataset...")

# Generate complete star schema dataset
dataset = dataset_builder.build_retail_star_schema_dataset(fact_rows=target_rows)

print("✅ Star Schema dataset generation completed!")





# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 💾 Save Data to Warehouse

# CELL ********************


print("💾 Saving generated data to warehouse...")

# Create schema if it doesn't exist
schema_name = "retail_star_large"
wu.create_schema_if_not_exists(schema_name)

# Save each table to the warehouse
total_rows_inserted = 0
for table_name, df in dataset.items():
    print(f"📊 Saving {table_name} with {len(df):,} rows...")
    
    try:
        # Use warehouse_utils abstract method to write data
        wu.write_to_table(
            df=df,
            table_name=table_name,
            schema_name=schema_name,
            mode="overwrite"
        )
        
        total_rows_inserted += len(df)
        print(f"✅ {table_name}: {len(df):,} rows saved")
        
    except Exception as e:
        print(f"❌ Error saving {table_name}: {e}")
        print(f"🔄 Skipping {table_name} due to error...")
        continue

print(f"📊 Total rows inserted: {total_rows_inserted:,}")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Data Quality Validation

# CELL ********************


print("🔍 Performing data quality validation...")

# Validate data in warehouse
schema_name = "retail_star_large"
validation_results = {}

for table_name in dataset.keys():
    full_table_name = f"[{schema_name}].[{table_name}]"
    
    try:
        # Get row count from warehouse using abstract method
        warehouse_count = wu.get_table_row_count(table_name=table_name, schema_name=schema_name)
        
        # Compare with generated DataFrame
        generated_count = len(dataset[table_name])
        
        validation_results[table_name] = {
            'generated': generated_count,
            'warehouse': warehouse_count,
            'match': generated_count == warehouse_count
        }
        
        status = "✅" if generated_count == warehouse_count else "⚠️"
        print(f"{status} {table_name}: Generated {generated_count:,}, Warehouse {warehouse_count:,}")
        
        # Show sample data using abstract method
        try:
            sample_result = wu.read_table(table_name=table_name, schema_name=schema_name, limit=5)
            if sample_result is not None and len(sample_result) > 0:
                print(f"📝 Sample data from {table_name}:")
                for i, row in sample_result.head(3).iterrows():  # Show first 3 rows
                    print(f"   {row.to_dict()}")
        except Exception as sample_error:
            print(f"📝 Could not retrieve sample data from {table_name}: {sample_error}")
        
    except Exception as e:
        print(f"❌ Validation error for {table_name}: {e}")
        validation_results[table_name] = {'error': str(e)}

# Overall validation
all_matched = all(
    result.get('match', False) for result in validation_results.values()
)

if all_matched:
    print("✅ All data validation checks passed!")
else:
    print("⚠️ Some data validation checks failed!")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📈 Performance Metrics

# CELL ********************


print("📈 Performance and storage metrics:")

schema_name = "retail_star_large"
total_rows_validated = 0
total_size_mb = 0

# Get storage information for each table
for table_name in dataset.keys():
    try:
        # Get row count using abstract method
        row_count = wu.get_table_row_count(table_name=table_name, schema_name=schema_name)
        
        # Try to get table metadata (size info may not be available in all environments)
        try:
            metadata = wu.get_table_metadata(table_name=table_name, schema_name=schema_name)
            size_mb = metadata.get('size_mb', 0) if metadata else 0
        except Exception:
            # Size information may not be available in local/dev environments
            size_mb = 0
        
        total_rows_validated += row_count
        total_size_mb += size_mb
        
        if size_mb > 0:
            print(f"💾 {table_name}: {size_mb:.2f} MB, {row_count:,} rows")
        else:
            print(f"💾 {table_name}: {row_count:,} rows (size info unavailable)")
        
    except Exception as e:
        print(f"⚠️ Could not get metrics for {table_name}: {e}")

print(f"📊 Total storage used: {total_size_mb:.2f} MB")
print(f"📊 Total rows validated: {total_rows_validated:,}")

if total_rows_validated > 0 and total_size_mb > 0:
    print(f"⚡ Storage efficiency: {total_rows_validated / total_size_mb:.0f} rows/MB")



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
print(f"🎯 Rows Generated: {total_rows_validated:,}")
print(f"📋 Tables Created: {len(dataset)}")
print(f"💾 Total Storage: {total_size_mb:.2f} MB")
print(f"🏛️ Schema: [{schema_name}]")

# List created tables
print("📋 Created tables:")
for table_name in dataset.keys():
    print(f"   - [{schema_name}].[{table_name}]")

# Return success
notebookutils.exit_notebook("success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "python"
# META }

