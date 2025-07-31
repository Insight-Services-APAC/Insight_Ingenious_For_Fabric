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
# META   "language": "python"
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
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    from ingen_fab.python_libs.pyspark.synthetic_data_utils import PySparkSyntheticDataGenerator, PySparkDatasetBuilder
    from ingen_fab.python_libs.common.synthetic_data_dataset_configs import DatasetConfigurationRepository
    notebookutils = NotebookUtilsFactory.create_instance() 
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        "ingen_fab/python_libs/pyspark/ddl_utils.py",
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py",
        "ingen_fab/python_libs/pyspark/synthetic_data_utils.py",
        "ingen_fab/python_libs/common/synthetic_data_dataset_configs.py"
    ]

    load_python_modules_from_path(mount_path, files_to_load)


# MARKDOWN ********************

# ## 『』Parameters

# PARAMETERS CELL ********************




# Default parameter values - will be overridden at runtime via Fabric parameters
dataset_id = "retail_oltp_small"              # Dataset to generate
target_rows = 10000                           # Number of rows to generate
chunk_size = 1000000                         # Chunk size for large datasets
output_mode = "table"                         # Output mode: "table", "parquet", or "csv"
seed_value = None                             # Seed for reproducible generation
generation_mode = "auto"                      # Generation mode: "python", "pyspark", or "auto"
target_environment = "lakehouse"              # Target environment (fixed for this template)

# Advanced parameters
scale_factor = 1.0                           # Scale factor for dataset size
enable_relationships = True                  # Enable table relationships
enable_partitioning = False                  # Enable table partitioning
custom_schema = None                         # Custom schema override (JSON string)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ⚙️ Dynamic Configuration Resolution

# CELL ********************


import time
import json

print("🔧 Resolving runtime configuration...")

# Resolve dataset configuration dynamically
try:
    dataset_config = DatasetConfigurationRepository.get_predefined_dataset(dataset_id)
except ValueError as e:
    available_datasets = DatasetConfigurationRepository.list_available_datasets()
    raise ValueError(f"Dataset '{dataset_id}' not found. Available: {list(available_datasets.keys())}")

# Apply custom schema if provided
if custom_schema:
    try:
        schema_override = json.loads(custom_schema)
        dataset_config.update(schema_override)
        print(f"🔧 Applied custom schema override")
    except json.JSONDecodeError as e:
        print(f"⚠️ Warning: Invalid custom schema JSON, ignoring: {e}")

# Determine optimal generation mode
if generation_mode == "auto":
    generation_mode = "pyspark" if target_rows > 1000000 else "pyspark"  # Always use PySpark for lakehouse

# Calculate scaled row counts for different tables
schema_pattern = dataset_config.get("schema_pattern", "oltp")
if schema_pattern == "oltp":
    customer_rows = int(target_rows * 0.1 * scale_factor)      # 10% of target rows
    product_rows = int(target_rows * 0.01 * scale_factor)      # 1% of target rows  
    order_rows = int(target_rows * scale_factor)               # Target rows
    order_item_rows = int(target_rows * 2.5 * scale_factor)    # 2.5x target rows
elif schema_pattern == "star_schema":
    dim_customer_rows = int(50000 * scale_factor)
    dim_product_rows = int(5000 * scale_factor)
    dim_store_rows = int(100 * scale_factor)
    fact_sales_rows = int(target_rows * scale_factor)
else:
    # Custom schema - use target_rows directly
    customer_rows = int(target_rows * scale_factor)

print(f"📊 Dataset: {dataset_config['dataset_name']}")
print(f"🎯 Target rows: {target_rows:,}")
print(f"📈 Scale factor: {scale_factor}")
print(f"⚙️ Generation mode: {generation_mode}")
print(f"💾 Output mode: {output_mode}")
print(f"🧩 Schema pattern: {schema_pattern}")

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

# Initialize lakehouse utils
target_lakehouse_id = get_config_value("config_lakehouse_id")
target_workspace_id = get_config_value("config_workspace_id")

lh_utils = lakehouse_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id
)

# Initialize PySpark synthetic data generator
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

# ## 📊 Dataset Information

# CELL ********************


# Display comprehensive dataset information
print("=" * 80)
print("SYNTHETIC DATA GENERATION - SINGLE DATASET MODE")
print("=" * 80)
print(f"{'Dataset ID':<25}: {dataset_config.get('dataset_id', 'N/A')}")
print(f"{'Dataset Name':<25}: {dataset_config.get('dataset_name', 'N/A')}")
print(f"{'Domain':<25}: {dataset_config.get('domain', 'N/A')}")
print(f"{'Schema Pattern':<25}: {dataset_config.get('schema_pattern', 'N/A')}")
print(f"{'Description':<25}: {dataset_config.get('description', 'N/A')}")
print(f"{'Target Rows':<25}: {target_rows:,}")
print(f"{'Scale Factor':<25}: {scale_factor}")
print(f"{'Chunk Size':<25}: {chunk_size:,}")
print(f"{'Output Mode':<25}: {output_mode}")
print(f"{'Generation Mode':<25}: {generation_mode}")
print(f"{'Enable Relationships':<25}: {enable_relationships}")
print(f"{'Enable Partitioning':<25}: {enable_partitioning}")
print("=" * 80)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔄 Generate Synthetic Data

# CELL ********************


# Track generation progress
generated_tables = {}
total_rows_generated = 0
execution_start_time = time.time()

print(f"🚀 Starting synthetic data generation...")

schema_pattern = dataset_config.get("schema_pattern", "oltp")

if schema_pattern == "oltp":
    # Generate OLTP tables
    print("📋 Generating OLTP dataset...")
    
    if "customers" in dataset_config.get("tables", []):
        print(f"  📋 Generating customers table ({customer_rows:,} rows)...")
        customers_df = generator.generate_customers_table(customer_rows)
        generated_tables["customers"] = customers_df
        total_rows_generated += customers_df.count()
        print(f"    ✅ Generated {customers_df.count():,} customer records")
        
    if "products" in dataset_config.get("tables", []):
        print(f"  🏷️ Generating products table ({product_rows:,} rows)...")
        products_df = generator.generate_products_table(product_rows)
        generated_tables["products"] = products_df
        total_rows_generated += products_df.count()
        print(f"    ✅ Generated {products_df.count():,} product records")
        
    if "orders" in dataset_config.get("tables", []) and "customers" in generated_tables:
        print(f"  🛒 Generating orders table ({order_rows:,} rows)...")
        orders_df = generator.generate_orders_table(
            order_rows,
            generated_tables["customers"],
            generated_tables.get("products")
        )
        generated_tables["orders"] = orders_df
        total_rows_generated += orders_df.count()
        print(f"    ✅ Generated {orders_df.count():,} order records")
        
    if "order_items" in dataset_config.get("tables", []) and "orders" in generated_tables:
        print(f"  📦 Generating order items table...")
        order_items_df = generator.generate_order_items_table(
            generated_tables["orders"],
            generated_tables.get("products")
        )
        generated_tables["order_items"] = order_items_df
        total_rows_generated += order_items_df.count()
        print(f"    ✅ Generated {order_items_df.count():,} order item records")

elif schema_pattern == "star_schema":
    # Generate star schema tables
    print("📐 Generating Star Schema dataset...")
    
    dimensions = {}
    
    # Generate dimension tables
    for dim in dataset_config.get("dimensions", []):
        print(f"  📐 Generating {dim}...")
        if dim == "dim_date":
            dim_df = generator.generate_date_dimension(2020, 2025)
        elif dim == "dim_customer":
            dim_df = generator.generate_customers_table(dim_customer_rows)
        elif dim == "dim_product":
            dim_df = generator.generate_products_table(dim_product_rows)
        elif dim == "dim_store":
            dim_df = generator.generate_customers_table(dim_store_rows)  # Generic store data
        else:
            continue
            
        dimensions[dim] = dim_df
        generated_tables[dim] = dim_df
        total_rows_generated += dim_df.count()
        print(f"    ✅ Generated {dim_df.count():,} records for {dim}")
    
    # Generate fact tables
    for fact in dataset_config.get("fact_tables", []):
        print(f"  📊 Generating {fact} ({fact_sales_rows:,} rows)...")
        if fact == "fact_sales":
            fact_df = generator.generate_fact_sales(fact_sales_rows, dimensions)
            generated_tables[fact] = fact_df
            total_rows_generated += fact_df.count()
            print(f"    ✅ Generated {fact_df.count():,} records for {fact}")

else:
    # Custom schema - use dataset builder
    print("🔧 Generating custom dataset using dataset builder...")
    dataset_tables = dataset_builder.build_custom_dataset(dataset_config)
    
    for table_name, table_df in dataset_tables.items():
        generated_tables[table_name] = table_df
        total_rows_generated += table_df.count()
        print(f"  ✅ Generated {table_name}: {table_df.count():,} rows")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 💾 Save Generated Data

# CELL ********************


print("💾 Saving generated tables...")

# Save generated tables based on output mode
if output_mode == "table":
    print("  📊 Saving to lakehouse tables...")
    for table_name, table_df in generated_tables.items():
        write_mode = "overwrite"
        if enable_partitioning and table_name in ["orders", "order_items", "fact_sales"]:
            # Enable partitioning for large transactional tables
            partition_cols = ["order_date"] if "order_date" in table_df.columns else None
            prefixed_table_name = f"synth_data_{table_name}"
            lh_utils.write_to_table(
                table_df, 
                prefixed_table_name, 
                mode=write_mode,
                partition_by=partition_cols
            )
            print(f"    ✅ Saved {prefixed_table_name} (partitioned) with {table_df.count():,} rows")
        else:
            prefixed_table_name = f"synth_data_{table_name}"
            lh_utils.write_to_table(table_df, prefixed_table_name, mode=write_mode)
            print(f"    ✅ Saved {prefixed_table_name} with {table_df.count():,} rows")

elif output_mode == "parquet":
    print("  📄 Saving as Parquet files...")
    output_path = f"synthetic_data/single/{dataset_id}"
    generator.export_to_parquet(generated_tables, output_path)
    for table_name, table_df in generated_tables.items():
        print(f"    ✅ Saved {output_path}/{table_name}.parquet with {table_df.count():,} rows")

elif output_mode == "csv":
    print("  📄 Saving as CSV files...")
    output_path = f"synthetic_data/single/{dataset_id}"
    generator.export_to_csv(generated_tables, output_path)
    for table_name, table_df in generated_tables.items():
        print(f"    ✅ Saved {output_path}/{table_name}.csv with {table_df.count():,} rows")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ✅ Generation Complete - Summary

# CELL ********************


execution_time = time.time() - execution_start_time

# Display comprehensive summary
print("=" * 80)
print("SYNTHETIC DATA GENERATION COMPLETE")
print("=" * 80)
print(f"{'Dataset':<25}: {dataset_config.get('dataset_name', 'N/A')}")
print(f"{'Generation Mode':<25}: {generation_mode}")
print(f"{'Schema Pattern':<25}: {schema_pattern}")
print(f"{'Target Rows':<25}: {target_rows:,}")
print(f"{'Scale Factor':<25}: {scale_factor}")
print(f"{'Tables Generated':<25}: {len(generated_tables)}")
print(f"{'Total Rows':<25}: {total_rows_generated:,}")
print(f"{'Execution Time':<25}: {execution_time:.2f} seconds")
print(f"{'Output Mode':<25}: {output_mode}")
print("=" * 80)

# Display table breakdown
print("\nTABLE BREAKDOWN:")
print("-" * 50)
for table_name, table_df in generated_tables.items():
    row_count = table_df.count()
    percentage = (row_count / total_rows_generated * 100) if total_rows_generated > 0 else 0
    print(f"{table_name:<20}: {row_count:>10,} rows ({percentage:5.1f}%)")
print("-" * 50)
print(f"{'TOTAL':<20}: {total_rows_generated:>10,} rows (100.0%)")

print("=" * 80)
print(f"⚡ Processing rate: {total_rows_generated/execution_time:,.0f} rows/second")

# Log completion
if 'notebookutils' in globals() and hasattr(notebookutils, 'log'):
    notebookutils.log(f"Single dataset generation completed: {dataset_id}, {total_rows_generated:,} rows, {execution_time:.1f}s")

print(f"\n🎉 Synthetic data generation completed successfully!")