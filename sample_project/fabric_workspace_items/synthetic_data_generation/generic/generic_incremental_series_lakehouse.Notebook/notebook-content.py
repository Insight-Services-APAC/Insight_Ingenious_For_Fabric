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
start_date = "2024-01-01"                     # Start date (YYYY-MM-DD)
end_date = "2024-01-30"                       # End date (YYYY-MM-DD)  
batch_size = 10                               # Number of days per batch
path_format = "nested"                        # Path format: "nested" (/YYYY/MM/DD/) or "flat" (YYYYMMDD_)
output_mode = "table"                         # Output mode: "table", "parquet", or "csv"
ignore_state = False                          # Whether to ignore existing state
seed_value = None                             # Seed for reproducible generation
generation_mode = "auto"                      # Generation mode: "python", "pyspark", or "auto"
target_environment = "lakehouse"              # Target environment (fixed for this template)

# Advanced parameters
parallel_workers = 1                          # Number of parallel workers
chunk_size = 1000000                         # Chunk size for large datasets
enable_data_drift = True                     # Enable data drift simulation
drift_percentage = 0.05                      # Data drift percentage
enable_seasonal_patterns = True             # Enable seasonal pattern simulation



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ⚙️ Dynamic Configuration Resolution

# CELL ********************


import time
from datetime import datetime, timedelta

print("🔧 Resolving runtime configuration...")

# Validate and parse parameters  
try:
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    if end_dt < start_dt:
        raise ValueError("End date must be after start date")
    total_days = (end_dt - start_dt).days + 1
except ValueError as e:
    raise ValueError(f"Invalid date format or range: {e}")

# Resolve dataset configuration dynamically
try:
    dataset_config = DatasetConfigurationRepository.get_predefined_dataset(dataset_id)
    incremental_config = DatasetConfigurationRepository.get_incremental_config(dataset_id)
    table_configs = DatasetConfigurationRepository.get_table_configs(dataset_id)
except ValueError as e:
    available_datasets = DatasetConfigurationRepository.list_available_datasets()
    raise ValueError(f"Dataset '{dataset_id}' not found. Available: {list(available_datasets.keys())}")

# Apply runtime parameter overrides
dataset_config["incremental_config"] = incremental_config
dataset_config["table_configs"] = table_configs

# Override configuration with runtime parameters
if enable_data_drift is not None:
    dataset_config["incremental_config"]["enable_data_drift"] = enable_data_drift
if drift_percentage is not None:
    dataset_config["incremental_config"]["drift_percentage"] = drift_percentage
if enable_seasonal_patterns is not None:
    dataset_config["incremental_config"]["enable_seasonal_patterns"] = enable_seasonal_patterns

# Determine optimal generation mode
if generation_mode == "auto":
    # Calculate total estimated rows
    daily_rows = sum(
        table_config.get("base_rows_per_day", 0) 
        for table_config in table_configs.values() 
        if table_config.get("type") == "incremental"
    )
    total_estimated_rows = daily_rows * total_days
    generation_mode = "pyspark" if total_estimated_rows > 1000000 else "pyspark"  # Always use PySpark for lakehouse

print(f"📊 Dataset: {dataset_config['dataset_name']}")
print(f"📅 Date range: {start_date} to {end_date} ({total_days} days)")
print(f"📦 Batch size: {batch_size} days")
print(f"📁 Path format: {path_format}")
print(f"💾 Output mode: {output_mode}")
print(f"⚙️ Generation mode: {generation_mode}")
print(f"🔄 Processing {len(table_configs)} tables")

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
print("INCREMENTAL SYNTHETIC DATA GENERATION - SERIES MODE")
print("=" * 80)
print(f"{'Dataset ID':<25}: {dataset_config.get('dataset_id', 'N/A')}")
print(f"{'Dataset Name':<25}: {dataset_config.get('dataset_name', 'N/A')}")
print(f"{'Domain':<25}: {dataset_config.get('domain', 'N/A')}")
print(f"{'Schema Pattern':<25}: {dataset_config.get('schema_pattern', 'N/A')}")
print(f"{'Description':<25}: {dataset_config.get('description', 'N/A')}")
print(f"{'Date Range':<25}: {start_date} to {end_date}")
print(f"{'Total Days':<25}: {total_days}")
print(f"{'Batch Size':<25}: {batch_size} days")
print(f"{'Path Format':<25}: {path_format}")
print(f"{'Output Mode':<25}: {output_mode}")
print(f"{'Generation Mode':<25}: {generation_mode}")
print(f"{'Ignore State':<25}: {ignore_state}")
print("=" * 80)

# Display table configurations
print("TABLE CONFIGURATIONS:")
print("-" * 80)
for table_name, table_config in table_configs.items():
    table_type = table_config.get("type", "unknown")
    if table_type == "incremental":
        rows_per_day = table_config.get("base_rows_per_day", 0)
        print(f"  {table_name:<20}: {table_type:<12} | {rows_per_day:>10,} rows/day")
    else:
        base_rows = table_config.get("base_rows", 0)
        frequency = table_config.get("frequency", "unknown")
        print(f"  {table_name:<20}: {table_type:<12} | {base_rows:>10,} rows ({frequency})")
print("=" * 80)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔄 Generate Date Batches

# CELL ********************


# Generate date batches for processing
date_batches = []
current_date = start_dt

while current_date <= end_dt:
    batch_end = min(current_date + timedelta(days=batch_size-1), end_dt)
    date_batches.append({
        "start_date": current_date,
        "end_date": batch_end,
        "days": (batch_end - current_date).days + 1
    })
    current_date = batch_end + timedelta(days=1)

print(f"📦 Generated {len(date_batches)} date batches:")
for i, batch in enumerate(date_batches, 1):
    print(f"  Batch {i}: {batch['start_date']} to {batch['end_date']} ({batch['days']} days)")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🎯 Process Incremental Data Generation

# CELL ********************


# Track overall progress
total_tables_generated = 0
total_rows_generated = 0
execution_start_time = time.time()
generation_log = []

print(f"🚀 Starting incremental data generation for {len(date_batches)} batches...")

# Process each date batch
for batch_num, batch in enumerate(date_batches, 1):
    batch_start_time = time.time()
    batch_tables = {}
    batch_rows = 0
    
    print(f"\n📦 Processing Batch {batch_num}/{len(date_batches)}: {batch['start_date']} to {batch['end_date']}")
    print("-" * 60)
    
    # Generate data for each day in the batch
    current_date = batch['start_date']
    while current_date <= batch['end_date']:
        print(f"📅 Generating data for {current_date.strftime('%Y-%m-%d')}...")
        
        # Process each table
        for table_name, table_config in table_configs.items():
            table_type = table_config.get("type", "snapshot")
            
            if table_type == "incremental":
                # Generate incremental data for this date
                base_rows = table_config.get("base_rows_per_day", 10000)
                
                # Apply seasonal patterns if enabled
                if enable_seasonal_patterns and table_config.get("seasonal_multipliers_enabled", False):
                    day_of_week = current_date.strftime("%A").lower()
                    seasonal_multipliers = incremental_config.get("seasonal_multipliers", {})
                    multiplier = seasonal_multipliers.get(day_of_week, 1.0)
                    adjusted_rows = int(base_rows * multiplier)
                else:
                    adjusted_rows = base_rows
                
                # Generate table data
                if table_name == "customers":
                    table_df = generator.generate_customers_table(adjusted_rows)
                elif table_name == "products":
                    table_df = generator.generate_products_table(adjusted_rows)
                elif table_name == "orders":
                    # For orders, we need customers data
                    customers_df = batch_tables.get("customers")
                    products_df = batch_tables.get("products")
                    table_df = generator.generate_orders_table(adjusted_rows, customers_df, products_df)
                elif table_name == "order_items":
                    # For order items, we need orders and products
                    orders_df = batch_tables.get("orders")
                    products_df = batch_tables.get("products")
                    if orders_df is not None:
                        table_df = generator.generate_order_items_table(orders_df, products_df)
                    else:
                        continue
                else:
                    # Generic table generation
                    table_df = generator.generate_customers_table(adjusted_rows)  # Fallback
                
                # Store in batch tables for dependencies
                batch_tables[table_name] = table_df
                
                # Save the data with appropriate path format
                if path_format == "flat":
                    date_path = current_date.strftime("%Y%m%d")
                    table_path = f"{table_name}_{date_path}"
                else:  # nested
                    date_path = current_date.strftime("%Y/%m/%d")
                    table_path = f"{table_name}/{date_path}"
                
                # Save based on output mode
                if output_mode == "table":
                    final_table_name = f"synth_data_{table_name}_incremental_{current_date.strftime('%Y%m%d')}"
                    lh_utils.write_to_table(table_df, final_table_name, mode="overwrite", options={"overwriteSchema": "true"})
                    print(f"    ✅ {table_name}: {table_df.count():,} rows → table '{final_table_name}'")
                elif output_mode in ["parquet", "csv"]:
                    # For file outputs, save with consistent path structure
                    if path_format == "flat":
                        file_path = f"synthetic_data/incremental/{dataset_id}/files/incremental/{table_name}_{current_date.strftime('%Y%m%d')}.{output_mode}"
                    else:  # nested
                        file_path = f"synthetic_data/incremental/{dataset_id}/files/incremental/{table_name}/{current_date.strftime('%Y%m%d')}/data.{output_mode}"
                    
                    lh_utils.write_file(table_df, file_path, output_mode, options={"header": "true", "mode": "overwrite"})
                    print(f"    ✅ {table_name}: {table_df.count():,} rows → {file_path}")
                
                batch_rows += table_df.count()
                total_rows_generated += table_df.count()
                
            elif table_type == "snapshot":
                # Handle snapshot tables (generate once per batch or less frequently)
                frequency = table_config.get("frequency", "daily")
                should_generate = False
                
                if frequency == "daily":
                    should_generate = True
                elif frequency == "weekly" and current_date.weekday() == 0:  # Monday
                    should_generate = True
                elif frequency == "monthly" and current_date.day == 1:  # First of month
                    should_generate = True
                
                if should_generate:
                    base_rows = table_config.get("base_rows", 10000)
                    table_df = generator.generate_customers_table(base_rows)  # Generic snapshot
                    batch_tables[table_name] = table_df
                    
                    if output_mode == "table":
                        snapshot_table_name = f"synth_data_{table_name}_snapshot_{current_date.strftime('%Y%m%d')}"
                        lh_utils.write_to_table(table_df, snapshot_table_name, mode="overwrite", options={"overwriteSchema": "true"})
                        print(f"    ✅ {table_name} (snapshot): {table_df.count():,} rows → table '{snapshot_table_name}'")
                    elif output_mode in ["parquet", "csv"]:
                        # For snapshot files, use consistent path structure
                        if path_format == "flat":
                            file_path = f"synthetic_data/incremental/{dataset_id}/files/snapshot/{table_name}_{current_date.strftime('%Y%m%d')}.{output_mode}"
                        else:  # nested
                            file_path = f"synthetic_data/incremental/{dataset_id}/files/snapshot/{table_name}/{current_date.strftime('%Y%m%d')}/data.{output_mode}"
                        
                        lh_utils.write_file(table_df, file_path, output_mode, options={"header": "true", "mode": "overwrite"})
                        print(f"    ✅ {table_name} (snapshot): {table_df.count():,} rows → {file_path}")
                    
                    batch_rows += table_df.count()
                    total_rows_generated += table_df.count()
        
        current_date += timedelta(days=1)
    
    # Log batch completion
    batch_duration = time.time() - batch_start_time
    generation_log.append({
        "batch": batch_num,
        "start_date": batch['start_date'].isoformat(),
        "end_date": batch['end_date'].isoformat(), 
        "days": batch['days'],
        "rows_generated": batch_rows,
        "duration_seconds": batch_duration
    })
    
    print(f"✅ Batch {batch_num} complete: {batch_rows:,} rows in {batch_duration:.1f}s")
    total_tables_generated += len(batch_tables)



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
print("INCREMENTAL SYNTHETIC DATA SERIES GENERATION COMPLETE")
print("=" * 80)
print(f"{'Dataset':<25}: {dataset_config.get('dataset_name', 'N/A')}")
print(f"{'Generation Mode':<25}: {generation_mode}")
print(f"{'Date Range':<25}: {start_date} to {end_date}")
print(f"{'Total Days':<25}: {total_days}")
print(f"{'Batches Processed':<25}: {len(date_batches)}")
print(f"{'Tables Generated':<25}: {total_tables_generated}")
print(f"{'Total Rows':<25}: {total_rows_generated:,}")
print(f"{'Execution Time':<25}: {execution_time:.2f} seconds")
print(f"{'Output Mode':<25}: {output_mode}")
print(f"{'Path Format':<25}: {path_format}")
print("=" * 80)

# Display batch-by-batch breakdown
print("\nBATCH BREAKDOWN:")
print("-" * 80)
print(f"{'Batch':<8} {'Date Range':<25} {'Days':<6} {'Rows':<12} {'Duration':<12}")
print("-" * 80)
for log_entry in generation_log:
    batch_num = log_entry['batch']
    date_range = f"{log_entry['start_date']} to {log_entry['end_date']}"
    days = log_entry['days']
    rows = f"{log_entry['rows_generated']:,}"
    duration = f"{log_entry['duration_seconds']:.1f}s"
    print(f"{batch_num:<8} {date_range:<25} {days:<6} {rows:<12} {duration:<12}")

print("=" * 80)
print(f"⚡ Average processing rate: {total_rows_generated/execution_time:,.0f} rows/second")
print(f"📊 Average rows per day: {total_rows_generated/total_days:,.0f}")

# Log completion
if 'notebookutils' in globals() and hasattr(notebookutils, 'log'):
    notebookutils.log(f"Incremental series generation completed: {dataset_id}, {total_rows_generated:,} rows, {execution_time:.1f}s")

print(f"\n🎉 Incremental synthetic data series generation completed successfully!")