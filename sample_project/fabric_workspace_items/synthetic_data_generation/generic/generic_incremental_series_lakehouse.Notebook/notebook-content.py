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
        if base_path.startswith("file:") or base_path.startswith("abfss:"):
            full_path = f"{base_path}/{relative_path}"
        else:
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
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    from ingen_fab.python_libs.pyspark.synthetic_data_utils import PySparkSyntheticDataGenerator, PySparkDatasetBuilder
    from ingen_fab.python_libs.pyspark.incremental_synthetic_data_utils import IncrementalSyntheticDataGenerator
    from ingen_fab.python_libs.common.synthetic_data_dataset_configs import DatasetConfigurationRepository
    notebookutils = NotebookUtilsFactory.create_instance() 
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        "ingen_fab/python_libs/pyspark/ddl_utils.py",
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py",
        "ingen_fab/python_libs/pyspark/synthetic_data_utils.py",
        "ingen_fab/python_libs/pyspark/incremental_synthetic_data_utils.py",
        "ingen_fab/python_libs/common/synthetic_data_dataset_configs.py"
    ]

    load_python_modules_from_path(mount_path, files_to_load)


# MARKDOWN ********************

# ## 『』Parameters

# PARAMETERS CELL ********************




# Job configurations - can be a single dict or list of dicts
# Multiple job configurations example - generate different datasets across time periods
job_configs = [
    
    {
        "job_name": "test_data_feb_small",
        "dataset_id": "retail_oltp_small",
        "start_date": "2024-02-01",
        "end_date": "2024-02-07",
        "batch_size": 1,                            # Daily batches for testing
        "path_format": "flat",
        "output_mode": "csv",
        "ignore_state": True,
        "seed_value": 99999,
        "generation_mode": "auto",
        "parallel_workers": 1,
        "chunk_size": 100000,
        "enable_data_drift": False,
        "drift_percentage": 0.0,
        "enable_seasonal_patterns": False
    },
    {
        "job_name": "q1_2024_retail_daily",
        "dataset_id": "retail_oltp_small",
        "start_date": "2024-01-01",
        "end_date": "2024-03-31",
        "batch_size": 30,                           # Monthly batches
        "path_format": "nested",
        "output_mode": "parquet",
        "ignore_state": False,
        "seed_value": 12345,
        "generation_mode": "auto",
        "parallel_workers": 1,
        "chunk_size": 1000000,
        "enable_data_drift": True,
        "drift_percentage": 0.03,
        "enable_seasonal_patterns": True
    },
    {
        "job_name": "high_volume_summer_data",
        "dataset_id": "retail_oltp_small",
        "start_date": "2024-06-01",
        "end_date": "2024-08-31",
        "batch_size": 7,                            # Weekly batches
        "path_format": "flat",
        "output_mode": "parquet",
        "ignore_state": False,
        "seed_value": 67890,
        "generation_mode": "pyspark",
        "parallel_workers": 2,
        "chunk_size": 2000000,
        "enable_data_drift": True,
        "drift_percentage": 0.08,                   # Higher drift for summer peak
        "enable_seasonal_patterns": True
    },
    {
        "job_name": "holiday_season_burst",
        "dataset_id": "retail_oltp_small",
        "start_date": "2024-11-15",
        "end_date": "2024-12-31",
        "batch_size": 3,                            # Small batches for frequent processing
        "path_format": "nested",
        "output_mode": "parquet",
        "ignore_state": True,                       # Ignore state for clean holiday data
        "seed_value": 11111,
        "generation_mode": "pyspark",
        "parallel_workers": 3,
        "chunk_size": 500000,
        "enable_data_drift": False,                 # Stable patterns during holidays
        "drift_percentage": 0.0,
        "enable_seasonal_patterns": True
    },
    {
        "job_name": "backfill_missing_april",
        "dataset_id": "retail_oltp_small",
        "start_date": "2024-04-01",
        "end_date": "2024-04-30",
        "batch_size": 5,                            # 5-day batches
        "path_format": "nested",
        "output_mode": "parquet",
        "ignore_state": False,
        "seed_value": 54321,
        "generation_mode": "pyspark",
        "parallel_workers": 1,
        "chunk_size": 750000,
        "enable_data_drift": True,
        "drift_percentage": 0.04,
        "enable_seasonal_patterns": True
    }
]

# Single job configuration (backward compatible) - uncomment to use
# job_configs = {
#     "dataset_id": "retail_oltp_small",
#     "start_date": "2024-01-01",
#     "end_date": "2024-01-30",
#     "batch_size": 10,
#     "path_format": "nested",
#     "output_mode": "parquet",
#     "ignore_state": False,
#     "seed_value": None,
#     "generation_mode": "auto",
#     "parallel_workers": 1,
#     "chunk_size": 1000000,
#     "enable_data_drift": True,
#     "drift_percentage": 0.05,
#     "enable_seasonal_patterns": True
# }

target_environment = "lakehouse"  # Target environment (fixed for this template)



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

# Normalize job_configs to always be a list
if isinstance(job_configs, dict):
    # Single job configuration - convert to list
    job_configs_list = [job_configs]
    print("📋 Single job configuration detected")
elif isinstance(job_configs, list):
    job_configs_list = job_configs
    print(f"📋 Multiple job configurations detected: {len(job_configs_list)} jobs")
else:
    raise ValueError("job_configs must be either a dictionary or a list of dictionaries")

# Validate all job configurations
valid_jobs = []
for idx, job_config in enumerate(job_configs_list):
    job_name = job_config.get("job_name", f"job_{idx + 1}")
    
    try:
        # Extract parameters with defaults
        dataset_id = job_config.get("dataset_id", "retail_oltp_small")
        start_date = job_config.get("start_date", "2024-01-01")
        end_date = job_config.get("end_date", "2024-01-30")
        batch_size = job_config.get("batch_size", 10)
        path_format = job_config.get("path_format", "nested")
        output_mode = job_config.get("output_mode", "parquet")
        ignore_state = job_config.get("ignore_state", False)
        seed_value = job_config.get("seed_value", None)
        generation_mode = job_config.get("generation_mode", "auto")
        parallel_workers = job_config.get("parallel_workers", 1)
        chunk_size = job_config.get("chunk_size", 1000000)
        enable_data_drift = job_config.get("enable_data_drift", True)
        drift_percentage = job_config.get("drift_percentage", 0.05)
        enable_seasonal_patterns = job_config.get("enable_seasonal_patterns", True)
        
        # Validate dates
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        if end_dt < start_dt:
            raise ValueError("End date must be after start date")
        total_days = (end_dt - start_dt).days + 1
        
        # Resolve dataset configuration
        dataset_config = DatasetConfigurationRepository.get_predefined_dataset(dataset_id)
        incremental_config = DatasetConfigurationRepository.get_incremental_config(dataset_id)
        table_configs = DatasetConfigurationRepository.get_table_configs(dataset_id)
        
        # Apply runtime parameter overrides
        dataset_config["incremental_config"] = incremental_config
        dataset_config["table_configs"] = table_configs
        
        if enable_data_drift is not None:
            dataset_config["incremental_config"]["enable_data_drift"] = enable_data_drift
        if drift_percentage is not None:
            dataset_config["incremental_config"]["drift_percentage"] = drift_percentage
        if enable_seasonal_patterns is not None:
            dataset_config["incremental_config"]["enable_seasonal_patterns"] = enable_seasonal_patterns
        
        # Determine optimal generation mode
        if generation_mode == "auto":
            daily_rows = sum(
                table_config.get("base_rows_per_day", 0) 
                for table_config in table_configs.values() 
                if table_config.get("type") == "incremental"
            )
            total_estimated_rows = daily_rows * total_days
            generation_mode = "pyspark" if total_estimated_rows > 1000000 else "pyspark"  # Always use PySpark for lakehouse
        
        # Create validated job configuration
        validated_job = {
            "job_name": job_name,
            "dataset_id": dataset_id,
            "dataset_config": dataset_config,
            "incremental_config": incremental_config,
            "table_configs": table_configs,
            "start_date": start_date,
            "end_date": end_date,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "total_days": total_days,
            "batch_size": batch_size,
            "path_format": path_format,
            "output_mode": output_mode,
            "ignore_state": ignore_state,
            "seed_value": seed_value,
            "generation_mode": generation_mode,
            "parallel_workers": parallel_workers,
            "chunk_size": chunk_size,
            "enable_data_drift": enable_data_drift,
            "drift_percentage": drift_percentage,
            "enable_seasonal_patterns": enable_seasonal_patterns
        }
        
        valid_jobs.append(validated_job)
        print(f"✅ Validated job '{job_name}': {dataset_config['dataset_name']} ({start_date} to {end_date})")
        
    except Exception as e:
        print(f"❌ Job '{job_name}' validation failed: {e}")
        continue

if not valid_jobs:
    raise ValueError("No valid job configurations found")

print(f"\n📊 Total valid jobs: {len(valid_jobs)}")



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
target_lakehouse_id = get_config_value("sample_lh_lakehouse_id")
target_workspace_id = get_config_value("sample_lh_workspace_id")

lh_utils = lakehouse_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark
)

# Initialize incremental synthetic data generator
base_generator = PySparkSyntheticDataGenerator(
    lakehouse_utils_instance=lh_utils,
    seed=seed_value
)
generator = IncrementalSyntheticDataGenerator(
    lakehouse_utils_instance=lh_utils,
    seed=seed_value
)
# Set the base generator after initialization
generator.base_generator = base_generator
dataset_builder = PySparkDatasetBuilder(base_generator)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Job Queue Information

# CELL ********************


# Display job queue information
print("=" * 80)
print("INCREMENTAL SYNTHETIC DATA GENERATION - JOB QUEUE")
print("=" * 80)
print(f"{'Total Jobs':<25}: {len(valid_jobs)}")
print(f"{'Target Environment':<25}: {target_environment}")
print("=" * 80)

# Display each job's configuration
for idx, job in enumerate(valid_jobs, 1):
    print(f"\n{'='*60}")
    print(f"JOB {idx}: {job['job_name']}")
    print(f"{'='*60}")
    print(f"{'Dataset ID':<25}: {job['dataset_config'].get('dataset_id', 'N/A')}")
    print(f"{'Dataset Name':<25}: {job['dataset_config'].get('dataset_name', 'N/A')}")
    print(f"{'Domain':<25}: {job['dataset_config'].get('domain', 'N/A')}")
    print(f"{'Schema Pattern':<25}: {job['dataset_config'].get('schema_pattern', 'N/A')}")
    print(f"{'Date Range':<25}: {job['start_date']} to {job['end_date']}")
    print(f"{'Total Days':<25}: {job['total_days']}")
    print(f"{'Batch Size':<25}: {job['batch_size']} days")
    print(f"{'Path Format':<25}: {job['path_format']}")
    print(f"{'Output Mode':<25}: {job['output_mode']}")
    print(f"{'Generation Mode':<25}: {job['generation_mode']}")
    print(f"{'Ignore State':<25}: {job['ignore_state']}")
    
    # Display table configurations for this job
    print("\nTABLE CONFIGURATIONS:")
    print("-" * 60)
    for table_name, table_config in job['table_configs'].items():
        table_type = table_config.get("type", "unknown")
        if table_type == "incremental":
            rows_per_day = table_config.get("base_rows_per_day", 0)
            print(f"  {table_name:<20}: {table_type:<12} | {rows_per_day:>10,} rows/day")
        else:
            base_rows = table_config.get("base_rows", 0)
            frequency = table_config.get("frequency", "unknown")
            print(f"  {table_name:<20}: {table_type:<12} | {base_rows:>10,} rows ({frequency})")
print("\n" + "=" * 80)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔄 Prepare Job Batches

# CELL ********************


# Prepare date batches for all jobs
all_job_batches = []

for job in valid_jobs:
    job_batches = []
    current_date = job['start_dt']
    
    while current_date <= job['end_dt']:
        batch_end = min(current_date + timedelta(days=job['batch_size']-1), job['end_dt'])
        job_batches.append({
            "start_date": current_date,
            "end_date": batch_end,
            "days": (batch_end - current_date).days + 1
        })
        current_date = batch_end + timedelta(days=1)
    
    all_job_batches.append({
        "job": job,
        "batches": job_batches
    })
    
    print(f"📦 Job '{job['job_name']}': Generated {len(job_batches)} date batches")
    if len(job_batches) <= 5:  # Show all batches if 5 or fewer
        for i, batch in enumerate(job_batches, 1):
            print(f"    Batch {i}: {batch['start_date']} to {batch['end_date']} ({batch['days']} days)")
    else:  # Show first 3 and last 2 if more than 5
        for i in range(3):
            batch = job_batches[i]
            print(f"    Batch {i+1}: {batch['start_date']} to {batch['end_date']} ({batch['days']} days)")
        print("    ...")
        for i in range(len(job_batches)-2, len(job_batches)):
            batch = job_batches[i]
            print(f"    Batch {i+1}: {batch['start_date']} to {batch['end_date']} ({batch['days']} days)")



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
job_results = []

print(f"🚀 Starting incremental data generation for {len(valid_jobs)} jobs...")

# Process each job
for job_idx, job_batch_info in enumerate(all_job_batches, 1):
    job = job_batch_info['job']
    job_batches = job_batch_info['batches']
    job_start_time = time.time()
    job_total_rows = 0
    job_generation_log = []
    
    print(f"\n{'='*80}")
    print(f"🔄 PROCESSING JOB {job_idx}/{len(valid_jobs)}: {job['job_name']}")
    print(f"{'='*80}")
    print(f"Dataset: {job['dataset_config']['dataset_name']}")
    print(f"Date Range: {job['start_date']} to {job['end_date']}")
    print(f"Total Batches: {len(job_batches)}")
    print(f"Output Mode: {job['output_mode']}")
    
    # Process each date batch for this job
    for batch_num, batch in enumerate(job_batches, 1):
        batch_start_time = time.time()
        batch_tables = {}
        batch_rows = 0
        
        print(f"\n📦 Processing Batch {batch_num}/{len(job_batches)}: {batch['start_date']} to {batch['end_date']}")
        print("-" * 60)
        
        # Generate data for each day in the batch
        current_date = batch['start_date']
        while current_date <= batch['end_date']:
            print(f"📅 Generating data for {current_date.strftime('%Y-%m-%d')}...")
            
            # Process each table
            for table_name, table_config in job['table_configs'].items():
                table_type = table_config.get("type", "snapshot")
                
                if table_type == "incremental":
                    # Generate incremental data for this date
                    base_rows = table_config.get("base_rows_per_day", 10000)
                    
                    # Apply seasonal patterns if enabled
                    if job['enable_seasonal_patterns'] and table_config.get("seasonal_multipliers_enabled", False):
                        day_of_week = current_date.strftime("%A").lower()
                        seasonal_multipliers = job['incremental_config'].get("seasonal_multipliers", {})
                        multiplier = seasonal_multipliers.get(day_of_week, 1.0)
                        adjusted_rows = int(base_rows * multiplier)
                    else:
                        adjusted_rows = base_rows
                    
                    # Generate table data using incremental generator with correct date
                    # Pass the current_date as the generation_date
                    state = {}  # You can maintain state across batches if needed
                    
                    if table_name == "customers":
                        table_df = generator._generate_customers_incremental(adjusted_rows, current_date, state)
                    elif table_name == "products":
                        table_df = generator._generate_products_incremental(adjusted_rows, current_date, state)
                    elif table_name == "orders":
                        # For orders, use incremental generator with proper date
                        table_df = generator._generate_orders_incremental(adjusted_rows, current_date, state)
                    elif table_name == "order_items":
                        # For order items, use incremental generator
                        table_df = generator._generate_order_items_incremental(adjusted_rows, current_date, state)
                    else:
                        # Generic table generation
                        table_df = generator._generate_generic_table_incremental(table_name, adjusted_rows, current_date, state)
                    
                    # Store in batch tables for dependencies
                    batch_tables[table_name] = table_df
                    
                    # Save the data with appropriate path format
                    if job['output_mode'] == "table":
                        final_table_name = f"synth_data_{job['job_name']}_{table_name}_incremental_{current_date.strftime('%Y%m%d')}"
                        lh_utils.write_to_table(table_df, final_table_name, mode="overwrite", options={"overwriteSchema": "true"})
                        print(f"    ✅ {table_name}: {table_df.count():,} rows → table '{final_table_name}'")
                    elif job['output_mode'] in ["parquet", "csv"]:
                        # For file outputs, save with consistent path structure
                        if job['path_format'] == "flat":
                            file_path = f"synthetic_data/{job['output_mode']}/series/{job['dataset_id']}/{job['job_name']}/flat/{table_name}/{table_name}_{current_date.strftime('%Y%m%d')}.{job['output_mode']}"
                        else:  # nested
                            year = current_date.strftime('%Y')
                            month = current_date.strftime('%m')
                            day = current_date.strftime('%d')
                            file_path = f"synthetic_data/{job['output_mode']}/series/{job['dataset_id']}/{job['job_name']}/nested/{table_name}/{year}/{month}/{day}/data.{job['output_mode']}"
                        
                        lh_utils.write_file(table_df, file_path, job['output_mode'], options={"header": "true", "mode": "overwrite"})
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
                        # Use incremental generator's snapshot method
                        table_df = generator._generate_generic_snapshot(table_name, base_rows, current_date)
                        batch_tables[table_name] = table_df
                        
                        if job['output_mode'] == "table":
                            snapshot_table_name = f"synth_data_{job['job_name']}_{table_name}_snapshot_{current_date.strftime('%Y%m%d')}"
                            lh_utils.write_to_table(table_df, snapshot_table_name, mode="overwrite", options={"overwriteSchema": "true"})
                            print(f"    ✅ {table_name} (snapshot): {table_df.count():,} rows → table '{snapshot_table_name}'")
                        elif job['output_mode'] in ["parquet", "csv"]:
                            # For snapshot files, use consistent path structure
                            if job['path_format'] == "flat":
                                file_path = f"synthetic_data/{job['output_mode']}/series/{job['dataset_id']}/{job['job_name']}/flat/snapshot_{table_name}/snapshot_{table_name}_{current_date.strftime('%Y%m%d')}.{job['output_mode']}"
                            else:  # nested
                                year = current_date.strftime('%Y')
                                month = current_date.strftime('%m')
                                day = current_date.strftime('%d')
                                file_path = f"synthetic_data/{job['output_mode']}/series/{job['dataset_id']}/{job['job_name']}/nested/snapshot_{table_name}/{year}/{month}/{day}/data.{job['output_mode']}"
                            
                            lh_utils.write_file(table_df, file_path, job['output_mode'], options={"header": "true", "mode": "overwrite"})
                            print(f"    ✅ {table_name} (snapshot): {table_df.count():,} rows → {file_path}")
                        
                        batch_rows += table_df.count()
                        total_rows_generated += table_df.count()
        
            current_date += timedelta(days=1)
        
        # Log batch completion
        batch_duration = time.time() - batch_start_time
        job_generation_log.append({
            "batch": batch_num,
            "start_date": batch['start_date'].isoformat(),
            "end_date": batch['end_date'].isoformat(), 
            "days": batch['days'],
            "rows_generated": batch_rows,
            "duration_seconds": batch_duration
        })
        
        print(f"✅ Batch {batch_num} complete: {batch_rows:,} rows in {batch_duration:.1f}s")
        total_tables_generated += len(batch_tables)
        job_total_rows += batch_rows
    
    # Log job completion
    job_duration = time.time() - job_start_time
    job_results.append({
        "job_name": job['job_name'],
        "dataset_id": job['dataset_id'],
        "start_date": job['start_date'],
        "end_date": job['end_date'],
        "total_rows": job_total_rows,
        "total_batches": len(job_batches),
        "duration": job_duration,
        "generation_log": job_generation_log
    })
    
    print(f"\n✅ Job '{job['job_name']}' complete: {job_total_rows:,} rows in {job_duration:.1f}s")



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
print(f"{'Total Jobs':<25}: {len(valid_jobs)}")
print(f"{'Tables Generated':<25}: {total_tables_generated}")
print(f"{'Total Rows':<25}: {total_rows_generated:,}")
print(f"{'Execution Time':<25}: {execution_time:.2f} seconds")
print("=" * 80)

# Display job-by-job summary
print("\nJOB SUMMARY:")
print("-" * 100)
print(f"{'Job Name':<30} {'Dataset':<25} {'Date Range':<25} {'Rows':<12} {'Duration':<10}")
print("-" * 100)
for job_result in job_results:
    job_name = job_result['job_name']
    dataset_id = job_result['dataset_id']
    date_range = f"{job_result['start_date']} to {job_result['end_date']}"
    rows = f"{job_result['total_rows']:,}"
    duration = f"{job_result['duration']:.1f}s"
    print(f"{job_name:<30} {dataset_id:<25} {date_range:<25} {rows:<12} {duration:<10}")
print("-" * 100)

# Display detailed batch breakdown for each job
if len(valid_jobs) == 1:
    # Single job - show full batch breakdown
    job_result = job_results[0]
    print(f"\nBATCH BREAKDOWN FOR '{job_result['job_name']}':")
    print("-" * 80)
    print(f"{'Batch':<8} {'Date Range':<25} {'Days':<6} {'Rows':<12} {'Duration':<12}")
    print("-" * 80)
    for log_entry in job_result['generation_log']:
        batch_num = log_entry['batch']
        date_range = f"{log_entry['start_date']} to {log_entry['end_date']}"
        days = log_entry['days']
        rows = f"{log_entry['rows_generated']:,}"
        duration = f"{log_entry['duration_seconds']:.1f}s"
        print(f"{batch_num:<8} {date_range:<25} {days:<6} {rows:<12} {duration:<12}")
else:
    # Multiple jobs - show summary statistics per job
    print("\nJOB STATISTICS:")
    print("-" * 80)
    for job_result in job_results:
        total_days = sum(log['days'] for log in job_result['generation_log'])
        avg_rows_per_day = job_result['total_rows'] / total_days if total_days > 0 else 0
        avg_rows_per_batch = job_result['total_rows'] / job_result['total_batches'] if job_result['total_batches'] > 0 else 0
        print(f"\n{job_result['job_name']}:")
        print(f"  • Total batches: {job_result['total_batches']}")
        print(f"  • Average rows/day: {avg_rows_per_day:,.0f}")
        print(f"  • Average rows/batch: {avg_rows_per_batch:,.0f}")
        print(f"  • Processing rate: {job_result['total_rows']/job_result['duration']:,.0f} rows/second")

print("=" * 80)
print(f"⚡ Overall processing rate: {total_rows_generated/execution_time:,.0f} rows/second")

# Log completion
if 'notebookutils' in globals() and hasattr(notebookutils, 'log'):
    notebookutils.log(f"Incremental series generation completed: {len(valid_jobs)} jobs, {total_rows_generated:,} rows, {execution_time:.1f}s")

print(f"\n🎉 Incremental synthetic data series generation completed successfully!")



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

