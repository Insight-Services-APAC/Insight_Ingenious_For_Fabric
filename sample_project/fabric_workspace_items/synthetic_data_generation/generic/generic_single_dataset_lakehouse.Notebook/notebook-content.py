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




# Job configurations - can be a single dict or list of dicts
# Multiple job configurations example - generate different dataset sizes and types
job_configs = [
    {
        "job_name": "development_small_dataset",
        "dataset_id": "retail_oltp_small",
        "target_rows": 5000,
        "chunk_size": 100000,
        "output_mode": "csv",
        "seed_value": 12345,
        "generation_mode": "auto",
        "scale_factor": 0.5,                        # Smaller scale for dev
        "enable_relationships": True,
        "enable_partitioning": False,
        "custom_schema": None
    },
    {
        "job_name": "testing_medium_dataset",
        "dataset_id": "retail_oltp_small",
        "target_rows": 50000,
        "chunk_size": 250000,
        "output_mode": "parquet",
        "seed_value": 67890,
        "generation_mode": "pyspark",
        "scale_factor": 2.0,                        # Medium scale for testing
        "enable_relationships": True,
        "enable_partitioning": True,
        "custom_schema": None
    },
    {
        "job_name": "production_large_dataset",
        "dataset_id": "retail_oltp_large",
        "target_rows": 1000000,
        "chunk_size": 500000,
        "output_mode": "parquet",
        "seed_value": 11111,
        "generation_mode": "pyspark",
        "scale_factor": 10.0,                       # Large scale for production-like data
        "enable_relationships": True,
        "enable_partitioning": True,
        "custom_schema": None
    },
    {
        "job_name": "analytics_star_schema",
        "dataset_id": "retail_star_large",
        "target_rows": 500000,
        "chunk_size": 1000000,
        "output_mode": "parquet",
        "seed_value": 22222,
        "generation_mode": "pyspark",
        "scale_factor": 5.0,                        # Star schema for analytics
        "enable_relationships": True,
        "enable_partitioning": True,
        "custom_schema": None
    },
    {
        "job_name": "csv_export_sample",
        "dataset_id": "retail_oltp_small",
        "target_rows": 25000,
        "chunk_size": 50000,
        "output_mode": "csv",
        "seed_value": 33333,
        "generation_mode": "auto",
        "scale_factor": 1.5,                        # Medium scale for CSV export
        "enable_relationships": False,              # Simplified relationships for CSV
        "enable_partitioning": False,
        "custom_schema": None
    }
]

# Single job configuration (backward compatible) - uncomment to use
# job_configs = {
#     "dataset_id": "retail_oltp_small",
#     "target_rows": 10000,
#     "chunk_size": 1000000,
#     "output_mode": "parquet",
#     "seed_value": None,
#     "generation_mode": "auto",
#     "scale_factor": 1.0,
#     "enable_relationships": True,
#     "enable_partitioning": False,
#     "custom_schema": None
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
import json

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
        target_rows = job_config.get("target_rows", 10000)
        chunk_size = job_config.get("chunk_size", 1000000)
        output_mode = job_config.get("output_mode", "parquet")
        seed_value = job_config.get("seed_value", None)
        generation_mode = job_config.get("generation_mode", "auto")
        scale_factor = job_config.get("scale_factor", 1.0)
        enable_relationships = job_config.get("enable_relationships", True)
        enable_partitioning = job_config.get("enable_partitioning", False)
        custom_schema = job_config.get("custom_schema", None)
        
        # Resolve dataset configuration dynamically
        dataset_config = DatasetConfigurationRepository.get_predefined_dataset(dataset_id)
        
        # Apply custom schema if provided
        if custom_schema:
            try:
                schema_override = json.loads(custom_schema)
                dataset_config.update(schema_override)
                print(f"🔧 Applied custom schema override for '{job_name}'")
            except json.JSONDecodeError as e:
                print(f"⚠️ Warning: Invalid custom schema JSON for '{job_name}', ignoring: {e}")
        
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
        
        # Create validated job configuration
        validated_job = {
            "job_name": job_name,
            "dataset_id": dataset_id,
            "dataset_config": dataset_config,
            "target_rows": target_rows,
            "chunk_size": chunk_size,
            "output_mode": output_mode,
            "seed_value": seed_value,
            "generation_mode": generation_mode,
            "scale_factor": scale_factor,
            "enable_relationships": enable_relationships,
            "enable_partitioning": enable_partitioning,
            "custom_schema": custom_schema,
            "schema_pattern": schema_pattern,
            "customer_rows": customer_rows if 'customer_rows' in locals() else 0,
            "product_rows": product_rows if 'product_rows' in locals() else 0,
            "order_rows": order_rows if 'order_rows' in locals() else 0,
            "order_item_rows": order_item_rows if 'order_item_rows' in locals() else 0,
            "dim_customer_rows": dim_customer_rows if 'dim_customer_rows' in locals() else 0,
            "dim_product_rows": dim_product_rows if 'dim_product_rows' in locals() else 0,
            "dim_store_rows": dim_store_rows if 'dim_store_rows' in locals() else 0,
            "fact_sales_rows": fact_sales_rows if 'fact_sales_rows' in locals() else 0
        }
        
        valid_jobs.append(validated_job)
        print(f"✅ Validated job '{job_name}': {dataset_config['dataset_name']} ({target_rows:,} target rows)")
        
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

# ## 📊 Job Queue Information

# CELL ********************


# Display job queue information
print("=" * 80)
print("SYNTHETIC DATA GENERATION - JOB QUEUE")
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
    print(f"{'Target Rows':<25}: {job['target_rows']:,}")
    print(f"{'Scale Factor':<25}: {job['scale_factor']}")
    print(f"{'Chunk Size':<25}: {job['chunk_size']:,}")
    print(f"{'Output Mode':<25}: {job['output_mode']}")
    print(f"{'Generation Mode':<25}: {job['generation_mode']}")
    print(f"{'Enable Relationships':<25}: {job['enable_relationships']}")
    print(f"{'Enable Partitioning':<25}: {job['enable_partitioning']}")
print("\n" + "=" * 80)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔄 Generate Synthetic Data

# CELL ********************


# Track overall progress
total_rows_generated = 0
execution_start_time = time.time()
job_results = []

print(f"🚀 Starting synthetic data generation for {len(valid_jobs)} jobs...")

# Process each job
for job_idx, job in enumerate(valid_jobs, 1):
    job_start_time = time.time()
    generated_tables = {}
    job_total_rows = 0
    
    print(f"\n{'='*80}")
    print(f"🔄 PROCESSING JOB {job_idx}/{len(valid_jobs)}: {job['job_name']}")
    print(f"{'='*80}")
    print(f"Dataset: {job['dataset_config']['dataset_name']}")
    print(f"Target Rows: {job['target_rows']:,}")
    print(f"Output Mode: {job['output_mode']}")
    
    # Update generator seed if specified
    if job['seed_value'] is not None:
        generator = PySparkSyntheticDataGenerator(
            lakehouse_utils_instance=lh_utils,
            seed=job['seed_value']
        )
        dataset_builder = PySparkDatasetBuilder(generator)
    
    schema_pattern = job['schema_pattern']
    
    if schema_pattern == "oltp":
        # Generate OLTP tables
        print("📋 Generating OLTP dataset...")
        
        if "customers" in job['dataset_config'].get("tables", []):
            print(f"  📋 Generating customers table ({job['customer_rows']:,} rows)...")
            customers_df = generator.generate_customers_table(job['customer_rows'])
            generated_tables["customers"] = customers_df
            job_total_rows += customers_df.count()
            print(f"    ✅ Generated {customers_df.count():,} customer records")
            
        if "products" in job['dataset_config'].get("tables", []):
            print(f"  🏷️ Generating products table ({job['product_rows']:,} rows)...")
            products_df = generator.generate_products_table(job['product_rows'])
            generated_tables["products"] = products_df
            job_total_rows += products_df.count()
            print(f"    ✅ Generated {products_df.count():,} product records")
            
        if "orders" in job['dataset_config'].get("tables", []) and "customers" in generated_tables:
            print(f"  🛒 Generating orders table ({job['order_rows']:,} rows)...")
            orders_df = generator.generate_orders_table(
                job['order_rows'],
                generated_tables["customers"],
                generated_tables.get("products")
            )
            generated_tables["orders"] = orders_df
            job_total_rows += orders_df.count()
            print(f"    ✅ Generated {orders_df.count():,} order records")
            
        if "order_items" in job['dataset_config'].get("tables", []) and "orders" in generated_tables:
            print(f"  📦 Generating order items table...")
            order_items_df = generator.generate_order_items_table(
                generated_tables["orders"],
                generated_tables.get("products")
            )
            generated_tables["order_items"] = order_items_df
            job_total_rows += order_items_df.count()
            print(f"    ✅ Generated {order_items_df.count():,} order item records")
    
    elif schema_pattern == "star_schema":
        # Generate star schema tables
        print("📐 Generating Star Schema dataset...")
        
        dimensions = {}
        
        # Generate dimension tables
        for dim in job['dataset_config'].get("dimensions", []):
            print(f"  📐 Generating {dim}...")
            if dim == "dim_date":
                dim_df = generator.generate_date_dimension(2020, 2025)
            elif dim == "dim_customer":
                dim_df = generator.generate_customers_table(job['dim_customer_rows'])
            elif dim == "dim_product":
                dim_df = generator.generate_products_table(job['dim_product_rows'])
            elif dim == "dim_store":
                dim_df = generator.generate_customers_table(job['dim_store_rows'])  # Generic store data
            else:
                continue
                
            dimensions[dim] = dim_df
            generated_tables[dim] = dim_df
            job_total_rows += dim_df.count()
            print(f"    ✅ Generated {dim_df.count():,} records for {dim}")
        
        # Generate fact tables
        for fact in job['dataset_config'].get("fact_tables", []):
            print(f"  📊 Generating {fact} ({job['fact_sales_rows']:,} rows)...")
            if fact == "fact_sales":
                fact_df = generator.generate_fact_sales(job['fact_sales_rows'], dimensions)
                generated_tables[fact] = fact_df
                job_total_rows += fact_df.count()
                print(f"    ✅ Generated {fact_df.count():,} records for {fact}")

    else:
        # Custom schema - use dataset builder
        print("🔧 Generating custom dataset using dataset builder...")
        dataset_tables = dataset_builder.build_custom_dataset(job['dataset_config'])
        
        for table_name, table_df in dataset_tables.items():
            generated_tables[table_name] = table_df
            job_total_rows += table_df.count()
            print(f"  ✅ Generated {table_name}: {table_df.count():,} rows")

    # Save generated tables based on output mode
    print("💾 Saving generated tables...")
    
    if job['output_mode'] == "table":
        print("  📊 Saving to lakehouse tables...")
        for table_name, table_df in generated_tables.items():
            write_mode = "overwrite"
            if job['enable_partitioning'] and table_name in ["orders", "order_items", "fact_sales"]:
                # Enable partitioning for large transactional tables
                partition_cols = ["order_date"] if "order_date" in table_df.columns else None
                prefixed_table_name = f"synth_data_{job['job_name']}_{table_name}"
                lh_utils.write_to_table(
                    table_df, 
                    prefixed_table_name, 
                    mode=write_mode,
                    partition_by=partition_cols
                )
                print(f"    ✅ Saved {prefixed_table_name} (partitioned) with {table_df.count():,} rows")
            else:
                prefixed_table_name = f"synth_data_{job['job_name']}_{table_name}"
                lh_utils.write_to_table(table_df, prefixed_table_name, mode=write_mode)
                print(f"    ✅ Saved {prefixed_table_name} with {table_df.count():,} rows")

    elif job['output_mode'] == "parquet":
        print("  📄 Saving as Parquet files...")
        output_path = f"synthetic_data/parquet/single/{job['dataset_id']}/{job['job_name']}"
        generator.export_to_parquet(generated_tables, output_path)
        for table_name, table_df in generated_tables.items():
            print(f"    ✅ Saved {output_path}/{table_name}.parquet with {table_df.count():,} rows")

    elif job['output_mode'] == "csv":
        print("  📄 Saving as CSV files...")
        output_path = f"synthetic_data/csv/single/{job['dataset_id']}/{job['job_name']}"
        generator.export_to_csv(generated_tables, output_path)
        for table_name, table_df in generated_tables.items():
            print(f"    ✅ Saved {output_path}/{table_name}.csv with {table_df.count():,} rows")
    
    # Calculate job completion time
    job_end_time = time.time()
    job_execution_time = job_end_time - job_start_time
    
    # Store job results
    job_result = {
        "job_name": job['job_name'],
        "dataset_id": job['dataset_id'],
        "target_rows": job['target_rows'],
        "actual_rows": job_total_rows,
        "tables_generated": len(generated_tables),
        "execution_time": job_execution_time,
        "output_mode": job['output_mode']
    }
    job_results.append(job_result)
    total_rows_generated += job_total_rows
    
    print(f"\n✅ Job '{job['job_name']}' completed in {job_execution_time:.2f}s: {job_total_rows:,} rows across {len(generated_tables)} tables")



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
print(f"{'Total Jobs Processed':<25}: {len(job_results)}")
print(f"{'Target Environment':<25}: {target_environment}")
print(f"{'Total Rows Generated':<25}: {total_rows_generated:,}")
print(f"{'Total Execution Time':<25}: {execution_time:.2f} seconds")
print("=" * 80)

# Display job-by-job breakdown
print("\nJOB-BY-JOB BREAKDOWN:")
print("-" * 100)
print(f"{'Job Name':<25} {'Dataset':<20} {'Target':<10} {'Actual':<10} {'Tables':<8} {'Time':<8} {'Output'}")
print("-" * 100)
for result in job_results:
    print(f"{result['job_name']:<25} {result['dataset_id']:<20} {result['target_rows']:>9,} {result['actual_rows']:>9,} {result['tables_generated']:>7} {result['execution_time']:>7.1f}s {result['output_mode']}")

print("-" * 100)
print(f"{'TOTALS':<25} {'':<20} {sum(r['target_rows'] for r in job_results):>9,} {total_rows_generated:>9,} {sum(r['tables_generated'] for r in job_results):>7} {execution_time:>7.1f}s")

print("=" * 80)
print(f"⚡ Overall processing rate: {total_rows_generated/execution_time:,.0f} rows/second")

# Log completion
if 'notebookutils' in globals() and hasattr(notebookutils, 'log'):
    notebookutils.log(f"Multi-job synthetic data generation completed: {len(job_results)} jobs, {total_rows_generated:,} total rows, {execution_time:.1f}s")

print(f"\n🎉 All {len(job_results)} synthetic data generation jobs completed successfully!")



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

