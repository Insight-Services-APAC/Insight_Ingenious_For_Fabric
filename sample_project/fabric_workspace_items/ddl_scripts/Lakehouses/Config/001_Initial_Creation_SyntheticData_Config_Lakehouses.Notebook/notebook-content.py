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
    
    notebookutils.fs.mount("abfss://REPLACE_WITH_CONFIG_WORKSPACE_NAME@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
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

# ## 🗂️ Now Load the Custom Python Libraries

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import *
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.create_instance() 
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        "ingen_fab/python_libs/pyspark/ddl_utils.py",
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py"
    ]

    load_python_modules_from_path(mount_path, files_to_load)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🆕 Instantiate Required Classes 

# CELL ********************


target_lakehouse_config_prefix = "Config"
configs: ConfigsObject = get_configs_as_object()
target_lakehouse_id = get_config_value(f"{target_lakehouse_config_prefix.lower()}_lakehouse_id")
target_workspace_id = get_config_value(f"{target_lakehouse_config_prefix.lower()}_workspace_id")

target_lakehouse = lakehouse_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark  # Pass the Spark session if available
)

du = ddl_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark  # Pass the Spark session if available
)

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType
)




# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🏃‍♂️‍➡️ Run DDL Cells 

# CELL ********************


# DDL cells are injected below:



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 001_config_synthetic_data_datasets.py

# CELL ********************

guid="d0f1058af1d3"
object_name = "001_config_synthetic_data_datasets"

def script_to_execute():
    # Configuration table for synthetic data generation datasets (Lakehouse version)
    # This creates a Delta table to define available dataset templates
    
    from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType
    
    # Define schema for config_synthetic_data_datasets table
    config_datasets_schema = StructType([
        StructField("dataset_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("dataset_type", StringType(), False),  # 'transactional' or 'analytical'
        StructField("schema_pattern", StringType(), False),  # 'oltp', 'star_schema', 'snowflake'
        StructField("domain", StringType(), False),  # e.g., 'retail', 'finance', 'healthcare'
        StructField("max_recommended_rows", LongType(), False),
        StructField("description", StringType(), True),
        StructField("config_json", StringType(), True),  # JSON configuration
        StructField("is_active", BooleanType(), False),
        StructField("created_date", TimestampType(), False),
        StructField("modified_date", TimestampType(), False)
    ])
    
    target_lakehouse.create_table(
        table_name="config_synthetic_data_datasets",
        schema=config_datasets_schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"
        }
    )
    
    print("✅ Created config_synthetic_data_datasets Delta table")
    

du.run_once(script_to_execute, "001_config_synthetic_data_datasets","d0f1058af1d3")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 002_config_synthetic_data_generation_jobs.py

# CELL ********************

guid="5f17bf2297f1"
object_name = "002_config_synthetic_data_generation_jobs"

def script_to_execute():
    # Configuration table for synthetic data generation jobs (Lakehouse version)
    # This creates a Delta table to track generation requests and their parameters
    
    from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
    
    # Define schema for config_synthetic_data_generation_jobs table
    config_jobs_schema = StructType([
        StructField("job_id", StringType(), False),
        StructField("dataset_id", StringType(), False),
        StructField("job_name", StringType(), False),
        StructField("target_rows", LongType(), False),
        StructField("generation_mode", StringType(), False),  # 'python' or 'pyspark'
        StructField("target_environment", StringType(), False),  # 'lakehouse' or 'warehouse'
        StructField("target_location", StringType(), True),  # lakehouse name or schema
        StructField("chunk_size", LongType(), True),
        StructField("parallel_workers", IntegerType(), True),
        StructField("seed_value", IntegerType(), True),
        StructField("custom_config_json", StringType(), True),
        StructField("status", StringType(), False),  # 'pending', 'running', 'completed', 'failed'
        StructField("created_date", TimestampType(), False),
        StructField("started_date", TimestampType(), True),
        StructField("completed_date", TimestampType(), True),
        StructField("error_message", StringType(), True)
    ])
    
    target_lakehouse.create_table(
        table_name="config_synthetic_data_generation_jobs",
        schema=config_jobs_schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"
        }
    )
    
    print("✅ Created config_synthetic_data_generation_jobs Delta table")
    

du.run_once(script_to_execute, "002_config_synthetic_data_generation_jobs","5f17bf2297f1")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 003_log_synthetic_data_generation.py

# CELL ********************

guid="60d2d54b3c1b"
object_name = "003_log_synthetic_data_generation"

def script_to_execute():
    # Log table for synthetic data generation execution tracking (Lakehouse version)
    # This creates a Delta table for detailed execution metrics
    
    from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DecimalType, TimestampType
    
    # Define schema for log_synthetic_data_generation table
    log_schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("job_id", StringType(), False),
        StructField("execution_step", StringType(), False),  # 'initialization', 'generation', 'validation'
        StructField("table_name", StringType(), True),
        StructField("rows_generated", LongType(), True),
        StructField("chunk_number", IntegerType(), True),
        StructField("execution_time_ms", LongType(), True),
        StructField("memory_usage_mb", DecimalType(10, 2), True),
        StructField("status", StringType(), False),  # 'started', 'completed', 'failed'
        StructField("message", StringType(), True),
        StructField("error_details", StringType(), True),
        StructField("execution_timestamp", TimestampType(), False)
    ])
    
    target_lakehouse.create_table(
        table_name="log_synthetic_data_generation",
        schema=log_schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"
        }
    )
    
    print("✅ Created log_synthetic_data_generation Delta table")
    

du.run_once(script_to_execute, "003_log_synthetic_data_generation","60d2d54b3c1b")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 004_sample_dataset_configurations.py

# CELL ********************

guid="24bacda13674"
object_name = "004_sample_dataset_configurations"

def script_to_execute():
    # Sample dataset configurations (Lakehouse version)
    # Insert predefined dataset templates into Delta table
    
    from datetime import datetime
    
    from pyspark.sql import Row
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    
    # Sample dataset configurations data using Row objects
    sample_configs = [
        # Retail Transactional (OLTP)
        Row(
            dataset_id="retail_oltp_small",
            dataset_name="Retail OLTP - Small",
            dataset_type="transactional",
            schema_pattern="oltp",
            domain="retail",
            max_recommended_rows=1000000,
            description="Small retail transactional system with customers, orders, products",
            config_json='{"tables": ["customers", "products", "orders", "order_items"], "relationships": "normalized"}',
            is_active=True,
            created_date=datetime(2024, 1, 15, 0, 0),
            modified_date=datetime(2024, 1, 15, 0, 0)
        ),
        Row(
            dataset_id="retail_oltp_large",
            dataset_name="Retail OLTP - Large",
            dataset_type="transactional",
            schema_pattern="oltp",
            domain="retail",
            max_recommended_rows=1000000000,
            description="Large-scale retail transactional system",
            config_json='{"tables": ["customers", "products", "orders", "order_items"], "relationships": "normalized", "partitioning": "date"}',
            is_active=True,
            created_date=datetime(2024, 1, 15, 0, 0),
            modified_date=datetime(2024, 1, 15, 0, 0)
        ),
        # Retail Analytics (Star Schema)
        Row(
            dataset_id="retail_star_small",
            dataset_name="Retail Star Schema - Small",
            dataset_type="analytical",
            schema_pattern="star_schema",
            domain="retail",
            max_recommended_rows=10000000,
            description="Small retail data warehouse with fact_sales and dimensions",
            config_json='{"fact_tables": ["fact_sales"], "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store"]}',
            is_active=True,
            created_date=datetime(2024, 1, 15, 0, 0),
            modified_date=datetime(2024, 1, 15, 0, 0)
        ),
        Row(
            dataset_id="retail_star_large",
            dataset_name="Retail Star Schema - Large",
            dataset_type="analytical",
            schema_pattern="star_schema",
            domain="retail",
            max_recommended_rows=5000000000,
            description="Large retail data warehouse with multiple fact tables",
            config_json='{"fact_tables": ["fact_sales", "fact_inventory"], "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store", "dim_supplier"]}',
            is_active=True,
            created_date=datetime(2024, 1, 15, 0, 0),
            modified_date=datetime(2024, 1, 15, 0, 0)
        ),
        # Financial Transactional
        Row(
            dataset_id="finance_oltp_small",
            dataset_name="Financial OLTP - Small",
            dataset_type="transactional",
            schema_pattern="oltp",
            domain="finance",
            max_recommended_rows=500000,
            description="Small financial system with accounts, transactions, customers",
            config_json='{"tables": ["customers", "accounts", "transactions", "account_types"], "compliance": "pci_dss"}',
            is_active=True,
            created_date=datetime(2024, 1, 15, 0, 0),
            modified_date=datetime(2024, 1, 15, 0, 0)
        ),
        Row(
            dataset_id="finance_oltp_large",
            dataset_name="Financial OLTP - Large",
            dataset_type="transactional",
            schema_pattern="oltp",
            domain="finance",
            max_recommended_rows=2000000000,
            description="Large financial system with high transaction volume",
            config_json='{"tables": ["customers", "accounts", "transactions", "account_types"], "compliance": "pci_dss", "high_frequency": true}',
            is_active=True,
            created_date=datetime(2024, 1, 15, 0, 0),
            modified_date=datetime(2024, 1, 15, 0, 0)
        ),
        # E-commerce Analytics
        Row(
            dataset_id="ecommerce_star_small",
            dataset_name="E-commerce Star Schema - Small",
            dataset_type="analytical",
            schema_pattern="star_schema",
            domain="ecommerce",
            max_recommended_rows=5000000,
            description="E-commerce analytics with web events and sales",
            config_json='{"fact_tables": ["fact_web_events", "fact_orders"], "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date"]}',
            is_active=True,
            created_date=datetime(2024, 1, 15, 0, 0),
            modified_date=datetime(2024, 1, 15, 0, 0)
        ),
        Row(
            dataset_id="ecommerce_star_large",
            dataset_name="E-commerce Star Schema - Large",
            dataset_type="analytical",
            schema_pattern="star_schema",
            domain="ecommerce",
            max_recommended_rows=10000000000,
            description="Large e-commerce analytics with clickstream data",
            config_json='{"fact_tables": ["fact_web_events", "fact_orders", "fact_page_views"], "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date", "dim_geography"]}',
            is_active=True,
            created_date=datetime(2024, 1, 15, 0, 0),
            modified_date=datetime(2024, 1, 15, 0, 0)
        )
    ]
    
    # Define schema
    schema = StructType([
        StructField("dataset_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("dataset_type", StringType(), False),
        StructField("schema_pattern", StringType(), False),
        StructField("domain", StringType(), False),
        StructField("max_recommended_rows", LongType(), False),
        StructField("description", StringType(), True),
        StructField("config_json", StringType(), True),
        StructField("is_active", BooleanType(), False),
        StructField("created_date", TimestampType(), False),
        StructField("modified_date", TimestampType(), True)
    ])
    
    # Create DataFrame from sample data using injected utils
    sample_df = target_lakehouse.get_connection.createDataFrame(sample_configs, schema)
    
    # Insert into the config table using injected utils
    target_lakehouse.write_to_table(
        df=sample_df,
        table_name="config_synthetic_data_datasets",
        mode="append"
    )
    
    print(f"✅ Inserted {len(sample_configs)} sample dataset configurations")
    
    # Show what was inserted using injected utils
    configs_table = target_lakehouse.read_table("config_synthetic_data_datasets")
    print("📋 Sample configurations:")
    configs_table.select("dataset_id", "dataset_name", "dataset_type", "domain").show(truncate=False)
    

du.run_once(script_to_execute, "004_sample_dataset_configurations","24bacda13674")

def script_to_execute():
    print("Script block is empty. No action taken.")






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📇 Print the execution log

# CELL ********************



du.print_log() 




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✔️ If we make it to the end return a successful result

# CELL ********************



notebookutils.exit_notebook("success")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

