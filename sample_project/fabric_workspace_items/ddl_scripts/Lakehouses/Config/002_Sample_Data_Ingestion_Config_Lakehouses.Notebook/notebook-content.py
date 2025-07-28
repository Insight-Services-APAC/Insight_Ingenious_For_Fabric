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

# ## 📄 Cell for 003_sample_data_insert.py

# CELL ********************

guid="15fc8b132572"
object_name = "003_sample_data_insert"

def script_to_execute():
    # Sample configuration data for flat file ingestion testing - Universal schema (Lakehouse version)
    from pyspark.sql import Row
    
    # Import the universal schema definition
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    
    schema = StructType([
        StructField("config_id", StringType(), nullable=False),
        StructField("config_name", StringType(), nullable=False),
        StructField("source_file_path", StringType(), nullable=False),
        StructField("source_file_format", StringType(), nullable=False),  # csv, json, parquet, avro, xml
        StructField("target_workspace_id", StringType(), nullable=False),  # Universal field for workspace
        StructField("target_datastore_id", StringType(), nullable=False),  # Universal field for lakehouse/warehouse
        StructField("target_datastore_type", StringType(), nullable=False),  # 'lakehouse' or 'warehouse'
        StructField("target_schema_name", StringType(), nullable=False),
        StructField("target_table_name", StringType(), nullable=False),
        StructField("staging_table_name", StringType(), nullable=True),  # For warehouse COPY INTO staging
        StructField("file_delimiter", StringType(), nullable=True),  # for CSV files
        StructField("has_header", BooleanType(), nullable=True),  # for CSV files
        StructField("encoding", StringType(), nullable=True),  # utf-8, latin-1, etc.
        StructField("date_format", StringType(), nullable=True),  # for date columns
        StructField("timestamp_format", StringType(), nullable=True),  # for timestamp columns
        StructField("schema_inference", BooleanType(), nullable=False),  # whether to infer schema
        StructField("custom_schema_json", StringType(), nullable=True),  # custom schema definition
        StructField("partition_columns", StringType(), nullable=True),  # comma-separated list
        StructField("sort_columns", StringType(), nullable=True),  # comma-separated list
        StructField("write_mode", StringType(), nullable=False),  # overwrite, append, merge
        StructField("merge_keys", StringType(), nullable=True),  # for merge operations
        StructField("data_validation_rules", StringType(), nullable=True),  # JSON validation rules
        StructField("error_handling_strategy", StringType(), nullable=False),  # fail, skip, log
        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False),
        StructField("created_date", StringType(), nullable=False),
        StructField("modified_date", StringType(), nullable=True),
        StructField("created_by", StringType(), nullable=False),
        StructField("modified_by", StringType(), nullable=True)
    ])
    
    # Sample configuration records for testing - Using synthetic data generator parquet files
    sample_configs = [
        Row(
            config_id="synthetic_customers_001",
            config_name="Synthetic Data - Customers (Retail OLTP Small)",
            source_file_path="Files/synthetic_data/retail_oltp_small/customers.parquet",
            source_file_format="parquet",
            target_workspace_id="REPLACE_WITH_CONFIG_WORKSPACE_GUID",
            target_datastore_id="REPLACE_WITH_CONFIG_LAKEHOUSE_GUID",
            target_datastore_type="lakehouse",
            target_schema_name="raw",
            target_table_name="synthetic_customers",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="",
            sort_columns="customer_id",
            write_mode="overwrite",
            merge_keys="",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=1,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None
        ),
        Row(
            config_id="synthetic_products_002",
            config_name="Synthetic Data - Products (Retail OLTP Small)",
            source_file_path="Files/synthetic_data/retail_oltp_small/products.parquet",
            source_file_format="parquet",
            target_workspace_id="REPLACE_WITH_CONFIG_WORKSPACE_GUID",
            target_datastore_id="REPLACE_WITH_CONFIG_LAKEHOUSE_GUID",
            target_datastore_type="lakehouse",
            target_schema_name="raw",
            target_table_name="synthetic_products",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="category",
            sort_columns="product_id",
            write_mode="overwrite",
            merge_keys="",
            data_validation_rules=None,
            error_handling_strategy="log",
            execution_group=1,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None
        ),
        Row(
            config_id="synthetic_orders_003",
            config_name="Synthetic Data - Orders (Retail OLTP Small)",
            source_file_path="Files/synthetic_data/retail_oltp_small/orders.parquet",
            source_file_format="parquet",
            target_workspace_id="REPLACE_WITH_CONFIG_WORKSPACE_GUID",
            target_datastore_id="REPLACE_WITH_CONFIG_LAKEHOUSE_GUID",
            target_datastore_type="lakehouse",
            target_schema_name="raw",
            target_table_name="synthetic_orders",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="",
            sort_columns="order_date",
            write_mode="overwrite",
            merge_keys="",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=1,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None
        ),
        Row(
            config_id="synthetic_order_items_004",
            config_name="Synthetic Data - Order Items (Retail OLTP Small)",
            source_file_path="Files/synthetic_data/retail_oltp_small/order_items.parquet",
            source_file_format="parquet",
            target_workspace_id="REPLACE_WITH_CONFIG_WORKSPACE_GUID",
            target_datastore_id="REPLACE_WITH_CONFIG_LAKEHOUSE_GUID",
            target_datastore_type="lakehouse",
            target_schema_name="raw",
            target_table_name="synthetic_order_items",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="",
            sort_columns="order_id",
            write_mode="overwrite",
            merge_keys="",
            data_validation_rules=None,
            error_handling_strategy="log",
            execution_group=1,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None
        ),
        Row(
            config_id="synthetic_customers_warehouse_001",
            config_name="Synthetic Data - Customers (Warehouse)",
            source_file_path="Files/synthetic_data/retail_oltp_small/customers.parquet",
            source_file_format="parquet",
            target_workspace_id="REPLACE_WITH_CONFIG_WORKSPACE_GUID",
            target_datastore_id="REPLACE_WITH_CONFIG_WAREHOUSE_GUID",
            target_datastore_type="warehouse",
            target_schema_name="raw",
            target_table_name="synthetic_customers",
            staging_table_name="staging_synthetic_customers",
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="",
            sort_columns="customer_id",
            write_mode="overwrite",
            merge_keys="",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=2,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None
        ),
        Row(
            config_id="synthetic_products_warehouse_002",
            config_name="Synthetic Data - Products (Warehouse)",
            source_file_path="Files/synthetic_data/retail_oltp_small/products.parquet",
            source_file_format="parquet",
            target_workspace_id="REPLACE_WITH_CONFIG_WORKSPACE_GUID",
            target_datastore_id="REPLACE_WITH_CONFIG_WAREHOUSE_GUID",
            target_datastore_type="warehouse",
            target_schema_name="raw",
            target_table_name="synthetic_products",
            staging_table_name="staging_synthetic_products",
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="",
            sort_columns="product_id",
            write_mode="overwrite",
            merge_keys="",
            data_validation_rules=None,
            error_handling_strategy="log",
            execution_group=2,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None
        ),
        Row(
            config_id="synthetic_orders_warehouse_003",
            config_name="Synthetic Data - Orders (Warehouse)",
            source_file_path="Files/synthetic_data/retail_oltp_small/orders.parquet",
            source_file_format="parquet",
            target_workspace_id="REPLACE_WITH_CONFIG_WORKSPACE_GUID",
            target_datastore_id="REPLACE_WITH_CONFIG_WAREHOUSE_GUID",
            target_datastore_type="warehouse",
            target_schema_name="raw",
            target_table_name="synthetic_orders",
            staging_table_name="staging_synthetic_orders",
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="",
            sort_columns="order_date",
            write_mode="overwrite",
            merge_keys="",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=2,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None
        )
    ]
    
    # Create DataFrame and insert records
    df = target_lakehouse.get_connection.createDataFrame(sample_configs, schema)
    target_lakehouse.write_to_table(
        df=df,
        table_name="config_flat_file_ingestion",
        mode="append"
    )
    
    print("✓ Inserted " + str(len(sample_configs)) + " sample configuration records using synthetic data generator parquet files")
    

du.run_once(script_to_execute, "003_sample_data_insert","15fc8b132572")

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

