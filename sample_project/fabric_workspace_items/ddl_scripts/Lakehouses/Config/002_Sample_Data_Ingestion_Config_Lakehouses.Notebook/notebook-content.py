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
        StructField("modified_by", StringType(), nullable=True),
        # Advanced CSV configuration fields
        StructField("quote_character", StringType(), nullable=True),  # Default: '"'
        StructField("escape_character", StringType(), nullable=True),  # Default: '"' (Excel style)
        StructField("multiline_values", BooleanType(), nullable=True),  # Default: True
        StructField("ignore_leading_whitespace", BooleanType(), nullable=True),  # Default: False
        StructField("ignore_trailing_whitespace", BooleanType(), nullable=True),  # Default: False
        StructField("null_value", StringType(), nullable=True),  # Default: ""
        StructField("empty_value", StringType(), nullable=True),  # Default: ""
        StructField("comment_character", StringType(), nullable=True),  # Default: None
        StructField("max_columns", IntegerType(), nullable=True),  # Default: 100
        StructField("max_chars_per_column", IntegerType(), nullable=True)  # Default: 50000
    ])
    
    # Sample configuration records for testing - Using synthetic data generator parquet files
    sample_configs = [
        Row(
            config_id="synthetic_customers_001",
            config_name="Synthetic Data - Customers (Retail OLTP Small)",
            source_file_path="Files/synthetic_data/retail_oltp_small/customers/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
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
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        Row(
            config_id="synthetic_products_002",
            config_name="Synthetic Data - Products (Retail OLTP Small)",
            source_file_path="Files/synthetic_data/retail_oltp_small/products/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
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
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        Row(
            config_id="synthetic_orders_003",
            config_name="Synthetic Data - Orders (Retail OLTP Small)",
            source_file_path="Files/synthetic_data/retail_oltp_small/orders/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
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
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        Row(
            config_id="synthetic_customers_warehouse_001",
            config_name="Synthetic Data - Customers (Warehouse)",
            source_file_path="Files/synthetic_data/retail_oltp_small/customers/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_wh_warehouse_id}}",
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
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        Row(
            config_id="synthetic_products_warehouse_002",
            config_name="Synthetic Data - Products (Warehouse)",
            source_file_path="Files/synthetic_data/retail_oltp_small/products/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_wh_warehouse_id}}",
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
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        Row(
            config_id="synthetic_orders_warehouse_003",
            config_name="Synthetic Data - Orders (Warehouse)",
            source_file_path="Files/synthetic_data/retail_oltp_small/orders/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_wh_warehouse_id}}",
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
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        # Large dataset configurations with partitioning and merge mode
        Row(
            config_id="star_dim_customer_large_001",
            config_name="Star Schema Large - Customer Dimension",
            source_file_path="Files/synthetic_data/retail_star_large/dim_customer/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
            target_datastore_type="lakehouse",
            target_schema_name="star",
            target_table_name="dim_customer_large",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="customer_segment",
            sort_columns="customer_key",
            write_mode="merge",
            merge_keys="customer_key",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=3,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        Row(
            config_id="star_dim_product_large_002",
            config_name="Star Schema Large - Product Dimension",
            source_file_path="Files/synthetic_data/retail_star_large/dim_product/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
            target_datastore_type="lakehouse",
            target_schema_name="star",
            target_table_name="dim_product_large",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="product_category",
            sort_columns="product_key",
            write_mode="merge",
            merge_keys="product_key",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=3,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        Row(
            config_id="star_dim_store_large_003",
            config_name="Star Schema Large - Store Dimension",
            source_file_path="Files/synthetic_data/retail_star_large/dim_store/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
            target_datastore_type="lakehouse",
            target_schema_name="star",
            target_table_name="dim_store_large",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="store_region",
            sort_columns="store_key",
            write_mode="merge",
            merge_keys="store_key",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=3,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        Row(
            config_id="star_dim_date_large_004",
            config_name="Star Schema Large - Date Dimension",
            source_file_path="Files/synthetic_data/retail_star_large/dim_date/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
            target_datastore_type="lakehouse",
            target_schema_name="star",
            target_table_name="dim_date_large",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="",
            sort_columns="date_key",
            write_mode="merge",
            merge_keys="date_key",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=3,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        Row(
            config_id="star_fact_sales_large_005",
            config_name="Star Schema Large - Sales Fact Table",
            source_file_path="Files/synthetic_data/retail_star_large/fact_sales/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
            target_datastore_type="lakehouse",
            target_schema_name="star",
            target_table_name="fact_sales_large",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="date_key,store_key",
            sort_columns="date_key,store_key,product_key",
            write_mode="merge",
            merge_keys="transaction_id",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=3,
            active_yn="Y",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        # Example of partition_swap mode for daily fact data refresh
        Row(
            config_id="daily_sales_partition_swap_001",
            config_name="Daily Sales Data - Partition Swap Mode",
            source_file_path="Files/synthetic_data/daily_sales/",
            source_file_format="parquet",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
            target_datastore_type="lakehouse",
            target_schema_name="staging",
            target_table_name="daily_sales_fact",
            staging_table_name=None,
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="sale_date",
            sort_columns="sale_date,store_id,product_id",
            write_mode="partition_swap",
            merge_keys="",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=4,
            active_yn="N",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None,
            quote_character='"',
            escape_character='"',
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
        ),
        # Example configurations for complex CSV files with advanced parsing
        Row(
            config_id="complex_csv_001",
            config_name="Complex CSV - Customer Comments with Newlines",
            source_file_path="Files/sample_data/complex_customer_feedback.csv",
            source_file_format="csv",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
            target_datastore_type="lakehouse",
            target_schema_name="raw",
            target_table_name="customer_feedback_complex",
            staging_table_name=None,
            file_delimiter=",",
            has_header=True,
            encoding="utf-8",
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="",
            sort_columns="feedback_id",
            write_mode="overwrite",
            merge_keys="",
            data_validation_rules=None,
            error_handling_strategy="fail",
            execution_group=5,
            active_yn="N",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None,
            # Advanced CSV configuration fields
            quote_character='"',
            escape_character='\\',
            multiline_values=True,
            ignore_leading_whitespace=True,
            ignore_trailing_whitespace=True,
            null_value="NULL",
            empty_value="",
            comment_character="#",
            max_columns=50,
            max_chars_per_column=10000
        ),
        Row(
            config_id="complex_csv_002",
            config_name="Complex CSV - Product Descriptions with Quotes and Commas",
            source_file_path="Files/sample_data/products_with_complex_descriptions.csv",
            source_file_format="csv",
            target_workspace_id="{{varlib:config_workspace_id}}",
            target_datastore_id="{{varlib:config_lakehouse_id}}",
            target_datastore_type="lakehouse",
            target_schema_name="raw",
            target_table_name="products_complex_descriptions",
            staging_table_name=None,
            file_delimiter=",",
            has_header=True,
            encoding="utf-8",
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="category",
            sort_columns="product_id",
            write_mode="merge",
            merge_keys="product_id",
            data_validation_rules=None,
            error_handling_strategy="log",
            execution_group=5,
            active_yn="N",
            created_date="2024-01-15",
            modified_date=None,
            created_by="system",
            modified_by=None,
            # Advanced CSV configuration for handling quotes and special characters
            quote_character='"',
            escape_character='"',  # Use double quotes for escaping quotes (Excel style)
            multiline_values=True,
            ignore_leading_whitespace=False,
            ignore_trailing_whitespace=False,
            null_value="",
            empty_value="",
            comment_character=None,
            max_columns=100,
            max_chars_per_column=50000
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



notebookutils.mssparkutils.notebook.exit("success")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

