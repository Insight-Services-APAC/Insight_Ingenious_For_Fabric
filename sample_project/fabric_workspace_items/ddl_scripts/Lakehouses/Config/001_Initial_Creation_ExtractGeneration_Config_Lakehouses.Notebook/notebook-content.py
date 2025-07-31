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

# ## 📄 Cell for 001_config_extract_generation_create.py

# CELL ********************

guid="50175b504e6a"
object_name = "001_config_extract_generation_create"

def script_to_execute():
    # Configuration tables for extract generation metadata - Lakehouse version
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    
    # Extract configuration main table schema
    extract_config_schema = StructType([
        StructField("extract_name", StringType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("trigger_name", StringType(), nullable=True),
        StructField("extract_pipeline_name", StringType(), nullable=True),
        
        # Source configuration
        StructField("extract_table_name", StringType(), nullable=True),
        StructField("extract_table_schema", StringType(), nullable=True),
        StructField("extract_view_name", StringType(), nullable=True),
        StructField("extract_view_schema", StringType(), nullable=True),
        
        # Load configuration
        StructField("is_full_load", BooleanType(), nullable=False),
        StructField("execution_group", StringType(), nullable=True),
        
        # Audit fields
        StructField("created_date", StringType(), nullable=False),
        StructField("created_by", StringType(), nullable=False),
        StructField("modified_date", StringType(), nullable=True),
        StructField("modified_by", StringType(), nullable=True),
    ])
    
    # Extract file details configuration schema
    extract_details_schema = StructType([
        StructField("extract_name", StringType(), nullable=False),
        StructField("file_generation_group", StringType(), nullable=True),
        StructField("extract_container", StringType(), nullable=False),
        StructField("extract_directory", StringType(), nullable=True),
        
        # File naming
        StructField("extract_file_name", StringType(), nullable=False),
        StructField("extract_file_name_timestamp_format", StringType(), nullable=True),
        StructField("extract_file_name_period_end_day", IntegerType(), nullable=True),
        StructField("extract_file_name_extension", StringType(), nullable=False),
        StructField("extract_file_name_ordering", IntegerType(), nullable=True),
        
        # File properties
        StructField("file_properties_column_delimiter", StringType(), nullable=False),
        StructField("file_properties_row_delimiter", StringType(), nullable=False),
        StructField("file_properties_encoding", StringType(), nullable=False),
        StructField("file_properties_quote_character", StringType(), nullable=False),
        StructField("file_properties_escape_character", StringType(), nullable=False),
        StructField("file_properties_header", BooleanType(), nullable=False),
        StructField("file_properties_null_value", StringType(), nullable=False),
        StructField("file_properties_max_rows_per_file", IntegerType(), nullable=True),
        
        # Output format
        StructField("output_format", StringType(), nullable=False),
        
        # Trigger file
        StructField("is_trigger_file", BooleanType(), nullable=False),
        StructField("trigger_file_extension", StringType(), nullable=True),
        
        # Compression
        StructField("is_compressed", BooleanType(), nullable=False),
        StructField("compressed_type", StringType(), nullable=True),
        StructField("compressed_level", StringType(), nullable=True),
        StructField("compressed_file_name", StringType(), nullable=True),
        StructField("compressed_extension", StringType(), nullable=True),
        
        # Fabric paths
        StructField("fabric_lakehouse_path", StringType(), nullable=True),
        
        # Performance options
        StructField("force_single_file", BooleanType(), nullable=True),
        
        # Audit fields
        StructField("created_date", StringType(), nullable=False),
        StructField("created_by", StringType(), nullable=False),
        StructField("modified_date", StringType(), nullable=True),
        StructField("modified_by", StringType(), nullable=True),
    ])
    
    # Create extract configuration table
    target_lakehouse.create_table(
        table_name="config_extract_generation",
        schema=extract_config_schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"
        }
    )
    
    # Create extract details configuration table
    target_lakehouse.create_table(
        table_name="config_extract_generation_details",
        schema=extract_details_schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"
        }
    )
    
    print("✓ Created extract generation configuration tables:")
    print("  - config_extract_generation")
    print("  - config_extract_generation_details")
    

du.run_once(script_to_execute, "001_config_extract_generation_create","50175b504e6a")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 002_sample_data_extract_generation_insert.py

# CELL ********************

guid="1d76b52ca628"
object_name = "002_sample_data_extract_generation_insert"

def script_to_execute():
    # Sample configuration data for extract generation testing - Lakehouse version
    from pyspark.sql import Row
    from datetime import datetime
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    
    # Define schema for sample data (matching config_create.py)
    extract_config_schema = StructType([
        StructField("extract_name", StringType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("trigger_name", StringType(), nullable=True),
        StructField("extract_pipeline_name", StringType(), nullable=True),
        
        # Source configuration
        StructField("extract_table_name", StringType(), nullable=True),
        StructField("extract_table_schema", StringType(), nullable=True),
        StructField("extract_view_name", StringType(), nullable=True),
        StructField("extract_view_schema", StringType(), nullable=True),
        
        # Load configuration
        StructField("is_full_load", BooleanType(), nullable=False),
        StructField("execution_group", StringType(), nullable=True),
        
        # Audit fields
        StructField("created_date", StringType(), nullable=False),
        StructField("created_by", StringType(), nullable=False),
        StructField("modified_date", StringType(), nullable=True),
        StructField("modified_by", StringType(), nullable=True),
    ])
    
    # Extract file details configuration schema
    extract_details_schema = StructType([
        StructField("extract_name", StringType(), nullable=False),
        StructField("file_generation_group", StringType(), nullable=True),
        StructField("extract_container", StringType(), nullable=False),
        StructField("extract_directory", StringType(), nullable=True),
        
        # File naming
        StructField("extract_file_name", StringType(), nullable=False),
        StructField("extract_file_name_timestamp_format", StringType(), nullable=True),
        StructField("extract_file_name_period_end_day", IntegerType(), nullable=True),
        StructField("extract_file_name_extension", StringType(), nullable=False),
        StructField("extract_file_name_ordering", IntegerType(), nullable=True),
        
        # File properties
        StructField("file_properties_column_delimiter", StringType(), nullable=False),
        StructField("file_properties_row_delimiter", StringType(), nullable=False),
        StructField("file_properties_encoding", StringType(), nullable=False),
        StructField("file_properties_quote_character", StringType(), nullable=False),
        StructField("file_properties_escape_character", StringType(), nullable=False),
        StructField("file_properties_header", BooleanType(), nullable=False),
        StructField("file_properties_null_value", StringType(), nullable=False),
        StructField("file_properties_max_rows_per_file", IntegerType(), nullable=True),
        
        # Output format
        StructField("output_format", StringType(), nullable=False),
        
        # Trigger file
        StructField("is_trigger_file", BooleanType(), nullable=False),
        StructField("trigger_file_extension", StringType(), nullable=True),
        
        # Compression
        StructField("is_compressed", BooleanType(), nullable=False),
        StructField("compressed_type", StringType(), nullable=True),
        StructField("compressed_level", StringType(), nullable=True),
        StructField("compressed_file_name", StringType(), nullable=True),
        StructField("compressed_extension", StringType(), nullable=True),
        
        # Fabric paths
        StructField("fabric_lakehouse_path", StringType(), nullable=True),
        
        # Performance options
        StructField("force_single_file", BooleanType(), nullable=True),
        
        # Audit fields
        StructField("created_date", StringType(), nullable=False),
        StructField("created_by", StringType(), nullable=False),
        StructField("modified_date", StringType(), nullable=True),
        StructField("modified_by", StringType(), nullable=True),
    ])
    
    # Sample extract configuration records that mirror the hardcoded samples
    sample_extract_configs = [
        Row(
            extract_name="SAMPLE_CUSTOMERS_LAKEHOUSE",
            is_active=True,
            trigger_name=None,
            extract_pipeline_name=None,
            extract_table_name="customers",
            extract_table_schema="default",
            extract_view_name=None,
            extract_view_schema=None,
            is_full_load=True,
            execution_group="LAKEHOUSE_EXTRACTS",
            created_date="2024-01-15",
            created_by="system",
            modified_date=None,
            modified_by=None
        ),
        Row(
            extract_name="SAMPLE_PRODUCTS_LAKEHOUSE",
            is_active=True,
            trigger_name=None,
            extract_pipeline_name=None,
            extract_table_name="products",
            extract_table_schema="default",
            extract_view_name=None,
            extract_view_schema=None,
            is_full_load=True,
            execution_group="LAKEHOUSE_EXTRACTS",
            created_date="2024-01-15",
            created_by="system",
            modified_date=None,
            modified_by=None
        ),
        Row(
            extract_name="SAMPLE_ORDERS_LAKEHOUSE",
            is_active=True,
            trigger_name=None,
            extract_pipeline_name=None,
            extract_table_name="orders",
            extract_table_schema="default",
            extract_view_name=None,
            extract_view_schema=None,
            is_full_load=True,
            execution_group="LAKEHOUSE_EXTRACTS",
            created_date="2024-01-15",
            created_by="system",
            modified_date=None,
            modified_by=None
        )
    ]
    
    # Sample extract details configuration records
    sample_extract_details = [
        Row(
            extract_name="SAMPLE_CUSTOMERS_LAKEHOUSE",
            file_generation_group="CUSTOMER_DATA",
            extract_container="Files/extracts",
            extract_directory="customers",
            extract_file_name="customers_lakehouse",
            extract_file_name_timestamp_format="yyyyMMdd_HHmmss",
            extract_file_name_period_end_day=None,
            extract_file_name_extension="csv",
            extract_file_name_ordering=1,
            file_properties_column_delimiter=",",
            file_properties_row_delimiter="\\n",
            file_properties_encoding="UTF-8",
            file_properties_quote_character='"',
            file_properties_escape_character="\\",
            file_properties_header=True,
            file_properties_null_value="",
            file_properties_max_rows_per_file=None,
            output_format="csv",
            is_trigger_file=False,
            trigger_file_extension=None,
            is_compressed=False,
            compressed_type=None,
            compressed_level=None,
            compressed_file_name=None,
            compressed_extension=None,
            fabric_lakehouse_path=None,
            force_single_file=True,
            created_date="2024-01-15",
            created_by="system",
            modified_date=None,
            modified_by=None
        ),
        Row(
            extract_name="SAMPLE_PRODUCTS_LAKEHOUSE",
            file_generation_group="PRODUCT_DATA",
            extract_container="Files/extracts",
            extract_directory="products",
            extract_file_name="products_catalog",
            extract_file_name_timestamp_format="yyyyMMdd",
            extract_file_name_period_end_day=None,
            extract_file_name_extension="parquet",
            extract_file_name_ordering=1,
            file_properties_column_delimiter=",",
            file_properties_row_delimiter="\\n",
            file_properties_encoding="UTF-8",
            file_properties_quote_character='"',
            file_properties_escape_character="\\",
            file_properties_header=True,
            file_properties_null_value="",
            file_properties_max_rows_per_file=None,
            output_format="parquet",
            is_trigger_file=True,
            trigger_file_extension=".done",
            is_compressed=False,
            compressed_type=None,
            compressed_level=None,
            compressed_file_name=None,
            compressed_extension=None,
            fabric_lakehouse_path=None,
            force_single_file=True,
            created_date="2024-01-15",
            created_by="system",
            modified_date=None,
            modified_by=None
        ),
        Row(
            extract_name="SAMPLE_ORDERS_LAKEHOUSE",
            file_generation_group="ORDER_DATA",
            extract_container="Files/extracts",
            extract_directory="orders",
            extract_file_name="orders_daily",
            extract_file_name_timestamp_format="yyyyMMdd_HHmmss",
            extract_file_name_period_end_day=None,
            extract_file_name_extension="csv",
            extract_file_name_ordering=1,
            file_properties_column_delimiter=",",
            file_properties_row_delimiter="\\n",
            file_properties_encoding="UTF-8",
            file_properties_quote_character='"',
            file_properties_escape_character="\\",
            file_properties_header=True,
            file_properties_null_value="",
            file_properties_max_rows_per_file=None,
            output_format="csv",
            is_trigger_file=False,
            trigger_file_extension=None,
            is_compressed=True,
            compressed_type="GZIP",
            compressed_level="NORMAL",
            compressed_file_name=None,
            compressed_extension=".gz",
            fabric_lakehouse_path=None,
            force_single_file=True,
            created_date="2024-01-15",
            created_by="system",
            modified_date=None,
            modified_by=None
        )
    ]
    
    # Create DataFrames and insert records
    config_df = target_lakehouse.get_connection.createDataFrame(sample_extract_configs, extract_config_schema)
    target_lakehouse.write_to_table(
        df=config_df,
        table_name="config_extract_generation",
        mode="append"
    )
    
    details_df = target_lakehouse.get_connection.createDataFrame(sample_extract_details, extract_details_schema)
    target_lakehouse.write_to_table(
        df=details_df,
        table_name="config_extract_generation_details",
        mode="append"
    )
    
    print("✓ Inserted " + str(len(sample_extract_configs)) + " extract configuration records")
    print("✓ Inserted " + str(len(sample_extract_details)) + " extract details configuration records")
    print("✓ Extract configurations include:")
    print("  - SAMPLE_CUSTOMERS_LAKEHOUSE: CSV format, uncompressed")
    print("  - SAMPLE_PRODUCTS_LAKEHOUSE: Parquet format, with trigger file")
    print("  - SAMPLE_ORDERS_LAKEHOUSE: CSV format, GZIP compressed")
    

du.run_once(script_to_execute, "002_sample_data_extract_generation_insert","1d76b52ca628")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 003_log_extract_generation_create.py

# CELL ********************

guid="466901542b21"
object_name = "003_log_extract_generation_create"

def script_to_execute():
    # Log table for extract generation run tracking - Lakehouse version
    from pyspark.sql.types import (
        BooleanType,
        IntegerType,
        StringType,
        TimestampType,
        StructField,
        StructType,
    )
    
    # Extract generation log schema
    extract_log_schema = StructType([
        StructField("log_id", StringType(), nullable=False),
        StructField("extract_name", StringType(), nullable=False),
        StructField("execution_group", StringType(), nullable=True),
        StructField("run_id", StringType(), nullable=False),
        StructField("run_timestamp", TimestampType(), nullable=False),
        StructField("run_status", StringType(), nullable=False),  # IN_PROGRESS, SUCCESS, FAILED
        StructField("run_type", StringType(), nullable=False),    # FULL, INCREMENTAL
        StructField("start_time", TimestampType(), nullable=False),
        StructField("end_time", TimestampType(), nullable=True),
        StructField("duration_seconds", IntegerType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
        
        # Source information
        StructField("source_type", StringType(), nullable=True),  # TABLE, VIEW
        StructField("source_object", StringType(), nullable=True),
        StructField("source_schema", StringType(), nullable=True),
        
        # Extract metrics
        StructField("rows_extracted", IntegerType(), nullable=True),
        StructField("rows_written", IntegerType(), nullable=True),
        StructField("files_generated", IntegerType(), nullable=True),
        
        # Output information
        StructField("output_format", StringType(), nullable=True),
        StructField("output_file_path", StringType(), nullable=True),
        StructField("output_file_name", StringType(), nullable=True),
        StructField("output_file_size_bytes", IntegerType(), nullable=True),
        
        # Compression information
        StructField("is_compressed", BooleanType(), nullable=True),
        StructField("compression_type", StringType(), nullable=True),
        
        # Trigger file information
        StructField("trigger_file_created", BooleanType(), nullable=True),
        StructField("trigger_file_path", StringType(), nullable=True),
        
        # Environment information
        StructField("workspace_id", StringType(), nullable=True),
        StructField("lakehouse_id", StringType(), nullable=True),
        StructField("created_by", StringType(), nullable=False),
    ])
    
    # Create extract generation log table
    target_lakehouse.create_table(
        table_name="log_extract_generation",
        schema=extract_log_schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"
        },
        partition_by=["run_status", "extract_name"]
    )
    
    print("✓ Created extract generation log table: log_extract_generation")
    print("✓ Partitioned by run_status and extract_name for efficient querying")
    

du.run_once(script_to_execute, "003_log_extract_generation_create","466901542b21")

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

