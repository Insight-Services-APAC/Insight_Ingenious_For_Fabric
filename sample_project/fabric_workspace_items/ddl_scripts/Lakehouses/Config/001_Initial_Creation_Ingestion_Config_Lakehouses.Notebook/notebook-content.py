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



# ## 『』Parameters



# Default parameters
# Add default parameters here


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }


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
    
    notebookutils.fs.mount("abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
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

# Always clear the module cache - We may remove this once the libs are stable
clear_module_cache("ingen_fab.python_libs")
clear_module_cache("ingen_fab")




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

# ## ⚙️ Configuration Settings

# CELL ********************


# variableLibraryInjectionStart: var_lib

# All variables as a dictionary
configs_dict = {'fabric_environment': 'local', 'fabric_deployment_workspace_id': '#####', 'synapse_source_database_1': 'test1', 'config_workspace_id': '#####', 'synapse_source_sql_connection': 'sansdaisyn-ondemand.sql.azuresynapse.net', 'config_lakehouse_name': 'config', 'edw_warehouse_name': 'edw', 'config_lakehouse_id': '2629d4cc-685c-458a-866b-b4705dde71a7', 'edw_workspace_id': '###', 'edw_warehouse_id': '###', 'edw_lakehouse_id': '6adb67d6-c8eb-4612-9053-890cae3a55d7', 'edw_lakehouse_name': 'edw', 'legacy_synapse_connection_name': 'synapse_connection', 'synapse_export_shortcut_path_in_onelake': 'exports/', 'raw_workspace_id': 'local_raw_workspace', 'raw_datastore_id': 'local_raw_datastore', 'config_warehouse_id': 'local-config-warehouse-id'}
# All variables as an object
from dataclasses import dataclass
@dataclass
class ConfigsObject:
    fabric_environment: str 
    fabric_deployment_workspace_id: str 
    synapse_source_database_1: str 
    config_workspace_id: str 
    synapse_source_sql_connection: str 
    config_lakehouse_name: str 
    edw_warehouse_name: str 
    config_lakehouse_id: str 
    edw_workspace_id: str 
    edw_warehouse_id: str 
    edw_lakehouse_id: str 
    edw_lakehouse_name: str 
    legacy_synapse_connection_name: str 
    synapse_export_shortcut_path_in_onelake: str 
    raw_workspace_id: str 
    raw_datastore_id: str 
    config_warehouse_id: str 
configs_object: ConfigsObject = ConfigsObject(**configs_dict)
# variableLibraryInjectionEnd: var_lib



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

# ## 📄 Cell for 001_config_flat_file_ingestion_create.py

# CELL ********************

guid="6acfb4b93bae"
object_name = "001_config_flat_file_ingestion_create"

def script_to_execute():
    # Configuration table for flat file ingestion metadata - Lakehouse version
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
        StructField("target_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("target_lakehouse_id", StringType(), nullable=False),
        StructField("target_schema_name", StringType(), nullable=False),
        StructField("target_table_name", StringType(), nullable=False),
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
    
    target_lakehouse.create_table(
        table_name="config_flat_file_ingestion",
        schema=schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"
        }
    )
    

du.run_once(script_to_execute, "001_config_flat_file_ingestion_create","6acfb4b93bae")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 001_config_synapse_extract_objects_create.py

# CELL ********************

guid="9278a7677589"
object_name = "001_config_synapse_extract_objects_create"

def script_to_execute():
    # Configuration table for synapse extract objects - Lakehouse version
    
    # Define schema for the table
    schema = StructType([
        StructField("synapse_connection_name", StringType(), False),
        StructField("source_schema_name", StringType(), False),
        StructField("source_table_name", StringType(), False),
        StructField("extract_mode", StringType(), False),
        StructField("single_date_filter", StringType(), True),
        StructField("date_range_filter", StringType(), True),
        StructField("execution_group", IntegerType(), False),
        StructField("active_yn", StringType(), False),
        StructField("pipeline_id", StringType(), False),
        StructField("synapse_datasource_name", StringType(), False),
        StructField("synapse_datasource_location", StringType(), False)
    ])
    
    target_lakehouse.create_table(
        table_name="config_synapse_extract_objects",
        schema=schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true",
            "overwriteSchema": "true"
        }
    )
    

du.run_once(script_to_execute, "001_config_synapse_extract_objects_create","9278a7677589")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 002_log_flat_file_ingestion_create.py

# CELL ********************

guid="af23899189d1"
object_name = "002_log_flat_file_ingestion_create"

def script_to_execute():
    # Log table for flat file ingestion execution tracking - Lakehouse version
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    
    schema = StructType([
        StructField("log_id", StringType(), nullable=False),
        StructField("config_id", StringType(), nullable=False),
        StructField("execution_id", StringType(), nullable=False),
        StructField("job_start_time", TimestampType(), nullable=False),
        StructField("job_end_time", TimestampType(), nullable=True),
        StructField("status", StringType(), nullable=False),  # running, completed, failed, cancelled
        StructField("source_file_path", StringType(), nullable=False),
        StructField("source_file_size_bytes", LongType(), nullable=True),
        StructField("source_file_modified_time", TimestampType(), nullable=True),
        StructField("target_table_name", StringType(), nullable=False),
        StructField("records_processed", LongType(), nullable=True),
        StructField("records_inserted", LongType(), nullable=True),
        StructField("records_updated", LongType(), nullable=True),
        StructField("records_deleted", LongType(), nullable=True),
        StructField("records_failed", LongType(), nullable=True),
        StructField("error_message", StringType(), nullable=True),
        StructField("error_details", StringType(), nullable=True),
        StructField("execution_duration_seconds", IntegerType(), nullable=True),
        StructField("spark_application_id", StringType(), nullable=True),
        StructField("created_date", TimestampType(), nullable=False),
        StructField("created_by", StringType(), nullable=False)
    ])
    
    target_lakehouse.create_table(
        table_name="log_flat_file_ingestion",
        schema=schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"
        }
    )
    

du.run_once(script_to_execute, "002_log_flat_file_ingestion_create","af23899189d1")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 002_log_synapse_extract_run_log_create.py

# CELL ********************

guid="fa2b6f874122"
object_name = "002_log_synapse_extract_run_log_create"

def script_to_execute():
    # Log table for synapse extract run log - Lakehouse version
    
    # Define schema for the table
    schema = StructType([
        StructField("master_execution_id", StringType(), True),
        StructField("execution_id", StringType(), True),
        StructField("pipeline_job_id", StringType(), True),
        StructField("execution_group", IntegerType(), True),
        StructField("master_execution_parameters", StringType(), True),
        StructField("trigger_type", StringType(), True),
        StructField("config_synapse_connection_name", StringType(), True),
        StructField("source_schema_name", StringType(), True),
        StructField("source_table_name", StringType(), True),
        StructField("extract_mode", StringType(), True),
        StructField("extract_start_dt", DateType(), True),
        StructField("extract_end_dt", DateType(), True),
        StructField("partition_clause", StringType(), True),
        StructField("output_path", StringType(), True),
        StructField("extract_file_name", StringType(), True),
        StructField("external_table", StringType(), True),
        StructField("start_timestamp", TimestampType(), True),
        StructField("end_timestamp", TimestampType(), True),
        StructField("duration_sec", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("error_messages", StringType(), True),
        StructField("end_timestamp_int", LongType(), True)
    ])
    
    target_lakehouse.create_table(
        table_name="log_synapse_extract_run_log",
        schema=schema,
        mode="overwrite",
        partition_by=["master_execution_id"],
        options={
            "parquet.vorder.default": "true",
            "overwriteSchema": "true"
        }
    )
    

du.run_once(script_to_execute, "002_log_synapse_extract_run_log_create","fa2b6f874122")

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

