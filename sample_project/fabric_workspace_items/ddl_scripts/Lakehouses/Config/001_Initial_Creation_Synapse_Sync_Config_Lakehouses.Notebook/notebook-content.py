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
    
    notebookutils.fs.mount("abfss://dev_jr@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
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

# ## 📄 Cell for 001_config_synapse_extract_objects_create.py

# CELL ********************

guid="d2062a1925e8"
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
    

du.run_once(script_to_execute, "001_config_synapse_extract_objects_create","d2062a1925e8")

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

guid="e5e5f221fd39"
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
    

du.run_once(script_to_execute, "002_log_synapse_extract_run_log_create","e5e5f221fd39")

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

