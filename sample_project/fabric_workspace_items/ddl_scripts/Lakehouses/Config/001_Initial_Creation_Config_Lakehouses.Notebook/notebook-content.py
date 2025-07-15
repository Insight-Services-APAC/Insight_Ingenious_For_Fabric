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
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    LongType
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

# ## 📄 Cell for 001_config_parquet_loads_create.py

# CELL ********************

guid="d4a04b9273bf"
object_name = "001_config_parquet_loads_create"

def script_to_execute():
    schema = StructType(
        [
            StructField("cfg_target_lakehouse_workspace_id", StringType(), nullable=False),
            StructField("cfg_target_lakehouse_id", StringType(), nullable=False),
            StructField("target_partition_columns", StringType(), nullable=False),
            StructField("target_sort_columns", StringType(), nullable=False),
            StructField("target_replace_where", StringType(), nullable=False),
            StructField("cfg_source_lakehouse_workspace_id", StringType(), nullable=False),
            StructField("cfg_source_lakehouse_id", StringType(), nullable=False),
            StructField("cfg_source_file_path", StringType(), nullable=False),
            StructField("source_file_path", StringType(), nullable=False),
            StructField("source_file_name", StringType(), nullable=False),
            StructField("cfg_legacy_synapse_connection_name", StringType(), nullable=False),
            StructField("synapse_source_schema_name", StringType(), nullable=False),
            StructField("synapse_source_table_name", StringType(), nullable=False),
            StructField("synapse_partition_clause", StringType(), nullable=False),
            StructField("execution_group", IntegerType(), nullable=False),
            StructField("active_yn", StringType(), nullable=False),
        ]
    )
    
    target_lakehouse.create_table(
            table_name="config_parquet_loads",
            schema=schema,
            mode="overwrite",
            options={
                "parquet.vorder.default": "true"  # Ensure Parquet files are written in vorder order
            },
    )
    
    

du.run_once(script_to_execute, "001_config_parquet_loads_create","d4a04b9273bf")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 002_config.synapse_extract_objects.py

# CELL ********************

guid="e11e271bff29"
object_name = "002_config.synapse_extract_objects"

def script_to_execute():
    schema = StructType([
        StructField("synapse_connection_name",     StringType(), nullable=False),
        StructField("source_schema_name",          StringType(), nullable=False),
        StructField("source_table_name",           StringType(), nullable=False),
        StructField("partition_clause",            StringType(), nullable=False),
        StructField("extract_mode",                StringType(), nullable=False),
        StructField("single_date_filter",          StringType(), nullable=True),
        StructField("date_range_filter",           StringType(), nullable=True),
        StructField("execution_group",             IntegerType(), nullable=False),
        StructField("active_yn",                   StringType(), nullable=False),
        StructField("pipeline_id",                 StringType(), nullable=False),
        StructField("synapse_datasource_name",     StringType(), nullable=False),
        StructField("synapse_datasource_location", StringType(), nullable=False),
    ])
    
    target_lakehouse.create_table(
            table_name="config_synapse_extract_objects",
            schema=schema,
            mode="overwrite",
            options={
                "parquet.vorder.default": "true"  # Ensure Parquet files are written in vorder order
            },
    )
    

du.run_once(script_to_execute, "002_config.synapse_extract_objects","e11e271bff29")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 003_log_parquet_loads_create.py

# CELL ********************

guid="60a85cc65151"
object_name = "003_log_parquet_loads_create"

def script_to_execute():
    schema = StructType(
        [
            StructField("execution_id", StringType(), nullable=False),
            StructField("cfg_target_lakehouse_workspace_id", StringType(), nullable=False),
            StructField("cfg_target_lakehouse_id", StringType(), nullable=False),
            StructField("partition_clause", StringType(), nullable=True),
            StructField("status", StringType(), nullable=False),
            StructField("error_messages", StringType(), nullable=True),
            StructField("start_date", LongType(), nullable=False),
            StructField("finish_date", LongType(), nullable=False),
            StructField("update_date", LongType(), nullable=False),
        ]
    )
    
    
    target_lakehouse.create_table(
            table_name="log_parquet_loads",
            schema=schema,
            mode="overwrite",
            options={
                "parquet.vorder.default": "true"  # Ensure Parquet files are written in vorder order
            },
    )
    
    
    
    

du.run_once(script_to_execute, "003_log_parquet_loads_create","60a85cc65151")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 004_log_synapse_loads_create.py

# CELL ********************

guid="9cc7fc5f2097"
object_name = "004_log_synapse_loads_create"

def script_to_execute():
    schema = StructType(
        [
            StructField("execution_id", StringType(), nullable=False),
            StructField("cfg_synapse_connection_name", StringType(), nullable=False),
            StructField("source_schema_name", StringType(), nullable=False),
            StructField("source_table_name", StringType(), nullable=False),
            StructField("extract_file_name", StringType(), nullable=False),
            StructField("partition_clause", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("error_messages", StringType(), nullable=True),
            StructField("start_date", LongType(), nullable=False),
            StructField("finish_date", LongType(), nullable=False),
            StructField("update_date", LongType(), nullable=False),
        ]
    )
    
    target_lakehouse.create_table(
            table_name="log_synapse_extracts",
            schema=schema,
            mode="overwrite",
            options={
                "parquet.vorder.default": "true"  # Ensure Parquet files are written in vorder order
            },
    )
    

du.run_once(script_to_execute, "004_log_synapse_loads_create","9cc7fc5f2097")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 005_config_synapse_loads_insert.py

# CELL ********************

guid="a749c941a925"
object_name = "005_config_synapse_loads_insert"

def script_to_execute():
    data = [
        ("legacy_synapse_connection_name", "dbo", "dim_customer", "", 1, "Y"),
        (
            "legacy_synapse_connection_name",
            "dbo",
            "fact_transactions",
            "where year = @year and month = @month",
            1,
            "Y",
        ),
    ]
    
    # 2. Create a DataFrame
    schema = StructType(
        [
            StructField("cfg_legacy_synapse_connection_name", StringType(), nullable=False),
            StructField("source_schema_name", StringType(), nullable=False),
            StructField("source_table_name", StringType(), nullable=False),
            StructField("partition_clause", StringType(), nullable=False),
            StructField("execution_group", IntegerType(), nullable=False),
            StructField("active_yn", StringType(), nullable=False),
        ]
    )
    insert_df = target_lakehouse.get_connection.createDataFrame(data, schema)
    
    target_lakehouse.write_to_table(
        table_name="config_synapse_extracts",
        df=insert_df,
        mode="append"
    )
    
    

du.run_once(script_to_execute, "005_config_synapse_loads_insert","a749c941a925")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 006_config_parquet_loads_insert.py

# CELL ********************

guid="2d4914d8bef6"
object_name = "006_config_parquet_loads_insert"

def script_to_execute():
    schema = StructType(
        [
            StructField("cfg_target_lakehouse_workspace_id", StringType(), nullable=False),
            StructField("cfg_target_lakehouse_id", StringType(), nullable=False),
            StructField("target_partition_columns", StringType(), nullable=False),
            StructField("target_sort_columns", StringType(), nullable=False),
            StructField("target_replace_where", StringType(), nullable=False),
            StructField("cfg_source_lakehouse_workspace_id", StringType(), nullable=False),
            StructField("cfg_source_lakehouse_id", StringType(), nullable=False),
            StructField("cfg_source_file_path", StringType(), nullable=False),
            StructField("source_file_path", StringType(), nullable=False),
            StructField("source_file_name", StringType(), nullable=False),
            StructField("cfg_legacy_synapse_connection_name", StringType(), nullable=False),
            StructField("synapse_source_schema_name", StringType(), nullable=False),
            StructField("synapse_source_table_name", StringType(), nullable=False),
            StructField("synapse_partition_clause", StringType(), nullable=False),
            StructField("execution_group", IntegerType(), nullable=False),
            StructField("active_yn", StringType(), nullable=False),
        ]
    )
    
    
    # ──────────────────────────────────────────────────────────────────────────────
    # Build the data rows
    # ──────────────────────────────────────────────────────────────────────────────
    data = [
        (
            "edw_workspace_id",
            "edw_lakehouse_id",
            "",  # target_partition_columns
            "",  # target_sort_columns
            "",  # target_replace_where
            "edw_lakehouse_workspace_id",
            "edw_lakehouse_id",
            "synapse_export_shortcut_path_in_onelake",
            "dbo_dim_customer",
            "",
            "legacy_synapse_connection_name",
            "dbo",
            "dim_customer",
            "",
            1,
            "Y",
        ),
        (
            "edw_workspace_id",
            "edw_lakehouse_id",
            "year, month",
            "year, month",
            "WHERE year = @year AND month = @month",
            "edw_workspace_id",
            "edw_lakehouse_id",
            "synapse_export_shortcut_path_in_onelake",
            "dbo_dim_customer",
            "",
            "legacy_synapse_connection_name",
            "dbo",
            "fact_transactions",
            "WHERE year = @year AND month = @month",
            1,
            "Y",
        ),
    ]
    
    insert_df = target_lakehouse.get_connection.createDataFrame(data, schema)
    
    target_lakehouse.write_to_table(
        table_name="config_parquet_loads",
        df=insert_df,
        mode="append"
    )
    
    

du.run_once(script_to_execute, "006_config_parquet_loads_insert","2d4914d8bef6")

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

