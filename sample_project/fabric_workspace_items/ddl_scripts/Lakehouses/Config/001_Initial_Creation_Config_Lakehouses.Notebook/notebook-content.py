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

# ## 📄 Cell for 001_sample_customer_table_create.py

# CELL ********************

guid="97cf4ee0b247"
object_name = "001_sample_customer_table_create"

def script_to_execute():
    # Sample DDL script for creating a customer table in lakehouse
    # This demonstrates the basic pattern for creating Delta tables
    
    # Create the sample customer table
    spark.sql("""
    CREATE TABLE IF NOT EXISTS sample.customers (
        customer_id BIGINT,
        first_name STRING,
        last_name STRING,
        email STRING,
        created_date TIMESTAMP,
        is_active BOOLEAN
    )
    USING DELTA
    LOCATION 'Tables/customers'
    """)
    
    print("✓ Created sample customers table")
    
    # Add some sample data
    spark.sql("""
    INSERT INTO sample.customers VALUES
    (1, 'John', 'Doe', 'john.doe@example.com', '2024-01-01 10:00:00', true),
    (2, 'Jane', 'Smith', 'jane.smith@example.com', '2024-01-02 11:00:00', true),
    (3, 'Bob', 'Johnson', 'bob.johnson@example.com', '2024-01-03 12:00:00', false)
    """)
    
    print("✓ Inserted sample data into customers table")
    

du.run_once(script_to_execute, "001_sample_customer_table_create","97cf4ee0b247")

def script_to_execute():
    print("Script block is empty. No action taken.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 001_Sample_Tables_Create.py

# CELL ********************

guid="2091c92f63e8"
object_name = "001_Sample_Tables_Create"

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
    
    empty_df = spark.createDataFrame([], schema)
    (
        empty_df.write.format("delta")
        .option("parquet.vorder.default", "true")
        .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
        .save(f"{lu.lakehouse_tables_uri()}config_parquet_loads")
    )
    
    

du.run_once(script_to_execute, "001_Sample_Tables_Create","2091c92f63e8")

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

