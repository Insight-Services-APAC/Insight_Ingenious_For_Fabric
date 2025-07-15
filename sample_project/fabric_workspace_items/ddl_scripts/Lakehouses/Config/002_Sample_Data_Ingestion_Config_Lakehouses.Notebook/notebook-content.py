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

# ## 📄 Cell for 003_config_synapse_extract_objects_insert.py

# CELL ********************

guid="928c2d831673"
object_name = "003_config_synapse_extract_objects_insert"

def script_to_execute():
    # Sample data for config_synapse_extract_objects - Lakehouse version
    
    from datetime import datetime
    
    # Sample data
    sample_data = [
        {
            "synapse_connection_name": "SynapseConnection",
            "source_schema_name": "dbo",
            "source_table_name": "DimCustomer",
            "extract_mode": "snapshot",
            "single_date_filter": None,
            "date_range_filter": None,
            "execution_group": 1,
            "active_yn": "Y",
            "pipeline_id": "00000000-0000-0000-0000-000000000000",
            "synapse_datasource_name": "SynapseDatasource",
            "synapse_datasource_location": "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files",
            "created_timestamp": datetime.now(),
            "updated_timestamp": datetime.now()
        },
        {
            "synapse_connection_name": "SynapseConnection",
            "source_schema_name": "dbo",
            "source_table_name": "FactSales",
            "extract_mode": "incremental",
            "single_date_filter": "WHERE DATE_SK = @date",
            "date_range_filter": "WHERE DATE_SK BETWEEN @start_date AND @end_date",
            "execution_group": 2,
            "active_yn": "Y",
            "pipeline_id": "00000000-0000-0000-0000-000000000000",
            "synapse_datasource_name": "SynapseDatasource",
            "synapse_datasource_location": "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files",
            "created_timestamp": datetime.now(),
            "updated_timestamp": datetime.now()
        }
    ]
    
    target_lakehouse.write_to_table(
        table_name="config_synapse_extract_objects",
        df=sample_data,
        mode="append"
    )
    

du.run_once(script_to_execute, "003_config_synapse_extract_objects_insert","928c2d831673")

def script_to_execute():
    print("Script block is empty. No action taken.")



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
    # Sample configuration data for flat file ingestion testing - Lakehouse version
    from pyspark.sql import Row
    
    # Import the schema definition
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
    
    # Sample configuration records for testing
    sample_configs = [
        Row(
            config_id="csv_test_001",
            config_name="CSV Sales Data Test",
            source_file_path="Files/sample_data/sales_data.csv",
            source_file_format="csv",
            target_lakehouse_workspace_id="#####",
            target_lakehouse_id="2629d4cc-685c-458a-866b-b4705dde71a7",
            target_schema_name="raw",
            target_table_name="sales_data",
            file_delimiter=",",
            has_header=True,
            encoding="utf-8",
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="",
            sort_columns="date",
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
            config_id="json_test_002",
            config_name="JSON Products Data Test",
            source_file_path="Files/sample_data/products.json",
            source_file_format="json",
            target_lakehouse_workspace_id="#####",
            target_lakehouse_id="2629d4cc-685c-458a-866b-b4705dde71a7",
            target_schema_name="raw",
            target_table_name="products",
            file_delimiter=None,
            has_header=None,
            encoding="utf-8",
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd'T'HH:mm:ss'Z'",
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
            config_id="parquet_test_003",
            config_name="Parquet Customers Data Test",
            source_file_path="Files/sample_data/customers.parquet",
            source_file_format="parquet",
            target_lakehouse_workspace_id="#####",
            target_lakehouse_id="2629d4cc-685c-458a-866b-b4705dde71a7",
            target_schema_name="raw",
            target_table_name="customers",
            file_delimiter=None,
            has_header=None,
            encoding=None,
            date_format="yyyy-MM-dd",
            timestamp_format="yyyy-MM-dd HH:mm:ss",
            schema_inference=True,
            custom_schema_json=None,
            partition_columns="region",
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
        )
    ]
    
    # Create DataFrame and insert records
    df = target_lakehouse.get_connection.createDataFrame(sample_configs, schema)
    target_lakehouse.write_to_table(
        df=df,
        table_name="config_flat_file_ingestion",
        mode="append"
    )
    
    print("✓ Inserted " + str(len(sample_configs)) + " sample configuration records")
    

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

