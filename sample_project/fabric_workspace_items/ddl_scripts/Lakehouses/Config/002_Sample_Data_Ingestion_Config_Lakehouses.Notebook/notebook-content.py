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

    
    # Python environment - no spark session needed
    spark = None
    
else:
    print("NotebookUtils not available, assumed running in local mode.")
    from ingen_fab.python_libs.python.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )
    notebookutils = NotebookUtilsFactory.create_instance()
    
    
    spark = None
    
    
    mount_path = None
    run_mode = "local"




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

# ## 📄 Cell for 003_sample_data_insert.py

# CELL ********************

guid="15fc8b132572"
object_name = "003_sample_data_insert"

def script_to_execute():
    # Sample configuration data for flat file ingestion testing - Lakehouse version
    from pyspark.sql import Row
    
    # Sample configuration records for testing
    sample_configs = [
        Row(
            config_id="csv_test_001",
            config_name="CSV Sales Data Test",
            source_file_path="Files/sample_data/sales_data.csv",
            source_file_format="csv",
            target_lakehouse_workspace_id="{{varlib:config_workspace_id}}",
            target_lakehouse_id="{{varlib:config_lakehouse_id}}",
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
            active_yn="Y"
        ),
        Row(
            config_id="json_test_002",
            config_name="JSON Products Data Test",
            source_file_path="Files/sample_data/products.json",
            source_file_format="json",
            target_lakehouse_workspace_id="{{varlib:config_workspace_id}}",
            target_lakehouse_id="{{varlib:config_lakehouse_id}}",
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
            active_yn="Y"
        ),
        Row(
            config_id="parquet_test_003",
            config_name="Parquet Customers Data Test",
            source_file_path="Files/sample_data/customers.parquet",
            source_file_format="parquet",
            target_lakehouse_workspace_id="{{varlib:config_workspace_id}}",
            target_lakehouse_id="{{varlib:config_lakehouse_id}}",
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
            active_yn="Y"
        )
    ]
    
    # Create DataFrame and insert records
    df = target_lakehouse.spark.createDataFrame(sample_configs, schema=target_lakehouse.schema)  # type: ignore # noqa: F821
    target_lakehouse.write_to_table(
        df=df,
        table_name="config_flat_file_ingestion",
        mode="append"
    )
    
    print(f"✓ Inserted {len(sample_configs)} sample configuration records")
    

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

