# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# CELL ********************


# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions


# CELL ********************


import sys

if "notebookutils" in sys.modules:
    import sys

    notebookutils.fs.mount(  # noqa: F821
        "abfss://{{varlib:config_workspace_id}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_workspace_name}}.Lakehouse/Files/",
        "/config_files",
    )  # type: ignore # noqa: F821
    new_Path = notebookutils.fs.getMountPath("/config_files")  # type: ignore # noqa: F821
    sys.path.insert(0, new_Path)
else:
    print("NotebookUtils not available, skipping config files mount.")
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )

    notebookutils = NotebookUtilsFactory.create_instance()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 🧪🧪 Run DDL Scripts


# CELL ********************

from ingen_fab.python_libs.common.config_utils import (
    ConfigsObject,
    get_configs_as_object,
)
from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

target_lakehouse_config_prefix = "EDW"

configs: ConfigsObject = get_configs_as_object()
config_lakehouse = lakehouse_utils(
    target_workspace_id=configs.edw_workspace_id,
    target_lakehouse_id=configs.edw_lakehouse_id,
    spark=spark,  # noqa: F821
)


target_lakehouse = lakehouse_utils(
    target_workspace_id=configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_workspace_id"),
    target_lakehouse_id=configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_lakehouse_id"),
    spark=spark,  # noqa: F821
)

du = ddl_utils(
    target_workspace_id=target_lakehouse.target_workspace_id,
    target_lakehouse_id=target_lakehouse.target_store_id,
)

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 001_config_parquet_loads_create.py

# CELL ********************

guid = "d4a04b9273bf"
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


du.run_once(script_to_execute, "001_config_parquet_loads_create", "d4a04b9273bf")


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
