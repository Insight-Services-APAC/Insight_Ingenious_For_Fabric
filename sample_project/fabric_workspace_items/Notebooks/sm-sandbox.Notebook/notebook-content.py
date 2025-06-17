# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *
import pyarrow as pa
import pyarrow.dataset as ds
import os
import shutil

config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
config_lakehouse_id = "23ba8378-7884-4ac7-bab5-10e11af43d6c"

class lakehouse_utils:
    def __init__(self, target_workspace_id, target_lakehouse_id):
        self.target_workspace_id = target_workspace_id
        self.target_lakehouse_id = target_lakehouse_id

    def lakehouse_tables_uri(self):
        return f"abfss://{self.target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self.target_lakehouse_id}/Tables/"

    def check_if_table_exists(self, table_path):
        try:
            _ = DeltaTable(table_path)
            return True
        except Exception:
            return False

lu = lakehouse_utils(config_workspace_id, config_lakehouse_id)

schema = StructType([
        StructField("synapse_connection_name", StringType(), nullable=False),
        StructField("source_schema_name", StringType(), nullable=False),
        StructField("source_table_name", StringType(), nullable=False),
        StructField("partition_clause", StringType(), nullable=False),
        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False),
        StructField("pipeline_id", StringType(), nullable=False),
        StructField("synapse_datasource_name", StringType(), nullable=False),
        StructField("synapse_datasource_location", StringType(), nullable=False),
    ])

empty_df = spark.createDataFrame([], schema)
(
    empty_df.write
    .format("delta")
    .option("parquet.vorder.default","true")
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .save(f"{lu.lakehouse_tables_uri()}config_synapse_extracts")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
