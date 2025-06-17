# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, VarcharType, TimestampType
from datetime import datetime

execution_log_schema = StructType([
    StructField("script_id", StringType(), nullable=False),
    StructField("script_name", StringType(), nullable=False),
    StructField("execution_status", StringType(), nullable=False),
    StructField("update_date", TimestampType(), nullable=False)
])

def write_to_execution_log(object_guid, object_name, script_status):
    data = [(object_guid, object_name, script_status, datetime.now())]
    new_df = spark.createDataFrame(data, execution_log_schema)
    new_df.write \
    .format("delta") \
    .mode("append") \
    .save(f"abfss://{config_workspace_id}@onelake.dfs.fabric.microsoft.com/{config_lakehouse_id}/Tables/ddl_script_executions")

def check_if_table_exists(table_path):
    table_exists = False
    try:
        if DeltaTable.isDeltaTable(spark, table_path):
            table_exists = True
            #print(f"Delta table already exists at path: {table_path}, skipping creation.")
    except Exception as e:
        # If the path does not exist or is inaccessible, isDeltaTable returns False or may throw.
        # Treat exceptions as "table does not exist".
        #print(f"Could not verify Delta table existence at {table_path} (exception: {e}); assuming it does not exist.")
        table_exists = False
    return table_exists

def print_log():
    df = spark.read.format("delta").load(execution_log_table_path)
    display(df)
    

def check_if_script_has_run(script_id) -> bool:
    from pyspark.sql.functions import col
    df = spark.read.format("delta").load(execution_log_table_path)
    #display(df)
    # Build filter condition
    cond = col("script_id") == script_id
    status = "Success"
    if status is not None:
        cond = cond & (col("execution_status") == status)
    matched = df.filter(cond).limit(1).take(1)
    if(len(matched)) == 0:
        #print("matched:", matched)
        return False
    else:   
        return True

def print_skipped_script_execution(guid, object_name):
    print(f"skipping {guid}:{object_name} as the script has already run on workspace_id:{config_workspace_id} | lakehouse_id {config_lakehouse_id}")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import List, Callable

import inspect, hashlib
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def run_once(
    work_fn: callable,
    object_name: str,
    guid: str 
):
    """
    Runs `work_fn()` exactly once, keyed by `guid`. If `guid` is None,
    it's computed by hashing the source code of `work_fn`.
    """
    # 1. Auto-derive GUID if not provided
    if guid is None:
        try:
            src = inspect.getsource(work_fn)
        except (OSError, TypeError):
            raise ValueError("work_fn must be a named function defined at top-level")
        # compute SHA256 and take first 12 hex chars
        digest = hashlib.sha256(src.encode("utf-8")).hexdigest()
        guid = digest
        print(f"Derived guid={guid} from work_fn source")

    # 2. Check execution
    if not script_executor_class.check_if_script_has_run(guid):
        try:
            work_fn()
            script_executor_class.write_to_execution_log(object_guid=guid,
                                object_name=object_name,
                                script_status="Success")
        except Exception as e:
            print(f"Error in work_fn for {guid}: {e}")
            script_executor_class.write_to_execution_log(object_guid=guid,
                                object_name=object_name,
                                script_status="Failure")
    else:
        script_executor_class.print_skipped_script_execution(guid, object_name)


def drop_all_tables():
        # ──────────────────────────────────────────────────────────────────────────────
        # 1. CONFIGURATION – adjust these to your environment:
        # ──────────────────────────────────────────────────────────────────────────────


        # Base ABFSS endpoint and the ‘Tables’ root
        lakehouse_root = (
            f"abfss://{config_workspace_id}"
            f"@onelake.dfs.fabric.microsoft.com/"
            f"{config_lakehouse_id}/Tables"
        )
        # ──────────────────────────────────────────────────────────────────────────────

        # 2. START spark and get Hadoop FS handle
        # ──────────────────────────────────────────────────────────────────────────────
        spark = SparkSession.builder.getOrCreate()

        # Access Hadoop’s FileSystem via the JVM gateway
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

        # Path object for the Tables/ directory
        root_path = spark._jvm.org.apache.hadoop.fs.Path(lakehouse_root)

        # ──────────────────────────────────────────────────────────────────────────────
        # 3. Iterate, detect Delta tables, and delete
        # ──────────────────────────────────────────────────────────────────────────────

        for status in fs.listStatus(root_path):
            table_path_obj = status.getPath()
            table_path = table_path_obj.toString()  # e.g. abfss://…/Tables/my_table

            try:
                # Check if this directory is a Delta table
                if DeltaTable.isDeltaTable(spark, table_path):
                    # Delete the directory (recursive=True)
                    deleted = fs.delete(table_path_obj, True)
                    if deleted:
                        print(f"✔ Dropped Delta table at: {table_path}")
                    else:
                        print(f"✖ Failed to delete: {table_path}")
                else:
                    print(f"— Skipping non-Delta path: {table_path}")
            except Exception as e:
                # e.g. permission issue, or not a Delta table
                print(f"⚠ Error checking/deleting {table_path}: {e}")

        print("✅ All eligible Delta tables under ‘Tables/’ have been dropped.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
