# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "22f5aa2c-44c1-4aa9-98ce-15ff25430ede",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "5555c1a9-026a-4f3c-9c85-cdd000943d7f",
# META       "known_lakehouses": [
# META         {
# META           "id": "22f5aa2c-44c1-4aa9-98ce-15ff25430ede"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

#lakehouse_name = "LH"  # name of your Lakehouse
#target_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
#target_lakehouse_id = "a29c9d15-c24e-4779-8344-0c7c237b3990"


# THESE SHOULD BE SET BY THE SCHEDULER 
config_lakehouse_name = "LH1"  # name of your Lakehouse
config_workspace_id = "5555c1a9-026a-4f3c-9c85-cdd000943d7f"
config_lakehouse_id = "22f5aa2c-44c1-4aa9-98ce-15ff25430ede"
EDW_Warehouse_Workspace_Id = '50fbcab0-7d56-46f7-90f6-80ceb00ac86d'
EDW_Warehouse_Id = '8a26f137-fd0b-47f6-92e1-e4551593c751'
EDW_Warehouse_Name = 'WH'
EDW_Lakehouse_Workspace_Id = '50fbcab0-7d56-46f7-90f6-80ceb00ac86d'
EDW_Lakehouse_Id = '8a26f137-fd0b-47f6-92e1-e4551593c751'
EDW_Lakehouse_Name = 'LH'
Legacy_Synapse_Connection_Name = 'synapse_connection'
Synapse_Export_Shortcut_Path_In_Onelake = 'exports/'

# ONLY USE THIS IN DEV
full_reset = False # ⚠️ Full reset -- DESTRUCTIVE - will drop all tables


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Functions

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
    .save(f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/ddl_script_executions")

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
    print(f"skipping {guid}:{object_name} as the script has already run on workspace_id:{target_workspace_id} | lakehouse_id {target_lakehouse_id}")

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
    if not check_if_script_has_run(guid):
        try:
            work_fn()
            write_to_execution_log(object_guid=guid,
                                   object_name=object_name,
                                   script_status="Success")
        except Exception as e:
            print(f"Error in work_fn for {guid}: {e}")
            write_to_execution_log(object_guid=guid,
                                   object_name=object_name,
                                   script_status="Failure")
    else:
        print_skipped_script_execution(guid, object_name)


def drop_all_tables():
        # ──────────────────────────────────────────────────────────────────────────────
        # 1. CONFIGURATION – adjust these to your environment:
        # ──────────────────────────────────────────────────────────────────────────────


        # Base ABFSS endpoint and the ‘Tables’ root
        lakehouse_root = (
            f"abfss://{target_workspace_id}"
            f"@onelake.dfs.fabric.microsoft.com/"
            f"{target_lakehouse_id}/Tables"
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

# MARKDOWN ********************

# ## ⚠️ Full reset -- DESTRUCTIVE - will drop all tables

# CELL ********************


if full_reset:
    from ipywidgets import ToggleButtons, VBox, Label, Button
    from IPython.display import display as pdisplay
    from IPython.display import clear_output

    # Build the widget
    prompt = Label("❗ This will drop all tables — do you want to proceed?")
    yesno = ToggleButtons(options=["No","Yes"], description="Confirm Destructive Action:")
    go = Button(description="Submit", button_style="warning")

    out = VBox([prompt, yesno, go])
    pdisplay(out)

    # Define what happens on click
    def on_click(b):
        clear_output()  # hide the widget after click
        if yesno.value == "Yes":
            print("Dropping tables…")
            drop_all_tables()
        else:
            print("Operation cancelled.")

    go.on_click(on_click)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ddl_script_executions

# CELL ********************

guid="b8c83c87-36d2-46a8-9686-ced38363e169"
object_name = "ddl_script_executions"
execution_log_table_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{object_name}"
if(check_if_table_exists(execution_log_table_path) == False):
    empty_df = spark.createDataFrame([], execution_log_schema)
    (
        empty_df.write
        .format("delta")
        .option("parquet.vorder.default","true")
        .mode("errorIfExists")  # will error if table exists; change to "overwrite" to replace.
        .save(execution_log_table_path)
    )
    write_to_execution_log(object_guid=guid, object_name=object_name, script_status="Success")
else:
    print(f"Skipping {object_name} as it already exists")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tables

# MARKDOWN ********************

# #### Config Parquet Loads

# CELL ********************

guid = "f3a1c9b2-5e47-4d8c-9a5d-2b7f0a6e3c14"
object_name = "config_parquet_loads"
def config_parquet_loads():
    # Initial create script for table
    schema = StructType([
        StructField("target_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("target_lakehouse_name", StringType(), nullable=False),
        StructField("target_partition_columns", StringType(), nullable=False),
        StructField("target_sort_columns", StringType(), nullable=False),
        StructField("target_replace_where", StringType(), nullable=False),

        StructField("source_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("source_lakehouse_name", StringType(), nullable=False),
        StructField("source_file_path", StringType(), nullable=False),
        StructField("source_file_name", StringType(), nullable=False),

        StructField("synapse_connection_name", StringType(), nullable=False),
        StructField("synapse_source_schema_name", StringType(), nullable=False),
        StructField("synapse_source_table_name", StringType(), nullable=False),
        StructField("synapse_partition_clause", StringType(), nullable=False),

        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False)
    ])

    empty_df = spark.createDataFrame([], schema)
    (
        empty_df.write
        .format("delta")
        .option("parquet.vorder.default","true")
        .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
        .save(f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{object_name}")
    )
run_once(config_parquet_loads, object_name, guid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Config Synapse Extracts

# CELL ********************

guid = "a8d4e1f9-3b72-42d0-af1c-7e6b9c4f2d58"
object_name = "config_synapse_extracts"
def config_synapse_extracts():
    schema = StructType([
        StructField("synapse_connection_name", StringType(), nullable=False),
        StructField("source_schema_name", StringType(), nullable=False),
        StructField("source_table_name", StringType(), nullable=False),
        StructField("partition_clause", StringType(), nullable=False),
        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False)
    ])

    empty_df = spark.createDataFrame([], schema)
    (
        empty_df.write
        .format("delta")
        .option("parquet.vorder.default","true")
        .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
        .save(f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{object_name}")
    )
run_once(config_synapse_extracts, object_name, guid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Log Synapse Extracts

# CELL ********************

guid="c2e5b6a0-8f31-4d29-bcf4-5a1d7e9b0c63"
object_name = "log_synapse_extracts"
def log_synapse_extracts():
        schema = StructType([
                StructField("execution_id", StringType(), nullable=False),
                StructField("synapse_connection_name", StringType(), nullable=False),
                StructField("source_schema_name", StringType(), nullable=False),
                StructField("source_table_name", StringType(), nullable=False),
                StructField("partition_clause", StringType(), nullable=False),
                StructField("status", StringType(), nullable=False),
                StructField("error_messages", StringType(), nullable=True),
                StructField("start_date", TimestampType(), nullable=False),
                StructField("finish_date", TimestampType(), nullable=False),
                StructField("update_date", TimestampType(), nullable=False),
        ])

        empty_df = spark.createDataFrame([], schema)
        (
            empty_df.write
            .format("delta")
            .option("parquet.vorder.default","true")
            .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
            .save(f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{object_name}")
        )

run_once(log_synapse_extracts, object_name, guid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Log Parquet Loads

# CELL ********************

object_name = "log_parquet_loads"
guid = "d7f8a2c3-1e54-49b6-9f0a-3c8d4b5e6a71"
def log_parquet_loads():
        schema = StructType([
                StructField("execution_id", StringType(), nullable=False),
                StructField("target_lakehouse_workspace_id", StringType(), nullable=False),
                StructField("target_lakehouse_name", StringType(), nullable=False),
                StructField("partition_clause", StringType(), nullable=True),
                StructField("status", StringType(), nullable=False),
                StructField("error_messages", StringType(), nullable=True),
                StructField("start_date", TimestampType(), nullable=False),
                StructField("finish_date", TimestampType(), nullable=False),
                StructField("update_date", TimestampType(), nullable=False),
        ])

        empty_df = spark.createDataFrame([], schema)
        (
            empty_df.write
            .format("delta")
            .option("parquet.vorder.default","true")
            .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
            .save(f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/{object_name}")
        )

run_once(log_parquet_loads, object_name,guid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Configuration Data

# MARKDOWN ********************

# ### Synapse Extracts

# CELL ********************

guid = "9a6e1f4b-3c72-48d2-8f5b-1e7c3d9a0b24"
def insert_synapse_extracts():
    # 1. Prepare your data as a list of tuples
    data = [
        (Legacy_Synapse_Connection_Name, "dbo", "dim_customer", "", 1, "Y"),
        (Legacy_Synapse_Connection_Name, "dbo", "fact_transactions", "where year = @year and month = @month", 1, "Y"),
    ]

    # 2. Create a DataFrame
    schema = StructType([
        StructField("synapse_connection_name", StringType(), nullable=False),
        StructField("source_schema_name", StringType(), nullable=False),
        StructField("source_table_name", StringType(), nullable=False),
        StructField("partition_clause", StringType(), nullable=False),
        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False)
    ])
    insert_df = spark.createDataFrame(data, schema)

    # 3. Append to the existing Delta table
    table_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/config_synapse_extracts" 

    insert_df.write \
        .format("delta") \
        .mode("append") \
        .save(table_path)

run_once(insert_synapse_extracts, "insert_synapse_extracts", guid)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Synapse Extracts - Update Example

# CELL ********************

guid = "e4b2d7c1-9f05-4a3e-b1c8-7d2f0e9a6b38"
def synapse_extracts_upd():
    updates = [
        (Legacy_Synapse_Connection_Name, "dbo", "dim_customer",
        "where year = @year and month = @month",  # new partition_clause
        1,
        "N"),  # new active_yn
        (Legacy_Synapse_Connection_Name, "dbo", "fact_transactions",
        "where year = @year and month = @month",  # new partition_clause
        1,
        "N"),  # new active_yn
    ]
    schema = StructType([
            StructField("synapse_connection_name", StringType(), nullable=False),
            StructField("source_schema_name", StringType(), nullable=False),
            StructField("source_table_name", StringType(), nullable=False),
            StructField("partition_clause", StringType(), nullable=False),
            StructField("execution_group", IntegerType(), nullable=False),
            StructField("active_yn", StringType(), nullable=False)
        ])
    updates_df = spark.createDataFrame(updates, schema)

    table_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/config_synapse_extracts" 
    delta_table = DeltaTable.forPath(spark, table_path)

    (
        delta_table.alias("tgt")
        .merge(
            updates_df.alias("src"),
            """
                tgt.synapse_connection_name = src.synapse_connection_name
                AND tgt.source_schema_name       = src.source_schema_name
                AND tgt.source_table_name        = src.source_table_name
            """
        )
        .whenMatchedUpdate(set = {
            "partition_clause": "src.partition_clause",
            "execution_group":   "src.execution_group",
            "active_yn":         "src.active_yn"
        })
        .whenNotMatchedInsertAll()
        .execute()
    )

run_once(synapse_extracts_upd,"synapse_extracts_update", guid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

guid = "47f29a3b-8c4d-4e2f-9b7a-3d1c5e6f8a02"
object_name = "insert_parquet_loads"
def insert_parquet_loads():
    schema = StructType([
        StructField("target_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("target_lakehouse_name", StringType(), nullable=False),
        StructField("target_partition_columns", StringType(), nullable=False),
        StructField("target_sort_columns", StringType(), nullable=False),
        StructField("target_replace_where", StringType(), nullable=False),

        StructField("source_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("source_lakehouse_name", StringType(), nullable=False),
        StructField("source_file_path", StringType(), nullable=False),
        StructField("source_file_name", StringType(), nullable=False),

        StructField("synapse_connection_name", StringType(), nullable=False),
        StructField("synapse_source_schema_name", StringType(), nullable=False),
        StructField("synapse_source_table_name", StringType(), nullable=False),
        StructField("synapse_partition_clause", StringType(), nullable=False),

        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False),
    ])


    # ──────────────────────────────────────────────────────────────────────────────
    # Build the data rows
    # ──────────────────────────────────────────────────────────────────────────────
    data = [
        (
            EDW_Lakehouse_Workspace_Id,
            EDW_Lakehouse_Name,
            "",  # target_partition_columns
            "",  # target_sort_columns
            "",  # target_replace_where

            EDW_Lakehouse_Workspace_Id,
            EDW_Lakehouse_Name,
            f"{Synapse_Export_Shortcut_Path_In_Onelake}dbo_dim_customer",
            "",

            "synapse_connection",
            "dbo",
            "dim_customer",
            "",

            1,
            "Y"
        ),
        (
            EDW_Lakehouse_Workspace_Id,
            EDW_Lakehouse_Name,
            "year, month",
            "year, month",
            f"WHERE year = @year AND month = @month",

            EDW_Lakehouse_Workspace_Id,
            EDW_Lakehouse_Name,
            f"{Synapse_Export_Shortcut_Path_In_Onelake}dbo_dim_customer",
            "",

            "synapse_connection",
            "dbo",
            "fact_transactions",
            f"WHERE year = @year AND month = @month",

            1,
            "Y"
        ),
    ]

    insert_df = spark.createDataFrame(data, schema)

    # 3. Append to the existing Delta table
    table_path = f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_lakehouse_id}/Tables/config_parquet_loads" 

    insert_df.write \
        .format("delta") \
        .mode("append") \
        .save(table_path)

run_once(insert_parquet_loads, "insert_parquet_loads", guid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Show the log contents

# CELL ********************

print_log()
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read config_parquet_loads table
df_loads = spark.read.format("delta").load(parquet_loads_table_uri)

# Collect all rows for processing
load_entries = df_loads.collect()

resolved_records = []

for row in load_entries:
    row_dict = row.asDict()
    resolved_row = row_dict.copy()

    # Resolve all cfg_* columns using the FabricConfig
    for key, value in row_dict.items():
        if key.startswith("cfg_") and value:
            target_attr = value
            try:
                resolved_value = configs.get_attribute(target_attr)
                resolved_row[target_attr] = resolved_value  # Add resolved field
            except AttributeError as e:
                print(f"Warning: {e}")

    resolved_records.append(resolved_row)

# Convert back to DataFrame if needed
resolved_df = spark.createDataFrame(resolved_records)
resolved_df.show(truncate=False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
