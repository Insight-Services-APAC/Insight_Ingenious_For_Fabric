from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType, LongType
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import List, Callable, Optional
import inspect, hashlib
from dataclasses import dataclass, asdict
from notebookutils import mssparkutils

class lakehouse_utils:
    """Utility helpers for interacting with a Spark lakehouse."""

    def __init__(self, target_workspace_id: str, target_lakehouse_id: str) -> None:
        self.target_workspace_id = target_workspace_id
        self.target_lakehouse_id = target_lakehouse_id

    @staticmethod
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

    def lakehouse_tables_uri(self):
        return f"abfss://{self.target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self.target_lakehouse_id}/Tables/"        

    def write_to_lakehouse_table(
        df,
        table_name: str,        
        format: str = "delta",
        mode: str = "overwrite",
        options: dict = None
    ):
        writer = df.write.format(format).mode(mode)        

        if options:
            for k, v in options.items():
                writer = writer.option(k, v)

        writer.save(f"{self.lakehouse_tables_uri()}{table_name}")
    
    def drop_all_tables(self):
            # Base ABFSS endpoint and the ‘Tables’ root
            lakehouse_root = (
                f"abfss://{self.target_workspace_id}"
                f"@onelake.dfs.fabric.microsoft.com/"
                f"{self.target_lakehouse_id}/Tables"
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

