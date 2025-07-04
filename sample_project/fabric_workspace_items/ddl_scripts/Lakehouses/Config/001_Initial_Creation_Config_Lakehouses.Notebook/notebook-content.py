# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************



# THESE SHOULD BE SET BY THE SCHEDULER 
fabric_environment = "development"  # fabric environment, e.g. development, test, production
config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
config_lakehouse_id = "f0121783-34cc-4e64-bfb5-6b44b1a7f04b"
target_lakehouse_config_prefix = ""
# ONLY USE THIS IN DEV
full_reset = False # ⚠️ Full reset -- DESTRUCTIVE - will drop all tables




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions

# CELL ********************




# Auto-generated library code from python_libs
# Files are ordered based on dependency analysis


# === config_utils.py ===
from dataclasses import dataclass
from typing import Any


class config_utils:
    # variableLibraryInjectionStart: var_lib

# All variables as a dictionary
configs_dict = {'fabric_environment': 'development', 'fabric_deployment_workspace_id': '3a4fc13c-f7c5-463e-a9de-57c4754699ff', 'synapse_source_database_1': 'test1', 'config_workspace_id': '3a4fc13c-f7c5-463e-a9de-57c4754699ff', 'synapse_source_sql_connection': 'sansdaisyn-ondemand.sql.azuresynapse.net', 'config_lakehouse_name': 'config', 'edw_warehouse_name': 'edw', 'config_lakehouse_id': '2629d4cc-685c-458a-866b-b4705dde71a7', 'edw_workspace_id': '50fbcab0-7d56-46f7-90f6-80ceb00ac86d', 'edw_warehouse_id': 's', 'edw_lakehouse_id': '6adb67d6-c8eb-4612-9053-890cae3a55d7', 'edw_lakehouse_name': 'edw', 'legacy_synapse_connection_name': 'synapse_connection', 'synapse_export_shortcut_path_in_onelake': 'exports/'}
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
configs_object: ConfigsObject = ConfigsObject(**configs_dict)
# variableLibraryInjectionEnd: var_lib

    def __init__(self):
        self.fabric_environments_table_name = "fabric_environments"
        self.fabric_environments_table_schema = "config"
        self.fabric_environments_table = f"{self.fabric_environments_table_schema}.{self.fabric_environments_table_name}"
        self._configs: dict[str, Any] = {}

    def get_configs_as_dict(self):
        return config_utils.configs_dict

    def get_configs_as_object(self):
        return self.configs_object


# === lakehouse_utils.py ===
from __future__ import annotations

from typing import Any

from delta.tables import DeltaTable

from pyspark.sql import SparkSession



class lakehouse_utils(DataStoreInterface):
    """Utility helpers for interacting with a Spark lakehouse.
    This class provides methods to manage Delta tables in a lakehouse environment,
    including checking table existence, writing data, listing tables, and dropping tables.
    It supports both local Spark sessions and Fabric environments.
    Attributes:
        target_workspace_id (str): The ID of the target workspace.
        target_lakehouse_id (str): The ID of the target lakehouse.
        spark_version (str): The Spark version being used, either 'local' or 'fabric'.
        spark (SparkSession): The Spark session instance.
    Methods:
        get_connection(): Returns the Spark session connection.
        check_if_table_exists(table_name: str, schema_name: str | None = None): Checks if a Delta table exists at the given table name.
        lakehouse_tables_uri(): Returns the ABFSS URI for the lakehouse Tables directory.
        write_to_table(df, table_name: str, schema_name: str | None = None
        , mode: str = "overwrite", options: dict[str, str] | None = None): Writes a DataFrame to a lakehouse table.
        list_tables(): Lists all tables in the lakehouse.
        drop_all_tables(schema_name: str | None = None, table_prefix: str | None = None): Drops all Delta tables in the lakehouse.
        execute_query(query): Executes a SQL query on the lakehouse.
    Usage:
        lakehouse = lakehouse_utils(target_workspace_id="your_workspace_id", target_lakehouse_id="your_lakehouse_id")
        spark_session = lakehouse.get_connection()
    """

    def __init__(self, target_workspace_id: str, target_lakehouse_id: str) -> None:
        super().__init__()
        self._target_workspace_id = target_workspace_id
        self._target_lakehouse_id = target_lakehouse_id
        self.spark_version = "fabric"
        self.spark = self._get_or_create_spark_session()

    @property
    def target_workspace_id(self) -> str:
        """Get the target workspace ID."""
        return self._target_workspace_id

    @property
    def target_store_id(self) -> str:
        """Get the target lakehouse ID."""
        return self._target_lakehouse_id

    def get_connection(self) -> SparkSession:
        """Get the Spark session connection."""
        return self.spark

    def _get_or_create_spark_session(self) -> SparkSession:
        """Get existing Spark session or create a new one."""
        if (
            "spark" not in locals()
            and "spark" not in globals()
            and self.spark_version == "fabric"
        ):
            # Create new Spark session if none exists
            self.spark_version = "local"
            print(
                "No active Spark session found, creating a new one with Delta support."
            )
            builder = (
                SparkSession.builder.appName("MyApp")
                .config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                )
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
            )
            from delta import configure_spark_with_delta_pip

            return configure_spark_with_delta_pip(builder).getOrCreate()
        else:
            print("Using existing spark .. Fabric environment   .")
            return spark  # type: ignore  # noqa: F821

    def check_if_table_exists(
        self, table_name: str, schema_name: str | None = None
    ) -> bool:
        """Check if a Delta table exists at the given table name."""
        # For lakehouse, schema_name is not used as tables are in the Tables directory
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        table_exists = False
        try:
            if DeltaTable.isDeltaTable(self.spark, table_path):
                table_exists = True
                print(
                    f"Delta table already exists at path: {table_path}, skipping creation."
                )
            else:
                print(f"Path {table_path} is not a Delta table.")
        except Exception as e:
            # If the path does not exist or is inaccessible, isDeltaTable returns False or may throw.
            # Treat exceptions as "table does not exist".
            print(
                f"Could not verify Delta table existence at {table_path} (exception: {e}); assuming it does not exist."
            )
            table_exists = False
        return table_exists

    def lakehouse_tables_uri(self) -> str:
        """Get the ABFSS URI for the lakehouse Tables directory."""
        if self.spark_version == "local":
            return f"file:///tmp/{self._target_lakehouse_id}/Tables/"
        else:
            return f"abfss://{self._target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse_id}/Tables/"

    def write_to_table(
        self,
        df,
        table_name: str,
        schema_name: str | None = None,
        mode: str = "overwrite",
        options: dict[str, str] | None = None,
    ) -> None:
        """Write a DataFrame to a lakehouse table."""
        # For lakehouse, schema_name is not used as tables are in the Tables directory
        writer = df.write.format("delta").mode(mode)

        if options:
            for k, v in options.items():
                writer = writer.option(k, v)

        writer.save(f"{self.lakehouse_tables_uri()}{table_name}")

        # Register the table in the Hive catalog - Only needed if local
        if self.spark_version == "local":
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{self.lakehouse_tables_uri()}{table_name}'"
            )

    def list_tables(self) -> list[str]:
        """List all tables in the lakehouse."""
        return self.spark.catalog.listTables()
        # return self.spark.sql("SHOW TABLES").collect()

    def drop_all_tables(
        self, schema_name: str | None = None, table_prefix: str | None = None
    ) -> None:
        """Drop all Delta tables in the lakehouse."""

        # ──────────────────────────────────────────────────────────────────────────────

        # 2. START spark and get Hadoop FS handle
        # ──────────────────────────────────────────────────────────────────────────────
        spark = self.spark

        # Access Hadoop's FileSystem via the JVM gateway
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

        # Path object for the Tables/ directory
        root_path = spark._jvm.org.apache.hadoop.fs.Path(self.lakehouse_tables_uri())

        # ──────────────────────────────────────────────────────────────────────────────
        # 3. Iterate, detect Delta tables, and delete
        # ──────────────────────────────────────────────────────────────────────────────

        for status in fs.listStatus(root_path):
            table_path_obj = status.getPath()
            print(f"— Checking path: {table_path_obj}")
            table_path = table_path_obj.toString()  # e.g. abfss://…/Tables/my_table
            print(f"— Full path: {table_path}")

            # Apply table_prefix filter if provided
            table_name_from_path = table_path.split("/")[-1]
            if table_prefix and not table_name_from_path.startswith(table_prefix):
                print(
                    f"— Skipping table {table_name_from_path} (doesn't match prefix '{table_prefix}')"
                )
                continue

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

        print("✅ All eligible Delta tables under 'Tables/' have been dropped.")

    def execute_query(self, query):
        """
        Execute a SQL query on the lakehouse.

        Args:
            query: SQL query to execute

        Returns:
            Query result (type varies by implementation)
        """
        spark = self.spark
        return spark.sql(query)

    def get_table_schema(
        self, table_name: str, schema_name: str | None = None
    ) -> dict[str, Any]:
        """
        Get the schema/column definitions for a table.
        """
        df = self.spark.read.format("delta").load(
            f"{self.lakehouse_tables_uri()}{table_name}"
        )
        return {field.name: field.dataType.simpleString() for field in df.schema.fields}

    def read_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        filters: dict[str, Any] | None = None,
    ) -> Any:
        """
        Read data from a table, optionally filtering columns, rows, or limiting results.
        """
        df = self.spark.read.format("delta").load(
            f"{self.lakehouse_tables_uri()}{table_name}"
        )
        if columns:
            df = df.select(*columns)
        if filters:
            for col, val in filters.items():
                df = df.filter(f"{col} = '{val}'")
        if limit:
            df = df.limit(limit)
        return df

    def delete_from_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> int:
        """
        Delete rows from a table matching filters. Returns number of rows deleted.
        """
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        delta_table = DeltaTable.forPath(self.spark, table_path)
        if filters:
            condition = " AND ".join(
                [f"{col} = '{val}'" for col, val in filters.items()]
            )
        else:
            condition = "true"
        before_count = delta_table.toDF().count()
        delta_table.delete(condition)
        after_count = delta_table.toDF().count()
        return before_count - after_count

    def rename_table(
        self,
        old_table_name: str,
        new_table_name: str,
        schema_name: str | None = None,
    ) -> None:
        """
        Rename a table by moving its directory and updating the metastore if local.
        """
        import shutil

        src = f"{self.lakehouse_tables_uri()}{old_table_name}"
        dst = f"{self.lakehouse_tables_uri()}{new_table_name}"
        shutil.move(src.replace("file://", ""), dst.replace("file://", ""))
        if self.spark_version == "local":
            self.spark.sql(f"ALTER TABLE {old_table_name} RENAME TO {new_table_name}")

    def create_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        schema: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
    ) -> None:
        """
        Create a new table with a given schema.
        """
        if schema is None:
            raise ValueError("Schema must be provided for table creation.")
        fields = ", ".join([f"{col} {dtype}" for col, dtype in schema.items()])
        sql = f"CREATE TABLE {table_name} ({fields}) USING DELTA LOCATION '{self.lakehouse_tables_uri()}{table_name}'"
        if options:
            opts = " ".join(
                [f"TBLPROPERTIES ('{k}'='{v}')" for k, v in options.items()]
            )
            sql += f" {opts}"
        self.spark.sql(sql)

    def drop_table(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> None:
        """
        Drop a single table.
        """
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        delta_table = DeltaTable.forPath(self.spark, table_path)
        delta_table.delete()
        # Optionally, remove the directory
        import shutil

        shutil.rmtree(table_path.replace("file://", ""), ignore_errors=True)
        if self.spark_version == "local":
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def list_schemas(self) -> list[str]:
        """
        List all schemas/namespaces in the lakehouse (returns ['default'] for lakehouse).
        """
        return ["default"]

    def get_table_row_count(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> int:
        """
        Get the number of rows in a table.
        """
        df = self.spark.read.format("delta").load(
            f"{self.lakehouse_tables_uri()}{table_name}"
        )
        return df.count()

    def get_table_metadata(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> dict[str, Any]:
        """
        Get metadata for a table (creation time, size, etc.).
        """
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        delta_table = DeltaTable.forPath(self.spark, table_path)
        details = delta_table.detail().toPandas().to_dict(orient="records")[0]
        return details

    def vacuum_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        retention_hours: int = 168,
    ) -> None:
        """
        Perform cleanup/compaction on a table (for systems like Delta Lake).
        """
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        self.spark.sql(f"VACUUM '{table_path}' RETAIN {retention_hours} HOURS")


# === ddl_utils.py ===
from __future__ import annotations

import hashlib
import inspect
from datetime import datetime

from pyspark.sql import SparkSession  # type: ignore # noqa: F401
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)



class ddl_utils(DDLUtilsInterface):
    def __init__(self, target_workspace_id: str, target_lakehouse_id: str) -> None:
        """
        Initializes the DDLUtils class with the target workspace and lakehouse IDs.
        """
        super().__init__(
            target_lakehouse_id=target_lakehouse_id,
            target_workspace_id=target_workspace_id,
        )
        self.target_workspace_id = target_workspace_id
        self.target_lakehouse_id = target_lakehouse_id
        self.lakehouse_utils = lakehouse_utils(target_workspace_id, target_lakehouse_id)
        self.execution_log_table_name = "ddl_script_executions"
        self.initialise_ddl_script_executions_table()

    @staticmethod
    def execution_log_schema() -> StructType:
        return StructType(
            [
                StructField("script_id", StringType(), nullable=False),
                StructField("script_name", StringType(), nullable=False),
                StructField("execution_status", StringType(), nullable=False),
                StructField("update_date", TimestampType(), nullable=False),
            ]
        )

    def print_log(self) -> None:
        df = self.lakehouse_utils.spark.read.format("delta").load(
            f"{self.lakehouse_utils.lakehouse_tables_uri()}{self.execution_log_table_name}"
        )
        if self.lakehouse_utils.spark_version == "local":
            df.show()
        else:
            display(df)  # type: ignore # noqa: F821

    def check_if_script_has_run(self, script_id: str) -> bool:
        from pyspark.sql.functions import col

        df = self.lakehouse_utils.spark.read.format("delta").load(
            f"{self.lakehouse_utils.lakehouse_tables_uri()}{self.execution_log_table_name}"
        )
        # display(df)
        # Build filter condition
        cond = col("script_id") == script_id
        status = "Success"
        if status is not None:
            cond = cond & (col("execution_status") == status)
        matched = df.filter(cond).limit(1).take(1)
        if (len(matched)) == 0:
            # print("matched:", matched)
            return False
        else:
            return True

    def print_skipped_script_execution(self, guid: str, object_name: str) -> None:
        print(
            f"skipping {guid}:{object_name} as the script has already run on workspace_id:"
            f"{self.target_workspace_id} | lakehouse_id {self.target_lakehouse_id}"
        )

    def write_to_execution_log(
        self, object_guid: str, object_name: str, script_status: str
    ) -> None:
        data = [(object_guid, object_name, script_status, datetime.now())]
        new_df = self.lakehouse_utils.spark.createDataFrame(
            data=data, schema=ddl_utils.execution_log_schema()
        )
        new_df.write.format("delta").mode("append").save(
            f"{self.lakehouse_utils.lakehouse_tables_uri()}{self.execution_log_table_name}"
        )

    def run_once(self, work_fn, object_name: str, guid: str | None = None) -> None:
        """
        Runs `work_fn()` exactly once, keyed by `guid`. If `guid` is None,
        it's computed by hashing the source code of `work_fn`.
        """
        # 1. Auto-derive GUID if not provided
        if guid is None:
            try:
                src = inspect.getsource(work_fn)
            except (OSError, TypeError):
                raise ValueError(
                    "work_fn must be a named function defined at top-level"
                )
            # compute SHA256 and take first 12 hex chars
            digest = hashlib.sha256(src.encode("utf-8")).hexdigest()
            guid = digest
            print(f"Derived guid={guid} from work_fn source")

        # 2. Check execution
        if not self.check_if_script_has_run(script_id=guid):
            try:
                work_fn()
                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Success"
                )
            except Exception as e:
                print(f"Error in work_fn for {guid}: {e}")
                self.write_to_execution_log(
                    object_guid=guid, object_name=object_name, script_status="Failure"
                )
                raise e
        else:
            self.print_skipped_script_execution(guid=guid, object_name=object_name)

    def initialise_ddl_script_executions_table(self) -> None:
        guid = "b8c83c87-36d2-46a8-9686-ced38363e169"
        object_name = "ddl_script_executions"
        # Check if the execution log table exists
        table_exists = self.lakehouse_utils.check_if_table_exists(
            self.execution_log_table_name
        )
        if not table_exists:
            print(
                f"Creating execution log table at {self.lakehouse_utils.lakehouse_tables_uri()}{self.execution_log_table_name}"
            )
            empty_df = self.lakehouse_utils.spark.createDataFrame(
                data=[], schema=ddl_utils.execution_log_schema()
            )
            (
                empty_df.write.format("delta")
                .option("parquet.vorder.default", "true")
                .mode(
                    "errorIfExists"
                )  # will error if table exists; change to "overwrite" to replace.
                .save(
                    f"{self.lakehouse_utils.lakehouse_tables_uri()}{self.execution_log_table_name}"
                )
            )
            self.write_to_execution_log(
                object_guid=guid, object_name=object_name, script_status="Success"
            )
        else:
            print(f"Skipping {object_name} as it already exists")






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Instantiate the Helper Classes

# CELL ********************



cu = config_utils(config_workspace_id,config_lakehouse_id)
configs = cu.get_configs_as_object(fabric_environment)
target_workspace_id = configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_workspace_id")
target_lakehouse_id = configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_lakehouse_id")
lu = lakehouse_utils(target_workspace_id, target_lakehouse_id)
du = ddl_utils(target_workspace_id, target_lakehouse_id)




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
    
    empty_df = spark.createDataFrame([], schema)
    (
        empty_df.write.format("delta")
        .option("parquet.vorder.default", "true")
        .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
        .save(f"{lu.lakehouse_tables_uri()}config_parquet_loads")
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

# ## 📄 Cell for 002_config_synapse_loads_create.py

# CELL ********************

guid="3ae71625bf5b"
object_name = "002_config_synapse_loads_create"

def script_to_execute():
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
    
    empty_df = spark.createDataFrame([], schema)
    (
        empty_df.write.format("delta")
        .option("parquet.vorder.default", "true")
        .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
        .save(f"{lu.lakehouse_tables_uri()}config_synapse_extracts")
    )
    
    

du.run_once(script_to_execute, "002_config_synapse_loads_create","3ae71625bf5b")

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
    
    empty_df = spark.createDataFrame([], schema)
    (
        empty_df.write.format("delta")
        .option("parquet.vorder.default", "true")
        .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
        .save(
            f"{lu.lakehouse_tables_uri()}log_parquet_loads"  # noqa: E501
        )
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
    
    empty_df = spark.createDataFrame([], schema)
    (
        empty_df.write.format("delta")
        .option("parquet.vorder.default", "true")
        .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
        .save(
            f"{lu.lakehouse_tables_uri()}log_synapse_extracts"  # noqa: E501
        )
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
    insert_df = spark.createDataFrame(data, schema)
    
    # 3. Append to the existing Delta table
    table_path = f"{lu.lakehouse_tables_uri()}config_synapse_extracts"
    
    insert_df.write.format("delta").mode("append").save(table_path)
    
    

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
    
    insert_df = spark.createDataFrame(data, schema)
    
    # 3. Append to the existing Delta table
    table_path = f"{lu.lakehouse_tables_uri()}config_parquet_loads"
    
    insert_df.write.format("delta").mode("append").save(table_path)
    
    

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



from notebookutils import mssparkutils
mssparkutils.notebook.exit("success")



