from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

import ingen_fab.python_libs.common.config_utils as cu
from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface


@dataclass
class FileInfo:
    """Custom FileInfo dataclass for file metadata."""
    path: str
    name: str
    size: int
    modified_ms: int  # Unix timestamp in milliseconds

    @property
    def size_mb(self) -> float:
        """Return size in megabytes."""
        return self.size / (1024 * 1024)

    @property
    def extension(self) -> str:
        """Return file extension."""
        return self.name.split('.')[-1] if '.' in self.name else ''

    @property
    def modified(self) -> datetime:
        """Return modified time as datetime object."""
        return datetime.fromtimestamp(self.modified_ms / 1000)


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

    def __init__(
        self,
        target_workspace_id: str | None = None,
        target_lakehouse_id: str | None = None,
        target_workspace_name: str | None = None,
        target_lakehouse_name: str | None = None,
        spark: SparkSession = None,
    ) -> None:
        super().__init__()

        # Prefer names over IDs, but accept both for backward compatibility
        self._target_workspace = target_workspace_name or target_workspace_id
        self._target_lakehouse = target_lakehouse_name or target_lakehouse_id

        if not self._target_workspace or not self._target_lakehouse:
            raise ValueError(
                "Either workspace name/ID and lakehouse name/ID must be provided"
            )

        self.spark_version = "fabric"
        if spark:
            if not isinstance(spark, SparkSession):
                raise TypeError("Provided spark must be a SparkSession instance.")
            self.spark = spark
        else:
            # If no Spark session is provided, create a new one
            self.spark = self._get_or_create_spark_session()

    @property
    def target_workspace_id(self) -> str:
        """Get the target workspace (ID or name, for backward compatibility)."""
        return self._target_workspace

    @property
    def target_store_id(self) -> str:
        """Get the target lakehouse (ID or name, for backward compatibility)."""
        return self._target_lakehouse

    @property
    def target_workspace_name(self) -> str:
        """Get the target workspace name (or ID if using ID-based config)."""
        return self._target_workspace

    @property
    def target_store_name(self) -> str:
        """Get the target lakehouse name (or ID if using ID-based config)."""
        return self._target_lakehouse

    @property
    def get_connection(self) -> SparkSession:
        """Get the Spark session connection."""
        return self.spark

    def _get_or_create_spark_session(self) -> SparkSession:
        """Get existing Spark session or create a new one."""
        if cu.get_configs_as_object().fabric_environment == "local":
            # Check if there's already an active Spark session
            try:
                existing_spark = SparkSession.getActiveSession()
                if existing_spark is not None:
                    print("Found existing Spark session, reusing it.")
                    self.spark_version = "local"
                    return existing_spark
            except Exception as e:
                print(f"No active Spark session found: {e}")

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
            return self.spark  # type: ignore  # noqa: F821

    def check_if_table_exists(
        self, table_name: str, schema_name: str | None = None
    ) -> bool:
        """Check if a Delta table exists at the given table name."""
        # Handle schema_name same as write_to_table
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        table_path = f"{self.lakehouse_tables_uri()}{table_full_name}"
        try:
            return DeltaTable.isDeltaTable(self.spark, table_path)
        except Exception as e:
            print(
                f"Could not verify Delta table existence at {table_path} (exception: {e}); assuming it does not exist."
            )
            return False

    def lakehouse_tables_uri(self) -> str:
        """Get the ABFSS URI for the lakehouse Tables directory."""
        if self.spark_version == "local":
            return f"file:///{Path.cwd()}/tmp/spark/Tables/"
        else:
            # Auto-detect: IDs contain hyphens, names typically don't
            if "-" in self._target_lakehouse:
                # ID-based format
                return f"abfss://{self._target_workspace}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse}/Tables/"
            else:
                # Name-based format with .Lakehouse suffix
                return f"abfss://{self._target_workspace}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse}.Lakehouse/Tables/"

    def lakehouse_files_uri(self) -> str:
        """Get the ABFSS URI for the lakehouse Files directory."""
        if self.spark_version == "local":
            return f"file:///{Path.cwd()}/tmp/spark/Files/"
        else:
            # Auto-detect: IDs contain hyphens, names typically don't
            if "-" in self._target_lakehouse:
                # ID-based format
                return f"abfss://{self._target_workspace}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse}/Files/"
            else:
                # Name-based format with .Lakehouse suffix
                return f"abfss://{self._target_workspace}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse}.Lakehouse/Files/"

    def write_to_table(
        self,
        df,
        table_name: str,
        schema_name: str | None = None,
        mode: str = "overwrite",
        options: dict[str, str] | None = None,
        partition_by: list[str] | None = None,
        table_properties: dict[str, str] | None = None,
    ) -> None:
        """Write a DataFrame to a lakehouse table with optional partitioning.

        Args:
            df: DataFrame to write
            table_name: Name of the table
            schema_name: Optional schema name (prepended to table name with underscore)
            mode: Write mode ('overwrite', 'append', etc.)
            options: Additional write options
            partition_by: Columns to partition by
            table_properties: Delta table properties (e.g., {'delta.isolationLevel': 'WriteSerializable'})
        """

        # For lakehouse, schema_name is not used as tables are in the Tables directory
        writer = df.write.format("delta").mode(mode)
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        # Apply options
        if options:
            for k, v in options.items():
                writer = writer.option(k, v)

        # Apply table properties
        if table_properties:
            for k, v in table_properties.items():
                writer = writer.option(k, v)

        # Apply partitioning
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(f"{self.lakehouse_tables_uri()}{table_full_name}")

        # Register the table in the Hive catalog - Only needed if local
        if self.spark_version == "local":
            print(
                f"⚠ Alert: Registering table '{table_full_name}' in the Hive catalog for local Spark."
            )
            full_table_path = f"{self.lakehouse_tables_uri()}{table_full_name}"
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_full_name} USING DELTA LOCATION '{full_table_path}'"
            )

    def merge_to_table(
        self,
        df,
        table_name: str,
        merge_keys: list[str],
        schema_name: str | None = None,
        immutable_columns: list[str] | None = None,
        custom_update_expressions: dict[str, str] | None = None,
        enable_schema_evolution: bool = False,
        partition_by: list[str] | None = None,
        table_properties: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Merge DataFrame into table using Delta Lake merge operation.

        Args:
            df: Source DataFrame to merge
            table_name: Name of the target table
            merge_keys: Columns to use as join condition for merge
            schema_name: Optional schema name (prepended to table name with underscore)
            immutable_columns: Columns to preserve from target during updates
                              (default: ["_raw_created_at"])
            custom_update_expressions: Custom SQL expressions for specific columns on update
                                     (e.g., {"attempt_count": "TARGET.attempt_count + 1"})
            enable_schema_evolution: Enable schema auto-merge for new columns
            partition_by: Optional list of columns to partition by (used on initial create)
            table_properties: Delta table properties (e.g., {'delta.isolationLevel': 'WriteSerializable'})

        Returns:
            Dictionary with merge metrics:
                - records_inserted: Number of new records inserted
                - records_updated: Number of existing records updated
                - records_deleted: Number of records deleted (always 0 for this operation)
                - target_row_count_before: Row count before merge
                - target_row_count_after: Row count after merge
        """
        import time

        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        table_path = f"{self.lakehouse_tables_uri()}{table_full_name}"
        immutable_cols = set(immutable_columns or ["_raw_created_at"])

        # Check if table exists
        table_exists = self.check_if_table_exists(table_name, schema_name)

        if not table_exists:
            # Initial load - create table with data
            write_options = {}
            if enable_schema_evolution:
                write_options["mergeSchema"] = "true"

            self.write_to_table(
                df=df,
                table_name=table_name,
                schema_name=schema_name,
                mode="overwrite",
                options=write_options,
                partition_by=partition_by,
                table_properties=table_properties,
            )

            # Get row count after initial load
            result_df = self.read_table(table_name, schema_name)
            row_count = result_df.count()

            return {
                "records_inserted": row_count,
                "records_updated": 0,
                "records_deleted": 0,
                "target_row_count_before": 0,
                "target_row_count_after": row_count,
            }

        # Table exists - perform merge
        delta_table = DeltaTable.forPath(self.spark, table_path)

        # Get row count before merge
        target_row_count_before = delta_table.toDF().count()

        # Build merge condition from merge_keys
        merge_condition = " AND ".join(
            [f"target.{key} = source.{key}" for key in merge_keys]
        )

        # Enable schema evolution if configured
        if enable_schema_evolution:
            self.spark.conf.set(
                "spark.databricks.delta.schema.autoMerge.enabled", "true"
            )

        # Build merge operation
        merge_builder = delta_table.alias("target").merge(
            df.alias("source"), merge_condition
        )

        # When matched: update all columns except immutable ones
        update_columns = {
            col: f"source.{col}" for col in df.columns if col not in immutable_cols
        }

        # Apply custom update expressions if provided
        if custom_update_expressions:
            update_columns.update(custom_update_expressions)

        merge_builder = merge_builder.whenMatchedUpdate(set=update_columns)

        # When not matched: insert all columns
        merge_builder = merge_builder.whenNotMatchedInsertAll()

        # Execute the merge
        merge_builder.execute()

        # Get metrics from Delta table history
        history = delta_table.history(1).select("operationMetrics").collect()[0]
        operation_metrics = history["operationMetrics"]

        # Extract merge metrics
        records_inserted = int(operation_metrics.get("numTargetRowsInserted", 0))
        records_updated = int(operation_metrics.get("numTargetRowsUpdated", 0))
        records_deleted = int(operation_metrics.get("numTargetRowsDeleted", 0))

        # Get row count after merge
        target_row_count_after = delta_table.toDF().count()

        return {
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_deleted": records_deleted,
            "target_row_count_before": target_row_count_before,
            "target_row_count_after": target_row_count_after,
        }

    def list_tables(self) -> list[str]:
        """List all tables in the lakehouse."""
        return [row.tableName for row in self.spark.sql("SHOW TABLES").collect()]

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
        # Handle schema_name same as write_to_table
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        df = self.spark.read.format("delta").load(
            f"{self.lakehouse_tables_uri()}{table_full_name}"
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
        # Handle schema_name same as write_to_table
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        df = self.spark.read.format("delta").load(
            f"{self.lakehouse_tables_uri()}{table_full_name}"
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
        # Handle schema_name same as write_to_table
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        table_path = f"{self.lakehouse_tables_uri()}{table_full_name}"
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
        mode: str = "overwrite",
        options: dict[str, Any] | None = None,
        partition_by: list[str] | None = None,
    ) -> None:
        """
        Create a new table with a given schema.
        """
        if schema is None:
            raise ValueError("Schema must be provided for table creation.")

        empty_df = self.get_connection.createDataFrame([], schema)
        writer = empty_df.write.format("delta").mode(mode)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        if options:
            for k, v in options.items():
                writer = writer.option(k, v)

        writer.save(f"{self.lakehouse_tables_uri()}{table_name}")

        # Register the table in the Hive catalog - Only needed if local
        if self.spark_version == "local":
            full_table_path = f"{self.lakehouse_tables_uri()}{table_name}"
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{full_table_path}'"
            )

    def drop_table(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> None:
        """
        Drop a single table.
        """
        # Handle schema_name same as write_to_table
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        table_path = f"{self.lakehouse_tables_uri()}{table_full_name}"
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
        # Handle schema_name same as write_to_table
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        df = self.spark.read.format("delta").load(
            f"{self.lakehouse_tables_uri()}{table_full_name}"
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
        # Handle schema_name same as write_to_table
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        table_path = f"{self.lakehouse_tables_uri()}{table_full_name}"
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
        # Handle schema_name same as write_to_table
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"

        table_path = f"{self.lakehouse_tables_uri()}{table_full_name}"
        self.spark.sql(f"VACUUM '{table_path}' RETAIN {retention_hours} HOURS")

    def read_file(
        self,
        file_path: str,
        file_format: str,
        options: dict[str, Any] | None = None,
    ) -> Any:
        """
        Read a file from the file system using the appropriate method for the environment.

        In local mode, uses standard Spark file access.
        In Fabric mode, uses notebookutils.fs for OneLake file access.
        """
        if options is None:
            options = {}
        logging.debug(f"Reading file from: {file_path}")
        reader = self.spark.read.format(file_format.lower())

        # Apply common options based on file format
        if file_format.lower() == "csv":
            # Apply basic CSV-specific options
            if "header" in options:
                reader = reader.option("header", str(options["header"]).lower())
            if "delimiter" in options:
                reader = reader.option("sep", options["delimiter"])
            if "encoding" in options:
                reader = reader.option("encoding", options["encoding"])
            if "inferSchema" in options:
                reader = reader.option(
                    "inferSchema", str(options["inferSchema"]).lower()
                )
            if "dateFormat" in options:
                reader = reader.option("dateFormat", options["dateFormat"])
            if "timestampFormat" in options:
                reader = reader.option("timestampFormat", options["timestampFormat"])

            # Apply advanced CSV options for complex scenarios
            if "quote" in options:
                reader = reader.option("quote", options["quote"])
            if "escape" in options:
                reader = reader.option("escape", options["escape"])
            if "multiLine" in options:
                reader = reader.option("multiLine", str(options["multiLine"]).lower())
            if "ignoreLeadingWhiteSpace" in options:
                reader = reader.option(
                    "ignoreLeadingWhiteSpace",
                    str(options["ignoreLeadingWhiteSpace"]).lower(),
                )
            if "ignoreTrailingWhiteSpace" in options:
                reader = reader.option(
                    "ignoreTrailingWhiteSpace",
                    str(options["ignoreTrailingWhiteSpace"]).lower(),
                )
            if "nullValue" in options:
                reader = reader.option("nullValue", options["nullValue"])
            if "emptyValue" in options:
                reader = reader.option("emptyValue", options["emptyValue"])
            if "comment" in options:
                reader = reader.option("comment", options["comment"])
            if "maxColumns" in options:
                reader = reader.option("maxColumns", int(options["maxColumns"]))
            if "maxCharsPerColumn" in options:
                reader = reader.option(
                    "maxCharsPerColumn", int(options["maxCharsPerColumn"])
                )
            if "unescapedQuoteHandling" in options:
                reader = reader.option(
                    "unescapedQuoteHandling", options["unescapedQuoteHandling"]
                )
            if "enforceSchema" in options:
                reader = reader.option(
                    "enforceSchema", str(options["enforceSchema"]).lower()
                )
            if "columnNameOfCorruptRecord" in options:
                reader = reader.option(
                    "columnNameOfCorruptRecord", options["columnNameOfCorruptRecord"]
                )

        elif file_format.lower() == "json":
            # Apply JSON-specific options
            if "dateFormat" in options:
                reader = reader.option("dateFormat", options["dateFormat"])
            if "timestampFormat" in options:
                reader = reader.option("timestampFormat", options["timestampFormat"])

        # Apply custom schema if provided
        if "schema" in options:
            reader = reader.schema(options["schema"])

        # Build full file path using the lakehouse files URI
        if file_path.startswith("/"):
            # Absolute local path - convert to file:// URI
            if self.spark_version == "local":
                # Clean up any double slashes that might come from list_files
                clean_path = file_path.replace("//", "/")
                full_file_path = f"file://{clean_path}"
            else:
                # For Fabric, absolute paths shouldn't happen, treat as relative
                full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
        elif not file_path.startswith(("file://", "abfss://")):
            # Relative path - combine with lakehouse files URI

            if self.spark_version == "local":
                # print("Using local Spark file access.")
                # use path utils to get first part of the path
                first_part = Path(file_path).parts[0]
                print(f"First part of path: {first_part}")
                if first_part == "Files":
                    file_path = Path(*Path(file_path).parts[1:])
                full_file_path = f"{self.lakehouse_files_uri()}{str(file_path)}"
            else:
                # print("Using Fabric Spark file access.")
                full_file_path = f"{self.lakehouse_files_uri()}{file_path}"

        else:
            # Already absolute path
            full_file_path = file_path
        logging.debug(f"Reading file from: {full_file_path}")
        return reader.load(full_file_path)

    def write_file(
        self,
        df: Any,
        file_path: str,
        file_format: str,
        options: dict[str, Any] | None = None,
    ) -> None:
        """
        Write a DataFrame to a file using the appropriate method for the environment.
        """
        if options is None:
            options = {}

        writer = df.write.format(file_format.lower())

        # Apply write mode if specified
        if "mode" in options:
            writer = writer.mode(options["mode"])
        else:
            writer = writer.mode("overwrite")  # Default mode

        # Apply format-specific options
        if file_format.lower() == "csv":
            if "header" in options:
                writer = writer.option("header", str(options["header"]).lower())
            if "delimiter" in options:
                writer = writer.option("sep", options["delimiter"])

        # Apply other options
        for key, value in options.items():
            if key not in ["mode", "header", "delimiter", "schema"]:
                writer = writer.option(key, value)

        # Build full file path using the lakehouse files URI
        if file_path.startswith("/"):
            # Absolute local path - convert to file:// URI
            if self.spark_version == "local":
                # Clean up any double slashes that might come from list_files
                clean_path = file_path.replace("//", "/")
                full_file_path = f"file://{clean_path}"
            else:
                # For Fabric, absolute paths shouldn't happen, treat as relative
                full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
        elif not file_path.startswith(("file://", "abfss://")):
            # Relative path - combine with lakehouse files URI
            full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
        else:
            # Already absolute path
            full_file_path = file_path

        writer.save(full_file_path)

    def file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists using the appropriate method for the environment.
        """
        try:
            # Build full file path using the lakehouse files URI
            if file_path.startswith("/"):
                # Absolute local path - convert to file:// URI
                if self.spark_version == "local":
                    full_file_path = f"file://{file_path}"
                else:
                    # For Fabric, absolute paths shouldn't happen, treat as relative
                    full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
            elif not file_path.startswith(("file://", "abfss://")):
                full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
            else:
                full_file_path = file_path

            if self.spark_version == "local":
                # Use standard Python file access for local
                if full_file_path.startswith("file://"):
                    full_file_path = full_file_path.replace("file://", "")
                return Path(full_file_path).exists()
            else:
                # Use notebookutils.fs for Fabric environment
                import sys

                if "notebookutils" in sys.modules:
                    # Try to access the file with notebookutils
                    try:
                        import notebookutils  # type: ignore

                        notebookutils.fs.head(full_file_path, 1)  # type: ignore
                        return True
                    except Exception:
                        return False
                else:
                    # Fallback to Spark for checking file existence
                    try:
                        # Read the file with standard file access
                        if full_file_path.startswith("file://"):
                            full_file_path = full_file_path.replace("file://", "")
                        # print to stdout for debugging
                        import sys

                        sys.stdout.write(
                            f"Checking file existence at: {full_file_path}\n"
                        )
                        return Path(full_file_path).exists()
                    except Exception:
                        return False
        except Exception:
            return False

    def move_file(self, source_path: str, destination_path: str) -> bool:
        """
        Move file from source to destination.

        Args:
            source_path: Source file path (relative to lakehouse Files/ or absolute URI)
            destination_path: Destination path (relative to lakehouse Files/ or absolute path)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Build full paths - check if already absolute URIs
            if source_path.startswith(("abfss://", "file://")):
                source_full = source_path
            else:
                source_full = f"{self.lakehouse_files_uri()}{source_path.lstrip('/')}"

            if destination_path.startswith(("abfss://", "file://")):
                dest_full = destination_path
            else:
                dest_full = f"{self.lakehouse_files_uri()}{destination_path.lstrip('/')}"

            if self.spark_version == "local":
                # Local environment - use Python shutil
                import shutil

                source_local = source_full.replace("file://", "")
                dest_local = dest_full.replace("file://", "")

                # Create destination directory if needed
                dest_dir = Path(dest_local).parent
                dest_dir.mkdir(parents=True, exist_ok=True)

                # Move file
                shutil.move(source_local, dest_local)
                return True
            else:
                # Fabric environment
                import sys

                if "notebookutils" in sys.modules:
                    import notebookutils  # type: ignore

                    # Use create_path=True to create parent directories automatically
                    notebookutils.fs.mv(source_full, dest_full, create_path=True)  # type: ignore
                    return True
                else:
                    # Fallback to Hadoop FileSystem
                    hadoop_conf = self.spark._jsc.hadoopConfiguration()
                    fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

                    source_path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(source_full)
                    dest_path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(dest_full)

                    # Create parent directory
                    dest_parent = dest_path_obj.getParent()
                    fs.mkdirs(dest_parent)

                    # Move file
                    return fs.rename(source_path_obj, dest_path_obj)

        except Exception as e:
            logging.warning(f"Failed to move file {source_path} to {destination_path}: {e}")
            return False

    def is_directory_empty(self, directory_path: str) -> bool:
        """
        Check if a directory is empty (no files, no subdirectories).

        Args:
            directory_path: Directory path (relative to lakehouse Files/)

        Returns:
            True if empty, False otherwise
        """
        try:
            # Build full path
            full_path = f"{self.lakehouse_files_uri()}{directory_path.lstrip('/')}"

            if self.spark_version == "local":
                # Local environment - use Python os
                import os

                local_path = full_path.replace("file://", "")
                if not os.path.exists(local_path):
                    return False
                return len(os.listdir(local_path)) == 0
            else:
                # Fabric environment
                import sys

                if "notebookutils" in sys.modules:
                    import notebookutils  # type: ignore

                    try:
                        items = notebookutils.fs.ls(full_path)  # type: ignore
                        return len(items) == 0
                    except Exception:
                        # Directory doesn't exist or access error
                        return False
                else:
                    # Fallback to Hadoop FileSystem
                    hadoop_conf = self.spark._jsc.hadoopConfiguration()
                    fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
                    path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(full_path)

                    if not fs.exists(path_obj):
                        return False

                    statuses = fs.listStatus(path_obj)
                    return len(statuses) == 0

        except Exception as e:
            logging.debug(f"Error checking if directory is empty {directory_path}: {e}")
            return False

    def delete_directory(self, directory_path: str) -> bool:
        """
        Delete an empty directory.

        Args:
            directory_path: Directory path (relative to lakehouse Files/)

        Returns:
            True if successfully deleted, False otherwise
        """
        try:
            # Build full path
            full_path = f"{self.lakehouse_files_uri()}{directory_path.lstrip('/')}"

            if self.spark_version == "local":
                # Local environment - use Python os
                import os

                local_path = full_path.replace("file://", "")
                os.rmdir(local_path)  # Only removes empty directories
                return True
            else:
                # Fabric environment
                import sys

                if "notebookutils" in sys.modules:
                    import notebookutils  # type: ignore

                    # rm with recurse=False to ensure it only deletes if empty
                    notebookutils.fs.rm(full_path, recurse=False)  # type: ignore
                    return True
                else:
                    # Fallback to Hadoop FileSystem
                    hadoop_conf = self.spark._jsc.hadoopConfiguration()
                    fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
                    path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(full_path)

                    # delete with recursive=False to only delete empty directories
                    return fs.delete(path_obj, False)

        except Exception as e:
            logging.warning(f"Failed to delete directory {directory_path}: {e}")
            return False

    def cleanup_empty_directories_recursive(
        self,
        file_path: str,
        base_path: str,
    ) -> list[str]:
        """
        Recursively clean up empty directories from file's parent up to base_path.

        Args:
            file_path: File path that was archived (relative to lakehouse Files/)
            base_path: Base path boundary - stop deletion at this level

        Returns:
            List of deleted directory paths
        """
        import os

        deleted_dirs = []

        try:
            # Normalize paths
            base_path = base_path.rstrip('/')
            file_path = file_path.lstrip('/')

            # Start from file's parent directory
            current_dir = os.path.dirname(file_path)

            # Walk up the directory tree
            while current_dir and current_dir != base_path:
                # Don't go above base_path
                if not current_dir.startswith(base_path):
                    break

                # Check if directory is empty
                if self.is_directory_empty(current_dir):
                    # Try to delete
                    if self.delete_directory(current_dir):
                        deleted_dirs.append(current_dir)
                    else:
                        # Deletion failed, stop here
                        break
                else:
                    # Directory not empty, stop here
                    break

                # Move up one level
                current_dir = os.path.dirname(current_dir)

        except Exception as e:
            logging.warning(f"Error during directory cleanup: {e}")

        return deleted_dirs

    def list_files(
        self,
        directory_path: str,
        pattern: str | None = None,
        recursive: bool = False,
    ) -> list[str]:
        """
        List files in a directory using the appropriate method for the environment.
        """
        # Build full directory path using the lakehouse files URI
        if not directory_path.startswith(("file://", "abfss://")):
            full_directory_path = f"{self.lakehouse_files_uri()}{directory_path}"
        else:
            full_directory_path = directory_path

        if self.spark_version == "local":
            # Use standard Python file operations for local
            if full_directory_path.startswith("file://"):
                full_directory_path = full_directory_path.replace("file://", "")

            dir_path = Path(full_directory_path)
            if not dir_path.exists():
                return []

            if recursive:
                files = dir_path.rglob(pattern or "*")
            else:
                files = dir_path.glob(pattern or "*")

            return [str(f) for f in files if f.is_file()]
        else:
            # Use notebookutils.fs for Fabric environment
            import sys

            if "notebookutils" in sys.modules:
                try:
                    import notebookutils  # type: ignore

                    files = notebookutils.fs.ls(full_directory_path)  # type: ignore
                    file_list = []
                    for file_info in files:
                        if file_info.isFile:
                            if pattern is None or pattern in file_info.name:
                                file_list.append(file_info.path)
                    return file_list
                except Exception:
                    return []
            else:
                # Fallback - this is limited but better than nothing
                return []

    def list_files_with_metadata(
        self,
        directory_path: str,
        pattern: str = "*",
        recursive: bool = False,
    ) -> list[FileInfo]:
        """
        List files matching a pattern with metadata.

        Args:
            directory_path: Directory path (supports abfss:// URIs or lakehouse paths)
            pattern: Glob pattern to match filenames
            recursive: If True, search subdirectories recursively

        Returns:
            List of FileInfo dataclass objects with metadata
        """
        matching_files = []

        # Build full directory path
        if not directory_path.startswith(("file://", "abfss://")):
            # Strip leading slash if present to avoid double slashes
            clean_path = directory_path.lstrip("/")
            full_directory_path = f"{self.lakehouse_files_uri()}{clean_path}"
        else:
            full_directory_path = directory_path

        def search(current_path, base_for_relative=None):
            try:
                if self.spark_version == "local":
                    # Use standard Python file operations for local
                    # Convert file:// URI to local path if needed
                    local_path = current_path.replace("file://", "")
                    dir_path = Path(local_path)

                    # For relative path calculations
                    if base_for_relative is None:
                        base_for_relative = dir_path

                    if not dir_path.exists():
                        print(f"ℹ️ Directory not found: {current_path}")
                        return

                    items = list(dir_path.iterdir())

                    for item in items:
                        if item.is_file():
                            # Calculate relative path from base directory
                            try:
                                relative_path = str(item.relative_to(base_for_relative))
                            except ValueError:
                                # Fallback to just the name if relative_to fails
                                relative_path = item.name

                            # Match against relative path if pattern contains '/', otherwise just filename
                            match_against = relative_path if '/' in pattern else item.name
                            matches = fnmatch(match_against, pattern)

                            if matches:
                                stat = item.stat()
                                file_info = FileInfo(
                                    path=str(item),
                                    name=item.name,
                                    size=stat.st_size,
                                    modified_ms=int(stat.st_mtime * 1000),
                                )
                                matching_files.append(file_info)
                        elif item.is_dir() and recursive:
                            search(str(item), base_for_relative)
                else:
                    # Use notebookutils.fs for Fabric environment
                    import sys

                    if "notebookutils" in sys.modules:
                        import notebookutils  # type: ignore

                        # For relative path calculations in Fabric
                        if base_for_relative is None:
                            base_for_relative = current_path

                        all_items = notebookutils.fs.ls(current_path)  # type: ignore

                        for item in all_items:
                            if item.isFile:
                                # Calculate relative path from base
                                # item.path is full ABFSS path, base_for_relative is also full ABFSS path
                                if item.path.startswith(base_for_relative):
                                    relative_path = item.path[len(base_for_relative):].lstrip('/')
                                else:
                                    relative_path = item.name

                                # Match against relative path if pattern contains '/', otherwise just filename
                                match_against = relative_path if '/' in pattern else item.name
                                matches = fnmatch(match_against, pattern)

                                if matches:
                                    file_info = FileInfo(
                                        path=item.path,
                                        name=item.name,
                                        size=item.size,
                                        modified_ms=item.modifyTime,
                                    )
                                    matching_files.append(file_info)
                            elif item.isDir and recursive:
                                search(item.path, base_for_relative)
            except Exception as e:
                # Catch specific errors related to directory not found
                error_msg = str(e).lower()
                if "not found" in error_msg or "does not exist" in error_msg or "pathnotfound" in error_msg:
                    print(f"ℹ️ Directory not found: {current_path}")
                else:
                    print(f"⚠️ Error accessing {current_path}: {e}")

        try:
            search(full_directory_path)
        except Exception as e:
            # Catch any top-level errors
            error_msg = str(e).lower()
            if "not found" in error_msg or "does not exist" in error_msg or "pathnotfound" in error_msg:
                print(f"ℹ️ Directory not found: {full_directory_path}")
            else:
                print(f"⚠️ Error listing files in {full_directory_path}: {e}")

        return matching_files

    def list_directories(
        self,
        directory_path: str,
        pattern: str | None = None,
        recursive: bool = False,
    ) -> list[str]:
        """
        List directories in a directory using the appropriate method for the environment.
        """
        # Build full directory path using the lakehouse files URI
        if not directory_path.startswith(("file://", "abfss://")):
            full_directory_path = f"{self.lakehouse_files_uri()}{directory_path}"
        else:
            full_directory_path = directory_path

        if self.spark_version == "local":
            # Use standard Python file operations for local
            if full_directory_path.startswith("file://"):
                full_directory_path = full_directory_path.replace("file://", "")

            dir_path = Path(full_directory_path)
            if not dir_path.exists():
                return []

            if recursive:
                dirs = dir_path.rglob(pattern or "*")
            else:
                dirs = dir_path.glob(pattern or "*")

            return [str(d) for d in dirs if d.is_dir()]
        else:
            # Use notebookutils.fs for Fabric environment
            import sys

            if "notebookutils" in sys.modules:
                import notebookutils  # type: ignore

                dir_list = []

                def search_directories(current_path: str) -> None:
                    """Recursively search for directories matching pattern."""
                    try:
                        items = notebookutils.fs.ls(current_path)  # type: ignore

                        for item in items:
                            if item.isDir:
                                # Add directory if it matches pattern
                                if pattern is None or pattern in item.name:
                                    dir_list.append(item.path)

                                # Recurse into subdirectories if recursive mode enabled
                                if recursive:
                                    search_directories(item.path)
                    except Exception as e:
                        # Log specific errors but don't fail silently
                        error_msg = str(e).lower()
                        if "not found" in error_msg or "does not exist" in error_msg or "pathnotfound" in error_msg:
                            logging.debug(f"Directory not found: {current_path}")
                        else:
                            # Re-raise unexpected errors so they're not silently ignored
                            raise

                try:
                    search_directories(full_directory_path)
                    return dir_list
                except Exception as e:
                    # Log top-level errors with context
                    error_msg = str(e).lower()
                    if "not found" in error_msg or "does not exist" in error_msg or "pathnotfound" in error_msg:
                        logging.info(f"Directory not found: {full_directory_path}")
                        return []
                    else:
                        # Re-raise unexpected errors
                        logging.error(f"Error listing directories in {full_directory_path}: {e}")
                        raise
            else:
                # Fallback - this is limited but better than nothing
                return []

    def get_file_info(self, file_path: str) -> dict[str, Any]:
        """
        Get information about a file (size, modification time, etc.).
        """
        try:
            # Build full file path using the lakehouse files URI
            if file_path.startswith("/"):
                # Absolute local path - convert to file:// URI
                if self.spark_version == "local":
                    full_file_path = f"file://{file_path}"
                else:
                    # For Fabric, absolute paths shouldn't happen, treat as relative
                    full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
            elif not file_path.startswith(("file://", "abfss://")):
                full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
            else:
                full_file_path = file_path

            if self.spark_version == "local":
                # Use standard Python file operations for local
                if full_file_path.startswith("file://"):
                    full_file_path = full_file_path.replace("file://", "")

                file_path_obj = Path(full_file_path)
                if not file_path_obj.exists():
                    return {}

                stat = file_path_obj.stat()
                from datetime import datetime

                return {
                    "path": str(file_path_obj),
                    "size": stat.st_size,
                    "modified_time": datetime.fromtimestamp(stat.st_mtime),
                    "is_file": file_path_obj.is_file(),
                    "is_directory": file_path_obj.is_dir(),
                }
            else:
                # Use notebookutils.fs for Fabric environment
                import sys

                if "notebookutils" in sys.modules:
                    try:
                        import notebookutils  # type: ignore

                        info = notebookutils.fs.ls(full_file_path)[0]  # type: ignore
                        return {
                            "path": info.path,
                            "size": info.size,
                            "modified_time": info.modificationTime,
                            "is_file": info.isFile,
                            "is_directory": info.isDir,
                        }
                    except Exception:
                        return {}
                else:
                    # Fallback - limited information
                    return {"path": full_file_path}
        except Exception:
            return {}

    def set_spark_config(self, key: str, value: str) -> None:
        """
        Set a Spark configuration property.

        Args:
            key: Configuration key (e.g., 'spark.sql.shuffle.partitions')
            value: Configuration value
        """
        self.spark.conf.set(key, value)

    def create_range_dataframe(
        self, start: int, end: int, num_partitions: int = None
    ) -> Any:
        """
        Create a DataFrame with a single column containing a range of values.

        Args:
            start: Start value (inclusive)
            end: End value (exclusive)
            num_partitions: Number of partitions for the DataFrame

        Returns:
            DataFrame with a single 'id' column
        """
        if num_partitions:
            return self.spark.range(start, end, numPartitions=num_partitions)
        else:
            return self.spark.range(start, end)

    def cache_dataframe(self, df: Any) -> Any:
        """
        Cache a DataFrame in memory.

        Args:
            df: DataFrame to cache

        Returns:
            The cached DataFrame
        """
        return df.cache()

    def repartition_dataframe(
        self, df: Any, num_partitions: int = None, partition_cols: list[str] = None
    ) -> Any:
        """
        Repartition a DataFrame.

        Args:
            df: DataFrame to repartition
            num_partitions: Number of partitions (optional)
            partition_cols: Columns to partition by (optional)

        Returns:
            The repartitioned DataFrame
        """
        if partition_cols:
            return df.repartition(*partition_cols)
        elif num_partitions:
            return df.repartition(num_partitions)
        else:
            return df.repartition()

    def union_dataframes(self, dfs: list[Any]) -> Any:
        """
        Union multiple DataFrames into one.

        Args:
            dfs: List of DataFrames to union

        Returns:
            The unioned DataFrame
        """
        if not dfs:
            raise ValueError("No DataFrames provided for union")

        result = dfs[0]
        for df in dfs[1:]:
            result = result.union(df)
        return result

    def save_dataframe_as_table(
        self,
        df: Any,
        table_name: str,
        mode: str = "overwrite",
        partition_cols: list[str] = None,
    ) -> None:
        """
        Save a DataFrame as a Delta table using saveAsTable.

        Args:
            df: DataFrame to save
            table_name: Name of the table
            mode: Write mode ('overwrite', 'append', etc.)
            partition_cols: Columns to partition by (optional)
        """
        writer = df.write.format("delta").mode(mode)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.saveAsTable(table_name)

        # Log the operation
        try:
            row_count = df.count()
            print(f"Written {row_count} rows to Delta table {table_name}")
        except Exception as e:
            print(f"Table {table_name} saved (row count unavailable: {e})")

    def list_all(
        self,
        directory_path: str,
        pattern: str | None = None,
        recursive: bool = False,
    ) -> list[str]:
        """
        List both files and directories in a directory using the appropriate method for the environment.
        This method combines the functionality of list_files() and list_directories().

        Args:
            directory_path: Path to directory to list
            pattern: Optional pattern to filter results
            recursive: Whether to search recursively

        Returns:
            List of paths (both files and directories)
        """
        # Build full directory path using the lakehouse files URI
        if not directory_path.startswith(("file://", "abfss://")):
            # Handle case where directory_path already starts with 'Files/'
            # to avoid duplication like Files/Files/synthetic_data
            if directory_path.startswith("Files/"):
                # Remove the Files/ prefix since lakehouse_files_uri already includes it
                clean_directory_path = directory_path[6:]  # Remove "Files/"
                full_directory_path = (
                    f"{self.lakehouse_files_uri()}{clean_directory_path}"
                )
            else:
                full_directory_path = f"{self.lakehouse_files_uri()}{directory_path}"
        else:
            full_directory_path = directory_path

        if self.spark_version == "local":
            # Use standard Python file operations for local
            if full_directory_path.startswith("file://"):
                # Handle URI path conversion - fix multiple slashes
                clean_path = full_directory_path.replace("file://", "").replace(
                    "//", "/"
                )
                # Ensure we don't have triple slashes
                while "///" in clean_path:
                    clean_path = clean_path.replace("///", "/")
                dir_path = Path(clean_path)
            else:
                dir_path = Path(full_directory_path)

            if not dir_path.exists():
                return []

            if recursive:
                items = dir_path.rglob(pattern or "*")
            else:
                items = dir_path.glob(pattern or "*")

            return [str(item) for item in items]
        else:
            # Use notebookutils.fs for Fabric environment
            import sys

            if "notebookutils" in sys.modules:
                try:
                    import notebookutils  # type: ignore

                    # Get directory listing from notebookutils
                    files = notebookutils.fs.ls(full_directory_path)  # type: ignore
                    result = []
                    for file_info in files:
                        if pattern is None or Path(file_info.name).match(pattern):
                            result.append(file_info.path)

                        # If recursive and this is a directory, recursively list its contents
                        if recursive and file_info.isDir:
                            subdirectory_items = self.list_all(
                                file_info.path, pattern, recursive
                            )
                            result.extend(subdirectory_items)
                    return result
                except Exception:
                    return []
            else:
                # Fallback to Spark file system operations
                try:
                    hadoop_conf = self.spark._jsc.hadoopConfiguration()
                    fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                        hadoop_conf
                    )
                    path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(
                        full_directory_path
                    )

                    result = []
                    if fs.exists(path_obj):
                        file_statuses = fs.listStatus(path_obj)
                        for status in file_statuses:
                            file_path = status.getPath().toString()
                            file_name = status.getPath().getName()

                            if pattern is None or Path(file_name).match(pattern):
                                result.append(file_path)

                            # If recursive and this is a directory, recursively list its contents
                            if recursive and status.isDirectory():
                                relative_path = file_path.replace(
                                    f"{self.lakehouse_files_uri()}", ""
                                )
                                subdirectory_items = self.list_all(
                                    relative_path, pattern, recursive
                                )
                                result.extend(subdirectory_items)
                    return result
                except Exception:
                    return []
