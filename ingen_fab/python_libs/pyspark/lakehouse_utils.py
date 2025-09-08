from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

import ingen_fab.python_libs.common.config_utils as cu
from ingen_fab.python_libs.interfaces.data_store_interface import DataStoreInterface


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
        target_workspace_id: str,
        target_lakehouse_id: str,
        spark: SparkSession = None,
    ) -> None:
        super().__init__()
        self._target_workspace_id = target_workspace_id
        self._target_lakehouse_id = target_lakehouse_id
        self.spark_version = "fabric"
        if spark:
            # Use the provided Spark session
            print("Using provided Spark session.")
            if not isinstance(spark, SparkSession):
                raise TypeError("Provided spark must be a SparkSession instance.")
            print(spark.__dict__)
            self.spark = spark
        else:
            # If no Spark session is provided, create a new one
            self.spark = self._get_or_create_spark_session()

    @property
    def target_workspace_id(self) -> str:
        """Get the target workspace ID."""
        return self._target_workspace_id

    @property
    def target_store_id(self) -> str:
        """Get the target lakehouse ID."""
        return self._target_lakehouse_id

    @property
    def get_connection(self) -> SparkSession:
        """Get the Spark session connection."""
        return self.spark

    def _get_or_create_spark_session(self) -> SparkSession:
        """Get existing Spark session or create a new one."""
        config = cu.get_configs_as_object()
        
        if config.fabric_environment == "local":
            # Use the SparkSessionFactory for local environments
            from ingen_fab.python_libs.common.spark_session_factory import SparkSessionFactory
            
            self.spark_version = "local"
            
            # Get the local Spark provider from config (defaults to "native")
            provider = getattr(config, "local_spark_provider", "native")
            
            print(f"Creating local Spark session with provider: {provider}")
            
            return SparkSessionFactory.get_or_create_spark_session(
                provider=provider,
                app_name="LakehouseUtils",
                lakesail_port=50051
            )
        else:
            print("Using existing spark .. Fabric environment   .")
            return self.spark  # type: ignore  # noqa: F821

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
                logging.debug(
                    f"Delta table already exists at path: {table_path}, skipping creation."
                )
            else:
                logging.debug(f"Path {table_path} is not a Delta table.")
        except Exception as e:
            # If the path does not exist or is inaccessible, isDeltaTable returns False or may throw.
            # Treat exceptions as "table does not exist".
            logging.warning(
                f"Could not verify Delta table existence at {table_path} (exception: {e}); assuming it does not exist."
            )
            table_exists = False
        return table_exists

    def lakehouse_tables_uri(self) -> str:
        """Get the ABFSS URI for the lakehouse Tables directory."""
        if self.spark_version == "local":
            return f"file://{Path.cwd()}/tmp/spark/Tables/"
        else:
            return f"abfss://{self._target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse_id}/Tables/"

    def lakehouse_files_uri(self) -> str:
        """Get the ABFSS URI for the lakehouse Files directory."""
        if self.spark_version == "local":
            return f"file://{Path.cwd()}/tmp/spark/Files/"
        else:
            return f"abfss://{self._target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse_id}/Files/"

    def write_to_table(
        self,
        df,
        table_name: str,
        schema_name: str | None = None,
        mode: str = "overwrite",
        options: dict[str, str] | None = None,
        partition_by: list[str] | None = None,
    ) -> None:
        """Write a DataFrame to a lakehouse table with optional partitioning."""

        # For lakehouse, schema_name is not used as tables are in the Tables directory
        writer = df.write.format("delta").mode(mode)
        table_full_name = table_name
        if schema_name:
            table_full_name = f"{schema_name}_{table_name}"
        if options:
            for k, v in options.items():
                writer = writer.option(k, v)
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(f"{self.lakehouse_tables_uri()}{table_full_name}")

        # Register the table in the Hive catalog - Only needed if local
        if self.spark_version == "local":
            logging.debug(
                f"⚠ Alert: Registering table '{table_full_name}' in the Hive catalog for local Spark."
            )
            full_table_path = f"{self.lakehouse_tables_uri()}{table_full_name}"
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_full_name} USING DELTA LOCATION '{full_table_path}'"
            )
        else:
            logging.debug(
                f"No need to register table '{table_full_name}' in Hive catalog for Fabric Spark."
            )

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
        Get the schema/column definitions for a table or view.
        
        This method automatically detects whether the table_name refers to:
        - A Delta table in lakehouse storage
        - A view or temporary view in Spark catalog
        """
        # Construct full table name if schema is provided
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        
        # First, check if it's a view or temporary view in Spark catalog
        try:
            if self.spark.catalog.tableExists(full_table_name):
                table_info = self.spark.catalog.getTable(full_table_name)
                if table_info.tableType in ["VIEW", "TEMPORARY_VIEW"]:
                    # It's a view - get schema from catalog
                    df = self.spark.table(full_table_name)
                else:
                    # It's a table but check if it exists in lakehouse storage
                    try:
                        df = self.spark.read.format("delta").load(
                            f"{self.lakehouse_tables_uri()}{table_name}"
                        )
                    except Exception:
                        # Fall back to reading from catalog if Delta read fails
                        df = self.spark.table(full_table_name)
            else:
                # Try reading as Delta table from lakehouse storage
                df = self.spark.read.format("delta").load(
                    f"{self.lakehouse_tables_uri()}{table_name}"
                )
        except Exception as e:
            # Final fallback: try reading from Spark catalog
            try:
                df = self.spark.table(full_table_name)
            except Exception:
                raise Exception(f"Could not read schema for table '{full_table_name}' as Delta table or view: {e}")
        
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
        Read data from a table or view, optionally filtering columns, rows, or limiting results.
        
        This method automatically detects whether the table_name refers to:
        - A Delta table in lakehouse storage
        - A view or temporary view in Spark catalog
        """
        logging.debug(f"Reading table: {table_name} with schema: {schema_name}")
        df = None
        
        # Construct full table name if schema is provided
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        
        # First, check if it's a view or temporary view in Spark catalog
        try:
            logging.debug(f"Checking existence in Spark catalog: {full_table_name}")
            if self.spark.catalog.tableExists(full_table_name):
                logging.debug(f"Getting Table info from Spark catalog: {full_table_name}")
                table_info = self.spark.catalog.getTable(full_table_name)
                logging.debug(f"Table catalog info - Name: {table_info.name}, Database: {table_info.database}, TableType: {table_info.tableType}")
                if table_info.tableType in ["VIEW", "TEMPORARY_VIEW"]:
                    # It's a view - read from catalog
                    logging.debug(f"Reading as view from Spark catalog: {full_table_name}")
                    df = self.spark.table(full_table_name)
                else:
                    # It's a table - try reading from catalog first (should have correct path now)
                    try:
                        logging.debug(f"Reading table from Spark catalog: {full_table_name}")
                        df = self.spark.table(full_table_name)
                        logging.info(f"Successfully read {table_name} from catalog")
                    except Exception as catalog_error:
                        logging.warning(f"Catalog read failed: {catalog_error}")
                        # Fall back to reading directly from Delta storage
                        logging.info(f"Attempting to read as Delta table from lakehouse storage: {self.lakehouse_tables_uri()}{table_name}")
                        df = self.spark.read.format("delta").load(
                            f"{self.lakehouse_tables_uri()}{table_name}"
                        )
            else:
                logging.info(f"Table {full_table_name} does not exist in catalog, trying as Delta table...")
                # Try reading as Delta table from lakehouse storage
                df = self.spark.read.format("delta").load(
                    f"{self.lakehouse_tables_uri()}{table_name}"
                )
        except Exception as e:
            # Final fallback: try reading from Spark catalog one more time
            try:
                logging.warning(f"Final fallback: attempting to read from Spark catalog: {full_table_name}")
                df = self.spark.table(full_table_name)
            except Exception as final_error:
                raise Exception(f"Could not read table '{full_table_name}' as Delta table or from catalog: {e}")
        
        # Apply optional filtering and column selection
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
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        delta_table = DeltaTable.forPath(self.spark, table_path)
        print(f"Dropping table at path: {table_path}")    
        delta_table.delete()
        # Remove Directory
        if self.spark_version == "local":
            import shutil
            shutil.rmtree(table_path.replace("file://", ""), ignore_errors=True)
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        else:
            import notebookutils # type: ignore  # noqa: I001
            notebookutils.fs.rm(table_path, True)

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

    def write_string_to_file(
        self,
        content: str,
        file_path: str,
        encoding: str = "utf-8",
        mode: str = "overwrite",
    ) -> None:
        """
        Write string content to a file.

        Args:
            content: String content to write
            file_path: Path where to write the file
            encoding: File encoding (default: utf-8)
            mode: Write mode ("overwrite" or "append")
        """
        # Build full file path using the lakehouse files URI
        if file_path.startswith("/"):
            # Absolute local path - convert to file:// URI
            if self.spark_version == "local":
                # Clean up any double slashes that might come from list_files
                clean_path = file_path.replace("//", "/")
                full_file_path = clean_path  # Keep as regular path for Python file operations
            else:
                # For Fabric, absolute paths shouldn't happen, treat as relative
                full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
        elif not file_path.startswith(("file://", "abfss://")):
            # Relative path - combine with lakehouse files URI
            if self.spark_version == "local":
                # For local, build the full path using the workspace directory
                full_file_path = f"{self.lakehouse_files_uri()}{file_path}".replace("file://", "")
            else:
                full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
        else:
            # Already absolute path
            full_file_path = file_path
            if self.spark_version == "local" and full_file_path.startswith("file://"):
                full_file_path = full_file_path.replace("file://", "")

        if self.spark_version == "local":
            # Use standard Python file I/O for local environment
            from pathlib import Path
            
            # Create parent directories if they don't exist
            file_path_obj = Path(full_file_path)
            file_path_obj.parent.mkdir(parents=True, exist_ok=True)
            
            # Write the file
            if mode == "append":
                with open(full_file_path, "a", encoding=encoding) as f:
                    f.write(content)
            else:  # overwrite
                with open(full_file_path, "w", encoding=encoding) as f:
                    f.write(content)
                    
            print(f"✅ Written {len(content)} characters to {file_path}")
        else:
            # Use notebookutils.fs for Fabric environment
            import sys
            
            if "notebookutils" in sys.modules:
                notebookutils = sys.modules["notebookutils"]
                
                # Convert content to bytes
                content_bytes = content.encode(encoding)
                
                # Use notebookutils.fs.put to write the file
                # Note: notebookutils.fs.put always overwrites, doesn't support append
                if mode == "append":
                    # For append mode, read existing content first if file exists
                    try:
                        existing_bytes = notebookutils.fs.head(full_file_path, maxBytes=-1)
                        content_bytes = existing_bytes + content_bytes
                    except:
                        # File doesn't exist, just write new content
                        pass
                
                notebookutils.fs.put(full_file_path, content_bytes, overwrite=True)
                print(f"✅ Written {len(content)} characters to {file_path}")
            else:
                # Fallback to using Spark RDD if notebookutils not available
                # Convert string to RDD and save as text file
                rdd = self.spark.sparkContext.parallelize([content])
                
                # For append mode, need to read existing file first
                if mode == "append" and self.file_exists(file_path):
                    existing_rdd = self.spark.sparkContext.textFile(full_file_path)
                    rdd = existing_rdd.union(rdd)
                
                # Save as text file (this will create a directory with part files)
                temp_path = f"{full_file_path}_temp"
                rdd.coalesce(1).saveAsTextFile(temp_path)
                
                # Move the part file to the desired location
                # This is a workaround since Spark creates directories for text files
                print(f"⚠️ File written as Spark text file directory at {temp_path}")

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
                try:
                    import notebookutils  # type: ignore

                    files = notebookutils.fs.ls(full_directory_path)  # type: ignore
                    dir_list = []
                    for file_info in files:
                        if file_info.isDir:
                            if pattern is None or pattern in file_info.name:
                                dir_list.append(file_info.path)
                    return dir_list
                except Exception:
                    return []
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
