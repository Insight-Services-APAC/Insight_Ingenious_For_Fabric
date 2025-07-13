from __future__ import annotations

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

    def __init__(self, target_workspace_id: str, target_lakehouse_id: str, spark: SparkSession = None) -> None:
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
        if (cu.get_configs_as_object().fabric_environment == "local"):
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
            print("No active Spark session found, creating a new one with Delta support.")
            
            builder = (
                SparkSession.builder.appName("MyApp")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
            return f"file:///{Path.cwd()}/tmp/spark/Tables/"
        else:
            return f"abfss://{self._target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse_id}/Tables/"

    def lakehouse_files_uri(self) -> str:
        """Get the ABFSS URI for the lakehouse Files directory."""
        if self.spark_version == "local":
            # Use FABRIC_WORKSPACE_REPO_DIR for file access in local mode
            import os
            workspace_dir = os.environ.get('FABRIC_WORKSPACE_REPO_DIR', str(Path.cwd()))
            return f"file:///{workspace_dir}/"
        else:
            return f"abfss://{self._target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse_id}/Files/"

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
            print(f"⚠ Alert: Registering table '{table_name}' in the Hive catalog for local Spark.")
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
        mode: str = "overwrite",
        options: dict[str, Any] | None = None,
    ) -> None:
        """
        Create a new table with a given schema.
        """
        if schema is None:
            raise ValueError("Schema must be provided for table creation.")
        
        empty_df = self.get_connection.createDataFrame([], schema)
        writer = empty_df.write.format("delta").mode(mode)

        if options:
            for k, v in options.items():
                writer = writer.option(k, v)

        writer.save(f"{self.lakehouse_tables_uri()}{table_name}")

        # Register the table in the Hive catalog - Only needed if local
        if self.spark_version == "local":
            self.spark.sql(
                f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{self.lakehouse_tables_uri()}{table_name}'"
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
            
        reader = self.spark.read.format(file_format.lower())
        
        # Apply common options based on file format
        if file_format.lower() == "csv":
            # Apply CSV-specific options
            if "header" in options:
                reader = reader.option("header", str(options["header"]).lower())
            if "delimiter" in options:
                reader = reader.option("sep", options["delimiter"])
            if "encoding" in options:
                reader = reader.option("encoding", options["encoding"])
            if "inferSchema" in options:
                reader = reader.option("inferSchema", str(options["inferSchema"]).lower())
            if "dateFormat" in options:
                reader = reader.option("dateFormat", options["dateFormat"])
            if "timestampFormat" in options:
                reader = reader.option("timestampFormat", options["timestampFormat"])
                
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
        if not file_path.startswith(("file://", "abfss://")):
            # Relative path - combine with lakehouse files URI
            full_file_path = f"{self.lakehouse_files_uri()}{file_path}"
        else:
            # Already absolute path
            full_file_path = file_path
            
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
        if not file_path.startswith(("file://", "abfss://")):
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
            if not file_path.startswith(("file://", "abfss://")):
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
                        self.spark.read.format("text").load(full_file_path).limit(1).count()
                        return True
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

    def get_file_info(self, file_path: str) -> dict[str, Any]:
        """
        Get information about a file (size, modification time, etc.).
        """
        try:
            # Build full file path using the lakehouse files URI
            if not file_path.startswith(("file://", "abfss://")):
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
