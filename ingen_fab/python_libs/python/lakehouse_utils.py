from __future__ import annotations

from typing import Any

from deltalake import DeltaTable, write_deltalake

from ingen_fab.python_libs.common.config_utils import ConfigUtils

from ..interfaces.data_store_interface import DataStoreInterface


class lakehouse_utils(DataStoreInterface):
    """Utility helpers for interacting with a Spark lakehouse using delta-rs (deltalake Python bindings).
    This class provides methods to manage Delta tables in a lakehouse environment,
    including checking table existence, writing data, listing tables, and dropping tables.
    All operations are performed using delta-rs APIs only (no direct SparkSession usage).
    """

    def __init__(self, target_workspace_id: str, target_lakehouse_id: str) -> None:
        super().__init__()
        self._target_workspace_id = target_workspace_id
        self._target_lakehouse_id = target_lakehouse_id
        self.config_utils = ConfigUtils()

    @property
    def target_workspace_id(self) -> str:
        """Get the target workspace ID."""
        return self._target_workspace_id

    @property
    def get_connection(self):
        """Get the connection."""
        return "Placeholder for Spark session, not used in this class"  # Placeholder for Spark session, not used in this class

    @property
    def target_store_id(self) -> str:
        """Get the target lakehouse ID."""
        return self._target_lakehouse_id

    def lakehouse_tables_uri(self) -> str:
        """Get the ABFSS URI for the lakehouse Tables directory."""
        if self.config_utils._is_local_environment:
            # Local environment uses file:// URI
            return f"file:///tmp/{self._target_lakehouse_id}/Tables/"
        else:
            return f"abfss://{self._target_workspace_id}@onelake.dfs.fabric.microsoft.com/{self._target_lakehouse_id}/Tables/"

    def check_if_table_exists(
        self, table_name: str, schema_name: str | None = None
    ) -> bool:
        """Check if a Delta table exists at the given table name."""
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        try:
            # delta-rs: DeltaTable will raise if not found
            DeltaTable(table_path)
            return True
        except Exception:
            return False

    def write_to_table(
        self,
        df: Any,
        table_name: str,
        schema_name: str | None = None,
        mode: str = "overwrite",
        options: dict[str, str] | None = None,
    ) -> None:
        """Write a DataFrame to a lakehouse table using delta-rs API."""
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        # df must be a pandas DataFrame or pyarrow Table for delta-rs
        write_opts = options or {}
        write_deltalake(
            table_path,
            df,
            mode=mode,
            **write_opts,
        )

    def list_tables(self) -> list[str]:
        """List all tables in the lakehouse directory using delta-rs."""
        # delta-rs does not provide a directory listing, so this is a stub.
        # You would need to list directories in the Tables/ path using a filesystem API.
        raise NotImplementedError("delta-rs does not support listing tables directly.")

    def drop_all_tables(
        self, schema_name: str | None = None, table_prefix: str | None = None
    ) -> None:
        """Drop all Delta tables in the lakehouse directory using delta-rs."""
        # delta-rs does not provide a directory listing, so this is a stub.
        # You would need to list directories in the Tables/ path using a filesystem API.
        raise NotImplementedError("delta-rs does not support dropping all tables directly.")

    def execute_query(self, query: str) -> Any:
        """delta-rs does not support SQL queries directly."""
        raise NotImplementedError("delta-rs does not support SQL queries directly.")

    def get_table_schema(
        self, table_name: str, schema_name: str | None = None
    ) -> dict[str, Any]:
        """Get the schema/column definitions for a table using delta-rs API."""
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        delta_table = DeltaTable(table_path)
        return {field.name: str(field.type) for field in delta_table.schema().fields}

    def read_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        filters: dict[str, Any] | None = None,
    ) -> Any:
        """Read data from a table using delta-rs API."""
        print(f"Reading table: {table_name} with columns: {columns}, limit: {limit}, filters: {filters}")
        print(f"Table path: {self.lakehouse_tables_uri()}{table_name}")
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        delta_table = DeltaTable(table_path)
        df = delta_table.to_pyarrow_table()
        if columns:
            df = df.select(columns)
        import pyarrow.compute as pc
        if filters:
            for col, val in filters.items():
                mask = pc.equal(df[col], val)
                df = df.filter(mask)
        if limit:
            df = df.slice(0, limit)
        return df

    def delete_from_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        filters: dict[str, Any] | None = None,
    ) -> int:
        """Delete rows from a table matching filters using delta-rs API. Returns number of rows deleted."""
        # delta-rs does not support row-level deletes via Python API as of now.
        raise NotImplementedError("delta-rs does not support row-level deletes via Python API.")

    def rename_table(
        self,
        old_table_name: str,
        new_table_name: str,
        schema_name: str | None = None,
    ) -> None:
        """Rename a table by moving its directory (delta-rs does not support this directly)."""
        import shutil

        src = f"{self.lakehouse_tables_uri()}{old_table_name}"
        dst = f"{self.lakehouse_tables_uri()}{new_table_name}"
        shutil.move(src.replace("file://", ""), dst.replace("file://", ""))

    def create_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        schema: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
    ) -> None:
        """Create a new table with a given schema (delta-rs does not support this directly)."""
        raise NotImplementedError("delta-rs does not support creating tables from schema directly.")

    def drop_table(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> None:
        """Drop a single table using delta-rs API."""
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        import shutil

        shutil.rmtree(table_path.replace("file://", ""), ignore_errors=True)

    def list_schemas(self) -> list[str]:
        """List all schemas/namespaces in the lakehouse (returns ['default'] for lakehouse)."""
        return ["default"]

    def get_table_row_count(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> int:
        """Get the number of rows in a table using delta-rs API."""
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        delta_table = DeltaTable(table_path)
        return delta_table.to_pyarrow_table().num_rows

    def get_table_metadata(
        self,
        table_name: str,
        schema_name: str | None = None,
    ) -> dict[str, Any]:
        """Get metadata for a table using delta-rs API."""
        table_path = f"{self.lakehouse_tables_uri()}{table_name}"
        delta_table = DeltaTable(table_path)
        # delta-rs: metadata() returns a pyarrow Schema and dict
        meta = delta_table.metadata()
        return {
            "id": meta.id,
            "name": meta.name,
            "description": meta.description,
            "created_time": meta.created_time,
            "partition_columns": meta.partition_columns,
            "configuration": meta.configuration,
            "schema_string": meta.schema_string,
        }

    def vacuum_table(
        self,
        table_name: str,
        schema_name: str | None = None,
        retention_hours: int = 168,
    ) -> None:
        """Perform cleanup/compaction on a table (delta-rs does not support this directly)."""
        raise NotImplementedError("delta-rs does not support vacuum directly.")