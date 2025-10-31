from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import typer
from rich.console import Console
from rich.table import Table

from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.fabric_api.utils import FabricApiUtils


@dataclass
class ColumnRecord:
    workspace_id: str
    lakehouse_id: Optional[str] = None
    lakehouse_name: Optional[str] = None
    warehouse_id: Optional[str] = None
    warehouse_name: Optional[str] = None
    sql_endpoint_id: Optional[str] = None
    schema_name: str = ""
    table_name: str = ""
    table_type: str | None = None
    column_name: str = ""
    data_type: str | None = None
    is_nullable: str | bool | None = None
    ordinal_position: int | None = None


@dataclass
class SchemaSummaryRecord:
    workspace_id: str
    lakehouse_id: Optional[str] = None
    warehouse_id: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: str = ""
    table_count: int = 0


def _render_output(
    rows: Iterable[ColumnRecord],
    *,
    output_format: str = "json",
    output_path: Optional[Path] = None,
    console: Console | None = None,
) -> None:
    console = console or Console()

    records = [r.__dict__ for r in rows]

    if output_format == "json":
        payload = json.dumps(records, indent=2)
        if output_path:
            output_path.write_text(payload, encoding="utf-8")
            ConsoleStyles.print_success(console, f"Wrote JSON to {output_path}")
        else:
            console.print(payload)
        return

    if output_format == "csv":
        fieldnames = (
            list(records[0].keys())
            if records
            else [
                "workspace_id",
                "lakehouse_id",
                "lakehouse_name",
                "sql_endpoint_id",
                "warehouse_id",
                "warehouse_name",
                "schema_name",
                "table_name",
                "table_type",
                "column_name",
                "data_type",
                "is_nullable",
                "ordinal_position",
            ]
        )
        if output_path:
            with output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)
            ConsoleStyles.print_success(console, f"Wrote CSV to {output_path}")
        else:
            # Write to stdout
            writer = csv.DictWriter(console.file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        return

    if output_format == "table":
        table = Table(title="Lakehouse Metadata (Columns)")
        cols = [
            ("schema_name", "Schema"),
            ("table_name", "Table"),
            ("column_name", "Column"),
            ("data_type", "Type"),
            ("is_nullable", "Nullable"),
            ("ordinal_position", "Position"),
        ]
        for _, header in cols:
            table.add_column(header)
        for r in records:
            table.add_row(
                str(r.get("schema_name", "")),
                str(r.get("table_name", "")),
                str(r.get("column_name", "")),
                str(r.get("data_type", "")),
                str(r.get("is_nullable", "")),
                str(r.get("ordinal_position", "")),
            )
        console.print(table)
        return

    raise ValueError("Invalid output format. Use: json, csv, or table.")


def _render_summary(
    rows: Iterable[SchemaSummaryRecord],
    *,
    output_format: str = "json",
    output_path: Optional[Path] = None,
    console: Console | None = None,
) -> None:
    console = console or Console()
    records = [r.__dict__ for r in rows]

    if output_format == "json":
        payload = json.dumps(records, indent=2)
        if output_path:
            output_path.write_text(payload, encoding="utf-8")
            ConsoleStyles.print_success(console, f"Wrote JSON to {output_path}")
        else:
            console.print(payload)
        return

    if output_format == "csv":
        fieldnames = (
            list(records[0].keys())
            if records
            else [
                "workspace_id",
                "lakehouse_id",
                "warehouse_id",
                "database_name",
                "schema_name",
                "table_count",
            ]
        )
        if output_path:
            with output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(records)
            ConsoleStyles.print_success(console, f"Wrote CSV to {output_path}")
        else:
            writer = csv.DictWriter(console.file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        return

    if output_format == "table":
        table = Table(title="Schema Table Counts")
        for header in ["Database", "Schema", "Table Count"]:
            table.add_column(header)
        for r in records:
            table.add_row(
                str(r.get("database_name", "")),
                str(r.get("schema_name", "")),
                str(r.get("table_count", 0)),
            )
        console.print(table)
        return

    raise ValueError("Invalid output format. Use: json, csv, or table.")


def _normalize_sql_rowset(result: dict[str, Any]) -> tuple[list[str], list[list[Any]]]:
    """Normalize a SQL rowset response to (columns, rows).

    Expected response shape (to be confirmed with Fabric API docs):
      { "columns": [ {"name": "col"}, ... ], "rows": [[...], ...] }

    Raises ValueError if the shape cannot be recognized.
    """
    if isinstance(result, dict) and "columns" in result and "rows" in result:
        columns = [
            c["name"] if isinstance(c, dict) else str(c) for c in result["columns"]
        ]
        rows = result["rows"]
        return columns, rows
    raise ValueError("Unexpected SQL result shape; expected keys: columns, rows")


def lakehouse_metadata(
    *,
    ctx,
    workspace_id: Optional[str],
    workspace_name: Optional[str],
    lakehouse_id: Optional[str],
    lakehouse_name: Optional[str],
    schema: Optional[str],
    table_filter: Optional[str],
    method: str,
    sql_endpoint_id: Optional[str],
    sql_endpoint_server: Optional[str] = None,
    output_format: str,
    output_path: Optional[Path],
    all_lakehouses: bool = False,
) -> None:
    console = Console()
    ConsoleStyles.print_info(console, "Preparing lakehouse metadata extraction...")

    # Resolve workspace id via precedence: explicit id > name > var lib from ctx
    fab = FabricApiUtils(
        environment=str(ctx.obj["fabric_environment"]),
        project_path=Path(ctx.obj["fabric_workspace_repo_dir"]),
        workspace_id=workspace_id if workspace_id else None,
    )

    if workspace_name and not workspace_id:
        resolved = fab.get_workspace_id_from_name(workspace_name)
        if not resolved:
            ConsoleStyles.print_error(console, f"Workspace not found: {workspace_name}")
            raise SystemExit(1)
        fab.workspace_id = resolved

    # Build queries (apply filters if supplied) early so nested helpers can use them
    where_clauses: list[str] = []
    if schema:
        where_clauses.append("table_schema = @schema")
    if table_filter:
        where_clauses.append("table_name LIKE @table")
    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    columns_query = (
        "SELECT table_schema, table_name, column_name, data_type, is_nullable, ordinal_position "
        "FROM INFORMATION_SCHEMA.COLUMNS"
        + where_sql
        + " ORDER BY table_schema, table_name, ordinal_position"
    )

    # Helper to run one lakehouse and return ColumnRecord list
    def _run_single(
        _lakehouse_id: str, _lakehouse_name: Optional[str]
    ) -> list[ColumnRecord]:
        nonlocal sql_endpoint_id, sql_endpoint_server

        result: dict | None = None
        # Prefer ODBC path first
        try:
            if not sql_endpoint_server:
                sql_endpoint_server = fab.get_sql_server_for_lakehouse(
                    fab.workspace_id, _lakehouse_id
                )
            db_name = _lakehouse_name or fab.get_lakehouse_name_from_id(
                fab.workspace_id, _lakehouse_id
            )
            if not db_name:
                raise RuntimeError("Could not resolve lakehouse name for ODBC path.")
            _rows = _execute_sql_via_odbc(
                sql_endpoint_server=sql_endpoint_server,
                database=f"{db_name}",
                query=columns_query.replace(
                    "@schema", f"'{schema}'" if schema else "@schema"
                )
                .replace(
                    "@table",
                    f"'%{table_filter}%'" if table_filter else "@table",
                )
                .replace(" WHERE  AND ", " WHERE ")
                .replace(" WHERE ORDER", " ORDER"),
            )
            cols = [
                {"name": "table_schema"},
                {"name": "table_name"},
                {"name": "column_name"},
                {"name": "data_type"},
                {"name": "is_nullable"},
                {"name": "ordinal_position"},
            ]
            result = {"columns": cols, "rows": _rows}
        except Exception as e:
            ConsoleStyles.print_warning(
                console, f"ODBC path failed for lakehouse {_lakehouse_id}: {e}"
            )

        # REST fallback
        if result is None and method in {"sql-endpoint", "sql-endpoint-rest"}:
            if not sql_endpoint_id:
                sql_endpoint_id = fab.get_sql_endpoint_id_for_lakehouse(
                    fab.workspace_id, _lakehouse_id
                )
            if not sql_endpoint_id:
                ConsoleStyles.print_warning(
                    console,
                    f"Could not resolve SQL endpoint ID via REST for lakehouse {_lakehouse_id}.",
                )
                return []
            result = fab.execute_sql_on_sql_endpoint(
                workspace_id=fab.workspace_id,
                sql_endpoint_id=sql_endpoint_id,
                query=columns_query,
                parameters={
                    "schema": schema,
                    "table": f"%{table_filter}%" if table_filter else None,
                },
            )

        if result is None:
            return []

        cols, rows = _normalize_sql_rowset(result)
        name_to_idx = {name.lower(): i for i, name in enumerate(cols)}
        out: list[ColumnRecord] = []
        for r in rows:
            out.append(
                ColumnRecord(
                    workspace_id=fab.workspace_id,
                    lakehouse_id=_lakehouse_id,
                    lakehouse_name=_lakehouse_name or db_name,
                    sql_endpoint_id=sql_endpoint_id,
                    schema_name=str(r[name_to_idx.get("table_schema")])
                    if name_to_idx.get("table_schema") is not None
                    else "",
                    table_name=str(r[name_to_idx.get("table_name")])
                    if name_to_idx.get("table_name") is not None
                    else "",
                    table_type=None,
                    column_name=str(r[name_to_idx.get("column_name")])
                    if name_to_idx.get("column_name") is not None
                    else "",
                    data_type=str(r[name_to_idx.get("data_type")])
                    if name_to_idx.get("data_type") is not None
                    else None,
                    is_nullable=r[name_to_idx.get("is_nullable")]
                    if name_to_idx.get("is_nullable") is not None
                    else None,
                    ordinal_position=int(r[name_to_idx.get("ordinal_position")])
                    if name_to_idx.get("ordinal_position") is not None
                    else None,
                )
            )
        return out

    # If processing all lakehouses
    if all_lakehouses:
        # Require a workspace id to enumerate
        wsid = fab.workspace_id
        lakehouses = fab.list_lakehouses_api(wsid)
        if not lakehouses:
            ConsoleStyles.print_warning(console, "No lakehouses found in workspace.")
            _render_output(
                [],
                output_format=output_format,
                output_path=output_path,
                console=console,
            )
            return
        all_rows: list[ColumnRecord] = []
        for lh in lakehouses:
            lh_id = lh.get("id")
            lh_name = lh.get("displayName")
            if not lh_id:
                continue
            all_rows.extend(_run_single(lh_id, lh_name))
        # Default cache path if not provided
        if output_path is None:
            base_dir = Path(ctx.obj["fabric_workspace_repo_dir"]) / "metadata"
            base_dir.mkdir(parents=True, exist_ok=True)
            ext = (
                ".json"
                if output_format == "json"
                else ".csv"
                if output_format == "csv"
                else ".txt"
            )
            output_path = base_dir / f"lakehouse_metadata_all{ext}"
        _render_output(
            all_rows,
            output_format=output_format,
            output_path=output_path,
            console=console,
        )
        return

    # Resolve single lakehouse id via precedence: explicit id > name
    if not lakehouse_id:
        if not lakehouse_name:
            ConsoleStyles.print_error(
                console, "Provide --lakehouse-id or --lakehouse-name"
            )
            raise SystemExit(1)
        lid = fab.get_lakehouse_id_from_name(fab.workspace_id, lakehouse_name)
        if not lid:
            ConsoleStyles.print_error(console, f"Lakehouse not found: {lakehouse_name}")
            raise SystemExit(1)
        lakehouse_id = lid

    # Method selection
    method = method.lower().strip()
    if method not in {
        "sql-endpoint",
        "sql-endpoint-rest",
        "sql-endpoint-odbc",
        "onelake",
    }:
        ConsoleStyles.print_error(
            console,
            "--method must be one of: sql-endpoint, sql-endpoint-rest, sql-endpoint-odbc, onelake",
        )
        raise SystemExit(1)

    if method == "onelake":
        ConsoleStyles.print_error(
            console, "The --method onelake path is not implemented yet."
        )
        raise SystemExit(2)

    out_rows = _run_single(lakehouse_id, lakehouse_name)
    # Default cache path if not provided
    if output_path is None:
        base_dir = Path(ctx.obj["fabric_workspace_repo_dir"]) / "metadata"
        base_dir.mkdir(parents=True, exist_ok=True)
        name_or_id = lakehouse_name or lakehouse_id or "unknown"
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(name_or_id)).lower()
        ext = (
            ".json"
            if output_format == "json"
            else ".csv"
            if output_format == "csv"
            else ".txt"
        )
        output_path = base_dir / f"lakehouse_{safe}_columns{ext}"
    _render_output(
        out_rows, output_format=output_format, output_path=output_path, console=console
    )


def warehouse_metadata(
    *,
    ctx,
    workspace_id: Optional[str],
    workspace_name: Optional[str],
    warehouse_id: Optional[str],
    warehouse_name: Optional[str],
    schema: Optional[str],
    table_filter: Optional[str],
    method: str,
    sql_endpoint_id: Optional[str],
    sql_endpoint_server: Optional[str] = None,
    output_format: str,
    output_path: Optional[Path],
) -> None:
    console = Console()
    ConsoleStyles.print_info(console, "Preparing warehouse metadata extraction...")

    fab = FabricApiUtils(
        environment=str(ctx.obj["fabric_environment"]),
        project_path=Path(ctx.obj["fabric_workspace_repo_dir"]),
        workspace_id=workspace_id if workspace_id else None,
    )

    if workspace_name and not workspace_id:
        resolved = fab.get_workspace_id_from_name(workspace_name)
        if not resolved:
            ConsoleStyles.print_error(console, f"Workspace not found: {workspace_name}")
            raise SystemExit(1)
        fab.workspace_id = resolved

    # Resolve warehouse id
    if not warehouse_id:
        if not warehouse_name:
            ConsoleStyles.print_error(
                console, "Provide --warehouse-id or --warehouse-name"
            )
            raise SystemExit(1)
        wid = fab.get_warehouse_id_from_name(fab.workspace_id, warehouse_name)
        if not wid:
            ConsoleStyles.print_error(console, f"Warehouse not found: {warehouse_name}")
            raise SystemExit(1)
        warehouse_id = wid

    method = method.lower().strip()
    if method not in {"sql-endpoint", "sql-endpoint-rest", "sql-endpoint-odbc"}:
        ConsoleStyles.print_error(
            console,
            "--method must be one of: sql-endpoint, sql-endpoint-rest, sql-endpoint-odbc",
        )
        raise SystemExit(1)

    # Build filters
    where_clauses: list[str] = []
    if schema:
        where_clauses.append("table_schema = @schema")
    if table_filter:
        where_clauses.append("table_name LIKE @table")
    where_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    columns_query = (
        "SELECT c.table_schema, c.table_name, c.column_name, c.data_type, c.is_nullable, c.ordinal_position, t.table_type "
        "FROM INFORMATION_SCHEMA.COLUMNS c "
        "LEFT JOIN INFORMATION_SCHEMA.TABLES t ON t.table_schema = c.table_schema AND t.table_name = c.table_name"
        + where_sql
        + " ORDER BY c.table_schema, c.table_name, c.ordinal_position"
    )

    # Prefer REST unless explicitly ODBC
    use_rest = method in {"sql-endpoint", "sql-endpoint-rest"}
    result: dict | None = None
    rest_error: Exception | None = None

    if use_rest:
        if not sql_endpoint_id:
            sql_endpoint_id = fab.get_sql_endpoint_id_for_warehouse(
                fab.workspace_id, warehouse_id
            )
        if not sql_endpoint_id:
            rest_error = Exception(
                "Could not resolve SQL endpoint ID for warehouse via REST."
            )
        else:
            try:
                result = fab.execute_sql_on_sql_endpoint(
                    workspace_id=fab.workspace_id,
                    sql_endpoint_id=sql_endpoint_id,
                    query=columns_query,
                    parameters={
                        "schema": schema,
                        "table": f"%{table_filter}%" if table_filter else None,
                    },
                )
            except Exception as e:
                rest_error = e

    if result is None and method == "sql-endpoint-odbc":
        # Fallback to ODBC path
        try:
            db_name = warehouse_name or fab.get_warehouse_name_from_id(
                fab.workspace_id, warehouse_id
            )
            if not db_name:
                raise RuntimeError("Could not resolve warehouse name for ODBC path.")
            _rows = _execute_sql_via_odbc(
                sql_endpoint_server=sql_endpoint_server,
                database=db_name,
                query=columns_query.replace(
                    "@schema", f"'{schema}'" if schema else "@schema"
                )
                .replace(
                    "@table",
                    f"'%{table_filter}%'" if table_filter else "@table",
                )
                .replace(" WHERE  AND ", " WHERE ")
                .replace(" WHERE ORDER", " ORDER"),
            )
            cols = [
                {"name": "table_schema"},
                {"name": "table_name"},
                {"name": "column_name"},
                {"name": "data_type"},
                {"name": "is_nullable"},
                {"name": "ordinal_position"},
                {"name": "table_type"},
            ]
            result = {"columns": cols, "rows": _rows}
        except Exception as e:
            if rest_error:
                ConsoleStyles.print_warning(console, f"REST path failed: {rest_error}")
            ConsoleStyles.print_error(console, f"ODBC path failed: {e}")
            raise SystemExit(1)

    if result is None:
        if rest_error:
            ConsoleStyles.print_error(console, f"REST path failed: {rest_error}")
        ConsoleStyles.print_error(
            console, "No result returned for warehouse metadata query."
        )
        raise SystemExit(1)

    cols, rows = _normalize_sql_rowset(result)
    name_to_idx = {name.lower(): i for i, name in enumerate(cols)}

    out_rows: list[ColumnRecord] = []
    for r in rows:
        out_rows.append(
            ColumnRecord(
                workspace_id=fab.workspace_id,
                warehouse_id=warehouse_id,
                warehouse_name=warehouse_name or db_name,
                sql_endpoint_id=sql_endpoint_id,
                schema_name=str(r[name_to_idx.get("table_schema")])
                if name_to_idx.get("table_schema") is not None
                else "",
                table_name=str(r[name_to_idx.get("table_name")])
                if name_to_idx.get("table_name") is not None
                else "",
                table_type=str(r[name_to_idx.get("table_type")])
                if name_to_idx.get("table_type") is not None
                else None,
                column_name=str(r[name_to_idx.get("column_name")])
                if name_to_idx.get("column_name") is not None
                else "",
                data_type=str(r[name_to_idx.get("data_type")])
                if name_to_idx.get("data_type") is not None
                else None,
                is_nullable=r[name_to_idx.get("is_nullable")]
                if name_to_idx.get("is_nullable") is not None
                else None,
                ordinal_position=int(r[name_to_idx.get("ordinal_position")])
                if name_to_idx.get("ordinal_position") is not None
                else None,
            )
        )

    # Default cache path if not provided
    if output_path is None:
        base_dir = Path(ctx.obj["fabric_workspace_repo_dir"]) / "metadata"
        base_dir.mkdir(parents=True, exist_ok=True)
        name_or_id = warehouse_name or warehouse_id or "unknown"
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(name_or_id)).lower()
        ext = (
            ".json"
            if output_format == "json"
            else ".csv"
            if output_format == "csv"
            else ".txt"
        )
        output_path = base_dir / f"warehouse_{safe}_columns{ext}"
    _render_output(
        out_rows, output_format=output_format, output_path=output_path, console=console
    )


def lakehouse_summary(
    *,
    ctx,
    workspace_id: Optional[str],
    workspace_name: Optional[str],
    lakehouse_id: Optional[str],
    lakehouse_name: Optional[str],
    method: str,
    sql_endpoint_id: Optional[str],
    sql_endpoint_server: Optional[str] = None,
    output_format: str = "csv",
    output_path: Optional[Path] = None,
    all_lakehouses: bool = False,
) -> None:
    console = Console()
    ConsoleStyles.print_info(console, "Preparing lakehouse schema summary...")

    fab = FabricApiUtils(
        environment=str(ctx.obj["fabric_environment"]),
        project_path=Path(ctx.obj["fabric_workspace_repo_dir"]),
        workspace_id=workspace_id if workspace_id else None,
    )
    if workspace_name and not workspace_id:
        resolved = fab.get_workspace_id_from_name(workspace_name)
        if not resolved:
            ConsoleStyles.print_error(console, f"Workspace not found: {workspace_name}")
            raise SystemExit(1)
        fab.workspace_id = resolved

    # Helper to run summary for a single lakehouse
    def _run_single(
        _lakehouse_id: str, _lakehouse_name: Optional[str]
    ) -> list[SchemaSummaryRecord]:
        nonlocal sql_endpoint_id, sql_endpoint_server
        result: dict | None = None

        # ODBC first
        try:
            if not sql_endpoint_server:
                sql_endpoint_server = fab.get_sql_server_for_lakehouse(
                    fab.workspace_id, _lakehouse_id
                )
            db_name = _lakehouse_name or fab.get_lakehouse_name_from_id(
                fab.workspace_id, _lakehouse_id
            )
            if not db_name:
                raise RuntimeError("Could not resolve lakehouse name for ODBC path.")
            _rows = _execute_sql_via_odbc(
                sql_endpoint_server=sql_endpoint_server,
                database=f"{db_name}",
                query=query,
            )
            cols = [
                {"name": "table_schema"},
                {"name": "table_count"},
            ]
            result = {"columns": cols, "rows": _rows}
        except Exception as e:
            ConsoleStyles.print_warning(
                console, f"ODBC path failed for lakehouse {_lakehouse_id}: {e}"
            )

        # REST fallback
        if result is None and method in {"sql-endpoint", "sql-endpoint-rest"}:
            if not sql_endpoint_id:
                sql_endpoint_id = fab.get_sql_endpoint_id_for_lakehouse(
                    fab.workspace_id, _lakehouse_id
                )
            if not sql_endpoint_id:
                ConsoleStyles.print_warning(
                    console,
                    f"Could not resolve SQL endpoint ID for lakehouse {_lakehouse_id} via REST.",
                )
                return []
            result = fab.execute_sql_on_sql_endpoint(
                workspace_id=fab.workspace_id,
                sql_endpoint_id=sql_endpoint_id,
                query=query,
            )

        if result is None:
            return []

        cols, rows = _normalize_sql_rowset(result)
        name_to_idx = {name.lower(): i for i, name in enumerate(cols)}
        db_name = _lakehouse_name or fab.get_lakehouse_name_from_id(
            fab.workspace_id, _lakehouse_id
        )
        out_rows: list[SchemaSummaryRecord] = []
        for r in rows:
            out_rows.append(
                SchemaSummaryRecord(
                    workspace_id=fab.workspace_id,
                    lakehouse_id=_lakehouse_id,
                    database_name=f"{db_name}" if db_name else None,
                    schema_name=str(r[name_to_idx.get("table_schema")])
                    if name_to_idx.get("table_schema") is not None
                    else "",
                    table_count=int(r[name_to_idx.get("table_count")])
                    if name_to_idx.get("table_count") is not None
                    else 0,
                )
            )
        return out_rows

    method = method.lower().strip()
    if method not in {"sql-endpoint", "sql-endpoint-rest", "sql-endpoint-odbc"}:
        ConsoleStyles.print_error(
            console,
            "--method must be one of: sql-endpoint, sql-endpoint-rest, sql-endpoint-odbc",
        )
        raise SystemExit(1)

    query = (
        "SELECT table_schema, COUNT(DISTINCT table_name) AS table_count "
        "FROM INFORMATION_SCHEMA.TABLES GROUP BY table_schema ORDER BY table_schema"
    )

    if all_lakehouses:
        lakehouses = fab.list_lakehouses_api(fab.workspace_id)
        all_rows: list[SchemaSummaryRecord] = []
        for lh in lakehouses:
            lh_id = lh.get("id")
            lh_name = lh.get("displayName")
            if not lh_id:
                continue
            all_rows.extend(_run_single(lh_id, lh_name))
        # Default cache path if not provided
        if output_path is None:
            base_dir = Path(ctx.obj["fabric_workspace_repo_dir"]) / "metadata"
            base_dir.mkdir(parents=True, exist_ok=True)
            ext = (
                ".json"
                if output_format == "json"
                else ".csv"
                if output_format == "csv"
                else ".txt"
            )
            output_path = base_dir / f"lakehouse_summary_all{ext}"
        _render_summary(
            all_rows,
            output_format=output_format,
            output_path=output_path,
            console=console,
        )
        return

    # Single lakehouse path: resolve missing ID if needed, then run
    if not lakehouse_id:
        if not lakehouse_name:
            ConsoleStyles.print_error(
                console, "Provide --lakehouse-id or --lakehouse-name"
            )
            raise SystemExit(1)
        lid = fab.get_lakehouse_id_from_name(fab.workspace_id, lakehouse_name)
        if not lid:
            ConsoleStyles.print_error(console, f"Lakehouse not found: {lakehouse_name}")
            raise SystemExit(1)
        lakehouse_id = lid
    out_rows = _run_single(lakehouse_id, lakehouse_name)
    # Default cache path if not provided
    if output_path is None:
        base_dir = Path(ctx.obj["fabric_workspace_repo_dir"]) / "metadata"
        base_dir.mkdir(parents=True, exist_ok=True)
        name_or_id = lakehouse_name or lakehouse_id or "unknown"
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(name_or_id)).lower()
        ext = (
            ".json"
            if output_format == "json"
            else ".csv"
            if output_format == "csv"
            else ".txt"
        )
        output_path = base_dir / f"lakehouse_{safe}_summary{ext}"
    _render_summary(
        out_rows, output_format=output_format, output_path=output_path, console=console
    )


def warehouse_summary(
    *,
    ctx,
    workspace_id: Optional[str],
    workspace_name: Optional[str],
    warehouse_id: Optional[str],
    warehouse_name: Optional[str],
    method: str,
    sql_endpoint_id: Optional[str],
    sql_endpoint_server: Optional[str] = None,
    output_format: str = "csv",
    output_path: Optional[Path] = None,
) -> None:
    console = Console()
    ConsoleStyles.print_info(console, "Preparing warehouse schema summary...")

    fab = FabricApiUtils(
        environment=str(ctx.obj["fabric_environment"]),
        project_path=Path(ctx.obj["fabric_workspace_repo_dir"]),
        workspace_id=workspace_id if workspace_id else None,
    )
    if workspace_name and not workspace_id:
        resolved = fab.get_workspace_id_from_name(workspace_name)
        if not resolved:
            ConsoleStyles.print_error(console, f"Workspace not found: {workspace_name}")
            raise SystemExit(1)
        fab.workspace_id = resolved

    if not warehouse_id:
        if not warehouse_name:
            ConsoleStyles.print_error(
                console, "Provide --warehouse-id or --warehouse-name"
            )
            raise SystemExit(1)
        wid = fab.get_warehouse_id_from_name(fab.workspace_id, warehouse_name)
        if not wid:
            ConsoleStyles.print_error(console, f"Warehouse not found: {warehouse_name}")
            raise SystemExit(1)
        warehouse_id = wid

    method = method.lower().strip()
    if method not in {"sql-endpoint", "sql-endpoint-rest", "sql-endpoint-odbc"}:
        ConsoleStyles.print_error(
            console,
            "--method must be one of: sql-endpoint, sql-endpoint-rest, sql-endpoint-odbc",
        )
        raise SystemExit(1)

    query = (
        "SELECT table_schema, COUNT(DISTINCT table_name) AS table_count "
        "FROM INFORMATION_SCHEMA.TABLES GROUP BY table_schema ORDER BY table_schema"
    )

    result: dict | None = None

    # ODBC first if requested
    if method == "sql-endpoint-odbc":
        try:
            db_name = warehouse_name or fab.get_warehouse_name_from_id(
                fab.workspace_id, warehouse_id
            )
            if not db_name:
                raise RuntimeError("Could not resolve warehouse name for ODBC path.")
            _rows = _execute_sql_via_odbc(
                sql_endpoint_server=sql_endpoint_server,
                database=db_name,
                query=query,
            )
            cols = [{"name": "table_schema"}, {"name": "table_count"}]
            result = {"columns": cols, "rows": _rows}
        except Exception as e:
            ConsoleStyles.print_warning(console, f"ODBC path failed: {e}")

    # REST (or fallback)
    if result is None and method in {"sql-endpoint", "sql-endpoint-rest"}:
        if not sql_endpoint_id:
            sql_endpoint_id = fab.get_sql_endpoint_id_for_warehouse(
                fab.workspace_id, warehouse_id
            )
        if not sql_endpoint_id:
            ConsoleStyles.print_error(
                console, "Could not resolve SQL endpoint ID for warehouse via REST."
            )
            raise SystemExit(1)
        result = fab.execute_sql_on_sql_endpoint(
            workspace_id=fab.workspace_id,
            sql_endpoint_id=sql_endpoint_id,
            query=query,
        )

    if result is None:
        ConsoleStyles.print_error(
            console, "No result returned for warehouse schema summary."
        )
        raise SystemExit(1)

    cols, rows = _normalize_sql_rowset(result)
    name_to_idx = {name.lower(): i for i, name in enumerate(cols)}
    db_name = warehouse_name or fab.get_warehouse_name_from_id(
        fab.workspace_id, warehouse_id
    )
    out_rows: list[SchemaSummaryRecord] = []
    for r in rows:
        out_rows.append(
            SchemaSummaryRecord(
                workspace_id=fab.workspace_id,
                warehouse_id=warehouse_id,
                database_name=db_name,
                schema_name=str(r[name_to_idx.get("table_schema")])
                if name_to_idx.get("table_schema") is not None
                else "",
                table_count=int(r[name_to_idx.get("table_count")])
                if name_to_idx.get("table_count") is not None
                else 0,
            )
        )

    # Default cache path if not provided
    if output_path is None:
        base_dir = Path(ctx.obj["fabric_workspace_repo_dir"]) / "metadata"
        base_dir.mkdir(parents=True, exist_ok=True)
        name_or_id = warehouse_name or warehouse_id or "unknown"
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(name_or_id)).lower()
        ext = (
            ".json"
            if output_format == "json"
            else ".csv"
            if output_format == "csv"
            else ".txt"
        )
        output_path = base_dir / f"warehouse_{safe}_summary{ext}"
    _render_summary(
        out_rows, output_format=output_format, output_path=output_path, console=console
    )


def _execute_sql_via_odbc(
    *, sql_endpoint_server: Optional[str], database: str, query: str
) -> list[list[Any]]:
    """Execute SQL via ODBC using Azure CLI token against Fabric SQL endpoint.

    - sql_endpoint_server: server prefix without domain; forms {server}.datawarehouse.fabric.microsoft.com
    - database: target database name (e.g., f<lakehouse_name>)
    - query: SQL text to execute
    """
    if not sql_endpoint_server:
        raise ValueError(
            "--sql-endpoint-server is required for ODBC path (e.g., 'myws-abc123')."
        )
    try:
        import struct
        from itertools import chain, repeat

        import pyodbc
        from azure.identity import AzureCliCredential
    except ImportError as e:
        raise RuntimeError(
            f"Missing dependency for ODBC path: {e}. Install 'pyodbc' and ensure ODBC Driver 18 is available."
        )

    credential = AzureCliCredential()
    sql_host = f"{sql_endpoint_server}"
    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};Server={sql_host},1433;"
        f"Database={database};Encrypt=Yes;TrustServerCertificate=No"
    )
    print(conn_str)  # Debugging output; can be removed later
    token_obj = credential.get_token("https://database.windows.net//.default")
    token_as_bytes = bytes(token_obj.token, "UTF-8")
    encoded_bytes = bytes(chain.from_iterable(zip(token_as_bytes, repeat(0))))
    token_bytes = struct.pack("<i", len(encoded_bytes)) + encoded_bytes
    attrs_before = {1256: token_bytes}

    conn = pyodbc.connect(conn_str, attrs_before=attrs_before)
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        return [list(row) for row in rows]
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


@dataclass
class MetadataDifference:
    """Represents a difference found between two metadata files."""
    type: str  # 'missing_table', 'missing_column', 'data_type_diff', 'nullable_diff'
    identifier: str  # Unique identifier for the table/column
    file1_value: str | None = None
    file2_value: str | None = None
    description: str = ""
    asset_name: str = ""  # For sorting
    schema_name: str = ""  # For sorting
    table_name: str = ""   # For sorting
    column_name: str = ""  # For sorting


def compare_metadata(
    ctx: typer.Context,
    file1: Path,
    file2: Path,
    output_path: Optional[Path] = None,
    output_format: str = "table",
) -> None:
    """Compare two metadata CSV files and report differences.
    
    Args:
        ctx: Typer context object
        file1: Path to first CSV metadata file
        file2: Path to second CSV metadata file  
        output_path: Optional path to write comparison report
        output_format: Output format ('table', 'json', 'csv')
    """
    import csv
    import json
    from collections import defaultdict
    from rich.console import Console
    from rich.table import Table
    from ingen_fab.cli_utils.console_styles import ConsoleStyles
    
    console = Console()
    
    # Validate input files exist
    if not file1.exists():
        ConsoleStyles.print_error(console, f"File not found: {file1}")
        raise typer.Exit(code=1)
    
    if not file2.exists():
        ConsoleStyles.print_error(console, f"File not found: {file2}")
        raise typer.Exit(code=1)
    
    # Read and parse CSV files
    def read_metadata_csv(file_path: Path) -> tuple[dict, dict]:
        """Read CSV and return tables dict and columns dict."""
        tables = set()  # {asset_name}.{schema_name}.{table_name}
        columns = {}    # {asset_name}.{schema_name}.{table_name}.{column_name}: {data_type, is_nullable}
        
        with open(file_path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Determine asset name (lakehouse or warehouse)
                asset_name = row.get('lakehouse_name') or row.get('warehouse_name', '')
                if not asset_name:
                    continue
                    
                schema_name = row.get('schema_name', '')
                table_name = row.get('table_name', '')
                column_name = row.get('column_name', '')
                
                if not table_name:
                    continue
                
                # Create table identifier
                table_id = f"{asset_name}.{schema_name}.{table_name}"
                tables.add(table_id)
                
                # Create column identifier if column data exists
                if column_name:
                    column_id = f"{table_id}.{column_name}"
                    columns[column_id] = {
                        'data_type': row.get('data_type', ''),
                        'is_nullable': str(row.get('is_nullable', '')).lower(),
                        'table_id': table_id,
                        'asset_name': asset_name,
                        'schema_name': schema_name,
                        'table_name': table_name,
                        'column_name': column_name
                    }
        
        return tables, columns
    
    try:
        ConsoleStyles.print_info(console, f"Reading metadata from {file1.name}...")
        tables1, columns1 = read_metadata_csv(file1)
        
        ConsoleStyles.print_info(console, f"Reading metadata from {file2.name}...")
        tables2, columns2 = read_metadata_csv(file2)
        
    except Exception as e:
        ConsoleStyles.print_error(console, f"Error reading CSV files: {e}")
        raise typer.Exit(code=1)
    
    # Find differences
    differences = []
    
    # Missing tables
    missing_in_file2 = tables1 - tables2
    missing_in_file1 = tables2 - tables1
    
    for table_id in missing_in_file2:
        # Parse table_id: {asset_name}.{schema_name}.{table_name}
        parts = table_id.split('.')
        asset_name = parts[0] if len(parts) > 0 else ""
        schema_name = parts[1] if len(parts) > 1 else ""
        table_name = parts[2] if len(parts) > 2 else ""
        
        differences.append(MetadataDifference(
            type="missing_table",
            identifier=table_id,
            file1_value="present",
            file2_value="missing",
            description=f"Table '{table_id}' exists in {file1.name} but not in {file2.name}",
            asset_name=asset_name,
            schema_name=schema_name,
            table_name=table_name,
            column_name=""
        ))
    
    for table_id in missing_in_file1:
        # Parse table_id: {asset_name}.{schema_name}.{table_name}
        parts = table_id.split('.')
        asset_name = parts[0] if len(parts) > 0 else ""
        schema_name = parts[1] if len(parts) > 1 else ""
        table_name = parts[2] if len(parts) > 2 else ""
        
        differences.append(MetadataDifference(
            type="missing_table", 
            identifier=table_id,
            file1_value="missing",
            file2_value="present",
            description=f"Table '{table_id}' exists in {file2.name} but not in {file1.name}",
            asset_name=asset_name,
            schema_name=schema_name,
            table_name=table_name,
            column_name=""
        ))
    
    # Missing columns
    all_columns1 = set(columns1.keys())
    all_columns2 = set(columns2.keys())
    
    missing_columns_in_file2 = all_columns1 - all_columns2
    missing_columns_in_file1 = all_columns2 - all_columns1
    
    for column_id in missing_columns_in_file2:
        col_info = columns1[column_id]
        differences.append(MetadataDifference(
            type="missing_column",
            identifier=column_id,
            file1_value="present",
            file2_value="missing", 
            description=f"Column '{col_info['column_name']}' in table '{col_info['table_id']}' exists in {file1.name} but not in {file2.name}",
            asset_name=col_info['asset_name'],
            schema_name=col_info['schema_name'],
            table_name=col_info['table_name'],
            column_name=col_info['column_name']
        ))
    
    for column_id in missing_columns_in_file1:
        col_info = columns2[column_id]
        differences.append(MetadataDifference(
            type="missing_column",
            identifier=column_id,
            file1_value="missing",
            file2_value="present",
            description=f"Column '{col_info['column_name']}' in table '{col_info['table_id']}' exists in {file2.name} but not in {file1.name}",
            asset_name=col_info['asset_name'],
            schema_name=col_info['schema_name'],
            table_name=col_info['table_name'],
            column_name=col_info['column_name']
        ))
    
    # Data type and nullable differences for common columns
    common_columns = all_columns1 & all_columns2
    
    for column_id in common_columns:
        col1 = columns1[column_id]
        col2 = columns2[column_id]
        
        # Compare data types
        if col1['data_type'] != col2['data_type']:
            differences.append(MetadataDifference(
                type="data_type_diff",
                identifier=column_id,
                file1_value=col1['data_type'],
                file2_value=col2['data_type'],
                description=f"Column '{col1['column_name']}' in table '{col1['table_id']}' has different data types: {col1['data_type']} vs {col2['data_type']}",
                asset_name=col1['asset_name'],
                schema_name=col1['schema_name'],
                table_name=col1['table_name'],
                column_name=col1['column_name']
            ))
        
        # Compare nullable settings
        if col1['is_nullable'] != col2['is_nullable']:
            differences.append(MetadataDifference(
                type="nullable_diff", 
                identifier=column_id,
                file1_value=col1['is_nullable'],
                file2_value=col2['is_nullable'],
                description=f"Column '{col1['column_name']}' in table '{col1['table_id']}' has different nullable settings: {col1['is_nullable']} vs {col2['is_nullable']}",
                asset_name=col1['asset_name'],
                schema_name=col1['schema_name'],
                table_name=col1['table_name'],
                column_name=col1['column_name']
            ))
    
    # Sort differences by asset_name, schema_name, table_name, column_name, type
    differences.sort(key=lambda x: (
        x.asset_name.lower(),
        x.schema_name.lower(), 
        x.table_name.lower(),
        x.column_name.lower(),
        x.type
    ))
    
    # Generate output
    _render_comparison_output(
        differences=differences,
        file1_name=file1.name,
        file2_name=file2.name,
        output_format=output_format,
        output_path=output_path,
        console=console
    )


def _render_comparison_output(
    differences: list[MetadataDifference],
    file1_name: str,
    file2_name: str,
    output_format: str,
    output_path: Optional[Path],
    console: Console,
) -> None:
    """Render the comparison output in the specified format."""
    import json
    import csv
    from collections import defaultdict
    
    if not differences:
        ConsoleStyles.print_success(console, f"No differences found between {file1_name} and {file2_name}")
        return
    
    # Group differences by type for summary
    by_type = defaultdict(int)
    for diff in differences:
        by_type[diff.type] += 1
    
    summary_msg = f"Found {len(differences)} differences between {file1_name} and {file2_name}:\n"
    for diff_type, count in by_type.items():
        summary_msg += f"  - {diff_type.replace('_', ' ').title()}: {count}\n"
    
    if output_format == "table":
        console.print(summary_msg)
        
        # Create rich table
        table = Table(title="Metadata Comparison Results")
        table.add_column("Type", style="cyan")
        table.add_column("Identifier", style="yellow") 
        table.add_column(f"{file1_name}", style="green")
        table.add_column(f"{file2_name}", style="red")
        table.add_column("Description", style="white")
        
        for diff in differences:
            table.add_row(
                diff.type.replace('_', ' ').title(),
                diff.identifier,
                str(diff.file1_value or ""),
                str(diff.file2_value or ""),
                diff.description
            )
        
        if output_path:
            # For file output with table format, convert to plain text
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(summary_msg + "\n")
                f.write("Type\tIdentifier\tFile1 Value\tFile2 Value\tDescription\tAsset Name\tSchema Name\tTable Name\tColumn Name\n")
                for diff in differences:
                    f.write(f"{diff.type}\t{diff.identifier}\t{diff.file1_value}\t{diff.file2_value}\t{diff.description}\t{diff.asset_name}\t{diff.schema_name}\t{diff.table_name}\t{diff.column_name}\n")
            ConsoleStyles.print_success(console, f"Comparison report written to {output_path}")
        
        console.print(table)
        
    elif output_format == "json":
        # Convert to JSON format
        output_data = {
            "summary": {
                "file1": file1_name,
                "file2": file2_name,
                "total_differences": len(differences),
                "by_type": dict(by_type)
            },
            "differences": [
                {
                    "type": diff.type,
                    "identifier": diff.identifier,
                    "file1_value": diff.file1_value,
                    "file2_value": diff.file2_value,
                    "description": diff.description,
                    "asset_name": diff.asset_name,
                    "schema_name": diff.schema_name,
                    "table_name": diff.table_name,
                    "column_name": diff.column_name
                }
                for diff in differences
            ]
        }
        
        json_output = json.dumps(output_data, indent=2)
        
        if output_path:
            output_path.write_text(json_output, encoding='utf-8')
            ConsoleStyles.print_success(console, f"JSON comparison report written to {output_path}")
        else:
            console.print(json_output)
            
    elif output_format == "csv":
        # Create CSV output
        fieldnames = ["type", "identifier", "file1_value", "file2_value", "description", "asset_name", "schema_name", "table_name", "column_name"]
        
        if output_path:
            with open(output_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for diff in differences:
                    writer.writerow({
                        "type": diff.type,
                        "identifier": diff.identifier,
                        "file1_value": diff.file1_value,
                        "file2_value": diff.file2_value,
                        "description": diff.description,
                        "asset_name": diff.asset_name,
                        "schema_name": diff.schema_name,
                        "table_name": diff.table_name,
                        "column_name": diff.column_name
                    })
            ConsoleStyles.print_success(console, f"CSV comparison report written to {output_path}")
        else:
            # Print CSV to console
            import io
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for diff in differences:
                writer.writerow({
                    "type": diff.type,
                    "identifier": diff.identifier, 
                    "file1_value": diff.file1_value,
                    "file2_value": diff.file2_value,
                    "description": diff.description,
                    "asset_name": diff.asset_name,
                    "schema_name": diff.schema_name,
                    "table_name": diff.table_name,
                    "column_name": diff.column_name
                })
            console.print(output.getvalue())
    
    # Always print summary to console
    if output_format != "table":
        console.print(summary_msg)
