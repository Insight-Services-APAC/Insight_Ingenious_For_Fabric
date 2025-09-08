import csv
import json
import re
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel

from ingen_fab.cli_utils.dbt_profile_manager import ensure_dbt_profile
from ingen_fab.notebook_utils.notebook_utils import NotebookUtils

console = Console()


def create_additional_notebooks(
    ctx: typer.Context, dbt_project: str, skip_profile_confirmation: bool = False
) -> None:
    """Create notebooks in fabric_workspace_items/{dbt_project} from dbt target outputs.

    This scans {workspace}/{dbt_project}/target/notebooks_fabric_py for Python notebooks,
    reads their contents, and creates Fabric notebooks under
    {workspace}/fabric_workspace_items/{dbt_project}/ using NotebookUtils.create_notebook_with_platform.
    """

    # Check and update dbt profile if needed
    if not ensure_dbt_profile(ctx, ask_confirmation=not skip_profile_confirmation, dbt_project_dir=dbt_project):
        raise typer.Exit(code=1)

    # Resolve workspace directory from context (set by main callback)
    workspace_dir = ctx.obj.get("fabric_workspace_repo_dir") if ctx.obj else None
    if not workspace_dir:
        console.print("[red]Fabric workspace repo dir not provided.[/red]")
        raise typer.Exit(code=1)

    workspace_dir = Path(workspace_dir)

    # Source dir containing dbt-generated notebook .py files
    source_dir = workspace_dir / dbt_project / "target" / "notebooks_fabric_py"

    if not source_dir.exists() or not source_dir.is_dir():
        console.print(f"[red]Source directory not found:[/red] {source_dir}")
        raise typer.Exit(code=1)

    # Target directory for Fabric notebooks
    target_dir = workspace_dir / "fabric_workspace_items" / dbt_project
    target_dir.mkdir(parents=True, exist_ok=True)

    # Initialize NotebookUtils with workspace root
    nb_utils = NotebookUtils(
        fabric_workspace_repo_dir=workspace_dir,
        output_dir=workspace_dir / "fabric_workspace_items",
        enable_console=True,
    )

    # Load manifest to map model unique_id -> path for folder structure
    manifest_path = workspace_dir / dbt_project / "target" / "manifest.json"
    manifest = {}
    if manifest_path.exists():
        try:
            import json

            with manifest_path.open("r", encoding="utf-8") as mf:
                manifest = json.load(mf)
        except Exception as e:
            console.print(
                f"[yellow]Warning: Could not read manifest.json: {e}[/yellow]"
            )

    nodes = manifest.get("nodes", {}) if isinstance(manifest, dict) else {}

    # Collect .py notebook files
    notebook_files: list[Path] = sorted(source_dir.glob("*.py"))
    if not notebook_files:
        console.print("[yellow]No notebooks found to import.[/yellow]")
        return

    console.print(
        Panel.fit(
            f"Creating notebooks for [bold]{len(notebook_files)}[/bold] dbt items",
            title=f"DBT Project: {dbt_project}",
            border_style="cyan",
        )
    )

    created = 0
    for nb_path in notebook_files:
        try:
            notebook_name = nb_path.stem  # keep original stem, may include dots
            with nb_path.open("r", encoding="utf-8") as f:
                content = f.read()

            # Determine category and destination folder
            stem = notebook_name
            dest_root = target_dir
            category = None
            model_name_for_group = None
            if stem.startswith("model."):
                # Replicate path structure from manifest under models/
                dest_root = target_dir / "models"
                category = "model"
                node = nodes.get(stem, {})
                rel_path = node.get("path") if isinstance(node, dict) else None
                if rel_path:
                    p = Path(rel_path)
                    parts = list(p.parts)
                    if "models" in parts:
                        try:
                            idx = parts.index("models")
                            # subdirs under models, excluding the filename
                            sub = (
                                Path(*parts[idx + 1 : -1])
                                if len(parts) > idx + 1
                                else Path()
                            )
                        except ValueError:
                            sub = p.parent
                    else:
                        sub = p.parent
                    # Append model name folder under the subpath
                    model_name_for_group = stem.split(".")[-1]
                    dest_root = dest_root / sub / model_name_for_group
                else:
                    # No manifest path; still place under models/<model_name>
                    model_name_for_group = stem.split(".")[-1]
                    dest_root = dest_root / model_name_for_group
            elif stem.startswith("seed."):
                dest_root = target_dir / "seeds"
                category = "seed"
            elif stem.startswith("test."):
                # Place tests alongside their associated model's folder structure
                dest_root = target_dir / "models"
                category = "test"
                node = nodes.get(stem, {})
                model_uid = None
                if isinstance(node, dict):
                    deps = node.get("depends_on", {})
                    if isinstance(deps, dict):
                        for dep_uid in deps.get("nodes", []) or []:
                            if isinstance(dep_uid, str) and dep_uid.startswith(
                                "model."
                            ):
                                model_uid = dep_uid
                                break
                if model_uid and model_uid in nodes:
                    model_node = nodes[model_uid]
                    rel_path = (
                        model_node.get("path") if isinstance(model_node, dict) else None
                    )
                    if rel_path:
                        p = Path(rel_path)
                        parts = list(p.parts)
                        if "models" in parts:
                            try:
                                idx = parts.index("models")
                                sub = (
                                    Path(*parts[idx + 1 : -1])
                                    if len(parts) > idx + 1
                                    else Path()
                                )
                            except ValueError:
                                sub = p.parent
                        else:
                            sub = p.parent
                        # Place test under models/<sub>/<model_name>
                        model_name_for_group = model_uid.split(".")[-1]
                        dest_root = dest_root / sub / model_name_for_group
                    else:
                        # No manifest path; fallback to models/<model_name>
                        model_name_for_group = model_uid.split(".")[-1]
                        dest_root = dest_root / model_name_for_group
                else:
                    # Fallback: keep alongside models bucket
                    dest_root = target_dir / "models"
                    category = "other"
            elif stem.startswith("master"):
                dest_root = target_dir / "masters"
                category = "master"
            else:
                # Fallback: keep alongside models bucket
                dest_root = target_dir / "models"
                category = "other"

            dest_root.mkdir(parents=True, exist_ok=True)

            # Build normalized folder name (strip category prefixes) but keep display_name original
            if category == "model":
                normalized_base = "model"
            elif category == "test":
                # Remove 'test.<project>.' prefix
                parts = stem.split(".")
                normalized_base = ".".join(parts[2:]) if len(parts) > 2 else stem
                # Also remove embedded model name to avoid duplication in the model folder
                if model_name_for_group:
                    # Replace _<model_name>_ with _ and clean up edges
                    normalized_base = normalized_base.replace(
                        f"_{model_name_for_group}_", "_"
                    )
                    if normalized_base.startswith(f"{model_name_for_group}_"):
                        normalized_base = normalized_base[
                            len(model_name_for_group) + 1 :
                        ]
                    if normalized_base.endswith(f"_{model_name_for_group}"):
                        normalized_base = normalized_base[
                            : -len(model_name_for_group) - 1
                        ]
                    # Collapse any double underscores
                    normalized_base = re.sub(r"__+", "_", normalized_base)
            elif category == "seed":
                parts = stem.split(".")
                normalized_base = ".".join(parts[2:]) if len(parts) > 2 else stem
            elif category == "master":
                # Drop potential project token: master_<proj>_<rest> -> master_<rest>
                normalized_base = re.sub(r"^master_([^_]+)_(.+)$", r"master_\2", stem)
            else:
                normalized_base = stem

            # Append _{dbt_project} to folder name
            notebook_folder_name = f"{normalized_base}_{dbt_project}"

            nb_utils.create_notebook_with_platform(
                notebook_name=notebook_folder_name,
                rendered_content=content,
                output_dir=dest_root,
                display_name=notebook_name,
            )
            created += 1
        except Exception as e:
            console.print(
                f"[yellow]Warning: Failed to create notebook for {nb_path.name}: {e}[/yellow]"
            )

    console.print(
        Panel.fit(
            f"[bold green]✓ Created {created} notebook(s) in[/bold green]\n{target_dir}",
            title="Completed",
            border_style="green",
        )
    )


def convert_tsql_to_spark_type(tsql_type: str) -> str:
    """Convert T-SQL/SQL Server data types to Spark SQL data types."""

    # Normalize the input type (remove size specifications, make lowercase)
    base_type = tsql_type.lower().strip()

    # Remove size/precision specifications like (10) or (10,2)
    if "(" in base_type:
        base_type = base_type.split("(")[0].strip()

    # Type mapping from T-SQL to Spark SQL
    type_mapping = {
        # String types
        "varchar": "string",
        "nvarchar": "string",
        "char": "string",
        "nchar": "string",
        "text": "string",
        "ntext": "string",
        # Numeric types
        "int": "int",
        "bigint": "bigint",
        "smallint": "smallint",
        "tinyint": "tinyint",
        "bit": "boolean",
        "decimal": "decimal",
        "numeric": "decimal",
        "float": "float",
        "real": "float",
        "money": "decimal(19,4)",
        "smallmoney": "decimal(10,4)",
        # Date/Time types
        "datetime": "timestamp",
        "datetime2": "timestamp",
        "date": "date",
        "time": "string",  # Spark doesn't have a time-only type
        "datetimeoffset": "timestamp",
        "smalldatetime": "timestamp",
        # Binary types
        "binary": "binary",
        "varbinary": "binary",
        "image": "binary",
        # Other types
        "uniqueidentifier": "string",
        "xml": "string",
        "sql_variant": "string",
        "hierarchyid": "string",
        "geometry": "string",
        "geography": "string",
    }

    # Return mapped type or original if not found
    return type_mapping.get(base_type, base_type)


def convert_metadata_to_dbt_format(
    ctx: typer.Context, dbt_project: str, skip_profile_confirmation: bool = False
) -> None:
    """Convert cached lakehouse metadata CSV to dbt metaextracts JSON format.

    Reads from {workspace}/metadata/lakehouse_metadata_all.csv and creates:
    - {workspace}/{dbt_project}/metaextracts/ListRelations.json
    - {workspace}/{dbt_project}/metaextracts/ListSchemas.json
    - {workspace}/{dbt_project}/metaextracts/DescribeRelations.json
    - {workspace}/{dbt_project}/metaextracts/MetaHashes.json
    """

    # Check and update dbt profile if needed
    if not ensure_dbt_profile(ctx, ask_confirmation=not skip_profile_confirmation, dbt_project_dir=dbt_project):
        raise typer.Exit(code=1)

    workspace_dir = ctx.obj.get("fabric_workspace_repo_dir") if ctx.obj else None
    if not workspace_dir:
        console.print("[red]Fabric workspace repo dir not provided.[/red]")
        raise typer.Exit(code=1)

    workspace_dir = Path(workspace_dir)

    # Source CSV file
    metadata_csv = workspace_dir / "metadata" / "lakehouse_metadata_all.csv"
    if not metadata_csv.exists():
        console.print(f"[red]Metadata CSV not found:[/red] {metadata_csv}")
        console.print(
            "[yellow]Run 'ingen_fab deploy get-metadata --target lakehouse' first to generate metadata.[/yellow]"
        )
        raise typer.Exit(code=1)

    # Target directory for dbt metaextracts (in the dbt_project root, not target)
    target_dir = workspace_dir / dbt_project / "metaextracts"
    target_dir.mkdir(parents=True, exist_ok=True)

    console.print(
        Panel.fit(
            f"Converting metadata for [bold]{dbt_project}[/bold]",
            title="DBT Metadata Conversion",
            border_style="cyan",
        )
    )

    # Read CSV and process data
    relations = []
    schemas = set()
    describe_relations = []  # Changed to a list instead of dict

    try:
        with metadata_csv.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            current_table = None
            current_workspace_id = None
            current_lakehouse_id = None

            for row in reader:
                # schema_name = row.get("schema_name", "").strip()
                table_name = row.get("table_name", "").strip()
                workspace_id = row.get("workspace_id", "").strip()
                lakehouse_id = row.get("lakehouse_id", "").strip()
                lakehouse_name = row.get("lakehouse_name", "").strip()

                if not lakehouse_name or not table_name:
                    continue

                schemas.add(lakehouse_name)

                # Build table key using lakehouse_name as namespace
                table_key = f"{lakehouse_name}.{table_name}"

                # If we've moved to a new table, save the previous one
                if current_table and current_table != table_key:
                    # Add relation entry for the previous table
                    namespace, tbl = current_table.split(".", 1)

                    # Build the information string similar to Spark's DESCRIBE EXTENDED
                    info_lines = [
                        "Catalog: spark_catalog",
                        f"Database: {namespace}",
                        f"Table: {tbl}",
                        "Owner: trusted-service-user",
                        "Created Time: Sat Aug 02 10:38:14 UTC 2025",
                        "Last Access: UNKNOWN",
                        "Created By: Spark 3.2.1-SNAPSHOT",
                        "Type: MANAGED",
                        "Provider: delta",
                        "Table Properties: [trident.autodiscovered.table=true]",
                        f"Location: abfss://{current_workspace_id}@onelake.dfs.fabric.microsoft.com/{current_lakehouse_id}/Tables/{tbl}",
                        "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat",
                        "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
                        "Partition Provider: Catalog",
                    ]

                    relation = {
                        "namespace": namespace,
                        "tableName": tbl,
                        "isTemporary": False,
                        "information": "\n".join(info_lines) + "\n",
                        "type": "MANAGED",
                    }
                    relations.append(relation)

                current_table = table_key
                current_workspace_id = workspace_id
                current_lakehouse_id = lakehouse_id

                # Add column information with type conversion
                tsql_type = row.get("data_type", "")
                spark_type = convert_tsql_to_spark_type(tsql_type)

                # Each column is now a separate entry with namespace and tableName
                column_info = {
                    "col_name": row.get("column_name", ""),
                    "data_type": spark_type,
                    "comment": None,
                    "namespace": lakehouse_name,
                    "tableName": table_name,
                }
                describe_relations.append(column_info)

        # Don't forget the last table's relation entry
        if current_table:
            namespace, tbl = current_table.split(".", 1)
            info_lines = [
                "Catalog: spark_catalog",
                f"Database: {namespace}",
                f"Table: {tbl}",
                "Owner: trusted-service-user",
                "Created Time: Sat Aug 02 10:38:14 UTC 2025",
                "Last Access: UNKNOWN",
                "Created By: Spark 3.2.1-SNAPSHOT",
                "Type: MANAGED",
                "Provider: delta",
                "Table Properties: [trident.autodiscovered.table=true]",
                f"Location: abfss://{current_workspace_id}@onelake.dfs.fabric.microsoft.com/{current_lakehouse_id}/Tables/{tbl}",
                "Serde Library: org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "InputFormat: org.apache.hadoop.mapred.SequenceFileInputFormat",
                "OutputFormat: org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
                "Partition Provider: Catalog",
            ]

            relation = {
                "namespace": namespace,
                "tableName": tbl,
                "isTemporary": False,
                "information": "\n".join(info_lines) + "\n",
                "type": "MANAGED",
            }
            relations.append(relation)

        # Write ListRelations.json
        list_relations_path = target_dir / "ListRelations.json"
        with list_relations_path.open("w", encoding="utf-8") as f:
            json.dump(relations, f, indent=2)
        console.print(f"[green]✓[/green] Created {list_relations_path}")

        # Write ListSchemas.json
        list_schemas_path = target_dir / "ListSchemas.json"
        schemas_list = [{"namespace": s} for s in sorted(schemas)]
        with list_schemas_path.open("w", encoding="utf-8") as f:
            json.dump(schemas_list, f, indent=2)
        console.print(f"[green]✓[/green] Created {list_schemas_path}")

        # Write DescribeRelations.json
        describe_relations_path = target_dir / "DescribeRelations.json"
        with describe_relations_path.open("w", encoding="utf-8") as f:
            json.dump(describe_relations, f, indent=2)
        console.print(f"[green]✓[/green] Created {describe_relations_path}")

        # Write MetaHashes.json (empty for now)
        meta_hashes_path = target_dir / "MetaHashes.json"
        with meta_hashes_path.open("w", encoding="utf-8") as f:
            json.dump({}, f, indent=2)
        console.print(f"[green]✓[/green] Created {meta_hashes_path}")

        console.print(
            Panel.fit(
                f"[bold green]✓ Metadata conversion complete[/bold green]\n"
                f"Found {len(relations)} tables across {len(schemas)} schemas",
                title="Completed",
                border_style="green",
            )
        )

    except Exception as e:
        console.print(f"[red]Error converting metadata: {e}[/red]")
        raise typer.Exit(code=1)
