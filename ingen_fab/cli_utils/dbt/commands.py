import csv
import json
import re
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel

from .profile_manager import ensure_dbt_profile
from ingen_fab.notebook_utils.notebook_utils import NotebookUtils
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

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


def create_python_classes(
    ctx: typer.Context,
    dbt_project: str,
    skip_profile_confirmation: bool = False,
    execution_mode: str = typer.Option(
        "static",
        "--execution-mode",
        "-m",
        help="Execution mode: 'static' embeds SQL in Python classes, 'dynamic' loads SQL at runtime"
    )
) -> None:
    """Create Python classes from dbt SQL JSON files.

    This scans {fabric_workspace_repo_dir}/{dbt_project}/target/sql for JSON files,
    reads their SQL contents, and creates Python class files under
    {fabric_workspace_repo_dir}/ingen_fab/packages/dbt/runtime/projects/{dbt_project}/.

    Execution modes:
    - static: Embeds SQL directly in Python classes (default)
    - dynamic: Creates lightweight wrappers that load SQL at runtime
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

    # Source dir containing dbt-generated SQL JSON files
    source_dir = workspace_dir / dbt_project / "target" / "sql"

    if not source_dir.exists() or not source_dir.is_dir():
        console.print(f"[red]Source directory not found:[/red] {source_dir}")
        raise typer.Exit(code=1)

    # Target directory for Python classes
    target_dir = workspace_dir / "ingen_fab" / "packages" / "dbt" / "runtime" / "projects" / dbt_project
    target_dir.mkdir(parents=True, exist_ok=True)

    # Load manifest to map model unique_id -> path for folder structure
    manifest_path = workspace_dir / dbt_project / "target" / "manifest.json"
    manifest = {}
    if manifest_path.exists():
        try:
            with manifest_path.open("r", encoding="utf-8") as mf:
                manifest = json.load(mf)
        except Exception as e:
            console.print(
                f"[yellow]Warning: Could not read manifest.json: {e}[/yellow]"
            )

    nodes = manifest.get("nodes", {}) if isinstance(manifest, dict) else {}

    # Collect JSON files
    json_files: list[Path] = sorted(source_dir.glob("*.json"))
    if not json_files:
        console.print("[yellow]No SQL JSON files found to process.[/yellow]")
        return

    # Check for models_python directory
    models_python_dir = workspace_dir / dbt_project / "models_python"
    models_python_count = 0
    if models_python_dir.exists():
        models_python_count = len(list(models_python_dir.glob("*.py")))
    
    # Display execution mode info
    mode_color = "green" if execution_mode == "dynamic" else "cyan"
    panel_text = f"Creating Python classes for [bold]{len(json_files)}[/bold] dbt SQL files"
    panel_text += f"\nExecution mode: [bold {mode_color}]{execution_mode}[/bold {mode_color}]"
    if models_python_count > 0:
        panel_text += f"\nFound [bold]{models_python_count}[/bold] Python files in models_python directory for injection"

    console.print(
        Panel.fit(
            panel_text,
            title=f"DBT Project: {dbt_project}",
            border_style="cyan",
        )
    )

    # Track all created directories for __init__.py files
    created_dirs = set()
    created = 0

    for json_path in json_files:
        try:
            # Read JSON file
            with json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)

            node_id = data.get("node_id", "")
            session_id = data.get("session_id", "")
            sql_statements = data.get("sql", [])

            if not node_id:
                console.print(f"[yellow]Warning: No node_id in {json_path.name}, skipping[/yellow]")
                continue

            # Determine category and destination folder (replicate create_notebooks logic exactly)
            stem = node_id
            dest_root = target_dir
            category = None
            model_name_for_group = None
            
            if stem.startswith("model."):
                # Replicate path structure from manifest under models/
                dest_root = target_dir / "models"
                category = "model"
                node = nodes.get(stem, {})
                if node == {}:
                    console.print(f"[yellow]Warning: No manifest entry for {stem}, manifest {list(nodes.keys())[:5]}[/yellow]")
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
                #print(f"{node}")
                if isinstance(node, dict):
                    deps = node.get("depends_on", {})
                    if isinstance(deps, dict):
                        for dep_uid in deps.get("nodes", []) or []:
                            if isinstance(dep_uid, str) and dep_uid.startswith(
                                "model."
                            ):
                                model_uid = dep_uid
                                break
                #print(f"model_uid={model_uid}")
                #print(f"nodes keys sample: {list(nodes.keys())[:5]}")
                if model_uid and model_uid in nodes:
                    model_node = nodes[model_uid]
                    rel_path = (
                        model_node.get("path") if isinstance(model_node, dict) else None
                    )
                    #print(f"rel_path={rel_path}")
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
                        console.print(f"[yellow]Warning: No path for model {model_uid} in manifest, placing test alongside models[/yellow]")
                        model_name_for_group = model_uid.split(".")[-1]
                        dest_root = dest_root / model_name_for_group
                else:
                    # Fallback: keep alongside models bucket
                    console.print(f"[yellow]Warning: No associated model found for test {stem}, placing alongside models[/yellow]")
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
            created_dirs.add(dest_root)

            # Build normalized base name (strip category prefixes) - replicate create_notebooks logic
            if category == "model":
                # Remove 'model.<project>.' prefix
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

            # Use normalized_base as the raw model name
            raw_model_name = normalized_base
            
            # Encode the model name for use as module file name (replace dots with __DOT__)
            encoded_module_name = raw_model_name.replace(".", "__DOT__")

            # Convert to Python class name using encoding function
            class_name = _encode_class_name(raw_model_name)

            # Look for matching Python file in models_python directory
            injected_python_code = None
            models_python_dir = workspace_dir / dbt_project / "models_python"
            if models_python_dir.exists():
                # Look for file matching the node_id
                potential_python_files = [
                    models_python_dir / f"{node_id}.py",
                    models_python_dir / f"{node_id.replace('.', '_')}.py",
                    models_python_dir / f"{raw_model_name}.py",
                ]
                
                for potential_file in potential_python_files:
                    if potential_file.exists():
                        try:
                            with potential_file.open("r", encoding="utf-8") as f:
                                injected_python_code = f.read()
                            
                            # Replace spark references with self.spark
                            # First check if already has self.spark to avoid double replacement
                            if 'self.spark' not in injected_python_code:
                                # Replace spark. at word boundaries
                                injected_python_code = re.sub(r'\bspark\.', 'self.spark.', injected_python_code)
                                # Replace standalone spark variable (not followed by .)
                                injected_python_code = re.sub(r'\bspark\b(?!\.)', 'self.spark', injected_python_code)
                            
                            console.print(f"[blue]Found Python code to inject from {potential_file.name}[/blue]")
                            break
                        except Exception as e:
                            console.print(f"[yellow]Warning: Could not read {potential_file.name}: {e}[/yellow]")

            # Generate Python class content based on execution mode
            if execution_mode == "dynamic":
                # Calculate relative path from class location to project root
                class_location = dest_root
                project_location = workspace_dir / dbt_project
                # Calculate relative path (going up from class location to workspace, then down to project)
                relative_parts = [".."] * (len(class_location.relative_to(workspace_dir).parts))
                default_project_path = "/".join(relative_parts + [dbt_project])

                class_content = _generate_dynamic_wrapper(
                    class_name=class_name,
                    model_name=raw_model_name,
                    node_id=node_id,
                    default_project_path=default_project_path,
                )
            else:
                class_content = _generate_python_class(
                    class_name=class_name,
                    model_name=raw_model_name,
                    node_id=node_id,
                    session_id=session_id,
                    sql_statements=sql_statements,
                    json_file_name=json_path.name,
                    injected_python_code=injected_python_code,
                )

            # Write Python class file using encoded module name
            class_file = dest_root / f"{encoded_module_name}.py"
            with class_file.open("w", encoding="utf-8") as f:
                f.write(class_content)

            # Calculate relative path for display
            rel_path = class_file.relative_to(target_dir)
            console.print(f"[green]✓[/green] Created {rel_path}")
            created += 1

        except Exception as e:
            console.print(f"[yellow]Warning: Failed to process {json_path.name}: {e}[/yellow]")

    # Create __init__.py files in all created directories
    for dir_path in created_dirs:
        init_file = dir_path / "__init__.py"
        if not init_file.exists():
            # Get all Python files in this directory
            py_files = sorted([f for f in dir_path.glob("*.py") if f.name != "__init__.py"])
            
            # Generate __init__.py content
            init_content = f'"""Generated Python classes for {dir_path.name}"""\n\n'
            
            if py_files:
                # Add imports for all classes in this directory
                for py_file in py_files:
                    module_name = py_file.stem
                    class_name = _encode_class_name(module_name)
                    init_content += f"from .{module_name} import {class_name}\n"
                
                # Add __all__ export
                class_names = [_encode_class_name(f.stem) for f in py_files]
                init_content += f"\n__all__ = {class_names}\n"
            
            with init_file.open("w", encoding="utf-8") as f:
                f.write(init_content)

    # Create main __init__.py for the project root if it doesn't exist
    main_init = target_dir / "__init__.py"
    if not main_init.exists():
        main_init_content = f'"""Generated Python classes for dbt project: {dbt_project}"""\n'
        with main_init.open("w", encoding="utf-8") as f:
            f.write(main_init_content)

    # Generate DAG executor class (only for static mode)
    if execution_mode == "static":
        try:
            console.print("[cyan]Generating DAG executor...[/cyan]")
            dag_executor_content = _generate_dag_executor_class(
                dbt_project=dbt_project,
                manifest=manifest,
                nodes=nodes,
                target_dir=target_dir,
                created_dirs=created_dirs
            )

            # Write DAG executor class
            dag_executor_file = target_dir / "dag_executor.py"
            with dag_executor_file.open("w", encoding="utf-8") as f:
                f.write(dag_executor_content)

            console.print(f"[green]✓[/green] Created DAG executor: dag_executor.py")
        except Exception as e:
            console.print(f"[yellow]Warning: Failed to generate DAG executor: {e}[/yellow]")
    else:
        # For dynamic mode, create a simple launcher that uses DynamicDAGExecutor
        try:
            console.print("[cyan]Creating dynamic DAG launcher...[/cyan]")

            # Calculate relative path from target_dir to project root
            relative_parts = [".."] * (len(target_dir.relative_to(workspace_dir).parts))
            default_project_path = "/".join(relative_parts + [dbt_project])

            dag_launcher_content = f'''"""
Dynamic DAG Launcher for dbt project: {dbt_project}

This launcher uses the DynamicDAGExecutor to execute dbt models at runtime.
"""

from pathlib import Path
from pyspark.sql import SparkSession

from ingen_fab.packages.dbt.runtime.dynamic import DynamicDAGExecutor


def create_dag_executor(spark: SparkSession, project_path: Path = None):
    """
    Create a DynamicDAGExecutor instance.

    Args:
        spark: Active SparkSession
        project_path: Path to dbt project (defaults to {default_project_path})

    Returns:
        DynamicDAGExecutor instance
    """
    if project_path is None:
        project_path = Path("{default_project_path}")

    return DynamicDAGExecutor(spark, project_path)


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("DBT Dynamic Executor").getOrCreate()
    executor = create_dag_executor(spark)

    # Validate DAG
    is_valid, cycles = executor.validate_dag()
    if not is_valid:
        print(f"DAG has cycles: {{cycles}}")
    else:
        print("DAG is valid")

    # Execute all models
    results = executor.execute_dag()
    print(f"Execution completed: {{results}}")
'''

            dag_launcher_file = target_dir / "dynamic_dag_launcher.py"
            with dag_launcher_file.open("w", encoding="utf-8") as f:
                f.write(dag_launcher_content)

            console.print(f"[green]✓[/green] Created dynamic DAG launcher: dynamic_dag_launcher.py")
        except Exception as e:
            console.print(f"[yellow]Warning: Failed to create dynamic DAG launcher: {e}[/yellow]")

    mode_info = f" ({execution_mode} mode)" if execution_mode == "dynamic" else ""
    console.print(
        Panel.fit(
            f"[bold green]✓ Created {created} Python classes{mode_info}[/bold green]\n"
            f"Output directory: {target_dir}",
            title="Completed",
            border_style="green",
        )
    )


def _encode_class_name(raw_name: str) -> str:
    """Encode a raw name to create a valid Python class name.
    
    Replaces dots with __DOT__ to make them valid Python identifiers.
    This encoding is deterministic and reversible.
    
    Args:
        raw_name: The raw name that may contain dots or other invalid characters
        
    Returns:
        A valid Python class name
    """
    # Replace dots with __DOT__
    encoded = raw_name.replace(".", "__DOT__")
    
    # Convert to PascalCase
    class_name = "".join(word.capitalize() for word in encoded.split("_") if word)
    
    return class_name


def _decode_class_name(class_name: str) -> str:
    """Decode a class name back to its original form.
    
    Args:
        class_name: The encoded class name
        
    Returns:
        The original raw name
    """
    # Convert from PascalCase back to underscore format
    import re
    
    # Insert underscores before capital letters (except the first one)
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', class_name)
    # Insert underscores before capital letters that follow lowercase letters or numbers
    underscored = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    
    # Replace __DOT__ back to dots
    decoded = underscored.replace("__dot__", ".")
    
    return decoded


def _generate_method_name(sql: str, statement_number: int, total_statements: int) -> str:
    """Generate a descriptive method name based on SQL content."""
    sql_lower = sql.lower()
    
    if "create or replace temporary view" in sql_lower:
        return "create_temp_view"
    elif "insert into" in sql_lower:
        return "insert_data"
    elif "delete from" in sql_lower:
        return "delete_existing_data"
    elif "create table" in sql_lower and "not exists" in sql_lower:
        return "create_table_if_not_exists"
    elif "drop table" in sql_lower:
        return "drop_table"
    elif "truncate" in sql_lower:
        return "truncate_table"
    elif statement_number == 1 and total_statements > 1:
        return "prepare_data"
    elif statement_number == total_statements:
        return "finalize_data"
    else:
        return f"execute_statement_{statement_number}"


def _generate_dynamic_wrapper(
    class_name: str,
    model_name: str,
    node_id: str,
    default_project_path: str,
) -> str:
    """Generate dynamic wrapper class content using Jinja2 template."""

    # Set up Jinja2 environment
    template_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)))

    try:
        template = env.get_template("dynamic_wrapper.py.j2")
    except TemplateNotFound:
        console.print(f"[red]Template not found: {template_dir}/dynamic_wrapper.py.j2[/red]")
        raise

    # Prepare template variables
    template_vars = {
        "class_name": class_name,
        "model_name": model_name,
        "node_id": node_id,
        "default_project_path": default_project_path,
    }

    # Render template
    return template.render(**template_vars)


def _generate_python_class(
    class_name: str,
    model_name: str,
    node_id: str,
    session_id: str,
    sql_statements: list,
    json_file_name: str,
    injected_python_code: Optional[str] = None,
) -> str:
    """Generate Python class content for a dbt SQL model using Jinja2 template."""
    
    # Set up Jinja2 environment
    template_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)))
    
    try:
        template = env.get_template("python_class.py.j2")
    except TemplateNotFound:
        console.print(f"[red]Template not found: {template_dir}/python_class.py.j2[/red]")
        raise
    
    # Prepare SQL methods data
    sql_methods = []
    for i, sql in enumerate(sql_statements, 1):
        method_name = _generate_method_name(sql, i, len(sql_statements))
        sql_methods.append({
            "method_name": method_name,
            "statement_number": i,
            "sql_content": sql,
        })
    
    # Prepare template variables
    template_vars = {
        "class_name": class_name,
        "model_name": model_name,
        "node_id": node_id,
        "session_id": session_id,
        "json_file_name": json_file_name,
        "sql_methods": sql_methods,
        "total_statements": len(sql_statements),
        "injected_python_code": injected_python_code,
        "has_injected_code": injected_python_code is not None and len(injected_python_code.strip()) > 0,
    }
    
    # Render template
    return template.render(**template_vars)


def _generate_dag_executor_class(
    dbt_project: str,
    manifest: dict,
    nodes: dict,
    target_dir: Path,
    created_dirs: set
) -> str:
    """Generate DAG executor class content using Jinja2 template."""
    
    # Set up Jinja2 environment
    template_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(str(template_dir)))
    
    try:
        template = env.get_template("dag_executor.py.j2")
    except TemplateNotFound:
        console.print(f"[red]Template not found: {template_dir}/dag_executor.py.j2[/red]")
        raise
    
    # Build comprehensive mapping from node_id to class information by analyzing created files
    node_to_class_map = {}
    schemas_info = {}
    
    # Process all Python files in the created directories
    for dir_path in created_dirs:
        rel_path = dir_path.relative_to(target_dir)
        py_files = sorted([f for f in dir_path.glob("*.py") if f.name != "__init__.py"])
        # console.print(f"[purple]Processing directory {dir_path}, found {len(py_files)} Python files[/purple]")
        for py_file in py_files:
            # console.print(f"[red]Analyzing file {py_file}[/red]")
            # Determine what node_id this file represents by reading the file content
            try:
                with py_file.open("r", encoding="utf-8") as f:
                    file_content = f.read()
                    
                # Extract node_id from the file content (it's in the docstring)
                if 'Node ID: ' in file_content:
                    for line in file_content.split('\n'):
                        if 'Node ID: ' in line:
                            node_id = line.split('Node ID: ')[1].strip()
                            if node_id.endswith('"""'):
                                node_id = node_id[:-3].strip()
                            break
                    else:
                        console.print(f"[yellow]Warning: Could not find node_id in {py_file}[/yellow]")
                        continue  # Skip if we can't find node_id
                else:
                    console.print(f"[yellow]Warning: No Node ID found in {py_file}[/yellow]")
                    continue  # Skip if no Node ID found
                # console.print(f"[blue]Processing file {py_file} for node_id {node_id}[/blue]")
                # Extract class name from file content  
                class_name_match = re.search(r'class (\w+):', file_content)
                if not class_name_match:
                    console.print(f"[yellow]Warning: Could not find class definition in {py_file}[/yellow]")
                    continue
                    
                class_name = class_name_match.group(1)
                module_name = py_file.stem
                
                # Build schema path for imports
                if rel_path.parts:
                    schema_path = ".".join(rel_path.parts)
                else:
                    schema_path = ""
                
                # Store mapping with absolute import path
                absolute_import_base = f"ingen_fab.packages.dbt.runtime.projects.{dbt_project}"
                node_to_class_map[node_id] = {
                    "class_name": class_name,
                    "module_name": module_name,
                    "schema_path": schema_path,
                    "file_path": py_file,
                    "import_path": f"{absolute_import_base}.{schema_path}.{module_name}" if schema_path else f"{absolute_import_base}.{module_name}"
                }
                
                # Organize by schema for imports (use first part of rel_path as schema)
                schema_key = rel_path.parts[0] if rel_path.parts else "root"
                if schema_key not in schemas_info:
                    schemas_info[schema_key] = []
                
                schemas_info[schema_key].append({
                    "class_name": class_name,
                    "module_name": module_name,
                    "import_path": f".{schema_path}.{module_name}" if schema_path else f".{module_name}"
                })
                
            except Exception as e:
                console.print(f"[yellow]Warning: Could not parse {py_file}: {e}[/yellow]")
                continue
    
    # Prepare nodes data for template using manifest
    nodes_data = []
    manifest_nodes = manifest.get("nodes", {})
    
    console.print(f"[cyan]DAG Generator: Found {len(node_to_class_map)} mapped classes, {len(manifest_nodes)} manifest nodes[/cyan]")
    # print all manifest nodes for debugging
    #for mn in list(manifest_nodes.keys()):
    #    console.print(f"[dim yellow]  Manifest node: {mn}[/dim yellow]")
    # print all mapped nodes for debugging
    #for mn in list(node_to_class_map.keys()):
    #    console.print(f"[dim green]  Mapped node: {mn}[/dim green]")
    for node_id, node_info in manifest_nodes.items():
        if node_id in node_to_class_map:  # Only include nodes we have classes for
            dependencies = node_info.get("depends_on", {}).get("nodes", [])
            # Filter dependencies to only include ones that exist in our class map
            filtered_deps = [dep for dep in dependencies if dep in node_to_class_map]
            
            nodes_data.append({
                "node_id": node_id,
                "resource_type": node_info.get("resource_type", "unknown"),
                "dependencies": filtered_deps,
                "class_info": node_to_class_map[node_id]
            })
        else:
            # Debug: print unmapped nodes
            console.print(f"[dim yellow]  Unmapped node: {node_id}[/dim yellow]")
    
    console.print(f"[cyan]DAG Generator: Including {len(nodes_data)} nodes in executor[/cyan]")
    
    # Generate executor class name
    executor_class_name = f"{dbt_project.title()}DagExecutor"
    
    # Prepare template variables
    template_vars = {
        "dbt_project": dbt_project,
        "executor_class_name": executor_class_name,
        "total_nodes": len(nodes_data),
        "nodes": nodes_data,
        "schemas": schemas_info,
        "node_to_class_map": node_to_class_map,
    }
    
    # Render template
    return template.render(**template_vars)
