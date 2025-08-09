import re
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel

from ingen_fab.notebook_utils.notebook_utils import NotebookUtils

console = Console()


def create_additional_notebooks(ctx: typer.Context, dbt_project: str) -> None:
    """Create notebooks in fabric_workspace_items/{dbt_project} from dbt target outputs.

    This scans {workspace}/{dbt_project}/target/notebooks_fabric_py for Python notebooks,
    reads their contents, and creates Fabric notebooks under
    {workspace}/fabric_workspace_items/{dbt_project}/ using NotebookUtils.create_notebook_with_platform.
    """

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
