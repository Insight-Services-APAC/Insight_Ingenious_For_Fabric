"""Commands for downloading artifacts from Microsoft Fabric workspaces."""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from ingen_fab.fabric_api.utils import FabricApiUtils

console = Console()
app = typer.Typer(
    help="Download artifacts from Microsoft Fabric workspaces",
    no_args_is_help=True,
)


@app.command("item")
def download_item(
    ctx: typer.Context,
    item_id: str = typer.Argument(..., help="The ID of the item to download"),
    workspace_id: Optional[str] = typer.Option(
        None, "--workspace-id", "-w", help="Workspace ID (uses environment default if not provided)"
    ),
    output_path: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Path to save the downloaded item"
    ),
    format: str = typer.Option(
        "DEFAULT", "--format", "-f", help="Download format (DEFAULT, TMSL, TMDL, etc.)"
    ),
) -> None:
    """Download any Fabric item by its ID."""
    try:
        # Get environment and project path from context
        environment = ctx.obj.get("fabric_environment", "development")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir", "."))
        
        console.print(f"Using Fabric workspace repo directory: {project_path}")
        console.print(f"Using Fabric environment: {environment}")
        
        api = FabricApiUtils(environment, project_path, workspace_id=workspace_id)
        workspace_id = workspace_id or api.workspace_id
        
        with console.status(f"[bold green]Downloading item {item_id}..."):
            result = api.download_item_definition(workspace_id, item_id, output_path, format)
        
        console.print(f"[green]✓[/green] Downloaded {result['item_type']}: {result['display_name']}")
        if "saved_to" in result:
            console.print(f"  Saved to: {result['saved_to']}")
        
        if result.get("files"):
            console.print(f"  Contains {len(result['files'])} file(s)")
            
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to download item: {e}")
        raise typer.Exit(code=1)


@app.command("semantic-model")
def download_semantic_model(
    ctx: typer.Context,
    model_id: str = typer.Argument(..., help="The ID of the semantic model to download"),
    workspace_id: Optional[str] = typer.Option(
        None, "--workspace-id", "-w", help="Workspace ID (uses environment default if not provided)"
    ),
    output_path: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Path to save the downloaded model"
    ),
    format: str = typer.Option(
        "TMDL", "--format", "-f", help="Download format (TMSL or TMDL)"
    ),
) -> None:
    """Download a semantic model (dataset) from Fabric."""
    try:
        # Get environment and project path from context
        environment = ctx.obj.get("fabric_environment", "development")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir", "."))
        
        console.print(f"Using Fabric workspace repo directory: {project_path}")
        console.print(f"Using Fabric environment: {environment}")
        
        api = FabricApiUtils(environment, project_path, workspace_id=workspace_id)
        workspace_id = workspace_id or api.workspace_id
        
        with console.status(f"[bold green]Downloading semantic model..."):
            result = api.download_semantic_model(workspace_id, model_id, output_path, format)
        
        console.print(f"[green]✓[/green] Downloaded semantic model")
        if "output_directory" in result:
            console.print(f"  Saved to: {result['output_directory']}")
        if "total_files" in result:
            console.print(f"  Total files: {result['total_files']}")
            
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to download semantic model: {e}")
        raise typer.Exit(code=1)


@app.command("report")
def download_report(
    ctx: typer.Context,
    report_id: str = typer.Argument(..., help="The ID of the report to download"),
    workspace_id: Optional[str] = typer.Option(
        None, "--workspace-id", "-w", help="Workspace ID (uses environment default if not provided)"
    ),
    output_path: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Path to save the .pbix file"
    ),
) -> None:
    """Download a Power BI report (.pbix file) from Fabric."""
    try:
        # Get environment and project path from context
        environment = ctx.obj.get("fabric_environment", "development")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir", "."))
        
        console.print(f"Using Fabric workspace repo directory: {project_path}")
        console.print(f"Using Fabric environment: {environment}")
        
        api = FabricApiUtils(environment, project_path, workspace_id=workspace_id)
        workspace_id = workspace_id or api.workspace_id
        
        with console.status(f"[bold green]Downloading report..."):
            api.download_report(workspace_id, report_id, output_path)
        
        console.print(f"[green]✓[/green] Downloaded report")
        if output_path:
            console.print(f"  Saved to: {output_path}")
            
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to download report: {e}")
        raise typer.Exit(code=1)


@app.command("notebook")
def download_notebook(
    ctx: typer.Context,
    notebook_id: str = typer.Argument(..., help="The ID of the notebook to download"),
    workspace_id: Optional[str] = typer.Option(
        None, "--workspace-id", "-w", help="Workspace ID (uses environment default if not provided)"
    ),
    output_path: Optional[Path] = typer.Option(
        None, "--output", "-o", help="Path to save the notebook"
    ),
    format: str = typer.Option(
        "ipynb", "--format", "-f", help="Download format (ipynb or py)"
    ),
) -> None:
    """Download a notebook from Fabric."""
    try:
        # Get environment and project path from context
        environment = ctx.obj.get("fabric_environment", "development")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir", "."))
        
        console.print(f"Using Fabric workspace repo directory: {project_path}")
        console.print(f"Using Fabric environment: {environment}")
        
        api = FabricApiUtils(environment, project_path, workspace_id=workspace_id)
        workspace_id = workspace_id or api.workspace_id
        
        with console.status(f"[bold green]Downloading notebook..."):
            result = api.download_notebook(workspace_id, notebook_id, output_path, format)
        
        console.print(f"[green]✓[/green] Downloaded notebook: {result['display_name']}")
        if "saved_to" in result:
            console.print(f"  Saved to: {result['saved_to']}")
            
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to download notebook: {e}")
        raise typer.Exit(code=1)


@app.command("all")
def download_all(
    ctx: typer.Context,
    workspace_id: Optional[str] = typer.Option(
        None, "--workspace-id", "-w", help="Workspace ID (uses environment default if not provided)"
    ),
    output_dir: Path = typer.Option(
        Path("fabric_downloads"), "--output-dir", "-o", help="Directory to save all downloads"
    ),
    item_types: Optional[str] = typer.Option(
        None, "--types", "-t", help="Comma-separated list of item types to download"
    ),
) -> None:
    """Download all items from a workspace."""
    try:
        # Get environment and project path from context
        environment = ctx.obj.get("fabric_environment", "development")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir", "."))
        
        console.print(f"Using Fabric workspace repo directory: {project_path}")
        console.print(f"Using Fabric environment: {environment}")
        
        api = FabricApiUtils(environment, project_path, workspace_id=workspace_id)
        workspace_id = workspace_id or api.workspace_id
        
        # Parse item types if provided
        types_list = None
        if item_types:
            types_list = [t.strip() for t in item_types.split(",")]
        
        with console.status(f"[bold green]Downloading items from workspace..."):
            results = api.download_all_items(workspace_id, output_dir, types_list)
        
        # Display results in a table
        table = Table(title="Download Results")
        table.add_column("Item Type", style="cyan")
        table.add_column("Total", style="magenta")
        table.add_column("Success", style="green")
        table.add_column("Failed", style="red")
        
        total_items = 0
        total_success = 0
        total_failed = 0
        
        for item_type, items in results.items():
            success_count = sum(1 for item in items if item.get("status") == "success")
            failed_count = sum(1 for item in items if item.get("status") == "failed")
            table.add_row(
                item_type,
                str(len(items)),
                str(success_count),
                str(failed_count),
            )
            total_items += len(items)
            total_success += success_count
            total_failed += failed_count
        
        console.print(table)
        console.print(f"\n[bold]Total: {total_items} items ({total_success} success, {total_failed} failed)")
        console.print(f"Downloaded to: {output_dir.absolute()}")
        
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to download items: {e}")
        raise typer.Exit(code=1)


@app.command("list")
def list_items(
    ctx: typer.Context,
    workspace_id: Optional[str] = typer.Option(
        None, "--workspace-id", "-w", help="Workspace ID (uses environment default if not provided)"
    ),
    item_type: Optional[str] = typer.Option(
        None, "--type", "-t", help="Filter by item type"
    ),
) -> None:
    """List all items in a workspace."""
    try:
        # Get environment and project path from context
        environment = ctx.obj.get("fabric_environment", "development")
        project_path = Path(ctx.obj.get("fabric_workspace_repo_dir", "."))
        
        console.print(f"Using Fabric workspace repo directory: {project_path}")
        console.print(f"Using Fabric environment: {environment}")
        
        api = FabricApiUtils(environment, project_path, workspace_id=workspace_id)
        workspace_id = workspace_id or api.workspace_id
        
        with console.status(f"[bold green]Listing workspace items..."):
            items = api.list_workspace_items(workspace_id)
        
        # Filter by type if specified
        if item_type:
            items = [item for item in items if item.get("type") == item_type]
        
        # Group items by type
        items_by_type = {}
        for item in items:
            item_type = item.get("type", "Unknown")
            if item_type not in items_by_type:
                items_by_type[item_type] = []
            items_by_type[item_type].append(item)
        
        # Display results
        for item_type, type_items in sorted(items_by_type.items()):
            console.print(f"\n[bold cyan]{item_type}[/bold cyan] ({len(type_items)} items):")
            for item in type_items[:10]:  # Show first 10 items
                console.print(f"  • {item.get('displayName')} (ID: {item.get('id')})")
            if len(type_items) > 10:
                console.print(f"  ... and {len(type_items) - 10} more")
        
        console.print(f"\n[bold]Total: {len(items)} items")
        
    except Exception as e:
        console.print(f"[red]✗[/red] Failed to list items: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()