from pathlib import Path

from rich.console import Console
from rich.table import Table

from ingen_fab.fabric_api.utils import FabricApiUtils


def delete_workspace_items(environment: str, project_path: Path, force: bool):
    console = Console()
    try:
        fabric_utils = FabricApiUtils(environment=environment, project_path=project_path)
        if not force:
            console.print(
                f"\n[yellow]Warning: This will delete all items in workspace {fabric_utils.workspace_id} except lakehouses and warehouses.[/yellow]"
            )
            console.print(f"Environment: [cyan]{environment}[/cyan]\n")
            import typer

            confirm = typer.confirm("Are you sure you want to continue?")
            if not confirm:
                console.print("[red]Operation cancelled.[/red]")
                raise typer.Abort()
        console.print("\n[cyan]Deleting workspace items...[/cyan]")
        result = fabric_utils.delete_all_items_except_lakehouses_and_warehouses()
        deleted_counts = result["deleted_counts"]
        errors = result["errors"]
        total_deleted = result["total_deleted"]
        if total_deleted > 0:
            console.print(f"\n[green]Successfully deleted {total_deleted} items:[/green]")
            table = Table(title="Deleted Items by Type")
            table.add_column("Item Type", style="cyan")
            table.add_column("Count", style="green")
            for item_type, count in sorted(deleted_counts.items()):
                table.add_row(item_type, str(count))
            console.print(table)
        else:
            console.print("\n[yellow]No items were deleted.[/yellow]")
        if errors:
            console.print(f"\n[red]Encountered {len(errors)} errors:[/red]")
            error_table = Table(title="Errors")
            error_table.add_column("Item Name", style="cyan")
            error_table.add_column("Type", style="yellow")
            error_table.add_column("Error", style="red")
            for error in errors[:10]:
                error_table.add_row(
                    error["item_name"],
                    error["item_type"],
                    f"Status {error['status_code']}: {error['error'][:50]}...",
                )
            console.print(error_table)
            if len(errors) > 10:
                console.print(f"[dim]... and {len(errors) - 10} more errors[/dim]")
        console.print("\n[green]Operation completed.[/green]")
    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        raise SystemExit(1)
