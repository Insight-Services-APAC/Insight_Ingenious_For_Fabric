"""CLI commands for the sample data package."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.table import Table

from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.packages.sample_data.compilation.sample_data_compiler import (
    SampleDataCompiler,
)
from ingen_fab.packages.sample_data.runtime.dataset_registry import DatasetRegistry
from ingen_fab.packages.sample_data.runtime.relational_datasets import RelationalDatasetRegistry

console = Console()
console_styles = ConsoleStyles()
logger = logging.getLogger(__name__)

# Create the sample_data sub-app
app = typer.Typer(help="Commands for working with sample datasets")


@app.command("list")
def list_datasets(
    source: Optional[str] = typer.Option(
        None, "--source", "-s", help="Filter datasets by source"
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed information"
    ),
):
    """List all available sample datasets."""
    try:
        registry = DatasetRegistry()
        
        if source:
            datasets = registry.get_datasets_by_source(source)
            if not datasets:
                console.print(f"[red]No datasets found for source: {source}[/red]")
                raise typer.Exit(1)
            console.print(f"\n[bold cyan]Datasets from {source}:[/bold cyan]")
        else:
            datasets = registry.list_datasets()
            console.print(f"\n[bold cyan]Available Sample Datasets:[/bold cyan]")
        
        if verbose:
            # Create a detailed table
            table = Table(title="Sample Datasets Registry")
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Description", style="white")
            table.add_column("Source", style="green")
            table.add_column("Type", style="yellow")
            table.add_column("Size", style="magenta")
            
            for dataset_name in sorted(datasets):
                info = registry.get_dataset(dataset_name)
                table.add_row(
                    dataset_name,
                    info.description,
                    info.source,
                    info.file_type.upper(),
                    info.size_estimate,
                )
            
            console.print(table)
        else:
            # Simple list format
            for dataset_name in sorted(datasets):
                info = registry.get_dataset(dataset_name)
                console.print(f"  • [cyan]{dataset_name}[/cyan]: {info.description}")
        
        console.print(f"\n[dim]Total: {len(datasets)} datasets[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error listing datasets: {e}[/red]")
        raise typer.Exit(1)


@app.command("info")
def dataset_info(
    name: str = typer.Argument(..., help="Name of the dataset to show info for"),
):
    """Show detailed information about a specific dataset."""
    try:
        registry = DatasetRegistry()
        info = registry.get_dataset(name)
        
        console.print(f"\n[bold cyan]Dataset: {name}[/bold cyan]")
        console.print(f"[white]Description:[/white] {info.description}")
        console.print(f"[white]Source:[/white] {info.source}")
        console.print(f"[white]File Type:[/white] {info.file_type}")
        console.print(f"[white]Size Estimate:[/white] {info.size_estimate}")
        console.print(f"[white]URL:[/white] {info.url}")
        
        if info.columns:
            console.print(f"[white]Columns:[/white] {', '.join(info.columns)}")
        
        if info.read_options:
            console.print(f"[white]Read Options:[/white] {info.read_options}")
        
    except KeyError:
        console.print(f"[red]Dataset '{name}' not found[/red]")
        console.print("\nUse 'ingen_fab sample-data list' to see available datasets")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error getting dataset info: {e}[/red]")
        raise typer.Exit(1)


@app.command("compile")
def compile_notebooks(
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Output directory for notebooks (defaults to workspace repo)",
    ),
    datasets: Optional[List[str]] = typer.Option(
        None,
        "--dataset",
        "-d",
        help="Specific datasets to include (can be used multiple times)",
    ),
    auto_load: bool = typer.Option(
        True,
        "--auto-load/--no-auto-load",
        help="Auto-load datasets when notebook runs",
    ),
    include_explorer: bool = typer.Option(
        True,
        "--explorer/--no-explorer",
        help="Include data explorer notebook",
    ),
    include_registry: bool = typer.Option(
        True,
        "--registry/--no-registry",
        help="Include dataset registry notebook",
    ),
    workspace_id: Optional[str] = typer.Option(
        None, "--workspace-id", help="Target workspace ID"
    ),
    lakehouse_id: Optional[str] = typer.Option(
        None, "--lakehouse-id", help="Target lakehouse ID"
    ),
):
    """Compile sample data notebooks for deployment to Fabric."""
    try:
        # Get workspace repo directory from environment or use current
        import os
        
        if output_dir is None:
            workspace_repo = os.environ.get("FABRIC_WORKSPACE_REPO_DIR")
            if workspace_repo:
                output_dir = Path(workspace_repo) / "fabric_workspace_items" / "notebooks"
            else:
                output_dir = Path.cwd() / "notebooks"
        
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        console.print(f"\n[bold cyan]Compiling Sample Data Notebooks[/bold cyan]")
        console.print(f"Output directory: {output_dir}")
        
        # Initialize compiler
        compiler = SampleDataCompiler(
            output_dir=output_dir,
            fabric_workspace_repo_dir=os.environ.get("FABRIC_WORKSPACE_REPO_DIR"),
        )
        
        notebooks_created = []
        
        # Compile main loader notebook
        console.print("\n[yellow]Creating sample data loader notebook...[/yellow]")
        loader_path = compiler.compile_sample_data_notebook(
            notebook_name="load_sample_datasets",
            datasets=datasets,
            auto_load=auto_load,
            include_custom_loader=True,
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
        )
        notebooks_created.append(loader_path)
        console.print(f"  ✓ Created: {loader_path.name}")
        
        # Compile explorer notebook if requested
        if include_explorer:
            console.print("\n[yellow]Creating data explorer notebook...[/yellow]")
            explorer_path = compiler.compile_data_explorer_notebook(
                notebook_name="explore_sample_data",
                include_profiling=True,
                include_visualization=True,
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
            )
            notebooks_created.append(explorer_path)
            console.print(f"  ✓ Created: {explorer_path.name}")
        
        # Compile registry notebook if requested
        if include_registry:
            console.print("\n[yellow]Creating dataset registry notebook...[/yellow]")
            registry_path = compiler.compile_dataset_registry_notebook(
                notebook_name="dataset_registry",
            )
            notebooks_created.append(registry_path)
            console.print(f"  ✓ Created: {registry_path.name}")
        
        # Summary
        console.print(f"\n[bold green]✓ Successfully created {len(notebooks_created)} notebooks[/bold green]")
        console.print("\n[dim]Notebooks created:[/dim]")
        for path in notebooks_created:
            console.print(f"  • {path}")
        
        console.print("\n[dim]Next steps:[/dim]")
        console.print("  1. Deploy notebooks to your Fabric workspace")
        console.print("  2. Run 'load_sample_datasets' to load data")
        console.print("  3. Use 'explore_sample_data' to analyze the data")
        
    except Exception as e:
        console.print(f"[red]Error compiling notebooks: {e}[/red]")
        logger.exception("Failed to compile sample data notebooks")
        raise typer.Exit(1)


@app.command("sources")
def list_sources():
    """List all available data sources."""
    try:
        registry = DatasetRegistry()
        all_datasets = registry.list_datasets()
        
        # Group by source
        sources = {}
        for dataset_name in all_datasets:
            info = registry.get_dataset(dataset_name)
            if info.source not in sources:
                sources[info.source] = 0
            sources[info.source] += 1
        
        console.print("\n[bold cyan]Available Data Sources:[/bold cyan]")
        
        # Sort by count
        for source, count in sorted(sources.items(), key=lambda x: x[1], reverse=True):
            console.print(f"  • [green]{source}[/green]: {count} datasets")
        
        console.print(f"\n[dim]Total: {len(sources)} sources[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error listing sources: {e}[/red]")
        raise typer.Exit(1)


@app.command("list-relational")
def list_relational_datasets(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show detailed information"
    ),
):
    """List all available relational datasets."""
    try:
        registry = RelationalDatasetRegistry()
        datasets = registry.list_datasets()
        
        console.print(f"\n[bold cyan]Available Relational Datasets:[/bold cyan]")
        
        if verbose:
            # Create a detailed table
            table = Table(title="Relational Datasets Registry")
            table.add_column("Name", style="cyan", no_wrap=True)
            table.add_column("Description", style="white")
            table.add_column("Source", style="green")
            table.add_column("Tables", style="yellow")
            table.add_column("Size", style="magenta")
            
            for dataset_name in sorted(datasets):
                info = registry.get_dataset(dataset_name)
                table.add_row(
                    dataset_name,
                    info.description,
                    info.source,
                    str(len(info.tables)),
                    info.size_estimate or "Unknown",
                )
            
            console.print(table)
        else:
            # Simple list format
            for dataset_name in sorted(datasets):
                info = registry.get_dataset(dataset_name)
                console.print(f"  • [cyan]{dataset_name}[/cyan]: {info.description} ({len(info.tables)} tables)")
        
        console.print(f"\n[dim]Total: {len(datasets)} relational datasets[/dim]")
        
    except Exception as e:
        console.print(f"[red]Error listing relational datasets: {e}[/red]")
        raise typer.Exit(1)


@app.command("schema")
def show_schema(
    name: str = typer.Argument(..., help="Name of the relational dataset"),
):
    """Show schema information for a relational dataset."""
    try:
        registry = RelationalDatasetRegistry()
        dataset_info = registry.get_dataset(name)
        
        console.print(f"\n[bold cyan]Schema for {name}[/bold cyan]")
        console.print(f"[white]Description:[/white] {dataset_info.description}")
        console.print(f"[white]Source:[/white] {dataset_info.source}")
        console.print(f"[white]Tables:[/white] {len(dataset_info.tables)}")
        console.print(f"[white]Load Order:[/white] {' → '.join(dataset_info.load_order)}")
        
        # Create schema table
        table = Table(title=f"{name} Schema")
        table.add_column("Table", style="cyan")
        table.add_column("Description", style="white")
        table.add_column("Primary Key", style="yellow")
        table.add_column("Foreign Keys", style="green")
        
        for table_name in dataset_info.load_order:
            if table_name in dataset_info.tables:
                table_info = dataset_info.tables[table_name]
                fk_str = ""
                if table_info.foreign_keys:
                    fk_str = ", ".join([f"{col}→{ref}" for col, ref in table_info.foreign_keys.items()])
                
                table.add_row(
                    table_name,
                    table_info.description,
                    table_info.primary_key or "",
                    fk_str
                )
        
        console.print("\n")
        console.print(table)
        
    except KeyError:
        console.print(f"[red]Relational dataset '{name}' not found[/red]")
        console.print("\nUse 'ingen_fab sample-data list-relational' to see available datasets")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error getting schema: {e}[/red]")
        raise typer.Exit(1)