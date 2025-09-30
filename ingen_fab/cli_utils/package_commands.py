"""
Package Commands CLI Utilities

This module contains the command implementations for various package subapps
(ingest, synapse, extract), consolidating common patterns and reducing code duplication.
"""

import json
from typing import Any, Dict, Optional

import typer
from rich.console import Console

from ingen_fab.cli_utils.console_styles import ConsoleStyles

console = Console()
console_styles = ConsoleStyles()


def parse_template_variables(template_vars: Optional[str]) -> Dict[str, Any]:
    """Parse JSON template variables with error handling."""
    vars_dict = {}
    if template_vars:
        try:
            vars_dict = json.loads(template_vars)
        except json.JSONDecodeError as e:
            console.print(f"[red]Error parsing template variables: {e}[/red]")
            raise typer.Exit(code=1)
    return vars_dict


def validate_target_datastore(target_datastore: str, valid_options: list) -> None:
    """Validate target datastore parameter."""
    if target_datastore not in valid_options:
        console.print(
            f"[red]Error: Invalid target datastore '{target_datastore}'. Must be one of: {', '.join(valid_options)}[/red]"
        )
        raise typer.Exit(code=1)


def handle_package_compilation_result(results: Dict[str, Any], package_name: str) -> None:
    """Handle compilation results with consistent output format."""
    if results["success"]:
        console.print(f"[green]✓ {package_name} package compiled successfully![/green]")
    else:
        console.print(f"[red]✗ Compilation failed: {results['errors']}[/red]")
        raise typer.Exit(code=1)


def handle_package_compilation_error(e: Exception, package_name: str) -> None:
    """Handle compilation errors with consistent output format."""
    console.print(f"[red]Error compiling {package_name} package: {e}[/red]")
    raise typer.Exit(code=1)


# =============================================================================
# INGEST APP COMMANDS
# =============================================================================


def ingest_compile(
    ctx: typer.Context,
    template_vars: Optional[str] = None,
    include_samples: bool = False,
    target_datastore: str = "lakehouse",
    add_debug_cells: bool = False,
):
    """Compile flat file ingestion package templates and DDL scripts."""
    from ingen_fab.packages.flat_file_ingestion.flat_file_ingestion import (
        compile_flat_file_ingestion_package,
    )

    # Parse template variables
    vars_dict = parse_template_variables(template_vars)

    # Get fabric workspace repo directory from context
    fabric_workspace_repo_dir = str(ctx.obj["fabric_workspace_repo_dir"])

    # Validate target datastore parameter
    valid_datastores = ["lakehouse", "warehouse", "both"]
    validate_target_datastore(target_datastore, valid_datastores)

    try:
        results = compile_flat_file_ingestion_package(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            template_vars=vars_dict,
            include_samples=include_samples,
            target_datastore=target_datastore,
            add_debug_cells=add_debug_cells,
        )

        handle_package_compilation_result(results, "Flat file ingestion")

    except Exception as e:
        handle_package_compilation_error(e, "flat file ingestion")


def ingest_run(
    ctx: typer.Context,
    config_id: str = "",
    execution_group: int = 1,
    environment: str = "development",
):
    """Run flat file ingestion for specified configuration or execution group."""
    console.print("[blue]Running flat file ingestion...[/blue]")
    console.print(f"Config ID: {config_id}")
    console.print(f"Execution Group: {execution_group}")
    console.print(f"Environment: {environment}")
    console.print(f"Fabric Workspace Repo Dir: {ctx.obj['fabric_workspace_repo_dir']}")

    console.print(
        "[yellow]Note: This command would typically execute the compiled notebook with the specified parameters.[/yellow]"
    )
    console.print(
        "[yellow]In a production environment, this would submit the notebook to Fabric for execution.[/yellow]"
    )


# =============================================================================
# SYNAPSE APP COMMANDS
# =============================================================================


def synapse_compile(
    ctx: typer.Context,
    template_vars: Optional[str] = None,
    include_samples: bool = False,
):
    """Compile synapse sync package templates and DDL scripts."""
    from ingen_fab.packages.synapse_sync.synapse_sync import (
        compile_synapse_sync_package,
    )

    # Parse template variables
    vars_dict = parse_template_variables(template_vars)

    # Get fabric workspace repo directory from context
    fabric_workspace_repo_dir = str(ctx.obj["fabric_workspace_repo_dir"])

    try:
        results = compile_synapse_sync_package(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            template_vars=vars_dict,
            include_samples=include_samples,
        )

        handle_package_compilation_result(results, "Synapse sync")

    except Exception as e:
        handle_package_compilation_error(e, "synapse sync")


def synapse_run(
    ctx: typer.Context,
    master_execution_id: str = "",
    work_items_json: str = "",
    max_concurrency: int = 10,
    include_snapshots: bool = True,
    environment: str = "development",
):
    """Run synapse sync extraction for specified configuration."""
    console.print("[blue]Running synapse sync extraction...[/blue]")
    console.print(f"Master Execution ID: {master_execution_id}")
    console.print(f"Work Items JSON: {work_items_json}")
    console.print(f"Max Concurrency: {max_concurrency}")
    console.print(f"Include Snapshots: {include_snapshots}")
    console.print(f"Environment: {environment}")
    console.print(f"Fabric Workspace Repo Dir: {ctx.obj['fabric_workspace_repo_dir']}")

    console.print(
        "[yellow]Note: This command would typically execute the compiled notebook with the specified parameters.[/yellow]"
    )
    console.print(
        "[yellow]In a production environment, this would submit the notebook to Fabric for execution.[/yellow]"
    )


# =============================================================================
# EXTRACT APP COMMANDS
# =============================================================================


def extract_compile(
    ctx: typer.Context,
    template_vars: Optional[str] = None,
    include_samples: bool = False,
    target_datastore: str = "warehouse",
):
    """Compile extract generation package templates and DDL scripts."""
    from ingen_fab.packages.extract_generation.extract_generation import (
        compile_extract_generation_package,
    )

    # Parse template variables
    vars_dict = parse_template_variables(template_vars)

    # Get fabric workspace repo directory from context
    fabric_workspace_repo_dir = str(ctx.obj["fabric_workspace_repo_dir"])

    try:
        results = compile_extract_generation_package(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            template_vars=vars_dict,
            include_samples=include_samples,
            target_datastore=target_datastore,
        )

        handle_package_compilation_result(results, "Extract generation")

    except Exception as e:
        handle_package_compilation_error(e, "extract generation")


def extract_run(
    ctx: typer.Context,
    extract_name: str = "",
    execution_group: str = "",
    environment: str = "development",
    run_type: str = "FULL",
):
    """Run extract generation for specified configuration or execution group."""
    console.print("[blue]Running extract generation...[/blue]")
    console.print(f"Extract Name: {extract_name}")
    console.print(f"Execution Group: {execution_group}")
    console.print(f"Environment: {environment}")
    console.print(f"Run Type: {run_type}")
    console.print(f"Fabric Workspace Repo Dir: {ctx.obj['fabric_workspace_repo_dir']}")

    console.print(
        "[yellow]Note: This command would typically execute the compiled notebook with the specified parameters.[/yellow]"
    )
    console.print(
        "[yellow]In a production environment, this would submit the notebook to Fabric for execution.[/yellow]"
    )
