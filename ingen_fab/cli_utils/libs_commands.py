"""
Libraries Commands CLI Utilities

This module contains the command implementations for library compilation commands,
handling the complex logic for compiling Python libraries with variable injection.
"""

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from ingen_fab.cli_utils.console_styles import ConsoleStyles

console = Console()
console_styles = ConsoleStyles()


def compile_specific_file(
    ctx: typer.Context, 
    target_file: str, 
    project_path: Path, 
    environment: str
) -> None:
    """Compile a specific file using the variable library."""
    from ingen_fab.config_utils.variable_lib_factory import (
        process_file_content_from_cli,
    )
    
    target_path = project_path / target_file
    console.print(f"Target file: {target_path}")
    
    if not target_path.exists():
        console.print(f"[red]Error: Target file not found: {target_path}[/red]")
        raise typer.Exit(code=1)
    
    try:
        # Use the modern factory method for processing single files
        was_updated = process_file_content_from_cli(
            ctx, 
            target_path, 
            replace_placeholders=True,  # For compilation, we want to replace placeholders
            inject_code=True
        )
        
        if was_updated:
            console.print(f"[green]✓ Successfully compiled: {target_file} with values from {environment} environment[/green]")
        else:
            console.print(f"[yellow]No changes needed for {target_file}[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error compiling {target_file}: {e}[/red]")
        raise typer.Exit(code=1)


def compile_default_config_utils(ctx: typer.Context, environment: str) -> None:
    """Compile the default config_utils.py file."""
    from ingen_fab.config_utils.variable_lib_factory import (
        process_file_content_from_cli,
    )
    
    # Compile config_utils.py as default - use absolute path since we're in the ingen_fab directory
    current_dir = Path.cwd()
    config_utils_path = current_dir / "ingen_fab" / "python_libs" / "common" / "config_utils.py"
    console.print(f"Target file: {config_utils_path}")
    
    if not config_utils_path.exists():
        console.print(f"[red]Error: config_utils.py not found at: {config_utils_path}[/red]")
        raise typer.Exit(code=1)
    
    try:
        # Use the modern factory method for processing single files
        was_updated = process_file_content_from_cli(
            ctx, 
            config_utils_path, 
            replace_placeholders=True,  # For compilation, we want to replace placeholders
            inject_code=True
        )
        
        if was_updated:
            console.print(f"[green]✓ Successfully compiled config_utils.py with values from {environment} environment[/green]")
        else:
            console.print("[yellow]No changes needed for config_utils.py[/yellow]")
            
    except Exception as e:
        console.print(f"[red]Error compiling config_utils.py: {e}[/red]")
        raise typer.Exit(code=1)


# =============================================================================
# LIBS APP COMMANDS
# =============================================================================

def libs_compile(
    ctx: typer.Context,
    target_file: Optional[str] = None,
):
    """Compile Python libraries by injecting variables from the variable library."""
    project_path = Path(ctx.obj["fabric_workspace_repo_dir"])
    environment = str(ctx.obj["fabric_environment"])
    
    console.print("[blue]Compiling Python libraries...[/blue]")
    console.print(f"Project path: {project_path}")
    console.print(f"Environment: {environment}")
    
    if target_file:
        compile_specific_file(ctx, target_file, project_path, environment)
    else:
        compile_default_config_utils(ctx, environment)