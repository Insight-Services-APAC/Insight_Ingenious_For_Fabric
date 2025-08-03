"""
Synthetic Data Generation CLI Commands

This module contains the command implementations for synthetic data generation,
following the established pattern of separating CLI logic from the main cli.py file.
"""

import json
from typing import Optional

import typer
from rich.console import Console

from ingen_fab.cli_utils.console_styles import ConsoleStyles

console = Console()
console_styles = ConsoleStyles()


def unified_generate(
    ctx: typer.Context,
    config: str,
    mode: str = "single",
    parameters: Optional[str] = None,
    output_path: Optional[str] = None,
    dry_run: bool = False,
    target_environment: str = "lakehouse",
    no_execute: bool = False,
):
    """
    Generate synthetic data notebooks and optionally execute them.
    
    This command creates ready-to-run notebooks with specific parameters injected,
    and by default executes them immediately to generate the synthetic data.
    """
    from ingen_fab.packages.synthetic_data_generation.unified_commands import (
        GenerationMode,
        UnifiedSyntheticDataGenerator,
    )
    
    # Parse parameters if provided
    params_dict = {}
    if parameters:
        try:
            params_dict = json.loads(parameters)
        except json.JSONDecodeError as e:
            console.print(f"[red]Error parsing parameters JSON: {e}[/red]")
            raise typer.Exit(code=1)
    
    # Validate mode
    try:
        mode_enum = GenerationMode(mode)
    except ValueError:
        console.print(f"[red]Error: Invalid mode '{mode}'. Must be one of: single, incremental, series[/red]")
        raise typer.Exit(code=1)
    
    # Initialize generator
    generator = UnifiedSyntheticDataGenerator(
        fabric_workspace_repo_dir=ctx.obj["fabric_workspace_repo_dir"],
        fabric_environment=ctx.obj["fabric_environment"]
    )
    
    # Generate (and optionally execute)
    result = generator.generate(
        config=config,
        mode=mode_enum,
        parameters=params_dict,
        output_path=output_path,
        dry_run=dry_run,
        target_environment=target_environment,
        execute=not no_execute
    )
    
    if result["success"]:
        console.print("[green]✅ Generation successful![/green]")
        console.print(f"Mode: {result['mode']}")
        console.print(f"Config: {result['config']}")
        if "notebook_path" in result:
            console.print(f"Notebook: {result['notebook_path']}")
        
        # Handle execution results
        if not no_execute and not dry_run:
            if result.get("execution_success"):
                console.print("[green]🚀 Notebook executed successfully![/green]")
                if result.get("execution_output"):
                    console.print("[blue]Execution Output:[/blue]")
                    console.print(result["execution_output"])
            else:
                console.print("[red]❌ Notebook execution failed![/red]")
                for error in result.get("execution_errors", []):
                    console.print(f"[red]  • {error}[/red]")
                if result.get("execution_stderr"):
                    console.print("[red]Error output:[/red]")
                    console.print(result["execution_stderr"])
                raise typer.Exit(code=1)
        
        if dry_run:
            console.print("[yellow]This was a dry run - no files were generated[/yellow]")
    else:
        console.print("[red]❌ Generation failed![/red]")
        for error in result.get("errors", []):
            console.print(f"[red]  • {error}[/red]")
        raise typer.Exit(code=1)


def unified_list(
    list_type: str = "all",
    output_format: str = "table",
):
    """
    List available synthetic data datasets and templates.
    
    Shows predefined dataset configurations and generic templates that can be used
    with the generate and compile commands.
    """
    from ingen_fab.packages.synthetic_data_generation.unified_commands import (
        ListType,
        UnifiedSyntheticDataGenerator,
        format_list_output,
    )
    
    # Validate type
    try:
        list_type_enum = ListType(list_type)
    except ValueError:
        console.print(f"[red]Error: Invalid type '{list_type}'. Must be one of: datasets, templates, all[/red]")
        raise typer.Exit(code=1)
    
    # Initialize generator (no context needed for listing)
    generator = UnifiedSyntheticDataGenerator()
    
    # Get items
    items = generator.list_items(list_type_enum)
    
    # Format output
    format_list_output(items, output_format)


def unified_compile(
    ctx: typer.Context,
    template: Optional[str] = None,
    runtime_config: Optional[str] = None,
    output_format: str = "all",
    target_environment: str = "lakehouse",
):
    """
    Compile template notebooks with placeholders for later parameterization.
    
    This command creates template notebooks that can be parameterized and executed later,
    as opposed to the 'generate' command which creates ready-to-run notebooks.
    """
    from ingen_fab.packages.synthetic_data_generation.unified_commands import (
        UnifiedSyntheticDataGenerator,
    )
    
    # Parse runtime config if provided
    config_dict = {}
    if runtime_config:
        try:
            config_dict = json.loads(runtime_config)
        except json.JSONDecodeError as e:
            console.print(f"[red]Error parsing runtime config JSON: {e}[/red]")
            raise typer.Exit(code=1)
    
    # Initialize generator
    generator = UnifiedSyntheticDataGenerator(
        fabric_workspace_repo_dir=ctx.obj["fabric_workspace_repo_dir"],
        fabric_environment=ctx.obj["fabric_environment"]
    )
    
    # If no template specified, compile all available templates
    if template is None:
        console.print("[blue]No template specified - compiling all available templates...[/blue]")
        _compile_all_templates(generator, config_dict, output_format, target_environment)
    else:
        # Compile specific template
        _compile_single_template(generator, template, config_dict, output_format, target_environment)


def _compile_single_template(
    generator,
    template: str,
    config_dict: dict,
    output_format: str,
    target_environment: str
):
    """Compile a single template."""
    result = generator.compile(
        template=template,
        runtime_config=config_dict,
        output_format=output_format,
        target_environment=target_environment
    )
    
    if result["success"]:
        console.print(f"[green]✅ Compilation successful for {template}![/green]")
        for item_type, item_path in result["compiled_items"].items():
            console.print(f"[dim]  • {item_type}: {item_path}[/dim]")
    else:
        console.print(f"[red]❌ Compilation failed for {template}![/red]")
        for error in result.get("errors", []):
            console.print(f"[red]  • {error}[/red]")
        raise typer.Exit(code=1)


def _compile_all_templates(
    generator,
    config_dict: dict,
    output_format: str,
    target_environment: str
):
    """Compile all available templates."""
    # Get available templates
    items = generator.list_items()
    
    # Get generic templates
    generic_templates = items.get("generic_templates", {})
    
    if not generic_templates:
        console.print("[yellow]No generic templates found to compile.[/yellow]")
        return
    
    console.print(f"[blue]Found {len(generic_templates)} templates to compile for {target_environment} environment[/blue]")
    
    compiled_count = 0
    failed_count = 0
    
    for template_name, description in generic_templates.items():
        # Filter templates by target environment if it's in the name
        if target_environment == "lakehouse" and "warehouse" in template_name:
            console.print(f"[dim]Skipping {template_name} (warehouse template)[/dim]")
            continue
        elif target_environment == "warehouse" and "lakehouse" in template_name:
            console.print(f"[dim]Skipping {template_name} (lakehouse template)[/dim]")
            continue
        
        console.print(f"[blue]Compiling {template_name}...[/blue]")
        
        try:
            result = generator.compile(
                template=template_name,
                runtime_config=config_dict,
                output_format=output_format,
                target_environment=target_environment
            )
            
            if result["success"]:
                console.print(f"[green]  ✅ {template_name} compiled successfully![/green]")
                for item_type, item_path in result["compiled_items"].items():
                    console.print(f"[dim]    • {item_type}: {item_path}[/dim]")
                compiled_count += 1
            else:
                # Check if the error is due to an unimplemented template
                error_messages = result.get("errors", [])
                if any("Unknown generic template" in str(error) for error in error_messages):
                    console.print(f"[yellow]  ⚠️ {template_name} not implemented yet, skipping...[/yellow]")
                    # Don't count as failed if template is not implemented
                    continue
                else:
                    console.print(f"[red]  ❌ {template_name} compilation failed![/red]")
                    for error in error_messages:
                        console.print(f"[red]    • {error}[/red]")
                    failed_count += 1
        except ValueError as e:
            if "Unknown generic template" in str(e):
                console.print(f"[yellow]  ⚠️ {template_name} not implemented yet, skipping...[/yellow]")
                # Don't count as failed if template is not implemented
                continue
            else:
                console.print(f"[red]  ❌ {template_name} compilation failed: {e}[/red]")
                failed_count += 1
        except Exception as e:
            console.print(f"[red]  ❌ {template_name} compilation failed with exception: {e}[/red]")
            failed_count += 1
    
    # Summary
    console.print("\n" + "="*60)
    console.print("[blue]COMPILATION SUMMARY[/blue]")
    console.print(f"[green]  ✅ Successfully compiled: {compiled_count} templates[/green]")
    if failed_count > 0:
        console.print(f"[red]  ❌ Failed to compile: {failed_count} templates[/red]")
        raise typer.Exit(code=1)
    else:
        console.print("[green]🎉 All templates compiled successfully![/green]")
    console.print("="*60)