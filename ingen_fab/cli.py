from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import lazy_import
import typer
from rich.console import Console
from typing_extensions import Annotated

# Lazy imports - these heavy modules will be imported only when accessed
deploy_commands = lazy_import.lazy_module("ingen_fab.cli_utils.deploy_commands")
init_commands = lazy_import.lazy_module("ingen_fab.cli_utils.init_commands")
notebook_commands = lazy_import.lazy_module("ingen_fab.cli_utils.notebook_commands")
workspace_commands = lazy_import.lazy_module("ingen_fab.cli_utils.workspace_commands")

from ingen_fab.cli_utils.console_styles import ConsoleStyles

# Lazy import for heavyweight modules
NotebookGenerator = lazy_import.lazy_callable("ingen_fab.ddl_scripts.notebook_generator.NotebookGenerator")
PathUtils = lazy_import.lazy_callable("ingen_fab.utils.path_utils.PathUtils")

console = Console()
console_styles = ConsoleStyles()

# Create main app and sub-apps
app = typer.Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)
deploy_app = typer.Typer()
init_app = typer.Typer()
ddl_app = typer.Typer()
test_app = typer.Typer()
test_local_app = typer.Typer()
test_platform_app = typer.Typer()
notebook_app = typer.Typer()
package_app = typer.Typer()
ingest_app = typer.Typer()
synapse_app = typer.Typer()
extract_app = typer.Typer()
synthetic_data_app = typer.Typer()
libs_app = typer.Typer()

# Add sub-apps to main app
test_app.add_typer(
    test_local_app,
    name="local",
    help="Commands for testing libraries and Python blocks locally.",
)
test_app.add_typer(
    test_platform_app,
    name="platform",
    help="Commands for testing libraries and notebooks in the Fabric platform.",
)

app.add_typer(
    deploy_app,
    name="deploy",
    help="Commands for deploying to environments and managing workspace items.",
)
app.add_typer(
    init_app, name="init", help="Commands for initializing solutions and projects."
)
app.add_typer(ddl_app, name="ddl", help="Commands for compiling DDL notebooks.")
app.add_typer(
    test_app, name="test", help="Commands for testing notebooks and Python blocks."
)
app.add_typer(
    notebook_app,
    name="notebook",
    help="Commands for managing and scanning notebook content.",
)
app.add_typer(
    package_app,
    name="package",
    help="Commands for running extension packages.",
)
app.add_typer(
    libs_app,
    name="libs",
    help="Commands for compiling and managing Python libraries.",
)



@app.callback()
def main(
    ctx: typer.Context,
    fabric_workspace_repo_dir: Annotated[
        Path | None,
        typer.Option(
            "--fabric-workspace-repo-dir",
            "-fwd",
            help="Directory containing fabric workspace repository files",
        ),
    ] = None,
    fabric_environment: Annotated[
        Path | None,
        typer.Option(
            "--fabric-environment",
            "-fe",
            help="The name of your fabric environment (e.g., development, production). This must match one of the valuesets in your variable library.",
        ),
    ] = None,
):
    # Track if values came from environment variables or defaults
    fabric_workspace_repo_dir_source = "option"
    fabric_environment_source = "option"
    
    if fabric_workspace_repo_dir is None:
        env_val = os.environ.get("FABRIC_WORKSPACE_REPO_DIR")
        if env_val:
            console_styles.print_warning(
                console,
                "Falling back to FABRIC_WORKSPACE_REPO_DIR environment variable.",
            )
            fabric_workspace_repo_dir = Path(env_val)
            fabric_workspace_repo_dir_source = "env"
        else:
            from ingen_fab.utils.path_utils import PathUtils
            fabric_workspace_repo_dir = PathUtils.get_workspace_repo_dir()
            fabric_workspace_repo_dir_source = "default"
    
    if fabric_environment is None:
        env_val = os.environ.get("FABRIC_ENVIRONMENT")
        if env_val:
            console_styles.print_warning(
                console, "Falling back to FABRIC_ENVIRONMENT environment variable."
            )
            fabric_environment = Path(env_val)
            fabric_environment_source = "env"
        else:
            fabric_environment = Path("development")
            fabric_environment_source = "default"

    # Skip validation for init new command and help
    # Note: We need to check the command path to differentiate between init subcommands
    import sys
    skip_validation = (
        ctx.invoked_subcommand is None or
        (ctx.params and ctx.params.get("help")) or
        (ctx.invoked_subcommand == "init" and len(sys.argv) > 2 and sys.argv[2] == "new")
    )
    
    if not skip_validation:
        # Validate that both fabric_environment and fabric_workspace_repo_dir are explicitly set
        if fabric_environment_source == "default":
            console_styles.print_error(
                console, "❌ FABRIC_ENVIRONMENT must be set. Use --fabric-environment or set the FABRIC_ENVIRONMENT environment variable."
            )
            raise typer.Exit(code=1)
        
        if fabric_workspace_repo_dir_source == "default":
            console_styles.print_error(
                console, "❌ FABRIC_WORKSPACE_REPO_DIR must be set. Use --fabric-workspace-repo-dir or set the FABRIC_WORKSPACE_REPO_DIR environment variable."
            )
            raise typer.Exit(code=1)
        
        # Validate that fabric_workspace_repo_dir exists
        if not fabric_workspace_repo_dir.exists():
            console_styles.print_error(
                console, f"❌ Fabric workspace repository directory does not exist: {fabric_workspace_repo_dir}"
            )
            console_styles.print_info(
                console, "💡 Use 'ingen_fab init new --project-name <name>' to create a new project."
            )
            raise typer.Exit(code=1)
        
        if not fabric_workspace_repo_dir.is_dir():
            console_styles.print_error(
                console, f"❌ Fabric workspace repository path is not a directory: {fabric_workspace_repo_dir}"
            )
            raise typer.Exit(code=1)

    console_styles.print_info(
        console, f"Using Fabric workspace repo directory: {fabric_workspace_repo_dir}"
    )
    console_styles.print_info(
        console, f"Using Fabric environment: {fabric_environment}"
    )
    ctx.obj = {
        "fabric_workspace_repo_dir": fabric_workspace_repo_dir,
        "fabric_environment": fabric_environment,
    }


# ddl commands


@ddl_app.command()
def compile(
    ctx: typer.Context,
    output_mode: Annotated[str, typer.Option("--output-mode", "-o", help="Output mode: fabric_workspace_repo or local")] = None,
    generation_mode: Annotated[
        str, typer.Option("--generation-mode", "-g", help="Generation mode: Lakehouse or Warehouse")
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
):
    """Compile the DDL notebooks in the specified project directory."""
    
    # Convert string parameters to enums with proper error handling
    # Import the actual class when we need it
    from ingen_fab.ddl_scripts.notebook_generator import NotebookGenerator
    
    if output_mode:
        try:
            output_mode = NotebookGenerator.OutputMode(output_mode)
        except ValueError:
            valid_modes = [mode.value for mode in NotebookGenerator.OutputMode]
            console.print(f"[red]Error: Invalid output mode '{output_mode}'[/red]")
            console.print(f"[yellow]Valid output modes: {', '.join(valid_modes)}[/yellow]")
            raise typer.Exit(code=1)
    
    if generation_mode:
        try:
            generation_mode = NotebookGenerator.GenerationMode(generation_mode)
        except ValueError:
            valid_modes = [mode.value for mode in NotebookGenerator.GenerationMode]
            console.print(f"[red]Error: Invalid generation mode '{generation_mode}'[/red]")
            console.print(f"[yellow]Valid generation modes: {', '.join(valid_modes)}[/yellow]")
            raise typer.Exit(code=1)
    
    notebook_commands.compile_ddl_notebooks(ctx, output_mode, generation_mode, verbose)


# Initialize commands


@init_app.command("new")
def init_solution(
    project_name: Annotated[str | None, typer.Option("--project-name", "-p", help="Name of the project to create")] = None,
    path: Annotated[Path, typer.Option("--path", help="Base path where the project will be created")] = Path("."),
):
    init_commands.init_solution(project_name, path)


@init_app.command("workspace")
def init_workspace(
    ctx: typer.Context,
    workspace_name: Annotated[str, typer.Option("--workspace-name", "-w", help="Name of the Fabric workspace to lookup and configure")],
    create_if_not_exists: Annotated[bool, typer.Option("--create-if-not-exists", "-c", help="Create the workspace if it doesn't exist")] = False,
):
    """Initialize workspace configuration by looking up workspace ID from name."""
    init_commands.init_workspace(ctx, workspace_name, create_if_not_exists)


# Deploy commands


@deploy_app.command()
def deploy(ctx: typer.Context):
    deploy_commands.deploy_to_environment(ctx)


@deploy_app.command()
def delete_all(
    ctx: typer.Context,
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
):
    workspace_commands.delete_workspace_items(
        environment=ctx.obj['fabric_environment'],
        project_path=ctx.obj['fabric_workspace_repo_dir'],
        force=force
    )


@deploy_app.command()
def upload_python_libs(
    ctx: typer.Context
):
    """Upload python_libs directory to Fabric config lakehouse using OneLakeUtils."""
    deploy_commands.upload_python_libs_to_config_lakehouse(
        environment=ctx.obj['fabric_environment'],
        project_path=ctx.obj['fabric_workspace_repo_dir'],
        console=console,
    )


# Test commands
@test_app.command()
def test_python_block():
    notebook_commands.test_python_block()


@test_app.command()
def run_simple_notebook(ctx: typer.Context):
    notebook_commands.run_simple_notebook(ctx)


@test_app.command()
def run_livy_notebook(
    ctx: typer.Context,
    workspace_id: Annotated[str, typer.Option("--workspace-id", "-w")],
    lakehouse_id: Annotated[str, typer.Option("--lakehouse-id", "-l")],
    code: Annotated[
        str, typer.Option("--code", "-c")
    ] = "print('Hello from Fabric Livy API!')",
    timeout: Annotated[int, typer.Option("--timeout", "-t")] = 600,
):
    notebook_commands.run_livy_notebook(ctx, workspace_id, lakehouse_id, code, timeout)


# Platform test generation command
@test_platform_app.command()
def generate(ctx: typer.Context):
    """Generate platform tests using the script in python_libs_tests."""
    from ingen_fab.python_libs_tests import generate_platform_tests

    gpt = generate_platform_tests.GeneratePlatformTests(
        environment=ctx.obj["fabric_environment"],
        project_directory=ctx.obj["fabric_workspace_repo_dir"],
    )
    gpt.generate()


# Pytest execution command for python_libs_tests/pyspark


@test_local_app.command()
def pyspark(
    lib: Annotated[
        str,
        typer.Argument(
            help="Optional test file (without _pytest.py) to run, e.g. 'my_utils'"
        ),
    ] = None,
):
    """Run pytest on ingen_fab/python_libs_tests/pyspark or a specific test file if provided."""
    import pytest
    
    # Check that FABRIC_ENVIRONMENT is set to "local" for local tests
    fabric_env = os.getenv("FABRIC_ENVIRONMENT")
    if fabric_env != "local":
        console.print(
            f"[red]Error: FABRIC_ENVIRONMENT must be set to 'local' for local tests. "
            f"Current value: {fabric_env}[/red]"
        )
        console.print("[yellow]Please set: FABRIC_ENVIRONMENT=local[/yellow]")
        raise typer.Exit(code=1)

    base = "ingen_fab/python_libs_tests/pyspark"
    if lib:
        test_file = f"{base}/{lib}_pytest.py"
        exit_code = pytest.main([test_file, "-v"])
    else:
        exit_code = pytest.main([base, "-v"])
    raise typer.Exit(code=exit_code)


@test_local_app.command()
def python(
    lib: Annotated[
        str | None,
        typer.Argument(
            help="Optional test file (without _pytest.py) to run, e.g. 'ddl_utils'"
        ),
    ] = None,
):
    """Run pytest on ingen_fab/python_libs_tests/python or a specific test file if provided."""
    import pytest
    
    # Check that FABRIC_ENVIRONMENT is set to "local" for local tests
    fabric_env = os.getenv("FABRIC_ENVIRONMENT")
    if fabric_env != "local":
        console.print(
            f"[red]Error: FABRIC_ENVIRONMENT must be set to 'local' for local tests. "
            f"Current value: {fabric_env}[/red]"
        )
        console.print("[yellow]Please set: FABRIC_ENVIRONMENT=local[/yellow]")
        raise typer.Exit(code=1)

    base = "ingen_fab/python_libs_tests/python"
    if lib:
        test_file = f"{base}/{lib}_pytest.py"
        exit_code = pytest.main([test_file])
    else:
        exit_code = pytest.main([base])
    raise typer.Exit(code=exit_code)


@test_local_app.command()
def common(
    lib: Annotated[
        str,
        typer.Argument(
            help="Optional test file (without _pytest.py) to run, e.g. 'my_utils'"
        ),
    ] = None,
):
    """Run pytest on ingen_fab/python_libs_tests/common or a specific test file if provided."""
    import pytest

    base = "ingen_fab/python_libs_tests/common"
    if lib:
        test_file = f"{base}/{lib}_pytest.py"
        exit_code = pytest.main([test_file, "-v"])
    else:
        exit_code = pytest.main([base, "-v"])
    raise typer.Exit(code=exit_code)


# Notebook commands
@notebook_app.command()
def find_notebook_content_files(
    base_dir: Annotated[Path, typer.Option("--base-dir", "-b")] = Path(
        "fabric_workspace_items"
    ),
):
    notebook_commands.find_notebook_content_files(base_dir)


@notebook_app.command()
def scan_notebook_blocks(
    base_dir: Annotated[Path, typer.Option("--base-dir", "-b")] = Path(
        "fabric_workspace_items"
    ),
    apply_replacements: Annotated[
        bool, typer.Option("--apply-replacements", "-a")
    ] = False,
):
    notebook_commands.scan_notebook_blocks(base_dir, apply_replacements)


@notebook_app.command()
def perform_code_replacements(
    ctx: typer.Context,
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output-dir",
        "-o",
        help="Directory to save updated files instead of modifying in place.",
    ),
    no_preserve_structure: bool = typer.Option(
        False,
        "--no-preserve-structure",
        help="When using --output-dir, don't preserve the directory structure (save all files to the root of output directory).",
    ),
):
    """Inject code between markers in notebook files (modifies files in place by default)."""
    deploy_commands.perform_code_replacements(ctx, output_dir=output_dir, preserve_structure=not no_preserve_structure)


# Package commands
package_app.add_typer(
    ingest_app,
    name="ingest",
    help="Commands for flat file ingestion package.",
)

package_app.add_typer(
    synapse_app,
    name="synapse",
    help="Commands for synapse sync package.",
)

package_app.add_typer(
    extract_app,
    name="extract",
    help="Commands for extract generation package.",
)
package_app.add_typer(
    synthetic_data_app,
    name="synthetic-data",
    help="Commands for synthetic data generation package.",
)


@synthetic_data_app.command("compile")
def synthetic_data_app_compile(
    ctx: typer.Context,
    dataset_id: Annotated[str, typer.Option("--dataset-id", "-d", help="Predefined dataset ID to compile")] = None,
    target_rows: Annotated[int, typer.Option("--target-rows", "-r", help="Number of rows to generate")] = 10000,
    target_environment: Annotated[str, typer.Option("--target-environment", "-e", help="Target environment (lakehouse or warehouse)")] = "lakehouse",
    generation_mode: Annotated[str, typer.Option("--generation-mode", "-m", help="Generation mode (python, pyspark, or auto)")] = "auto",
    seed_value: Annotated[int, typer.Option("--seed", "-s", help="Seed value for reproducible generation")] = None,
    include_ddl: Annotated[bool, typer.Option("--include-ddl", help="Include DDL scripts for configuration tables")] = True,
    output_mode: Annotated[str, typer.Option("--output-mode", "-o", help="Output mode (table, parquet, or csv)")] = "table",
    enhanced: Annotated[bool, typer.Option("--enhanced", help="Use enhanced template with runtime parameters and advanced features")] = False,
    config_template: Annotated[str, typer.Option("--config-template", "-t", help="Configuration template ID for enhanced mode")] = None,
    path_pattern: Annotated[str, typer.Option("--path-pattern", "-p", help="File path pattern (nested_daily, flat_with_date, hive_partitioned, etc.)")] = None,
):
    """Compile synthetic data generation notebooks and DDL scripts."""
    from ingen_fab.packages.synthetic_data_generation.synthetic_data_generation import SyntheticDataGenerationCompiler
    
    # Validate parameters
    valid_generation_modes = ["python", "pyspark", "auto"]
    if generation_mode not in valid_generation_modes:
        console.print(f"[red]Error: Invalid generation mode '{generation_mode}'[/red]")
        console.print(f"[yellow]Valid generation modes: {', '.join(valid_generation_modes)}[/yellow]")
        raise typer.Exit(code=1)
    
    valid_target_environments = ["lakehouse", "warehouse"]
    if target_environment not in valid_target_environments:
        console.print(f"[red]Error: Invalid target environment '{target_environment}'[/red]")
        console.print(f"[yellow]Valid target environments: {', '.join(valid_target_environments)}[/yellow]")
        raise typer.Exit(code=1)
    
    valid_output_modes = ["table", "parquet", "csv"]
    if output_mode not in valid_output_modes:
        console.print(f"[red]Error: Invalid output mode '{output_mode}'[/red]")
        console.print(f"[yellow]Valid output modes: {', '.join(valid_output_modes)}[/yellow]")
        raise typer.Exit(code=1)
    
    # Initialize compiler
    compiler = SyntheticDataGenerationCompiler(
        fabric_workspace_repo_dir=ctx.obj["fabric_workspace_repo_dir"],
        fabric_environment=ctx.obj["fabric_environment"]
    )
    
    try:
        if enhanced:
            console.print("[bold blue]🚀 Using Enhanced Synthetic Data Generation[/bold blue]")
            
            # Show available templates if requested
            if config_template == "list":
                try:
                    templates = compiler.get_available_configuration_templates()
                    console.print("\n[bold]Available Configuration Templates:[/bold]")
                    for template_id, description in templates.items():
                        console.print(f"  • [cyan]{template_id}[/cyan]: {description}")
                    
                    patterns = compiler.get_available_file_path_patterns()
                    console.print("\n[bold]Available File Path Patterns:[/bold]")
                    for pattern_id, description in patterns.items():
                        console.print(f"  • [cyan]{pattern_id}[/cyan]: {description}")
                except Exception:
                    console.print("[yellow]Enhanced configuration system not available. Make sure dependencies are installed.[/yellow]")
                return
            
            # Build runtime overrides
            runtime_overrides = {}
            if path_pattern:
                runtime_overrides["output_settings"] = {
                    "path_format": path_pattern,
                    "output_mode": output_mode
                }
            
            if dataset_id or config_template:
                # Compile specific enhanced dataset
                template_to_use = config_template or dataset_id
                console.print(f"[blue]Compiling enhanced synthetic data notebook for: {template_to_use}[/blue]")
                console.print(f"[dim]Using enhanced template with runtime parameters[/dim]")
                
                if path_pattern:
                    console.print(f"[dim]File path pattern: {path_pattern}[/dim]")
                
                try:
                    notebook_path = compiler.compile_configurable_dataset_notebook(
                        config_template_id=template_to_use,
                        dataset_id=dataset_id or template_to_use,
                        customizations={
                            "target_rows": target_rows,
                            "incremental_config": {"seed_value": seed_value},
                            "output_settings": {"output_mode": output_mode}
                        },
                        target_environment=target_environment
                    )
                    
                    console.print(f"[green]✅ Enhanced notebook compiled: {notebook_path}[/green]")
                    console.print(f"[yellow]💡 You can now modify parameters at runtime in the notebook![/yellow]")
                    
                except Exception as e:
                    if "Enhanced configuration system not available" in str(e):
                        console.print(f"[yellow]Enhanced features not available, falling back to standard mode[/yellow]")
                        # Fall back to standard compilation
                        notebook_path = compiler.compile_predefined_dataset_notebook(
                            dataset_id=dataset_id,
                            target_rows=target_rows,
                            target_environment=target_environment,
                            generation_mode=generation_mode,
                            seed_value=seed_value,
                            output_mode=output_mode
                        )
                        console.print(f"[green]✅ Standard notebook compiled: {notebook_path}[/green]")
                    elif "not found" in str(e):
                        console.print(f"[red]❌ Configuration template '{template_to_use}' not found[/red]")
                        console.print("[yellow]Run with --config-template list to see available templates[/yellow]")
                        raise typer.Exit(1)
                    else:
                        raise
                
            else:
                # Compile all enhanced packages
                console.print(f"[blue]Compiling all enhanced synthetic data packages for {target_environment}[/blue]")
                
                try:
                    customizations = {}
                    if path_pattern:
                        customizations = {
                            "retail_oltp_enhanced": {"output_settings": {"path_format": path_pattern}},
                            "retail_star_enhanced": {"output_settings": {"path_format": path_pattern}}
                        }
                    
                    results = compiler.compile_all_enhanced_synthetic_data_notebooks(
                        target_environment=target_environment,
                        customizations=customizations,
                        output_mode=output_mode
                    )
                    
                    if results["success"]:
                        console.print("[green]✅ All enhanced synthetic data packages compiled successfully![/green]")
                        console.print(f"[yellow]💡 Enhanced notebooks support runtime parameter modification![/yellow]")
                        
                        # Show compiled items
                        for func_name, result in results["compiled_items"].items():
                            console.print(f"[dim]  - {func_name}: {result}[/dim]")
                    else:
                        console.print("[red]❌ Some enhanced packages failed to compile:[/red]")
                        for error in results["errors"]:
                            console.print(f"[red]  Error: {error}[/red]")
                        raise typer.Exit(code=1)
                        
                except Exception as e:
                    if "Enhanced configuration system not available" in str(e):
                        console.print(f"[yellow]Enhanced features not available, falling back to standard mode[/yellow]")
                        # Fall back to standard compilation
                        results = compiler.compile_all_synthetic_data_notebooks(
                            target_environment=target_environment,
                            output_mode=output_mode
                        )
                        if results["success"]:
                            console.print("[green]✅ All synthetic data packages compiled successfully![/green]")
                            for func_name, result in results["compiled_items"].items():
                                console.print(f"[dim]  - {func_name}: {result}[/dim]")
                    else:
                        raise
        
        elif dataset_id:
            # Compile specific predefined dataset (legacy mode)
            console.print(f"[blue]Compiling synthetic data notebook for dataset: {dataset_id}[/blue]")
            
            notebook_path = compiler.compile_predefined_dataset_notebook(
                dataset_id=dataset_id,
                target_rows=target_rows,
                target_environment=target_environment,
                generation_mode=generation_mode,
                seed_value=seed_value,
                output_mode=output_mode
            )
            
            console.print(f"[green]✅ Notebook compiled: {notebook_path}[/green]")
            
        else:
            # Compile all synthetic data packages (legacy mode)
            console.print(f"[blue]Compiling all synthetic data generation packages for {target_environment}[/blue]")
            
            results = compiler.compile_all_synthetic_data_notebooks(
                target_environment=target_environment,
                output_mode=output_mode
            )
            
            if results["success"]:
                console.print("[green]✅ All synthetic data packages compiled successfully![/green]")
                
                # Show compiled items
                for func_name, result in results["compiled_items"].items():
                    console.print(f"[dim]  - {func_name}: {result}[/dim]")
            else:
                console.print("[red]❌ Compilation failed![/red]")
                for error in results["errors"]:
                    console.print(f"[red]  Error: {error}[/red]")
                
        # Compile DDL scripts if requested
        if include_ddl:
            console.print(f"[blue]Compiling DDL scripts for {target_environment}[/blue]")
            ddl_results = compiler.compile_ddl_scripts(target_environment)
            
            for target_dir, files in ddl_results.items():
                console.print(f"[green]✅ DDL scripts compiled to: {target_dir}[/green]")
                for file_path in files:
                    console.print(f"[dim]  - {file_path.name}[/dim]")
                    
    except Exception as e:
        console.print(f"[red]❌ Error during compilation: {e}[/red]")
        raise typer.Exit(1)


@synthetic_data_app.command("list-datasets")
def synthetic_data_list_datasets():
    """List available predefined dataset configurations."""
    from ingen_fab.packages.synthetic_data_generation.synthetic_data_generation import SyntheticDataGenerationCompiler
    
    compiler = SyntheticDataGenerationCompiler()
    configs = compiler._get_predefined_dataset_configs()
    
    console.print("[bold blue]📊 Available Synthetic Dataset Configurations[/bold blue]")
    console.print()
    
    for dataset_id, config in configs.items():
        console.print(f"[bold]{dataset_id}[/bold]")
        console.print(f"  Name: {config['dataset_name']}")
        console.print(f"  Type: {config['dataset_type']} ({config['schema_pattern']})")
        console.print(f"  Domain: {config['domain']}")
        console.print(f"  Description: {config['description']}")
        
        if 'tables' in config:
            console.print(f"  Tables: {', '.join(config['tables'])}")
        if 'fact_tables' in config:
            console.print(f"  Fact Tables: {', '.join(config['fact_tables'])}")
        if 'dimensions' in config:
            console.print(f"  Dimensions: {', '.join(config['dimensions'])}")
        
        console.print()


@synthetic_data_app.command("list-enhanced")
def synthetic_data_list_enhanced():
    """List available enhanced configuration templates and file path patterns."""
    from ingen_fab.packages.synthetic_data_generation.synthetic_data_generation import SyntheticDataGenerationCompiler
    
    compiler = SyntheticDataGenerationCompiler()
    
    console.print("[bold blue]🚀 Enhanced Synthetic Data Generation Options[/bold blue]")
    console.print()
    
    try:
        # List configuration templates
        templates = compiler.get_available_configuration_templates()
        if templates:
            console.print("[bold green]📋 Available Configuration Templates:[/bold green]")
            for template_id, description in templates.items():
                console.print(f"  • [cyan]{template_id}[/cyan]: {description}")
        else:
            console.print("[yellow]No enhanced configuration templates available[/yellow]")
        
        console.print()
        
        # List file path patterns
        patterns = compiler.get_available_file_path_patterns()
        if patterns:
            console.print("[bold green]📁 Available File Path Patterns:[/bold green]")
            for pattern_id, description in patterns.items():
                console.print(f"  • [cyan]{pattern_id}[/cyan]: {description}")
        else:
            console.print("[yellow]No enhanced file path patterns available[/yellow]")
        
        console.print()
        console.print("[bold]Usage Examples:[/bold]")
        console.print("  # Compile with enhanced features")
        console.print("  python -m ingen_fab.cli package synthetic-data compile --enhanced")
        console.print()
        console.print("  # Use specific template with custom path pattern")
        console.print("  python -m ingen_fab.cli package synthetic-data compile --enhanced \\")
        console.print("    --config-template retail_oltp_enhanced --path-pattern hive_partitioned")
        console.print()
        console.print("  # List all options")
        console.print("  python -m ingen_fab.cli package synthetic-data compile --enhanced --config-template list")
        
    except Exception as e:
        console.print(f"[yellow]Enhanced configuration system not available: {e}[/yellow]")
        console.print("[dim]Make sure all dependencies are installed and accessible[/dim]")


@synthetic_data_app.command("generate")
def synthetic_data_generate(
    ctx: typer.Context,
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID to generate")],
    target_rows: Annotated[int, typer.Option("--target-rows", "-r", help="Number of rows to generate")] = 10000,
    target_environment: Annotated[str, typer.Option("--target-environment", "-e", help="Target environment (lakehouse or warehouse)")] = "lakehouse",
    generation_mode: Annotated[str, typer.Option("--generation-mode", "-m", help="Generation mode (python, pyspark, or auto)")] = "auto",
    seed_value: Annotated[int, typer.Option("--seed", "-s", help="Seed value for reproducible generation")] = None,
    execute_notebook: Annotated[bool, typer.Option("--execute", help="Execute the notebook after compilation")] = False,
    output_mode: Annotated[str, typer.Option("--output-mode", "-o", help="Output mode (table, parquet, or csv)")] = "table",
):
    """Generate synthetic data for a specific dataset configuration."""
    console.print(f"[blue]🎲 Generating synthetic data for dataset: {dataset_id}[/blue]")
    console.print(f"📊 Target rows: {target_rows:,}")
    console.print(f"🏗️ Target environment: {target_environment}")
    console.print(f"🔧 Generation mode: {generation_mode}")
    console.print(f"💾 Output mode: {output_mode}")
    if seed_value:
        console.print(f"🌱 Seed value: {seed_value}")
    
    # Validate parameters
    valid_generation_modes = ["python", "pyspark", "auto"]
    if generation_mode not in valid_generation_modes:
        console.print(f"[red]Error: Invalid generation mode '{generation_mode}'[/red]")
        console.print(f"[yellow]Valid generation modes: {', '.join(valid_generation_modes)}[/yellow]")
        raise typer.Exit(code=1)
    
    valid_target_environments = ["lakehouse", "warehouse"]
    if target_environment not in valid_target_environments:
        console.print(f"[red]Error: Invalid target environment '{target_environment}'[/red]")
        console.print(f"[yellow]Valid target environments: {', '.join(valid_target_environments)}[/yellow]")
        raise typer.Exit(code=1)
    
    valid_output_modes = ["table", "parquet", "csv"]
    if output_mode not in valid_output_modes:
        console.print(f"[red]Error: Invalid output mode '{output_mode}'[/red]")
        console.print(f"[yellow]Valid output modes: {', '.join(valid_output_modes)}[/yellow]")
        raise typer.Exit(code=1)
    
    from ingen_fab.packages.synthetic_data_generation.synthetic_data_generation import SyntheticDataGenerationCompiler
    
    # Initialize compiler
    compiler = SyntheticDataGenerationCompiler(
        fabric_workspace_repo_dir=ctx.obj["fabric_workspace_repo_dir"],
        fabric_environment=ctx.obj["fabric_environment"]
    )
    
    try:
        # Compile the notebook
        notebook_path = compiler.compile_predefined_dataset_notebook(
            dataset_id=dataset_id,
            target_rows=target_rows,
            target_environment=target_environment,
            generation_mode=generation_mode,
            seed_value=seed_value,
            output_mode=output_mode
        )
        
        console.print(f"[green]✅ Notebook compiled: {notebook_path}[/green]")
        
        if execute_notebook:
            console.print("[yellow]📓 Executing notebook...[/yellow]")
            console.print("[yellow]Note: In a production environment, this would submit the notebook to Fabric for execution.[/yellow]")
            console.print(f"[yellow]To run manually, execute: {notebook_path}[/yellow]")
        else:
            console.print(f"[dim]💡 To execute the notebook, add --execute flag or run it manually in your environment[/dim]")
            
    except Exception as e:
        console.print(f"[red]❌ Error during generation: {e}[/red]")
        raise typer.Exit(1)


@synthetic_data_app.command("generate-incremental")
def synthetic_data_generate_incremental(
    ctx: typer.Context,
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID to generate")],
    date: Annotated[str, typer.Option("--date", "-d", help="Generation date (YYYY-MM-DD)")] = None,
    path_format: Annotated[str, typer.Option("--path-format", "-p", help="Path format (nested or flat)")] = "nested",
    target_environment: Annotated[str, typer.Option("--target-environment", "-e", help="Target environment (lakehouse or warehouse)")] = "lakehouse",
    generation_mode: Annotated[str, typer.Option("--generation-mode", "-m", help="Generation mode (python, pyspark, or auto)")] = "auto",
    seed_value: Annotated[int, typer.Option("--seed", "-s", help="Seed value for reproducible generation")] = None,
    state_management: Annotated[bool, typer.Option("--state-management", help="Enable state management for consistent IDs")] = True,
    execute_notebook: Annotated[bool, typer.Option("--execute", help="Execute the notebook after compilation")] = False,
):
    """Generate incremental synthetic data for a specific date."""
    from datetime import datetime, date as dt_date
    from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
    
    # Parse date or use today
    if date:
        try:
            generation_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            console.print(f"[red]Error: Invalid date format '{date}'. Use YYYY-MM-DD format.[/red]")
            raise typer.Exit(code=1)
    else:
        generation_date = dt_date.today()
        console.print(f"[yellow]No date specified, using today: {generation_date}[/yellow]")
    
    console.print(f"[blue]🎲 Generating incremental synthetic data for dataset: {dataset_id}[/blue]")
    console.print(f"📅 Date: {generation_date}")
    console.print(f"📁 Path format: {path_format}")
    console.print(f"🏗️ Target environment: {target_environment}")
    console.print(f"🔧 Generation mode: {generation_mode}")
    if seed_value:
        console.print(f"🌱 Seed value: {seed_value}")
    
    # Validate parameters
    valid_path_formats = ["nested", "flat"]
    if path_format not in valid_path_formats:
        console.print(f"[red]Error: Invalid path format '{path_format}'[/red]")
        console.print(f"[yellow]Valid path formats: {', '.join(valid_path_formats)}[/yellow]")
        raise typer.Exit(code=1)
    
    # Initialize compiler
    compiler = IncrementalSyntheticDataGenerationCompiler(
        fabric_workspace_repo_dir=ctx.obj["fabric_workspace_repo_dir"],
        fabric_environment=ctx.obj["fabric_environment"]
    )
    
    try:
        # Get dataset configuration
        configs = compiler._get_incremental_dataset_configs()
        if dataset_id not in configs:
            console.print(f"[red]Error: Dataset '{dataset_id}' not found[/red]")
            console.print(f"[yellow]Available datasets: {', '.join(configs.keys())}[/yellow]")
            raise typer.Exit(code=1)
        
        dataset_config = configs[dataset_id]
        if seed_value:
            dataset_config["seed_value"] = seed_value
        
        # Compile the notebook
        notebook_path = compiler.compile_incremental_dataset_notebook(
            dataset_config=dataset_config,
            generation_date=generation_date,
            target_environment=target_environment,
            generation_mode=generation_mode,
            path_format=path_format,
            state_management=state_management
        )
        
        console.print(f"[green]✅ Notebook compiled: {notebook_path}[/green]")
        
        if execute_notebook:
            console.print("[yellow]📓 Executing notebook...[/yellow]")
            console.print("[yellow]Note: In a production environment, this would submit the notebook to Fabric for execution.[/yellow]")
        else:
            console.print(f"[dim]💡 To execute the notebook, add --execute flag or run it manually in your environment[/dim]")
            
    except Exception as e:
        console.print(f"[red]❌ Error during generation: {e}[/red]")
        raise typer.Exit(1)


@synthetic_data_app.command("generate-series")
def synthetic_data_generate_series(
    ctx: typer.Context,
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID to generate")],
    start_date: Annotated[str, typer.Option("--start-date", "-s", help="Start date (YYYY-MM-DD)")] = None,
    end_date: Annotated[str, typer.Option("--end-date", "-e", help="End date (YYYY-MM-DD)")] = None,
    batch_size: Annotated[int, typer.Option("--batch-size", "-b", help="Number of days to process in each batch")] = 10,
    path_format: Annotated[str, typer.Option("--path-format", "-p", help="Path format (nested or flat)")] = "nested",
    target_environment: Annotated[str, typer.Option("--target-environment", "-t", help="Target environment (lakehouse or warehouse)")] = "lakehouse",
    generation_mode: Annotated[str, typer.Option("--generation-mode", "-m", help="Generation mode (python, pyspark, or auto)")] = "auto",
    output_mode: Annotated[str, typer.Option("--output-mode", "-o", help="Output mode (parquet, csv, or table)")] = "parquet",
    seed_value: Annotated[int, typer.Option("--seed", help="Seed value for reproducible generation")] = None,
    ignore_state: Annotated[bool, typer.Option("--ignore-state", help="Ignore existing state and regenerate all files")] = False,
    execute_notebook: Annotated[bool, typer.Option("--execute", help="Execute the notebook after compilation")] = False,
):
    """Generate a series of incremental synthetic data for a date range."""
    from datetime import datetime, date as dt_date, timedelta
    from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
    
    # Parse dates
    if not start_date:
        console.print(f"[red]Error: --start-date is required[/red]")
        raise typer.Exit(code=1)
    
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    except ValueError:
        console.print(f"[red]Error: Invalid start date format '{start_date}'. Use YYYY-MM-DD format.[/red]")
        raise typer.Exit(code=1)
    
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            console.print(f"[red]Error: Invalid end date format '{end_date}'. Use YYYY-MM-DD format.[/red]")
            raise typer.Exit(code=1)
    else:
        end_dt = dt_date.today()
        console.print(f"[yellow]No end date specified, using today: {end_dt}[/yellow]")
    
    if start_dt > end_dt:
        console.print(f"[red]Error: Start date must be before or equal to end date[/red]")
        raise typer.Exit(code=1)
    
    total_days = (end_dt - start_dt).days + 1
    
    console.print(f"[blue]🎲 Generating incremental synthetic data series for dataset: {dataset_id}[/blue]")
    console.print(f"📅 Date range: {start_dt} to {end_dt} ({total_days} days)")
    console.print(f"📦 Batch size: {batch_size} days")
    console.print(f"📁 Path format: {path_format}")
    console.print(f"🏗️ Target environment: {target_environment}")
    console.print(f"🔧 Generation mode: {generation_mode}")
    console.print(f"💾 Output mode: {output_mode}")
    if seed_value:
        console.print(f"🌱 Seed value: {seed_value}")
    
    # Validate parameters
    valid_path_formats = ["nested", "flat"]
    if path_format not in valid_path_formats:
        console.print(f"[red]Error: Invalid path format '{path_format}'[/red]")
        console.print(f"[yellow]Valid path formats: {', '.join(valid_path_formats)}[/yellow]")
        raise typer.Exit(code=1)
    
    valid_output_modes = ["table", "parquet", "csv"]
    if output_mode not in valid_output_modes:
        console.print(f"[red]Error: Invalid output mode '{output_mode}'[/red]")
        console.print(f"[yellow]Valid output modes: {', '.join(valid_output_modes)}[/yellow]")
        raise typer.Exit(code=1)
    
    # Initialize compiler
    compiler = IncrementalSyntheticDataGenerationCompiler(
        fabric_workspace_repo_dir=ctx.obj["fabric_workspace_repo_dir"],
        fabric_environment=ctx.obj["fabric_environment"]
    )
    
    try:
        # Get dataset configuration
        configs = compiler._get_incremental_dataset_configs()
        if dataset_id not in configs:
            console.print(f"[red]Error: Dataset '{dataset_id}' not found[/red]")
            console.print(f"[yellow]Available datasets: {', '.join(configs.keys())}[/yellow]")
            raise typer.Exit(code=1)
        
        dataset_config = configs[dataset_id]
        if seed_value:
            dataset_config["seed_value"] = seed_value
        
        # Compile the notebook
        notebook_path = compiler.compile_incremental_dataset_series_notebook(
            dataset_config=dataset_config,
            start_date=start_dt,
            end_date=end_dt,
            batch_size=batch_size,
            target_environment=target_environment,
            generation_mode=generation_mode,
            path_format=path_format,
            output_mode=output_mode,
            ignore_state=ignore_state
        )
        
        console.print(f"[green]✅ Notebook compiled: {notebook_path}[/green]")
        
        if execute_notebook:
            console.print("[yellow]📓 Executing notebook...[/yellow]")
            console.print("[yellow]Note: In a production environment, this would submit the notebook to Fabric for execution.[/yellow]")
        else:
            console.print(f"[dim]💡 To execute the notebook, add --execute flag or run it manually in your environment[/dim]")
            
    except Exception as e:
        console.print(f"[red]❌ Error during generation: {e}[/red]")
        raise typer.Exit(1)


@synthetic_data_app.command("list-incremental-datasets")
def synthetic_data_list_incremental_datasets():
    """List available incremental dataset configurations."""
    from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
    
    compiler = IncrementalSyntheticDataGenerationCompiler()
    configs = compiler._get_incremental_dataset_configs()
    
    console.print("[bold blue]📊 Available Incremental Synthetic Dataset Configurations[/bold blue]")
    console.print()
    
    for dataset_id, config in configs.items():
        console.print(f"[bold]{dataset_id}[/bold]")
        console.print(f"  Name: {config['dataset_name']}")
        console.print(f"  Type: {config['dataset_type']} ({config['schema_pattern']})")
        console.print(f"  Domain: {config['domain']}")
        console.print(f"  Description: {config['description']}")
        
        if "incremental_config" in config:
            inc_config = config["incremental_config"]
            console.print(f"  Snapshot Frequency: {inc_config.get('snapshot_frequency', 'daily')}")
            if inc_config.get('enable_seasonal_patterns'):
                console.print("  Seasonal Patterns: Enabled")
            if inc_config.get('growth_rate'):
                console.print(f"  Growth Rate: {inc_config['growth_rate'] * 100:.1f}% daily")
        
        if "table_configs" in config:
            snapshot_tables = [t for t, cfg in config["table_configs"].items() if cfg.get("type") == "snapshot"]
            incremental_tables = [t for t, cfg in config["table_configs"].items() if cfg.get("type") == "incremental"]
            
            if snapshot_tables:
                console.print(f"  Snapshot Tables: {', '.join(snapshot_tables)}")
            if incremental_tables:
                console.print(f"  Incremental Tables: {', '.join(incremental_tables)}")
        
        console.print()


@ingest_app.command("compile")
def ingest_app_compile(
    ctx: typer.Context,
    template_vars: Annotated[str, typer.Option("--template-vars", "-t", help="JSON string of template variables")] = None,
    include_samples: Annotated[bool, typer.Option("--include-samples", "-s", help="Include sample data DDL and files")] = False,
    target_datastore: Annotated[str, typer.Option("--target-datastore", "-d", help="Target datastore type: lakehouse, warehouse, or both")] = "lakehouse",
):
    """Compile flat file ingestion package templates and DDL scripts."""
    import json

    from ingen_fab.packages.flat_file_ingestion.flat_file_ingestion import (
        compile_flat_file_ingestion_package,
    )
    
    # Parse template variables if provided
    vars_dict = {}
    if template_vars:
        try:
            vars_dict = json.loads(template_vars)
        except json.JSONDecodeError as e:
            console.print(f"[red]Error parsing template variables: {e}[/red]")
            raise typer.Exit(code=1)
    
    # Get fabric workspace repo directory from context
    fabric_workspace_repo_dir = str(ctx.obj["fabric_workspace_repo_dir"])
    
    # Validate target datastore parameter
    valid_datastores = ["lakehouse", "warehouse", "both"]
    if target_datastore not in valid_datastores:
        console.print(f"[red]Error: Invalid target datastore '{target_datastore}'. Must be one of: {', '.join(valid_datastores)}[/red]")
        raise typer.Exit(code=1)
    
    try:
        results = compile_flat_file_ingestion_package(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            template_vars=vars_dict,
            include_samples=include_samples,
            target_datastore=target_datastore
        )
        
        if results["success"]:
            console.print("[green]✓ Flat file ingestion package compiled successfully![/green]")
        else:
            console.print(f"[red]✗ Compilation failed: {results['errors']}[/red]")
            raise typer.Exit(code=1)
            
    except Exception as e:
        console.print(f"[red]Error compiling package: {e}[/red]")
        raise typer.Exit(code=1)


@ingest_app.command()
def run(
    ctx: typer.Context,
    config_id: Annotated[str, typer.Option("--config-id", "-c", help="Specific configuration ID to process")] = "",
    execution_group: Annotated[int, typer.Option("--execution-group", "-g", help="Execution group number")] = 1,
    environment: Annotated[str, typer.Option("--environment", "-e", help="Environment name")] = "development",
):
    """Run flat file ingestion for specified configuration or execution group."""
    console.print(f"[blue]Running flat file ingestion...[/blue]")
    console.print(f"Config ID: {config_id}")
    console.print(f"Execution Group: {execution_group}")
    console.print(f"Environment: {environment}")
    console.print(f"Fabric Workspace Repo Dir: {ctx.obj['fabric_workspace_repo_dir']}")
    
    console.print("[yellow]Note: This command would typically execute the compiled notebook with the specified parameters.[/yellow]")
    console.print("[yellow]In a production environment, this would submit the notebook to Fabric for execution.[/yellow]")


@synapse_app.command("compile")
def synapse_app_compile(
    ctx: typer.Context,
    template_vars: Annotated[str, typer.Option("--template-vars", "-t", help="JSON string of template variables")] = None,
    include_samples: Annotated[bool, typer.Option("--include-samples", "-s", help="Include sample data DDL and files")] = False,
):
    """Compile synapse sync package templates and DDL scripts."""
    import json

    from ingen_fab.packages.synapse_sync.synapse_sync import (
        compile_synapse_sync_package,
    )
    
    # Parse template variables if provided
    vars_dict = {}
    if template_vars:
        try:
            vars_dict = json.loads(template_vars)
        except json.JSONDecodeError as e:
            console.print(f"[red]Error parsing template variables: {e}[/red]")
            raise typer.Exit(code=1)
    
    # Get fabric workspace repo directory from context
    fabric_workspace_repo_dir = str(ctx.obj["fabric_workspace_repo_dir"])
    
    try:
        results = compile_synapse_sync_package(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            template_vars=vars_dict,
            include_samples=include_samples
        )
        
        if results["success"]:
            console.print("[green]✓ Synapse sync package compiled successfully![/green]")
        else:
            console.print(f"[red]✗ Compilation failed: {results['errors']}[/red]")
            raise typer.Exit(code=1)
            
    except Exception as e:
        console.print(f"[red]Error compiling package: {e}[/red]")
        raise typer.Exit(code=1)


@synapse_app.command()
def run(
    ctx: typer.Context,
    master_execution_id: Annotated[str, typer.Option("--master-execution-id", "-m", help="Master execution ID")] = "",
    work_items_json: Annotated[str, typer.Option("--work-items-json", "-w", help="JSON string of work items for historical mode")] = "",
    max_concurrency: Annotated[int, typer.Option("--max-concurrency", "-c", help="Maximum concurrency level")] = 10,
    include_snapshots: Annotated[bool, typer.Option("--include-snapshots", "-s", help="Include snapshot tables")] = True,
    environment: Annotated[str, typer.Option("--environment", "-e", help="Environment name")] = "development",
):
    """Run synapse sync extraction for specified configuration."""
    console.print(f"[blue]Running synapse sync extraction...[/blue]")
    console.print(f"Master Execution ID: {master_execution_id}")
    console.print(f"Work Items JSON: {work_items_json}")
    console.print(f"Max Concurrency: {max_concurrency}")
    console.print(f"Include Snapshots: {include_snapshots}")
    console.print(f"Environment: {environment}")
    console.print(f"Fabric Workspace Repo Dir: {ctx.obj['fabric_workspace_repo_dir']}")
    
    console.print("[yellow]Note: This command would typically execute the compiled notebook with the specified parameters.[/yellow]")
    console.print("[yellow]In a production environment, this would submit the notebook to Fabric for execution.[/yellow]")


# Extract generation commands
@extract_app.command("compile")
def extract_app_compile(
    ctx: typer.Context,
    template_vars: Annotated[str, typer.Option("--template-vars", "-t", help="JSON string of template variables")] = None,
    include_samples: Annotated[bool, typer.Option("--include-samples", "-s", help="Include sample data DDL and source tables")] = False,
    target_datastore: Annotated[str, typer.Option("--target-datastore", "-d", help="Target datastore type (warehouse)")] = "warehouse",
):
    """Compile extract generation package templates and DDL scripts."""
    import json

    from ingen_fab.packages.extract_generation.extract_generation import (
        compile_extract_generation_package,
    )
    
    # Parse template variables if provided
    vars_dict = {}
    if template_vars:
        try:
            vars_dict = json.loads(template_vars)
        except json.JSONDecodeError as e:
            console.print(f"[red]Error parsing template variables: {e}[/red]")
            raise typer.Exit(code=1)
    
    # Get fabric workspace repo directory from context
    fabric_workspace_repo_dir = str(ctx.obj["fabric_workspace_repo_dir"])
    
    try:
        results = compile_extract_generation_package(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            template_vars=vars_dict,
            include_samples=include_samples,
            target_datastore=target_datastore
        )
        
        if results["success"]:
            console.print("[green]✓ Extract generation package compiled successfully![/green]")
        else:
            console.print(f"[red]✗ Compilation failed: {results['errors']}[/red]")
            raise typer.Exit(code=1)
            
    except Exception as e:
        console.print(f"[red]Error compiling package: {e}[/red]")
        raise typer.Exit(code=1)


@extract_app.command()
def extract_run(
    ctx: typer.Context,
    extract_name: Annotated[str, typer.Option("--extract-name", "-n", help="Specific extract configuration to process")] = "",
    execution_group: Annotated[str, typer.Option("--execution-group", "-g", help="Execution group to process")] = "",
    environment: Annotated[str, typer.Option("--environment", "-e", help="Environment name")] = "development",
    run_type: Annotated[str, typer.Option("--run-type", "-r", help="Run type: FULL or INCREMENTAL")] = "FULL",
):
    """Run extract generation for specified configuration or execution group."""
    console.print(f"[blue]Running extract generation...[/blue]")
    console.print(f"Extract Name: {extract_name}")
    console.print(f"Execution Group: {execution_group}")
    console.print(f"Environment: {environment}")
    console.print(f"Run Type: {run_type}")
    console.print(f"Fabric Workspace Repo Dir: {ctx.obj['fabric_workspace_repo_dir']}")
    
    console.print("[yellow]Note: This command would typically execute the compiled notebook with the specified parameters.[/yellow]")
    console.print("[yellow]In a production environment, this would submit the notebook to Fabric for execution.[/yellow]")


# Library compilation commands
@libs_app.command()
def compile(
    ctx: typer.Context,
    target_file: Annotated[str, typer.Option("--target-file", "-f", help="Specific python file to compile (relative to project root)")] = None,
):
    """Compile Python libraries by injecting variables from the variable library."""
    from pathlib import Path
    from ingen_fab.config_utils.variable_lib import inject_variables_into_file, VariableLibraryUtils
    
    project_path = Path(ctx.obj["fabric_workspace_repo_dir"])
    environment = str(ctx.obj["fabric_environment"])
    
    console.print(f"[blue]Compiling Python libraries...[/blue]")
    console.print(f"Project path: {project_path}")
    console.print(f"Environment: {environment}")
    
    if target_file:
        # Compile specific file
        target_path = project_path / target_file
        console.print(f"Target file: {target_path}")
        
        if not target_path.exists():
            console.print(f"[red]Error: Target file not found: {target_path}[/red]")
            raise typer.Exit(code=1)
            
        try:
            # Create a VariableLibraryUtils instance and directly call perform_code_replacements
            varlib_utils = VariableLibraryUtils(project_path, environment)
            
            # Read the file content
            with open(target_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            # Perform the replacements
            updated_content = varlib_utils.perform_code_replacements(content)
            
            # Write back if changed
            if updated_content != content:
                with open(target_path, "w", encoding="utf-8") as f:
                    f.write(updated_content)
                console.print(f"[green]✓ Successfully compiled: {target_file} with values from {environment} environment[/green]")
            else:
                console.print(f"[yellow]No changes needed for {target_file}[/yellow]")
                
        except Exception as e:
            console.print(f"[red]Error compiling {target_file}: {e}[/red]")
            raise typer.Exit(code=1)
    else:
        # Compile config_utils.py as default - use absolute path since we're in the ingen_fab directory
        from pathlib import Path as PathlibPath
        current_dir = PathlibPath.cwd()
        config_utils_path = current_dir / "ingen_fab" / "python_libs" / "common" / "config_utils.py"
        console.print(f"Target file: {config_utils_path}")
        
        if not config_utils_path.exists():
            console.print(f"[red]Error: config_utils.py not found at: {config_utils_path}[/red]")
            raise typer.Exit(code=1)
            
        try:
            # Create a VariableLibraryUtils instance and directly call perform_code_replacements
            varlib_utils = VariableLibraryUtils(project_path, environment)
            
            # Read the file content
            with open(config_utils_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            # Perform the replacements
            updated_content = varlib_utils.perform_code_replacements(content)
            
            # Write back if changed
            if updated_content != content:
                with open(config_utils_path, "w", encoding="utf-8") as f:
                    f.write(updated_content)
                console.print(f"[green]✓ Successfully compiled config_utils.py with values from {environment} environment[/green]")
            else:
                console.print(f"[yellow]No changes needed for config_utils.py[/yellow]")
                
        except Exception as e:
            console.print(f"[red]Error compiling config_utils.py: {e}[/red]")
            raise typer.Exit(code=1)


# New Generic Template Commands

@synthetic_data_app.command("compile-generic-templates")
def compile_generic_templates(
    ctx: typer.Context,
    target_environment: Annotated[str, typer.Option("--target-environment", "-t", help="Target environment (lakehouse or warehouse)")] = "lakehouse",
    force_recompile: Annotated[bool, typer.Option("--force", help="Force recompilation even if templates exist")] = False,
    template_type: Annotated[str, typer.Option("--template-type", help="Template type (all, series, single)")] = "all"
):
    """Compile generic runtime-parameterized templates."""
    from .packages.synthetic_data_generation.synthetic_data_generation import SyntheticDataGenerationCompiler
    from .python_libs.common.notebook_parameter_validator import SmartDefaultProvider
    
    console.print(f"🔧 Compiling generic templates for {target_environment} environment...")
    
    try:
        fabric_workspace_repo_dir = ctx.obj.get('fabric_workspace_repo_dir')
        fabric_environment = ctx.obj.get('fabric_environment')
        
        compiler = SyntheticDataGenerationCompiler(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            fabric_environment=fabric_environment
        )
        
        compiled_templates = []
        
        # Compile incremental series templates
        if template_type in ["all", "series"]:
            if target_environment == "lakehouse":
                template_path = compiler.compile_notebook_from_template(
                    template_name="generic_incremental_series_lakehouse.py.jinja",
                    output_notebook_name="generic_incremental_series_lakehouse",
                    template_vars={
                        "target_environment": "lakehouse",
                        "default_dataset_id": "retail_oltp_small_incremental",
                        "default_start_date": "2024-01-01",
                        "default_end_date": "2024-01-30",
                        "default_batch_size": 10,
                        "default_path_format": "nested",
                        "default_output_mode": "table",
                        "default_ignore_state": False,
                        "default_seed_value": None
                    },
                    display_name="Generic Incremental Series - Lakehouse",
                    description="Generic runtime-parameterized template for incremental synthetic data series generation in lakehouse environment",
                    output_subdir="synthetic_data_generation/generic"
                )
                compiled_templates.append(("Incremental Series Lakehouse", template_path))
            
            if target_environment == "warehouse":
                template_path = compiler.compile_notebook_from_template(
                    template_name="generic_incremental_series_warehouse.py.jinja",
                    output_notebook_name="generic_incremental_series_warehouse",
                    template_vars={
                        "target_environment": "warehouse",
                        "default_dataset_id": "retail_oltp_small_incremental",
                        "default_start_date": "2024-01-01",
                        "default_end_date": "2024-01-30",
                        "default_batch_size": 10,
                        "default_path_format": "nested",
                        "default_output_mode": "table",
                        "default_ignore_state": False,
                        "default_seed_value": None
                    },
                    display_name="Generic Incremental Series - Warehouse",
                    description="Generic runtime-parameterized template for incremental synthetic data series generation in warehouse environment",
                    output_subdir="synthetic_data_generation/generic"
                )
                compiled_templates.append(("Incremental Series Warehouse", template_path))
        
        # Compile single dataset templates
        if template_type in ["all", "single"]:
            if target_environment == "lakehouse":
                template_path = compiler.compile_notebook_from_template(
                    template_name="generic_single_dataset_lakehouse.py.jinja",
                    output_notebook_name="generic_single_dataset_lakehouse",
                    template_vars={
                        "target_environment": "lakehouse",
                        "default_dataset_id": "retail_oltp_small",
                        "default_target_rows": 10000,
                        "default_scale_factor": 1.0,
                        "default_output_mode": "table",
                        "default_seed_value": None
                    },
                    display_name="Generic Single Dataset - Lakehouse",
                    description="Generic runtime-parameterized template for single synthetic dataset generation in lakehouse environment",
                    output_subdir="synthetic_data_generation/generic"
                )
                compiled_templates.append(("Single Dataset Lakehouse", template_path))
        
        # Display results
        console.print(f"[green]✅ Successfully compiled {len(compiled_templates)} generic templates:[/green]")
        for template_name, template_path in compiled_templates:
            console.print(f"  📄 {template_name}")
            console.print(f"     📂 {template_path}")
        
        console.print(f"\n💡 Use 'execute-with-parameters' command to run these templates with custom parameters")
    
    except Exception as e:
        console.print(f"[red]❌ Error during template compilation: {e}[/red]")
        raise typer.Exit(1)


@synthetic_data_app.command("execute-with-parameters")
def execute_with_parameters(
    ctx: typer.Context,
    notebook_name: Annotated[str, typer.Argument(help="Generic notebook name (e.g., generic_incremental_series_lakehouse)")],
    dataset_id: Annotated[str, typer.Option("--dataset-id", help="Dataset ID to generate")] = "retail_oltp_small_incremental",
    # Incremental series parameters
    start_date: Annotated[str, typer.Option("--start-date", help="Start date (YYYY-MM-DD) for series generation")] = None,
    end_date: Annotated[str, typer.Option("--end-date", help="End date (YYYY-MM-DD) for series generation")] = None,
    batch_size: Annotated[int, typer.Option("--batch-size", help="Number of days per batch")] = None,
    # Single dataset parameters  
    target_rows: Annotated[int, typer.Option("--target-rows", help="Target rows for single dataset generation")] = None,
    scale_factor: Annotated[float, typer.Option("--scale-factor", help="Scale factor for dataset size")] = None,
    # Common parameters
    path_format: Annotated[str, typer.Option("--path-format", help="Path format (nested or flat)")] = None,
    output_mode: Annotated[str, typer.Option("--output-mode", help="Output mode (table, parquet, csv)")] = None,
    seed_value: Annotated[int, typer.Option("--seed", help="Seed for reproducible generation")] = None,
    generation_mode: Annotated[str, typer.Option("--generation-mode", help="Generation mode (python, pyspark, auto)")] = None,
    ignore_state: Annotated[bool, typer.Option("--ignore-state", help="Ignore existing state")] = None,
    # Advanced parameters
    custom_schema: Annotated[str, typer.Option("--custom-schema", help="Custom schema JSON string")] = None,
    enable_partitioning: Annotated[bool, typer.Option("--enable-partitioning", help="Enable table partitioning")] = None,
    validate_only: Annotated[bool, typer.Option("--validate-only", help="Only validate parameters, don't execute")] = False,
    show_recommendations: Annotated[bool, typer.Option("--show-recommendations", help="Show parameter recommendations")] = False
):
    """Execute a generic notebook with runtime parameters."""
    from .python_libs.common.notebook_parameter_validator import NotebookParameterValidator, SmartDefaultProvider
    import json
    
    console.print(f"🚀 Preparing to execute generic notebook: {notebook_name}")
    
    try:
        # Build parameters dictionary
        parameters = {
            "dataset_id": dataset_id,
            "seed_value": seed_value,
            "generation_mode": generation_mode,
            "output_mode": output_mode,
            "custom_schema": custom_schema
        }
        
        # Determine notebook type and add specific parameters
        if "series" in notebook_name.lower():
            notebook_type = "incremental_series"
            if start_date: parameters["start_date"] = start_date
            if end_date: parameters["end_date"] = end_date
            if batch_size: parameters["batch_size"] = batch_size
            if path_format: parameters["path_format"] = path_format
            if ignore_state is not None: parameters["ignore_state"] = ignore_state
            
            # Set defaults if not provided
            if not start_date: parameters["start_date"] = "2024-01-01"
            if not end_date: parameters["end_date"] = "2024-01-30"
            if not batch_size: parameters["batch_size"] = 10
            
        elif "single" in notebook_name.lower():
            notebook_type = "single_dataset"
            if target_rows: parameters["target_rows"] = target_rows
            if scale_factor: parameters["scale_factor"] = scale_factor
            if enable_partitioning is not None: parameters["enable_partitioning"] = enable_partitioning
            
            # Set defaults if not provided
            if not target_rows: parameters["target_rows"] = 10000
            if not scale_factor: parameters["scale_factor"] = 1.0
        else:
            raise ValueError(f"Unknown notebook type from name: {notebook_name}")
        
        # Determine environment from notebook name
        if "lakehouse" in notebook_name.lower():
            parameters["target_environment"] = "lakehouse"
        elif "warehouse" in notebook_name.lower():
            parameters["target_environment"] = "warehouse"
        else:
            parameters["target_environment"] = "lakehouse"  # default
        
        # Remove None values
        parameters = {k: v for k, v in parameters.items() if v is not None}
        
        # Show recommendations if requested
        if show_recommendations:
            recommendations = SmartDefaultProvider.get_parameter_recommendations(
                notebook_type, parameters
            )
            console.print("\n💡 Parameter Recommendations:")
            console.print("-" * 50)
            for key, value in recommendations.items():
                current_value = parameters.get(key, "Not set")
                if current_value != value:
                    console.print(f"  {key:<20}: {current_value} → [green]{value}[/green] (recommended)")
                else:
                    console.print(f"  {key:<20}: {value} ✓")
            console.print("-" * 50)
            
            if not typer.confirm("Would you like to apply these recommendations?"):
                console.print("ℹ️ Continuing with current parameters...")
            else:
                parameters.update(recommendations)
        
        # Validate parameters
        console.print("🔍 Validating parameters...")
        if notebook_type == "incremental_series":
            is_valid, errors = NotebookParameterValidator.validate_incremental_series_parameters(parameters)
        else:
            is_valid, errors = NotebookParameterValidator.validate_single_dataset_parameters(parameters)
        
        if not is_valid:
            console.print("[red]❌ Parameter validation failed:[/red]")
            for error in errors:
                console.print(f"  • {error}")
            raise typer.Exit(1)
        
        console.print("[green]✅ Parameter validation passed[/green]")
        
        # Display parameters
        console.print("\n📋 Execution Parameters:")
        console.print("-" * 50)
        for key, value in sorted(parameters.items()):
            console.print(f"  {key:<20}: {value}")
        console.print("-" * 50)
        
        if validate_only:
            console.print("✅ Validation complete. Use without --validate-only to execute.")
            return
        
        # For now, just display the parameters that would be used
        # In a full implementation, this would actually execute the notebook with these parameters
        console.print(f"\n🎯 Would execute notebook '{notebook_name}' with the above parameters")
        console.print("💡 Note: Full notebook execution integration coming soon!")
        console.print("📝 For now, you can manually run the generic notebook and set these parameter values")
        
        # Show parameter cell content that user can copy
        console.print(f"\n📄 Parameter cell content to use:")
        console.print("=" * 60)
        for key, value in sorted(parameters.items()):
            if isinstance(value, str):
                console.print(f'{key} = "{value}"')
            else:
                console.print(f'{key} = {value}')
        console.print("=" * 60)
    
    except Exception as e:
        console.print(f"[red]❌ Error during execution: {e}[/red]")
        raise typer.Exit(1)


@synthetic_data_app.command("list-generic-templates")
def list_generic_templates(
    ctx: typer.Context,
    target_environment: Annotated[str, typer.Option("--target-environment", "-t", help="Filter by target environment")] = None
):
    """List available generic templates."""
    console.print("📄 Available Generic Templates:")
    console.print("=" * 80)
    
    templates = [
        {
            "name": "generic_incremental_series_lakehouse",
            "type": "Incremental Series",
            "environment": "lakehouse",
            "description": "Generate incremental data series over date ranges using PySpark"
        },
        {
            "name": "generic_incremental_series_warehouse", 
            "type": "Incremental Series",
            "environment": "warehouse",
            "description": "Generate incremental data series over date ranges using Python"
        },
        {
            "name": "generic_single_dataset_lakehouse",
            "type": "Single Dataset", 
            "environment": "lakehouse",
            "description": "Generate single datasets with configurable size using PySpark"
        }
    ]
    
    for template in templates:
        if target_environment and template["environment"] != target_environment:
            continue
            
        console.print(f"📄 {template['name']}")
        console.print(f"   Type: {template['type']}")
        console.print(f"   Environment: {template['environment']}")
        console.print(f"   Description: {template['description']}")
        console.print()
    
    console.print("💡 Use 'compile-generic-templates' to create these templates")
    console.print("💡 Use 'execute-with-parameters' to run templates with custom parameters")


if __name__ == "__main__":
    app()
