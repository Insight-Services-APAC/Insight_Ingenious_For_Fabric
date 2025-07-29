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
    output_mode: Annotated[str, typer.Option("--output-mode", "-o", help="Output mode (table or parquet)")] = "table",
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
    
    valid_output_modes = ["table", "parquet"]
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
        if dataset_id:
            # Compile specific predefined dataset
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
            # Compile all synthetic data packages
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


@synthetic_data_app.command("generate")
def synthetic_data_generate(
    ctx: typer.Context,
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID to generate")],
    target_rows: Annotated[int, typer.Option("--target-rows", "-r", help="Number of rows to generate")] = 10000,
    target_environment: Annotated[str, typer.Option("--target-environment", "-e", help="Target environment (lakehouse or warehouse)")] = "lakehouse",
    generation_mode: Annotated[str, typer.Option("--generation-mode", "-m", help="Generation mode (python, pyspark, or auto)")] = "auto",
    seed_value: Annotated[int, typer.Option("--seed", "-s", help="Seed value for reproducible generation")] = None,
    execute_notebook: Annotated[bool, typer.Option("--execute", help="Execute the notebook after compilation")] = False,
    output_mode: Annotated[str, typer.Option("--output-mode", "-o", help="Output mode (table or parquet)")] = "table",
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
    
    valid_output_modes = ["table", "parquet"]
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
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID to generate incremental data for")],
    generation_date: Annotated[str, typer.Option("--date", "-d", help="Generation date (YYYY-MM-DD). Defaults to today")] = None,
    target_environment: Annotated[str, typer.Option("--target-environment", "-e", help="Target environment (lakehouse or warehouse)")] = "lakehouse",
    generation_mode: Annotated[str, typer.Option("--generation-mode", "-m", help="Generation mode (python, pyspark, or auto)")] = "auto",
    path_format: Annotated[str, typer.Option("--path-format", "-p", help="Path format (nested: /YYYY/MM/DD/ or flat: YYYYMMDD_)")] = "nested",
    seed_value: Annotated[int, typer.Option("--seed", "-s", help="Seed value for reproducible generation")] = None,
    execute_notebook: Annotated[bool, typer.Option("--execute", help="Execute the notebook after compilation")] = False,
):
    """Generate incremental synthetic data for a specific date."""
    import datetime
    
    # Default to today if no date provided
    if generation_date is None:
        generation_date = datetime.date.today().isoformat()
    
    console.print(f"[blue]🎲 Generating incremental synthetic data for dataset: {dataset_id}[/blue]")
    console.print(f"📅 Generation date: {generation_date}")
    console.print(f"🏗️ Target environment: {target_environment}")
    console.print(f"🔧 Generation mode: {generation_mode}")
    console.print(f"📁 Path format: {path_format}")
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
    
    valid_path_formats = ["nested", "flat"]
    if path_format not in valid_path_formats:
        console.print(f"[red]Error: Invalid path format '{path_format}'[/red]")
        console.print(f"[yellow]Valid path formats: {', '.join(valid_path_formats)}[/yellow]")
        raise typer.Exit(code=1)
    
    try:
        # Validate date format
        datetime.datetime.strptime(generation_date, "%Y-%m-%d")
    except ValueError:
        console.print(f"[red]Error: Invalid date format '{generation_date}'. Use YYYY-MM-DD format.[/red]")
        raise typer.Exit(code=1)
    
    from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
    
    # Initialize compiler
    compiler = IncrementalSyntheticDataGenerationCompiler(
        fabric_workspace_repo_dir=ctx.obj["fabric_workspace_repo_dir"],
        fabric_environment=ctx.obj["fabric_environment"]
    )
    
    try:
        # Get enhanced predefined configurations
        predefined_configs = compiler.get_enhanced_predefined_dataset_configs()
        
        if dataset_id not in predefined_configs:
            console.print(f"[red]Error: Unknown dataset_id '{dataset_id}'[/red]")
            console.print(f"[yellow]Available datasets: {', '.join(predefined_configs.keys())}[/yellow]")
            raise typer.Exit(code=1)
        
        # Get the dataset configuration
        dataset_config = predefined_configs[dataset_id].copy()
        if seed_value:
            dataset_config["incremental_config"]["seed_value"] = seed_value
        
        # Compile the notebook
        notebook_path = compiler.compile_incremental_dataset_notebook(
            dataset_config=dataset_config,
            generation_date=generation_date,
            target_environment=target_environment,
            generation_mode=generation_mode,
            path_format=path_format,
            state_management=True
        )
        
        console.print(f"[green]✅ Incremental notebook compiled: {notebook_path}[/green]")
        
        if execute_notebook:
            console.print("[yellow]📓 Executing notebook...[/yellow]")
            console.print("[yellow]Note: In a production environment, this would submit the notebook to Fabric for execution.[/yellow]")
        else:
            console.print(f"[dim]💡 To execute the notebook, add --execute flag or run it manually in your environment[/dim]")
        
        # Show path format information
        if path_format == "nested":
            console.print(f"[dim]📁 Files will be saved to: Files/synthetic_data/{dataset_id}/YYYY/MM/DD/[table_name].parquet[/dim]")
        else:
            console.print(f"[dim]📁 Files will be saved to: Files/synthetic_data/{dataset_id}/YYYYMMDD_[table_name].parquet[/dim]")
            
    except Exception as e:
        console.print(f"[red]❌ Error during incremental generation: {e}[/red]")
        raise typer.Exit(1)


@synthetic_data_app.command("generate-series")
def synthetic_data_generate_series(
    ctx: typer.Context,
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID to generate incremental series for")],
    start_date: Annotated[str, typer.Option("--start-date", "-s", help="Start date (YYYY-MM-DD)")],
    end_date: Annotated[str, typer.Option("--end-date", "-e", help="End date (YYYY-MM-DD)")],
    target_environment: Annotated[str, typer.Option("--target-environment", "-t", help="Target environment (lakehouse or warehouse)")] = "lakehouse",
    generation_mode: Annotated[str, typer.Option("--generation-mode", "-m", help="Generation mode (python, pyspark, or auto)")] = "auto",
    path_format: Annotated[str, typer.Option("--path-format", "-p", help="Path format (nested: /YYYY/MM/DD/ or flat: YYYYMMDD_)")] = "nested",
    batch_size: Annotated[int, typer.Option("--batch-size", "-b", help="Number of days to process in each batch")] = 30,
    seed_value: Annotated[int, typer.Option("--seed", help="Seed value for reproducible generation")] = None,
    execute_notebook: Annotated[bool, typer.Option("--execute", help="Execute the notebook after compilation")] = False,
):
    """Generate incremental synthetic data for a series of dates."""
    import datetime
    
    console.print(f"[blue]🎲 Generating incremental synthetic data series for dataset: {dataset_id}[/blue]")
    console.print(f"📅 Date range: {start_date} to {end_date}")
    console.print(f"🏗️ Target environment: {target_environment}")
    console.print(f"🔧 Generation mode: {generation_mode}")
    console.print(f"📁 Path format: {path_format}")
    console.print(f"📦 Batch size: {batch_size} days")
    if seed_value:
        console.print(f"🌱 Seed value: {seed_value}")
    
    # Validate date formats
    try:
        start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError:
        console.print(f"[red]Error: Invalid date format. Use YYYY-MM-DD format.[/red]")
        raise typer.Exit(code=1)
    
    if start_date_obj > end_date_obj:
        console.print(f"[red]Error: Start date must be before or equal to end date.[/red]")
        raise typer.Exit(code=1)
    
    total_days = (end_date_obj - start_date_obj).days + 1
    console.print(f"📊 Total days to generate: {total_days}")
    
    # Validate other parameters
    valid_generation_modes = ["python", "pyspark", "auto"]
    if generation_mode not in valid_generation_modes:
        console.print(f"[red]Error: Invalid generation mode '{generation_mode}'[/red]")
        raise typer.Exit(code=1)
    
    valid_target_environments = ["lakehouse", "warehouse"]
    if target_environment not in valid_target_environments:
        console.print(f"[red]Error: Invalid target environment '{target_environment}'[/red]")
        raise typer.Exit(code=1)
    
    valid_path_formats = ["nested", "flat"]
    if path_format not in valid_path_formats:
        console.print(f"[red]Error: Invalid path format '{path_format}'[/red]")
        raise typer.Exit(code=1)
    
    if batch_size <= 0:
        console.print(f"[red]Error: Batch size must be positive.[/red]")
        raise typer.Exit(code=1)
    
    from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
    
    # Initialize compiler
    compiler = IncrementalSyntheticDataGenerationCompiler(
        fabric_workspace_repo_dir=ctx.obj["fabric_workspace_repo_dir"],
        fabric_environment=ctx.obj["fabric_environment"]
    )
    
    try:
        # Get enhanced predefined configurations
        predefined_configs = compiler.get_enhanced_predefined_dataset_configs()
        
        if dataset_id not in predefined_configs:
            console.print(f"[red]Error: Unknown dataset_id '{dataset_id}'[/red]")
            console.print(f"[yellow]Available datasets: {', '.join(predefined_configs.keys())}[/yellow]")
            raise typer.Exit(code=1)
        
        # Get the dataset configuration
        dataset_config = predefined_configs[dataset_id].copy()
        if seed_value:
            dataset_config["incremental_config"]["seed_value"] = seed_value
        
        # Compile the series notebook
        notebook_path = compiler.compile_incremental_dataset_series_notebook(
            dataset_config=dataset_config,
            start_date=start_date,
            end_date=end_date,
            target_environment=target_environment,
            generation_mode=generation_mode,
            path_format=path_format,
            batch_size=batch_size
        )
        
        console.print(f"[green]✅ Incremental series notebook compiled: {notebook_path}[/green]")
        
        if execute_notebook:
            console.print("[yellow]📓 Executing notebook...[/yellow]")
            console.print("[yellow]Note: In a production environment, this would submit the notebook to Fabric for execution.[/yellow]")
        else:
            console.print(f"[dim]💡 To execute the notebook, add --execute flag or run it manually in your environment[/dim]")
        
        # Show estimated work
        estimated_batches = (total_days + batch_size - 1) // batch_size  # Ceiling division
        console.print(f"[dim]📊 Will process in approximately {estimated_batches} batches[/dim]")
        
        # Show path format information
        if path_format == "nested":
            console.print(f"[dim]📁 Files will be saved to: Files/synthetic_data/{dataset_id}/YYYY/MM/DD/[table_name].parquet[/dim]")
        else:
            console.print(f"[dim]📁 Files will be saved to: Files/synthetic_data/{dataset_id}/YYYYMMDD_[table_name].parquet[/dim]")
            
    except Exception as e:
        console.print(f"[red]❌ Error during series generation: {e}[/red]")
        raise typer.Exit(1)


@synthetic_data_app.command("list-incremental-datasets")
def synthetic_data_list_incremental_datasets():
    """List available predefined dataset configurations with incremental capabilities."""
    from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler
    
    compiler = IncrementalSyntheticDataGenerationCompiler()
    configs = compiler.get_enhanced_predefined_dataset_configs()
    
    console.print("[bold blue]📊 Available Incremental Synthetic Dataset Configurations[/bold blue]")
    console.print()
    
    for dataset_id, config in configs.items():
        console.print(f"[bold]{dataset_id}[/bold]")
        console.print(f"  Name: {config['dataset_name']}")
        console.print(f"  Type: {config['dataset_type']} ({config['schema_pattern']})")
        console.print(f"  Domain: {config['domain']}")
        console.print(f"  Description: {config['description']}")
        
        # Show table configuration summary
        table_configs = config.get('table_configs', {})
        if table_configs:
            snapshot_tables = [name for name, tconfig in table_configs.items() if tconfig.get('type') == 'snapshot']
            incremental_tables = [name for name, tconfig in table_configs.items() if tconfig.get('type') == 'incremental']
            
            if snapshot_tables:
                console.print(f"  Snapshot Tables: {', '.join(snapshot_tables)}")
            if incremental_tables:
                console.print(f"  Incremental Tables: {', '.join(incremental_tables)}")
        
        # Show incremental configuration highlights
        incremental_config = config.get('incremental_config', {})
        if incremental_config:
            growth_rate = incremental_config.get('growth_rate', 0) * 100
            console.print(f"  Daily Growth Rate: {growth_rate:.2f}%")
            seasonal = incremental_config.get('enable_seasonal_patterns', False)
            console.print(f"  Seasonal Patterns: {'Yes' if seasonal else 'No'}")
        
        console.print()
```
