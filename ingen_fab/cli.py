from __future__ import annotations

import os
from pathlib import Path

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
def perform_code_replacements(ctx: typer.Context):
    deploy_commands.perform_code_replacements(ctx)


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
            inject_variables_into_file(project_path, target_path, environment)
            console.print(f"[green]✓ Successfully compiled: {target_file}[/green]")
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
            inject_variables_into_file(project_path, config_utils_path, environment)
            console.print(f"[green]✓ Successfully compiled config_utils.py[/green]")
        except Exception as e:
            console.print(f"[red]Error compiling config_utils.py: {e}[/red]")
            raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
