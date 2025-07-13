from __future__ import annotations

import os
from pathlib import Path

import typer
from rich.console import Console
from typing_extensions import Annotated

from ingen_fab.cli_utils import (
    deploy_commands,
    init_commands,
    notebook_commands,
    workspace_commands,
)
from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.ddl_scripts.notebook_generator import NotebookGenerator

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
    if fabric_workspace_repo_dir is None:
        env_val = os.environ.get("FABRIC_WORKSPACE_REPO_DIR")
        if env_val:
            console_styles.print_warning(
                console,
                "Falling back to FABRIC_WORKSPACE_REPO_DIR environment variable.",
            )
        fabric_workspace_repo_dir = (
            Path(env_val) if env_val else Path("./sample_project")
        )
    if fabric_environment is None:
        env_val = os.environ.get("FABRIC_ENVIRONMENT")
        if env_val:
            console_styles.print_warning(
                console, "Falling back to FABRIC_ENVIRONMENT environment variable."
            )
        fabric_environment = Path(env_val) if env_val else Path("development")

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
    output_mode: Annotated[str, typer.Option("--output-mode", "-o")] = None,
    generation_mode: Annotated[
        str, typer.Option("--generation-mode", "-g")
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
):
    """Compile the DDL notebooks in the specified project directory."""
    # Convert string parameters to enums
    if output_mode:
        output_mode = NotebookGenerator.OutputMode(output_mode)
    if generation_mode:
        generation_mode = NotebookGenerator.GenerationMode(generation_mode)
    
    notebook_commands.compile_ddl_notebooks(ctx, output_mode, generation_mode, verbose)


# Initialize commands


@init_app.command()
def init_solution(
    project_name: Annotated[str, typer.Option(...)] = "",
    path: Annotated[Path, typer.Option("--path")] = Path("."),
):
    init_commands.init_solution(project_name, path)


# Deploy commands


@deploy_app.command()
def deploy(ctx: typer.Context):
    deploy_commands.deploy_to_environment(ctx)


@deploy_app.command()
def delete_all(
    environment: Annotated[str, typer.Option("--environment", "-e")] = "development",
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
):
    workspace_commands.delete_workspace_items(environment, force)


@deploy_app.command()
def upload_python_libs(
    environment: Annotated[
        str, typer.Option("--environment", "-e", help="Fabric environment name")
    ] = "development_jr",
    project_path: Annotated[
        str, typer.Option("--project-path", "-p", help="Project path")
    ] = "sample_project",
):
    """Upload python_libs directory to Fabric config lakehouse using OneLakeUtils."""
    deploy_commands.upload_python_libs_to_config_lakehouse(
        environment=environment,
        project_path=project_path,
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


@ingest_app.command("compile")
def ingest_app_compile(
    ctx: typer.Context,
    template_vars: Annotated[str, typer.Option("--template-vars", "-t", help="JSON string of template variables")] = None,
    include_samples: Annotated[bool, typer.Option("--include-samples", "-s", help="Include sample data DDL and files")] = False,
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
    
    try:
        results = compile_flat_file_ingestion_package(
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
            template_vars=vars_dict,
            include_samples=include_samples
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


if __name__ == "__main__":
    app()
