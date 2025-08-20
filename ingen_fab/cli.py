from __future__ import annotations

import os
import shutil
import subprocess
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
synthetic_data_commands = lazy_import.lazy_module(
    "ingen_fab.cli_utils.synthetic_data_commands"
)
package_commands = lazy_import.lazy_module("ingen_fab.cli_utils.package_commands")
test_commands = lazy_import.lazy_module("ingen_fab.cli_utils.test_commands")
libs_commands = lazy_import.lazy_module("ingen_fab.cli_utils.libs_commands")
dbt_commands = lazy_import.lazy_module("ingen_fab.cli_utils.dbt_commands")
profile_commands = lazy_import.lazy_module("ingen_fab.cli_utils.profile_commands")

from ingen_fab.cli_utils.console_styles import ConsoleStyles

# Lazy import for heavyweight modules
notebook_generator = lazy_import.lazy_module("ingen_fab.ddl_scripts.notebook_generator")
PathUtils = lazy_import.lazy_callable(
    "ingen_fab.python_libs.common.utils.path_utils.PathUtils"
)

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
dbt_app = typer.Typer()
profile_app = typer.Typer()

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
app.add_typer(
    dbt_app,
    name="dbt",
    help="Proxy commands to dbt_wrapper inside the Fabric workspace repo.",
)
app.add_typer(
    profile_commands.app,
    name="profile",
    help="Commands for data profiling and quality analysis.",
)

# New: extract commands
app.add_typer(
    extract_app,
    name="extract",
    help="Data extraction and package commands (keep compile, extract-run).",
)


@app.callback()
def main(
    ctx: typer.Context,
    fabric_workspace_repo_dir: Annotated[
        Optional[Path],
        typer.Option(
            "--fabric-workspace-repo-dir",
            "-fwd",
            help="Directory containing fabric workspace repository files",
        ),
    ] = None,
    fabric_environment: Annotated[
        Optional[Path],
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
            from ingen_fab.python_libs.common.utils.path_utils import PathUtils

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
        ctx.invoked_subcommand is None
        or (ctx.params and ctx.params.get("help"))
        or "--help" in sys.argv
        or "-h" in sys.argv
        or (
            ctx.invoked_subcommand == "init"
            and len(sys.argv) > 2
            and sys.argv[2] == "new"
        )
    )

    if not skip_validation:
        # Validate that both fabric_environment and fabric_workspace_repo_dir are explicitly set
        if fabric_environment_source == "default":
            console_styles.print_error(
                console,
                "❌ FABRIC_ENVIRONMENT must be set. Use --fabric-environment or set the FABRIC_ENVIRONMENT environment variable.",
            )
            raise typer.Exit(code=1)

        if fabric_workspace_repo_dir_source == "default":
            console_styles.print_error(
                console,
                "❌ FABRIC_WORKSPACE_REPO_DIR must be set. Use --fabric-workspace-repo-dir or set the FABRIC_WORKSPACE_REPO_DIR environment variable.",
            )
            raise typer.Exit(code=1)

        # Validate that fabric_workspace_repo_dir exists
        if not fabric_workspace_repo_dir.exists():
            console_styles.print_error(
                console,
                f"❌ Fabric workspace repository directory does not exist: {fabric_workspace_repo_dir}",
            )
            console_styles.print_info(
                console,
                "💡 Use 'ingen_fab init new --project-name <name>' to create a new project.",
            )
            raise typer.Exit(code=1)

        if not fabric_workspace_repo_dir.is_dir():
            console_styles.print_error(
                console,
                f"❌ Fabric workspace repository path is not a directory: {fabric_workspace_repo_dir}",
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
    output_mode: Annotated[
        Optional[str],
        typer.Option(
            "--output-mode", "-o", help="Output mode: fabric_workspace_repo or local"
        ),
    ] = None,
    generation_mode: Annotated[
        Optional[str],
        typer.Option(
            "--generation-mode", "-g", help="Generation mode: Lakehouse or Warehouse"
        ),
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
):
    """Compile the DDL notebooks in the specified project directory."""

    # Convert string parameters to enums with proper error handling
    # notebook_generator module is already lazy imported at the top of the file

    if output_mode:
        try:
            output_mode = notebook_generator.NotebookGenerator.OutputMode(output_mode)
        except ValueError:
            valid_modes = [
                mode.value for mode in notebook_generator.NotebookGenerator.OutputMode
            ]
            console.print(f"[red]Error: Invalid output mode '{output_mode}'[/red]")
            console.print(
                f"[yellow]Valid output modes: {', '.join(valid_modes)}[/yellow]"
            )
            raise typer.Exit(code=1)

    if generation_mode:
        try:
            generation_mode = notebook_generator.NotebookGenerator.GenerationMode(
                generation_mode
            )
        except ValueError:
            valid_modes = [
                mode.value
                for mode in notebook_generator.NotebookGenerator.GenerationMode
            ]
            console.print(
                f"[red]Error: Invalid generation mode '{generation_mode}'[/red]"
            )
            console.print(
                f"[yellow]Valid generation modes: {', '.join(valid_modes)}[/yellow]"
            )
            raise typer.Exit(code=1)

    notebook_commands.compile_ddl_notebooks(ctx, output_mode, generation_mode, verbose)


# Initialize commands


@init_app.command("new")
def init_solution(
    project_name: Annotated[
        Optional[str],
        typer.Option("--project-name", "-p", help="Name of the project to create"),
    ] = None,
    path: Annotated[
        Path, typer.Option("--path", help="Base path where the project will be created")
    ] = Path("."),
):
    """Create a new Fabric Project."""
    init_commands.init_solution(project_name, path)


@init_app.command("workspace")
def init_workspace(
    ctx: typer.Context,
    workspace_name: Annotated[
        str,
        typer.Option(
            "--workspace-name",
            "-w",
            help="Name of the Fabric workspace to lookup and configure",
        ),
    ],
    create_if_not_exists: Annotated[
        bool,
        typer.Option(
            "--create-if-not-exists",
            "-c",
            help="Create the workspace if it doesn't exist",
        ),
    ] = False,
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
        environment=ctx.obj["fabric_environment"],
        project_path=ctx.obj["fabric_workspace_repo_dir"],
        force=force,
    )


@deploy_app.command()
def upload_python_libs(ctx: typer.Context):
    """Inject code into python_libs (in-place) and upload to Fabric config lakehouse."""
    deploy_commands.upload_python_libs_to_config_lakehouse(
        environment=ctx.obj["fabric_environment"],
        project_path=ctx.obj["fabric_workspace_repo_dir"],
        console=console,
    )


@deploy_app.command("get-metadata")
def deploy_get_metadata(
    ctx: typer.Context,
    workspace_id: Annotated[
        Optional[str], typer.Option("--workspace-id", help="Workspace ID")
    ] = None,
    workspace_name: Annotated[
        Optional[str], typer.Option("--workspace-name", help="Workspace name")
    ] = None,
    lakehouse_id: Annotated[
        Optional[str], typer.Option("--lakehouse-id", help="Lakehouse ID")
    ] = None,
    lakehouse_name: Annotated[
        Optional[str], typer.Option("--lakehouse-name", help="Lakehouse name")
    ] = None,
    warehouse_id: Annotated[
        Optional[str], typer.Option("--warehouse-id", help="Warehouse ID")
    ] = None,
    warehouse_name: Annotated[
        Optional[str], typer.Option("--warehouse-name", help="Warehouse name")
    ] = None,
    schema: Annotated[
        Optional[str], typer.Option("--schema", "-s", help="Schema name filter")
    ] = None,
    table: Annotated[
        Optional[str],
        typer.Option("--table", "-t", help="Table name filter (substring match)"),
    ] = None,
    method: Annotated[
        str,
        typer.Option(
            "--method",
            "-m",
            help="Extraction method: 'sql-endpoint' (default) or 'sql-endpoint-odbc'",
        ),
    ] = "sql-endpoint",
    sql_endpoint_id: Annotated[
        Optional[str],
        typer.Option("--sql-endpoint-id", help="Explicit SQL endpoint ID to use"),
    ] = None,
    sql_endpoint_server: Annotated[
        Optional[str],
        typer.Option(
            "--sql-endpoint-server",
            help=(
                "SQL endpoint server prefix (without domain), e.g., 'myws-abc123' to form myws-abc123.datawarehouse.fabric.microsoft.com"
            ),
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format", "-f", help="Output format: csv (default), json, or table"
        ),
    ] = "csv",
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output", "-o", help="Write output to file (defaults to metadata cache)"
        ),
    ] = None,
    all_lakehouses: Annotated[
        bool,
        typer.Option(
            "--all", help="For lakehouse target, extract all lakehouses in workspace"
        ),
    ] = False,
    target: Annotated[
        str,
        typer.Option(
            "--target",
            "-tgt",
            help="Target asset type: 'lakehouse', 'warehouse', or 'both' (default lakehouse)",
        ),
    ] = "lakehouse",
):
    """Get schema/table/column metadata for lakehouse/warehouse/both.

    This consolidates prior extract commands under deploy for easier access.
    """
    from ingen_fab.cli_utils import extract_commands

    selected = (target or "lakehouse").strip().lower()
    if selected not in {"lakehouse", "warehouse", "both"}:
        console_styles.print_error(
            console, "--target must be one of: lakehouse, warehouse, both"
        )
        raise typer.Exit(code=2)

    if selected in {"lakehouse", "both"}:
        extract_commands.lakehouse_metadata(
            ctx=ctx,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            lakehouse_id=lakehouse_id,
            lakehouse_name=lakehouse_name,
            schema=schema,
            table_filter=table,
            method=method,
            sql_endpoint_id=sql_endpoint_id,
            sql_endpoint_server=sql_endpoint_server,
            output_format=output_format,
            output_path=output,
            all_lakehouses=all_lakehouses,
        )

    if selected in {"warehouse", "both"}:
        extract_commands.warehouse_metadata(
            ctx=ctx,
            workspace_id=workspace_id,
            workspace_name=workspace_name,
            warehouse_id=warehouse_id,
            warehouse_name=warehouse_name,
            schema=schema,
            table_filter=table,
            method=method,
            sql_endpoint_id=sql_endpoint_id,
            sql_endpoint_server=sql_endpoint_server,
            output_format=output_format,
            output_path=output,
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
    test_commands.test_platform_generate(ctx)


# Pytest execution command for python_libs_tests/pyspark


@test_local_app.command()
def pyspark(
    lib: Annotated[
        Optional[str],
        typer.Argument(
            help="Optional test file (without _pytest.py) to run, e.g. 'my_utils'"
        ),
    ] = None,
):
    """Run pytest on ingen_fab/python_libs_tests/pyspark or a specific test file if provided."""
    test_commands.test_local_pyspark(lib)


@test_local_app.command()
def python(
    lib: Annotated[
        Optional[str],
        typer.Argument(
            help="Optional test file (without _pytest.py) to run, e.g. 'ddl_utils'"
        ),
    ] = None,
):
    """Run pytest on ingen_fab/python_libs_tests/python or a specific test file if provided."""
    test_commands.test_local_python(lib)


@test_local_app.command()
def common(
    lib: Annotated[
        Optional[str],
        typer.Argument(
            help="Optional test file (without _pytest.py) to run, e.g. 'my_utils'"
        ),
    ] = None,
):
    """Run pytest on ingen_fab/python_libs_tests/common or a specific test file if provided."""
    test_commands.test_local_common(lib)


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
    deploy_commands.perform_code_replacements(
        ctx, output_dir=output_dir, preserve_structure=not no_preserve_structure
    )


# Extract commands (moved to deploy.get-metadata)
## Removed: lakehouse-metadata (now under deploy get-metadata)
def extract_lakehouse_metadata(
    ctx: typer.Context,
    workspace_id: Annotated[
        Optional[str],
        typer.Option(
            "--workspace-id",
            "-w",
            help="Workspace ID (overrides environment workspace)",
        ),
    ] = None,
    workspace_name: Annotated[
        Optional[str],
        typer.Option(
            "--workspace-name",
            "-wn",
            help="Workspace name (resolved to ID if provided)",
        ),
    ] = None,
    lakehouse_id: Annotated[
        Optional[str],
        typer.Option("--lakehouse-id", "-l", help="Target Lakehouse ID"),
    ] = None,
    lakehouse_name: Annotated[
        Optional[str],
        typer.Option("--lakehouse-name", "-ln", help="Target Lakehouse name"),
    ] = None,
    schema: Annotated[
        Optional[str],
        typer.Option("--schema", "-s", help="Schema to include (exact match)"),
    ] = None,
    table: Annotated[
        Optional[str],
        typer.Option("--table", "-t", help="Table name filter (substring match)"),
    ] = None,
    method: Annotated[
        str,
        typer.Option(
            "--method",
            "-m",
            help="Extraction method: 'sql-endpoint' (default) or 'onelake'",
        ),
    ] = "sql-endpoint",
    sql_endpoint_id: Annotated[
        Optional[str],
        typer.Option(
            "--sql-endpoint-id",
            help="Explicit SQL endpoint ID to use for the lakehouse",
        ),
    ] = None,
    sql_endpoint_server: Annotated[
        Optional[str],
        typer.Option(
            "--sql-endpoint-server",
            help=(
                "SQL endpoint server prefix (without domain), e.g., 'myws-abc123' to form myws-abc123.datawarehouse.fabric.microsoft.com"
            ),
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format", "-f", help="Output format: csv (default), json, or table"
        ),
    ] = "csv",
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output", "-o", help="Write output to file (defaults to stdout)"
        ),
    ] = None,
    all_lakehouses: Annotated[
        bool,
        typer.Option(
            "--all",
            help="Extract metadata for all lakehouses in the resolved workspace",
        ),
    ] = False,
):
    """Extract lakehouse schema/table/column metadata.

    Uses the lakehouse SQL endpoint to query INFORMATION_SCHEMA and sys.* views.
    """
    from ingen_fab.cli_utils import extract_commands  # backward compat shim

    extract_commands.lakehouse_metadata(
        ctx=ctx,
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        lakehouse_id=lakehouse_id,
        lakehouse_name=lakehouse_name,
        schema=schema,
        table_filter=table,
        method=method,
        sql_endpoint_id=sql_endpoint_id,
        sql_endpoint_server=sql_endpoint_server,
        output_format=output_format,
        output_path=output,
        all_lakehouses=all_lakehouses,
    )


## Removed: warehouse-metadata (now under deploy get-metadata)
def extract_warehouse_metadata(
    ctx: typer.Context,
    workspace_id: Annotated[
        Optional[str], typer.Option("--workspace-id", help="Workspace ID")
    ] = None,
    workspace_name: Annotated[
        Optional[str], typer.Option("--workspace-name", help="Workspace name")
    ] = None,
    warehouse_id: Annotated[
        Optional[str], typer.Option("--warehouse-id", help="Warehouse ID")
    ] = None,
    warehouse_name: Annotated[
        Optional[str], typer.Option("--warehouse-name", help="Warehouse name")
    ] = None,
    schema: Annotated[
        Optional[str], typer.Option("--schema", "-s", help="Schema name filter")
    ] = None,
    table: Annotated[
        Optional[str],
        typer.Option("--table", "-t", help="Table name filter (substring match)"),
    ] = None,
    method: Annotated[
        str,
        typer.Option(
            "--method",
            "-m",
            help="Extraction method: 'sql-endpoint' (default) or 'sql-endpoint-odbc'",
        ),
    ] = "sql-endpoint",
    sql_endpoint_id: Annotated[
        Optional[str],
        typer.Option(
            "--sql-endpoint-id",
            help="Explicit SQL endpoint ID to use for the warehouse",
        ),
    ] = None,
    sql_endpoint_server: Annotated[
        Optional[str],
        typer.Option(
            "--sql-endpoint-server",
            help=(
                "SQL endpoint server prefix (without domain), e.g., 'myws-abc123' to form myws-abc123.datawarehouse.fabric.microsoft.com"
            ),
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format", "-f", help="Output format: csv (default), json, or table"
        ),
    ] = "csv",
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output", "-o", help="Write output to file (defaults to stdout)"
        ),
    ] = None,
):
    """Extract warehouse schema/table/column metadata via Fabric SQL endpoint.

    Uses INFORMATION_SCHEMA and joins to enrich with table_type where available.
    """
    from ingen_fab.cli_utils import extract_commands  # backward compat shim

    extract_commands.warehouse_metadata(
        ctx=ctx,
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        warehouse_id=warehouse_id,
        warehouse_name=warehouse_name,
        schema=schema,
        table_filter=table,
        method=method,
        sql_endpoint_id=sql_endpoint_id,
        sql_endpoint_server=sql_endpoint_server,
        output_format=output_format,
        output_path=output,
    )


## Removed: lakehouse-summary (now covered by deploy get-metadata output files)
def extract_lakehouse_summary(
    ctx: typer.Context,
    workspace_id: Annotated[
        Optional[str], typer.Option("--workspace-id", help="Workspace ID")
    ] = None,
    workspace_name: Annotated[
        Optional[str], typer.Option("--workspace-name", help="Workspace name")
    ] = None,
    lakehouse_id: Annotated[
        Optional[str], typer.Option("--lakehouse-id", help="Lakehouse ID")
    ] = None,
    lakehouse_name: Annotated[
        Optional[str], typer.Option("--lakehouse-name", help="Lakehouse name")
    ] = None,
    method: Annotated[
        str,
        typer.Option(
            "--method",
            "-m",
            help="Extraction method: 'sql-endpoint' (default) or 'sql-endpoint-odbc'",
        ),
    ] = "sql-endpoint",
    sql_endpoint_id: Annotated[
        Optional[str],
        typer.Option(
            "--sql-endpoint-id",
            help="Explicit SQL endpoint ID to use for the lakehouse",
        ),
    ] = None,
    sql_endpoint_server: Annotated[
        Optional[str],
        typer.Option(
            "--sql-endpoint-server",
            help=(
                "SQL endpoint server prefix (without domain), e.g., 'myws-abc123' to form myws-abc123.datawarehouse.fabric.microsoft.com"
            ),
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format", "-f", help="Output format: csv (default), json, or table"
        ),
    ] = "csv",
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output", "-o", help="Write output to file (defaults to stdout)"
        ),
    ] = None,
    all_lakehouses: Annotated[
        bool,
        typer.Option(
            "--all",
            help="Summarize all lakehouses in the resolved workspace",
        ),
    ] = False,
):
    """Summarize lakehouse tables by schema (database, schema, table_count)."""
    from ingen_fab.cli_utils import extract_commands

    extract_commands.lakehouse_summary(
        ctx=ctx,
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        lakehouse_id=lakehouse_id,
        lakehouse_name=lakehouse_name,
        method=method,
        sql_endpoint_id=sql_endpoint_id,
        sql_endpoint_server=sql_endpoint_server,
        output_format=output_format,
        output_path=output,
        all_lakehouses=all_lakehouses,
    )


## Removed: warehouse-summary (now covered by deploy get-metadata output files)
def extract_warehouse_summary(
    ctx: typer.Context,
    workspace_id: Annotated[
        Optional[str], typer.Option("--workspace-id", help="Workspace ID")
    ] = None,
    workspace_name: Annotated[
        Optional[str], typer.Option("--workspace-name", help="Workspace name")
    ] = None,
    warehouse_id: Annotated[
        Optional[str], typer.Option("--warehouse-id", help="Warehouse ID")
    ] = None,
    warehouse_name: Annotated[
        Optional[str], typer.Option("--warehouse-name", help="Warehouse name")
    ] = None,
    method: Annotated[
        str,
        typer.Option(
            "--method",
            "-m",
            help="Extraction method: 'sql-endpoint' (default) or 'sql-endpoint-odbc'",
        ),
    ] = "sql-endpoint",
    sql_endpoint_id: Annotated[
        Optional[str],
        typer.Option(
            "--sql-endpoint-id",
            help="Explicit SQL endpoint ID to use for the warehouse",
        ),
    ] = None,
    sql_endpoint_server: Annotated[
        Optional[str],
        typer.Option(
            "--sql-endpoint-server",
            help=(
                "SQL endpoint server prefix (without domain), e.g., 'myws-abc123' to form myws-abc123.datawarehouse.fabric.microsoft.com"
            ),
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--format", "-f", help="Output format: csv (default), json, or table"
        ),
    ] = "csv",
    output: Annotated[
        Optional[Path],
        typer.Option(
            "--output", "-o", help="Write output to file (defaults to stdout)"
        ),
    ] = None,
):
    """Summarize warehouse tables by schema (database, schema, table_count)."""
    from ingen_fab.cli_utils import extract_commands

    extract_commands.warehouse_summary(
        ctx=ctx,
        workspace_id=workspace_id,
        workspace_name=workspace_name,
        warehouse_id=warehouse_id,
        warehouse_name=warehouse_name,
        method=method,
        sql_endpoint_id=sql_endpoint_id,
        sql_endpoint_server=sql_endpoint_server,
        output_format=output_format,
        output_path=output,
    )


# DBT commands
@dbt_app.command("create-notebooks")
def dbt_create_notebooks(
    ctx: typer.Context,
    dbt_project: Annotated[
        str,
        typer.Option(
            "--dbt-project",
            "-p",
            help="Name of the dbt project directory under the workspace repo",
        ),
    ],
    skip_profile_confirmation: Annotated[
        bool,
        typer.Option(
            "--skip-profile-confirmation",
            help="Skip confirmation prompt when updating dbt profile",
        ),
    ] = False,
):
    """Create Fabric notebooks from dbt-generated Python notebooks.

    Scans {workspace}/{dbt_project}/target/notebooks_fabric_py and creates notebooks under
    {workspace}/fabric_workspace_items/{dbt_project}/.
    """
    dbt_commands.create_additional_notebooks(
        ctx, dbt_project, skip_profile_confirmation
    )


@dbt_app.command("convert-metadata")
def dbt_convert_metadata(
    ctx: typer.Context,
    dbt_project: Annotated[
        str,
        typer.Option(
            "--dbt-project",
            "-p",
            help="Name of the dbt project directory under the workspace repo",
        ),
    ],
    skip_profile_confirmation: Annotated[
        bool,
        typer.Option(
            "--skip-profile-confirmation",
            help="Skip confirmation prompt when updating dbt profile",
        ),
    ] = False,
):
    """Convert cached lakehouse metadata to dbt metaextracts format.

    Reads from {workspace}/metadata/lakehouse_metadata_all.csv and creates JSON files
    in {workspace}/{dbt_project}/metaextracts/ for dbt_wrapper to use.

    The metadata must first be extracted using:
    ingen_fab deploy get-metadata --target lakehouse
    """
    dbt_commands.convert_metadata_to_dbt_format(
        ctx, dbt_project, skip_profile_confirmation
    )


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
    help="Generate synthetic data for testing and development. Supports predefined datasets, custom templates, and runtime parameterization.",
)


# ===== NEW UNIFIED COMMANDS =====


@synthetic_data_app.command("generate")
def synthetic_data_unified_generate(
    ctx: typer.Context,
    config: Annotated[
        str,
        typer.Argument(
            help="Dataset ID (e.g. 'retail_oltp_small') or generic template name (e.g. 'generic_single_dataset_lakehouse')"
        ),
    ],
    mode: Annotated[
        str,
        typer.Option(
            "--mode",
            "-m",
            help="Generation mode: 'single' (one-time), 'incremental' (date-based), or 'series' (date range)",
        ),
    ] = "single",
    parameters: Annotated[
        Optional[str],
        typer.Option(
            "--parameters",
            "-p",
            help='JSON string of runtime parameters (e.g. \'{"target_rows": 50000, "seed_value": 42}\')',
        ),
    ] = None,
    output_path: Annotated[
        Optional[str],
        typer.Option(
            "--output-path",
            "-o",
            help="Override default output directory (e.g. 'my_custom_dir')",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Validate parameters without generating files"),
    ] = False,
    target_environment: Annotated[
        str,
        typer.Option(
            "--target-environment",
            "-e",
            help="Target environment: 'lakehouse' or 'warehouse'",
        ),
    ] = "lakehouse",
    no_execute: Annotated[
        bool,
        typer.Option(
            "--no-execute",
            help="Compile notebook only without executing (useful for testing/validation)",
        ),
    ] = False,
):
    """
    Generate synthetic data notebooks and optionally execute them.

    This command creates ready-to-run notebooks with specific parameters injected,
    and by default executes them immediately to generate the synthetic data.

    Examples:
      # Generate and execute retail data (10K rows by default)
      ingen_fab package synthetic-data generate retail_oltp_small

      # Generate incremental data for specific date
      ingen_fab package synthetic-data generate retail_oltp_small --mode incremental --parameters '{"generation_date": "2024-01-15"}'

      # Compile only (don't execute)
      ingen_fab package synthetic-data generate retail_oltp_small --no-execute

      # Custom parameters with higher row count
      ingen_fab package synthetic-data generate retail_oltp_small --parameters '{"target_rows": 100000, "seed_value": 42}'
    """
    synthetic_data_commands.unified_generate(
        ctx=ctx,
        config=config,
        mode=mode,
        parameters=parameters,
        output_path=output_path,
        dry_run=dry_run,
        target_environment=target_environment,
        no_execute=no_execute,
    )


# ===== DBT WRAPPER COMMANDS =====


@dbt_app.command(
    "exec", context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def dbt_exec(
    ctx: typer.Context,
):
    """Run dbt_wrapper from within the Fabric workspace repo, then return to the original directory."""
    from ingen_fab.cli_utils.dbt_profile_manager import ensure_dbt_profile_for_exec

    workspace_dir = Path(ctx.obj["fabric_workspace_repo_dir"]).resolve()
    if not workspace_dir.exists():
        console_styles.print_error(
            console, f"❌ Fabric workspace repo not found: {workspace_dir}"
        )
        raise typer.Exit(code=1)

    # Check and update dbt profile with exec-specific behavior
    # Always prompts if saved info is missing/invalid, notifies if using saved preference
    if not ensure_dbt_profile_for_exec(ctx):
        raise typer.Exit(code=1)

    # Locate the wrapper executable
    exe = shutil.which("dbt_wrapper") or shutil.which("dbt-wrapper")
    if not exe:
        console_styles.print_error(
            console,
            "❌ Could not find 'dbt_wrapper' on PATH. Ensure the wrapper is installed.",
        )
        raise typer.Exit(code=1)

    # Always return to the original directory, even on failure
    original_cwd = Path.cwd()
    try:
        os.chdir(workspace_dir)
        passthrough_args = list(ctx.args)
        console_styles.print_info(
            console,
            f"Running dbt_wrapper in: {workspace_dir} -> {' '.join(passthrough_args) or '--help'}",
        )
        result = subprocess.run([exe, *passthrough_args], check=False)
        rc = result.returncode
    finally:
        os.chdir(original_cwd)
        console_styles.print_info(console, f"Returned to: {original_cwd}")

    if rc != 0:
        raise typer.Exit(code=rc)


@synthetic_data_app.command("list")
def synthetic_data_unified_list(
    list_type: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
            help="What to list: 'datasets' (predefined configs), 'templates' (generic templates), or 'all'",
        ),
    ] = "all",
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help="Output format: 'table' (formatted) or 'json' (machine-readable)",
        ),
    ] = "table",
):
    """
    List available synthetic data datasets and templates.

    Shows predefined dataset configurations and generic templates that can be used
    with the generate and compile commands.

    Examples:
      # List all available items
      ingen_fab package synthetic-data list

      # Show only predefined datasets
      ingen_fab package synthetic-data list --type datasets

      # Output in JSON format for scripting
      ingen_fab package synthetic-data list --format json
    """
    synthetic_data_commands.unified_list(
        list_type=list_type, output_format=output_format
    )


@synthetic_data_app.command("compile")
def synthetic_data_unified_compile(
    ctx: typer.Context,
    template: Annotated[
        Optional[str],
        typer.Argument(
            help="Template name (e.g. 'generic_single_dataset_lakehouse') or dataset ID for standard compilation. If not provided, compiles all available templates."
        ),
    ] = None,
    runtime_config: Annotated[
        Optional[str],
        typer.Option(
            "--runtime-config",
            "-c",
            help="JSON configuration for template compilation (e.g. '{\"target_rows\": 50000}')",
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            "--output-format",
            "-f",
            help="Output artifacts: 'notebook' (only notebook), 'ddl' (only DDL), or 'all' (both)",
        ),
    ] = "all",
    target_environment: Annotated[
        str,
        typer.Option(
            "--target-environment",
            "-e",
            help="Target environment: 'lakehouse' or 'warehouse'",
        ),
    ] = "lakehouse",
):
    """
    Compile template notebooks with placeholders for later parameterization.

    This command creates template notebooks that can be parameterized and executed later,
    as opposed to the 'generate' command which creates ready-to-run notebooks.

    Useful for creating reusable notebook templates that can be deployed and
    parameterized at runtime in different environments.

    Examples:
      # Compile all available templates
      ingen_fab package synthetic-data compile

      # Compile a specific generic template for lakehouse
      ingen_fab package synthetic-data compile generic_single_dataset_lakehouse

      # Compile with runtime configuration
      ingen_fab package synthetic-data compile generic_single_dataset_lakehouse --runtime-config '{"language_group": "python"}'

      # Compile only notebooks for all templates (no DDL)
      ingen_fab package synthetic-data compile --output-format notebook

      # Compile all templates for warehouse environment
      ingen_fab package synthetic-data compile --target-environment warehouse
    """
    synthetic_data_commands.unified_compile(
        ctx=ctx,
        template=template,
        runtime_config=runtime_config,
        output_format=output_format,
        target_environment=target_environment,
    )


@ingest_app.command("compile")
def ingest_app_compile(
    ctx: typer.Context,
    template_vars: Annotated[
        Optional[str],
        typer.Option("--template-vars", "-t", help="JSON string of template variables"),
    ] = None,
    include_samples: Annotated[
        bool,
        typer.Option(
            "--include-samples", "-s", help="Include sample data DDL and files"
        ),
    ] = False,
    target_datastore: Annotated[
        str,
        typer.Option(
            "--target-datastore",
            "-d",
            help="Target datastore type: lakehouse, warehouse, or both",
        ),
    ] = "lakehouse",
    add_debug_cells: Annotated[
        bool,
        typer.Option(
            "--add-debug-cells",
            help="Add debug cells with embedded configurations for testing",
        ),
    ] = False,
):
    """Compile flat file ingestion package templates and DDL scripts."""
    package_commands.ingest_compile(
        ctx=ctx,
        template_vars=template_vars,
        include_samples=include_samples,
        target_datastore=target_datastore,
        add_debug_cells=add_debug_cells,
    )


@ingest_app.command()
def run(
    ctx: typer.Context,
    config_id: Annotated[
        str,
        typer.Option("--config-id", "-c", help="Specific configuration ID to process"),
    ] = "",
    execution_group: Annotated[
        int, typer.Option("--execution-group", "-g", help="Execution group number")
    ] = 1,
    environment: Annotated[
        str, typer.Option("--environment", "-e", help="Environment name")
    ] = "development",
):
    """Run flat file ingestion for specified configuration or execution group."""
    package_commands.ingest_run(
        ctx=ctx,
        config_id=config_id,
        execution_group=execution_group,
        environment=environment,
    )


@synapse_app.command("compile")
def synapse_app_compile(
    ctx: typer.Context,
    template_vars: Annotated[
        Optional[str],
        typer.Option("--template-vars", "-t", help="JSON string of template variables"),
    ] = None,
    include_samples: Annotated[
        bool,
        typer.Option(
            "--include-samples", "-s", help="Include sample data DDL and files"
        ),
    ] = False,
):
    """Compile synapse sync package templates and DDL scripts."""
    package_commands.synapse_compile(
        ctx=ctx, template_vars=template_vars, include_samples=include_samples
    )


@synapse_app.command("run")
def synapse_app_run(
    ctx: typer.Context,
    master_execution_id: Annotated[
        str, typer.Option("--master-execution-id", "-m", help="Master execution ID")
    ] = "",
    work_items_json: Annotated[
        str,
        typer.Option(
            "--work-items-json",
            "-w",
            help="JSON string of work items for historical mode",
        ),
    ] = "",
    max_concurrency: Annotated[
        int, typer.Option("--max-concurrency", "-c", help="Maximum concurrency level")
    ] = 10,
    include_snapshots: Annotated[
        bool, typer.Option("--include-snapshots", "-s", help="Include snapshot tables")
    ] = True,
    environment: Annotated[
        str, typer.Option("--environment", "-e", help="Environment name")
    ] = "development",
):
    """Run synapse sync extraction for specified configuration."""
    package_commands.synapse_run(
        ctx=ctx,
        master_execution_id=master_execution_id,
        work_items_json=work_items_json,
        max_concurrency=max_concurrency,
        include_snapshots=include_snapshots,
        environment=environment,
    )


# Extract generation commands
@extract_app.command("compile")
def extract_app_compile(
    ctx: typer.Context,
    template_vars: Annotated[
        Optional[str],
        typer.Option("--template-vars", "-t", help="JSON string of template variables"),
    ] = None,
    include_samples: Annotated[
        bool,
        typer.Option(
            "--include-samples", "-s", help="Include sample data DDL and source tables"
        ),
    ] = False,
    target_datastore: Annotated[
        str,
        typer.Option(
            "--target-datastore", "-d", help="Target datastore type (warehouse)"
        ),
    ] = "warehouse",
):
    """Compile extract generation package templates and DDL scripts."""
    package_commands.extract_compile(
        ctx=ctx,
        template_vars=template_vars,
        include_samples=include_samples,
        target_datastore=target_datastore,
    )


@extract_app.command()
def extract_run(
    ctx: typer.Context,
    extract_name: Annotated[
        str,
        typer.Option(
            "--extract-name", "-n", help="Specific extract configuration to process"
        ),
    ] = "",
    execution_group: Annotated[
        str, typer.Option("--execution-group", "-g", help="Execution group to process")
    ] = "",
    environment: Annotated[
        str, typer.Option("--environment", "-e", help="Environment name")
    ] = "development",
    run_type: Annotated[
        str, typer.Option("--run-type", "-r", help="Run type: FULL or INCREMENTAL")
    ] = "FULL",
):
    """Run extract generation for specified configuration or execution group."""
    package_commands.extract_run(
        ctx=ctx,
        extract_name=extract_name,
        execution_group=execution_group,
        environment=environment,
        run_type=run_type,
    )


# Library compilation commands
@libs_app.command("compile")
def libs_app_compile(
    ctx: typer.Context,
    target_file: Annotated[
        Optional[str],
        typer.Option(
            "--target-file",
            "-f",
            help="Specific python file to compile (relative to project root)",
        ),
    ] = None,
):
    """Compile Python libraries by injecting variables from the variable library."""
    libs_commands.libs_compile(ctx=ctx, target_file=target_file)


if __name__ == "__main__":
    app()
