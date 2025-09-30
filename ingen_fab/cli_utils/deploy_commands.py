import traceback
from pathlib import Path

from rich.console import Console

from ingen_fab.az_cli.onelake_utils import OneLakeUtils
from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.config_utils.variable_lib_factory import VariableLibraryFactory
from ingen_fab.fabric_cicd.promotion_utils import SyncToFabricEnvironment


def deploy_to_environment(ctx):
    if ctx.obj.get("fabric_workspace_repo_dir") is None:
        ConsoleStyles.print_error(
            Console(),
            "Fabric workspace repository directory not set. Use --fabric-workspace-repo-dir directly after ingen_fab to specify it.",
        )
        raise SystemExit(1)
    if ctx.obj.get("fabric_environment") is None:
        ConsoleStyles.print_error(
            Console(),
            "Fabric environment not set. Use --fabric-environment directly after ingen_fab to specify it.",
        )
        raise SystemExit(1)

    # Check if environment is local. If so raise an error
    if ctx.obj.get("fabric_environment") == "local":
        ConsoleStyles.print_error(
            Console(),
            "Cannot deploy to local environment. Please specify a different environment.",
        )
        raise SystemExit(1)

    stf = SyncToFabricEnvironment(
        project_path=Path(ctx.obj.get("fabric_workspace_repo_dir")),
        environment=str(ctx.obj.get("fabric_environment")),
    )
    stf.sync_environment()


def perform_code_replacements(ctx, output_dir=None, preserve_structure=True):
    if ctx.obj.get("fabric_workspace_repo_dir") is None:
        ConsoleStyles.print_error(
            Console(),
            "Fabric workspace repository directory not set. Use --fabric-workspace-repo-dir directly after ingen_fab to specify it.",
        )
        raise SystemExit(1)
    if ctx.obj.get("fabric_environment") is None:
        ConsoleStyles.print_error(
            Console(),
            "Fabric environment not set. Use --fabric-environment directly after ingen_fab to specify it.",
        )
        raise SystemExit(1)

    vlu = VariableLibraryFactory.for_deployment_from_cli(ctx)

    # Call inject_variables_into_template with specific parameters:
    # - output_dir=None for in-place (default), or specified output directory
    # - replace_placeholders=False (only inject code, don't replace {{varlib:...}})
    # - inject_code=True (inject code between markers)
    vlu.inject_variables_into_template(
        output_dir=output_dir,
        preserve_structure=preserve_structure,
        replace_placeholders=False,  # Don't replace {{varlib:...}} placeholders
        inject_code=True,  # Only inject code between markers
    )


def upload_python_libs_to_config_lakehouse(environment: str, project_path: str, console: Console = Console()):
    """Uploads Python libraries to the config lakehouse in OneLake with variable injection during upload."""
    try:
        # Variable injection will be performed automatically during OneLake upload
        # This eliminates redundant processing and improves performance
        onelake_utils = OneLakeUtils(environment=environment, project_path=Path(project_path), console=console)

        # OneLakeUtils now handles all the UI messaging and progress tracking
        results = onelake_utils.upload_python_libs_to_config_lakehouse()

        # Show any failed uploads in detail if needed
        if results["failed"]:
            console.print("\n[red]Failed uploads (details):[/red]")
            for result in results["failed"]:
                console.print(f"  [red]✗[/red] [dim]{result['local_path']}:[/dim] {result['error']}")

    except Exception as e:
        ConsoleStyles.print_error(console, f"Error: {str(e)}")
        ConsoleStyles.print_error(console, "Stack trace:")
        ConsoleStyles.print_error(console, traceback.format_exc())
        ConsoleStyles.print_error(
            console,
            "Make sure you're authenticated with Azure and have the correct permissions.",
        )


def upload_dbt_project_to_config_lakehouse(
    environment: str,
    dbt_project_name: str,
    project_path: str,
    console: Console = Console(),
):
    """Uploads a dbt project to the config lakehouse in OneLake with variable injection during upload."""
    try:
        # Variable injection will be performed automatically during OneLake upload
        # This eliminates redundant processing and improves performance
        onelake_utils = OneLakeUtils(environment=environment, project_path=Path(project_path), console=console)

        # OneLakeUtils now handles all the UI messaging and progress tracking
        results = onelake_utils.upload_dbt_project_to_config_lakehouse(
            dbt_project_name=dbt_project_name, dbt_project_path=project_path
        )

        # Show any failed uploads in detail if needed
        if results["failed"]:
            console.print("\n[red]Failed uploads (details):[/red]")
            for result in results["failed"]:
                console.print(f"  [red]✗[/red] [dim]{result['local_path']}:[/dim] {result['error']}")

    except Exception as e:
        ConsoleStyles.print_error(console, f"Error: {str(e)}")
        ConsoleStyles.print_error(console, "Stack trace:")
        ConsoleStyles.print_error(console, traceback.format_exc())
        ConsoleStyles.print_error(
            console,
            "Make sure you're authenticated with Azure and have the correct permissions.",
        )
