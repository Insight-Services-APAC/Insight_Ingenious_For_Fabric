import traceback
from pathlib import Path

from rich.console import Console

from ingen_fab.az_cli.onelake_utils import OneLakeUtils
from ingen_fab.cli_utils.console_styles import ConsoleStyles
from ingen_fab.config_utils.variable_lib import VariableLibraryUtils
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


def perform_code_replacements(ctx):
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
    vlu = VariableLibraryUtils(
        environment=ctx.obj.get("fabric_environment"),
        project_path=Path(ctx.obj.get("fabric_workspace_repo_dir")),
    )
    vlu.inject_variables_into_template()


def upload_python_libs_to_config_lakehouse(environment: str, project_path: str, console: Console = Console()):
    """Uploads Python libraries to the config lakehouse in OneLake."""
    try:
        onelake_utils = OneLakeUtils(environment=environment, project_path=Path(project_path))
        ConsoleStyles.print_info(console, f"Initialized OneLakeUtils for environment: {environment}")
        ConsoleStyles.print_info(console, f"Workspace ID: {onelake_utils.workspace_id}")

        config_lakehouse_id = onelake_utils.get_config_lakehouse_id()
        ConsoleStyles.print_info(console, f"Config lakehouse ID: {config_lakehouse_id}")

        ConsoleStyles.print_info(console, "\nStarting upload of python_libs to config lakehouse...")
        results = onelake_utils.upload_python_libs_to_config_lakehouse()

        ConsoleStyles.print_success(console, "\nUpload completed!")
        ConsoleStyles.print_info(console, f"Total files processed: {results['total_files']}")
        ConsoleStyles.print_info(console, f"Successful uploads: {len(results['successful'])}")
        ConsoleStyles.print_info(console, f"Failed uploads: {len(results['failed'])}")

        if results['successful']:
            ConsoleStyles.print_success(console, "\nSuccessful uploads:")
            for result in results['successful'][:5]:
                ConsoleStyles.print_success(console, f"  ✓ {result['local_path']} -> {result['remote_path']}")
            if len(results['successful']) > 5:
                ConsoleStyles.print_info(console, f"  ... and {len(results['successful']) - 5} more")

        if results['failed']:
            ConsoleStyles.print_error(console, "\nFailed uploads:")
            for result in results['failed']:
                ConsoleStyles.print_error(console, f"  ✗ {result['local_path']}: {result['error']}")

    except Exception as e:
        ConsoleStyles.print_error(console, f"Error: {str(e)}")
        ConsoleStyles.print_error(console, "Stack trace:")
        ConsoleStyles.print_error(console, traceback.format_exc())
        ConsoleStyles.print_error(console, "Make sure you're authenticated with Azure and have the correct permissions.")

