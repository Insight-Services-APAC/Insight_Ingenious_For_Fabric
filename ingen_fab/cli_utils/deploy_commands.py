from pathlib import Path

from rich.console import Console

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
