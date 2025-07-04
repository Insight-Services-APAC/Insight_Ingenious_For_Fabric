
from __future__ import annotations

import os
from pathlib import Path

import typer
from typing_extensions import Annotated

from ingen_fab.cli_utils import (
    deploy_commands,
    init_commands,
    notebook_commands,
    workspace_commands,
)

app = typer.Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)


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
    ] = None
):
    if fabric_workspace_repo_dir is None:
        env_val = os.environ.get("FABRIC_WORKSPACE_REPO_DIR")
        if env_val:
            typer.echo("Falling back to FABRIC_WORKSPACE_REPO_DIR environment variable.")
        fabric_workspace_repo_dir = Path(env_val) if env_val else Path("./sample_project")
    if fabric_environment is None:
        env_val = os.environ.get("FABRIC_ENVIRONMENT")
        if env_val:
            typer.echo("Falling back to FABRIC_ENVIRONMENT environment variable.")
        fabric_environment = Path(env_val) if env_val else Path("development")
    ctx.obj = {
        "fabric_workspace_repo_dir": fabric_workspace_repo_dir,
        "fabric_environment": fabric_environment,
    }


@app.command()
def compile_ddl_notebooks(
    ctx: typer.Context,
    output_mode: Annotated[str, typer.Option("--output-mode", "-o")] = "local",
    generation_mode: Annotated[
        str, typer.Option("--generation-mode", "-g")
    ] = "warehouse",
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
):
    """Compile the DDL notebooks in the specified project directory."""
    notebook_commands.compile_ddl_notebooks(ctx, output_mode, generation_mode, verbose)


@app.command()
def test_python_block():
    notebook_commands.test_python_block()


@app.command()
def run_simple_notebook(ctx: typer.Context):
    notebook_commands.run_simple_notebook(ctx)


@app.command()
def find_notebook_content_files(
    base_dir: Annotated[Path, typer.Option("--base-dir", "-b")] = Path(
        "fabric_workspace_items"
    ),
):
    notebook_commands.find_notebook_content_files(base_dir)


@app.command()
def scan_notebook_blocks(
    base_dir: Annotated[Path, typer.Option("--base-dir", "-b")] = Path(
        "fabric_workspace_items"
    ),
    apply_replacements: Annotated[
        bool, typer.Option("--apply-replacements", "-a")
    ] = False,
):
    notebook_commands.scan_notebook_blocks(base_dir, apply_replacements)


@app.command()
def deploy_to_environment(ctx: typer.Context):
    deploy_commands.deploy_to_environment(ctx)


@app.command()
def perform_code_replacements(ctx: typer.Context):
    deploy_commands.perform_code_replacements(ctx)


@app.command()
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


@app.command()
def init_solution(
    project_name: Annotated[str, typer.Option(...)] = "",
    path: Annotated[Path, typer.Option("--path")] = Path("."),
):
    init_commands.init_solution(project_name, path)


@app.command()
def delete_workspace_items(
    environment: Annotated[str, typer.Option("--environment", "-e")] = "development",
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
):
    workspace_commands.delete_workspace_items(environment, force)


if __name__ == "__main__":
    app()
