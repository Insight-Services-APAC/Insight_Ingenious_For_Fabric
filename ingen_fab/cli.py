import sys
from pathlib import Path
import typer

# Add current file's directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from ddl_scripts.notebook_generator import NotebookGenerator
from notebook_utils.notebook_block_injector import NotebookContentFinder
from python_libs.python.promotion_utils import promotion_utils
from project_config import load_project_config
from rich.console import Console
from rich.table import Table
from rich.theme import Theme
from typing_extensions import Annotated

app = typer.Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)


@app.callback()
def main(
    ctx: typer.Context,
    config_dir: Annotated[
        Path | None,
        typer.Option(
            "--config-dir",
            "-c",
            help="Directory containing project.yml configuration",
        ),
    ] = None,
):
    """Load project configuration and store in context."""
    ctx.obj = {"config": load_project_config(config_dir)}

custom_theme = Theme(
    {
        "info": "dim cyan",
        "warning": "dark_orange",
        "danger": "bold red",
        "error": "bold red",
        "debug": "khaki1",
    }
)

console = Console(theme=custom_theme)


def docs_options():
    return ["generate", "serve"]


def log_levels():
    return ["DEBUG", "INFO", "WARNING", "ERROR"]


@app.command()
def compile_ddl_notebooks(
    ctx: typer.Context,
    output_mode: NotebookGenerator.OutputMode = typer.Option(
        NotebookGenerator.OutputMode.local,
        "--output-mode", "-o",
        help="Where to write the generated notebooks fabric_workspace_repo or local"
    ),
    generation_mode: NotebookGenerator.GenerationMode = typer.Option(
        NotebookGenerator.GenerationMode.warehouse,
        "--generation-mode", "-g",
        help="What to generate  lakehouse or warehouse"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose", "-v",
        help="Enable verbose output"
    )
):
    """
    compile_ddl_notebooks compiles the DDL notebooks in the specified project directory.
    """
    project_cfg = ctx.obj.get("config", {}) if ctx.obj else {}
    repo_dir = project_cfg.get("fabric_workspace_repo_dir")

    nbg = NotebookGenerator(
        generation_mode=generation_mode,
        output_mode=output_mode,
        fabric_workspace_repo_dir=repo_dir,
    )
    nbg.run_all()


@app.command()
def do_other(
    project_dir: Annotated[
        str,
        typer.Argument(help="The path to the config file. "),
    ] = None,
):
    """
    compile_ddl_notebooks compiles the DDL notebooks in the specified project directory.
    """
    pass


@app.command()
def find_notebook_content_files(
    base_dir: Path = typer.Option(
        Path("fabric_workspace_items"),
        "--base-dir",
        "-b",
        help="Directory to scan for notebook-content.py files",
    ),
):
    """Search ``base_dir`` recursively for notebook-content files."""

    finder = NotebookContentFinder(base_dir)
    notebooks = finder.find()

    if not notebooks:
        console.print("[yellow]No notebook-content.py files found.[/yellow]")
        return

    table = Table(title="Notebook Content Files")
    table.add_column("Notebook")
    table.add_column("Relative Path")
    table.add_column("Lakehouse")

    for nb in notebooks:
        table.add_row(nb["notebook_name"], nb["relative_path"], nb.get("lakehouse") or "")

    console.print(table)


@app.command()
def scan_notebook_blocks(
    base_dir: Path = typer.Option(
        Path("fabric_workspace_items"),
        "--base-dir",
        "-b",
        help="Directory to scan for notebook-content.py files",
    ),
):
    """Scan for notebook-content.py files and display content block summary."""
    finder = NotebookContentFinder(base_dir)
    finder.scan_and_display_blocks()


@app.command()
def promote_items(
    workspace_id: Annotated[str, typer.Option("--workspace-id", "-w", help="Target workspace ID")],
    repo_dir: Annotated[Path, typer.Option("--repo-dir", "-r", help="Path to fabric workspace items")] = Path("fabric_workspace_items"),
    environment: Annotated[str, typer.Option("--environment", "-e", help="Target environment name")] = "N/A",
    unpublish_orphans: Annotated[bool, typer.Option("--unpublish-orphans", help="Remove items not present in repository")] = False,
):
    """Publish Fabric items from repository to a workspace."""

    promoter = promotion_utils(
        workspace_id=workspace_id,
        repository_directory=repo_dir,
        environment=environment,
    )
    promoter.promote(delete_orphans=unpublish_orphans)


if __name__ == "__cli__":
    app()
