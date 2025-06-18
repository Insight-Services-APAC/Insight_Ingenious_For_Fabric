
import sys
from pathlib import Path

# Add current file's directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

import typer
from ddl_scripts.notebook_generator import NotebookGenerator
from notebook_utils import NotebookContentFinder
from rich.console import Console
from rich.table import Table
from rich.theme import Theme
from typing_extensions import Annotated

app = typer.Typer(no_args_is_help=True, pretty_exceptions_show_locals=False)

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
    nbg = NotebookGenerator(
        generation_mode=generation_mode,
        output_mode=output_mode,
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


if __name__ == "__cli__":
    app()
