
import sys
import os
from typing import Optional
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ddl_scripts.notebook_generator import NotebookGenerator
import typer
from rich.console import Console
from rich.theme import Theme
from typing_extensions import Annotated
from pathlib import Path


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
        NotebookGenerator.GenerationMode.lakehouse,
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


if __name__ == "__cli__":
    app()
