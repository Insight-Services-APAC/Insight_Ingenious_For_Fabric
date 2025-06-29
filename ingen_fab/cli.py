import html
import re
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.theme import Theme
from typing_extensions import Annotated

from ingen_fab.config_utils.variable_lib import VariableLibraryUtils
from ingen_fab.ddl_scripts.notebook_generator import NotebookGenerator
from ingen_fab.fabric_api.utils import FabricApiUtils
from ingen_fab.fabric_cicd.promotion_utils import SyncToFabricEnvironment
from ingen_fab.notebook_utils.fabric_cli_notebook import (
    FabricCLINotebook,
    FabricLivyNotebook,
)
from ingen_fab.notebook_utils.fabric_code_tester import FabricCodeTester
from ingen_fab.notebook_utils.notebook_block_injector import NotebookContentFinder

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
    ] = Path("./sample_project"),
    fabric_environment: Annotated[
        Path | None,
        typer.Option(
            "--fabric-environment",
            "-fe",
            help="The name of your fabric environment (e.g., development, production). This must match one of the valuesets in your variable library.",
        ),
    ] = Path("development"),
):
    """Load project configuration and store in context."""
    ctx.obj = {
        "fabric_workspace_repo_dir": fabric_workspace_repo_dir,
        "fabric_environment": fabric_environment,
    }


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
        "--output-mode",
        "-o",
        help="Where to write the generated notebooks fabric_workspace_repo or local",
    ),
    generation_mode: NotebookGenerator.GenerationMode = typer.Option(
        NotebookGenerator.GenerationMode.warehouse,
        "--generation-mode",
        "-g",
        help="What to generate  lakehouse or warehouse",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable verbose output"
    ),
):
    """
    compile_ddl_notebooks compiles the DDL notebooks in the specified project directory.
    """
    fabric_workspace_repo_dir = (
        ctx.obj.get("fabric_workspace_repo_dir", None) if ctx.obj else None
    )

    nbg = NotebookGenerator(
        generation_mode=generation_mode,
        output_mode=output_mode,
        fabric_workspace_repo_dir=fabric_workspace_repo_dir,
    )
    nbg.run_all()


@app.command()
def test_python_block():

    # Read he code from ingen_fab\python_libs\python\config_utils.py
    with open(Path(__file__).parent / "python_libs" / "python" / "config_utils.py", "r") as f:
        code = f.read()

    fct = FabricCodeTester()
    fct.test_code(
        code=code,
    )


@app.command()
def run_simple_notebook(ctx: typer.Context):
    fclin: FabricCLINotebook = FabricCLINotebook("Trial_Cap")
    try:
        fclin.generate_functional_test_notebook()
        fclin.upload(
            notebook_path=Path.cwd() / "output" / "codex_test_notebook",
            notebook_name="codex_test_notebook",
        )
        r: str | None = fclin.run(notebook_name="codex_test_notebook")
        job_id = None

        # Extract job_id from the response string
        if r:
            # Decode HTML entities first
            decoded_response = html.unescape(r)
            # Use regex to extract the job ID (UUID format)
            job_id_match = re.search(
                r"Job instance '([a-f0-9-]{36})'", decoded_response
            )
            if job_id_match:
                job_id = job_id_match.group(1)
                console.print(f"[info]Notebook started with job ID: {job_id}[/info]")
            else:
                console.print(
                    "[warning]Could not extract job ID from response.[/warning]"
                )
                console.print(f"[debug]Response: {r}[/debug]")
        else:
            console.print(
                "[warning]No response returned from notebook execution.[/warning]"
            )

        if job_id:
            # Check the status of the notebook execution loop
            console.print("[info]Waiting for notebook execution to complete...[/info]")

            # Poll status until completion
            max_wait_time = 300  # 5 minutes
            poll_interval = 10  # 10 seconds
            elapsed_time = 0

            while elapsed_time < max_wait_time:
                time.sleep(poll_interval)
                elapsed_time += poll_interval

                status_response = fclin.status(
                    notebook_name="codex_test_notebook", job_id=job_id
                )
                parsed_status = parse_status_response(status_response)

                if parsed_status:
                    status = parsed_status["status"]
                    console.print(
                        f"[info]Job status: {status} (elapsed: {elapsed_time}s)[/info]"
                    )

                    if status in ["Succeeded", "Failed", "Cancelled"]:
                        if status == "Succeeded":
                            console.print(
                                "[info]✅ Notebook execution completed successfully![/info]"
                            )
                        elif status == "Failed":
                            failure_reason = parsed_status.get(
                                "failureReason", "Unknown"
                            )
                            console.print(
                                f"[error]❌ Notebook execution failed: {failure_reason}[/error]"
                            )
                        else:
                            console.print(
                                "[warning]⚠️ Notebook execution was cancelled[/warning]"
                            )
                        break
                else:
                    console.print("[warning]Could not parse status response[/warning]")
                    console.print(f"[debug]Raw response: {status_response}[/debug]")

            if elapsed_time >= max_wait_time:
                console.print(
                    f"[warning]⏱️ Timeout after {max_wait_time}s waiting for notebook completion[/warning]"
                )

        else:
            console.print("[error]Cannot check status without job ID.[/error]")
    except Exception as e:
        print(f"Notebook execution failed: {e}")


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
        table.add_row(
            nb["notebook_name"], nb["relative_path"], nb.get("lakehouse") or ""
        )

    console.print(table)


@app.command()
def scan_notebook_blocks(
    base_dir: Path = typer.Option(
        Path("fabric_workspace_items"),
        "--base-dir",
        "-b",
        help="Directory to scan for notebook-content.py files",
    ),
    apply_replacements: bool = typer.Option(
        False,  # Default to False, so it doesn't modify files by default
        "--apply-replacements",
        "-a",
        help="Apply content block replacements from python_libs to notebook-content.py files.",
    ),
):
    """Scan for notebook-content.py files and display content block summary.
    Optionally applies replacements from python_libs."""  # Updated help text
    finder = NotebookContentFinder(base_dir)
    all_blocks = finder.scan_and_display_blocks()  # Store the returned blocks

    if apply_replacements and all_blocks:
        console.print(
            Panel.fit(
                "[bold cyan]Initiating Block Replacements Process... [/bold cyan]",
                border_style="cyan",
            )
        )
        finder.apply_replacements(all_blocks)
    elif apply_replacements and not all_blocks:
        console.print("[red]No content blocks found to apply replacements to.[/red]")


@app.command()
def deploy_to_environment(ctx: typer.Context):
    """Publish Fabric items from repository to a workspace."""
    if ctx.obj.get("fabric_workspace_repo_dir") is None:
        console.print(
            "[error]Fabric workspace repository directory not set. Use --fabric-workspace-repo-dir directly after ingen_fab to specify it.[/error]"
        )
        raise typer.Exit(code=1)

    if ctx.obj.get("fabric_environment") is None:
        console.print(
            "[error]Fabric environment not set. Use --fabric-environment directly after ingen_fab to specify it.[/error]"
        )
        raise typer.Exit(code=1)

    stf = SyncToFabricEnvironment(
        project_path=Path(ctx.obj.get("fabric_workspace_repo_dir")),
        environment=str(ctx.obj.get("fabric_environment")),
    )

    stf.sync_environment()


@app.command()
def perform_code_replacements(ctx: typer.Context):
    """Publish Fabric items from repository to a workspace."""
    if ctx.obj.get("fabric_workspace_repo_dir") is None:
        console.print(
            "[error]Fabric workspace repository directory not set. Use --fabric-workspace-repo-dir directly after ingen_fab to specify it.[/error]"
        )
        raise typer.Exit(code=1)

    if ctx.obj.get("fabric_environment") is None:
        console.print(
            "[error]Fabric environment not set. Use --fabric-environment directly after ingen_fab to specify it.[/error]"
        )
        raise typer.Exit(code=1)

    vlu = VariableLibraryUtils(
        environment=ctx.obj.get("fabric_environment"),
        project_path=Path(ctx.obj.get("fabric_workspace_repo_dir")),
    )
    # To Do Add Reusable Libraries
    vlu.inject_variables_into_template()


def parse_status_response(status_text: str) -> dict | None:
    """Parse the status response table and extract job status information."""
    if not status_text:
        return None

    lines = status_text.strip().split("\n")
    if len(lines) < 3:  # Need header + separator + data
        return None

    # Find the data line (skip header and separator)
    data_line = None
    for line in lines[2:]:  # Skip header and separator
        if line.strip() and not line.startswith("-"):
            data_line = line.strip()
            break

    if not data_line:
        return None

    # Split by multiple spaces to handle column separation
    fields = re.split(r"\s{2,}", data_line)

    if len(fields) >= 5:
        return {
            "id": fields[0].strip(),
            "itemId": fields[1].strip(),
            "jobType": fields[2].strip(),
            "invokeType": fields[3].strip(),
            "status": fields[4].strip(),
            "failureReason": fields[5].strip() if len(fields) > 5 else None,
            "rootActivityId": fields[6].strip() if len(fields) > 6 else None,
            "startTimeUtc": fields[7].strip() if len(fields) > 7 else None,
            "endTimeUtc": fields[8].strip() if len(fields) > 8 else None,
        }

    return None


@app.command()
def run_livy_notebook(
    ctx: typer.Context,
    workspace_id: Annotated[
        str,
        typer.Option(
            "--workspace-id", "-w", help="Fabric workspace ID for Livy endpoint"
        ),
    ],
    lakehouse_id: Annotated[
        str,
        typer.Option(
            "--lakehouse-id", "-l", help="Fabric lakehouse ID for Spark session"
        ),
    ],
    code: Annotated[
        str,
        typer.Option(
            "--code", "-c", help="Python code to execute (default: simple test)"
        ),
    ] = "print('Hello from Fabric Livy API!')",
    timeout: Annotated[
        int,
        typer.Option(
            "--timeout", "-t", help="Timeout in seconds for notebook execution"
        ),
    ] = 600,
):
    """
    Run a notebook using the Fabric Livy API endpoint instead of CLI.
    """
    try:
        console.print("[info]Starting notebook execution via Livy API...[/info]")

        # Initialize Livy notebook client
        livy_client = FabricLivyNotebook(
            workspace_id=workspace_id, lakehouse_id=lakehouse_id
        )

        # Execute the code using the template format with error handling
        console.print(
            f"[info]Executing code: {code[:50]}{'...' if len(code) > 50 else ''}[/info]"
        )
        result = livy_client.run_template_notebook(code, timeout=timeout)

        # Process results
        if result.get("error"):
            console.print(f"[error]❌ Execution failed: {result['error']}[/error]")
        elif result.get("state") == "available":
            console.print("[info]✅ Notebook execution completed successfully![/info]")

            # Display output if available
            output = result.get("output", {})
            if output.get("data"):
                console.print("[info]Output:[/info]")
                if output["data"].get("text/plain"):
                    console.print(output["data"]["text/plain"])

            # Display execution info
            console.print(f"[info]Session ID: {result.get('session_id')}[/info]")
            console.print(f"[info]Statement ID: {result.get('statement_id')}[/info]")

        elif result.get("state") == "error":
            console.print("[error]❌ Notebook execution failed[/error]")
            output = result.get("output", {})
            if output.get("evalue"):
                console.print(f"[error]Error: {output['evalue']}[/error]")
            if output.get("traceback"):
                console.print("[error]Traceback:[/error]")
                for line in output["traceback"]:
                    console.print(f"[error]{line}[/error]")
        else:
            console.print(
                f"[warning]⚠️ Unexpected state: {result.get('state', 'unknown')}[/warning]"
            )
            console.print(f"[debug]Full result: {result}[/debug]")

    except Exception as e:
        console.print(f"[error]❌ Livy notebook execution failed: {e}[/error]")
        import traceback

        console.print(f"[debug]Traceback: {traceback.format_exc()}[/debug]")


@app.command()
def init_solution(
    project_name: str = typer.Option(..., help="Name of the project"),
    path: Path = typer.Option(".", help="Path to create the project structure"),
):
    """Initialize a new Fabric solution with the standard directory structure."""
    project_path = Path(path)

    # Create directory structure
    directories = [
        "ddl_scripts/Lakehouses",
        "ddl_scripts/Warehouses",
        "fabric_workspace_items",
        "var_lib",
        "diagrams",
    ]

    for dir_path in directories:
        (project_path / dir_path).mkdir(parents=True, exist_ok=True)

    # Load templates
    templates_dir = Path(__file__).parent / "project_templates"
    dev_vars_template = (templates_dir / "development.yml").read_text()
    prod_vars_template = (templates_dir / "production.yml").read_text()
    gitignore_template = (templates_dir / ".gitignore").read_text()
    readme_template = (templates_dir / "README.md").read_text()

    # Write variable files
    with open(project_path / "var_lib" / "development.yml", "w") as f:
        f.write(dev_vars_template.format(project_name=project_name))

    with open(project_path / "var_lib" / "production.yml", "w") as f:
        f.write(prod_vars_template.format(project_name=project_name))

    # Write .gitignore
    with open(project_path / ".gitignore", "w") as f:
        f.write(gitignore_template)

    # Write README
    with open(project_path / "README.md", "w") as f:
        f.write(readme_template.format(project_name=project_name))

    console.print(
        f"[green]✓ Initialized Fabric solution '{project_name}' at {project_path}[/green]"
    )
    console.print("\nNext steps:")
    console.print("1. Update var_lib/*.yml files with your workspace IDs")
    console.print("2. Create DDL scripts in ddl_scripts/")
    console.print(
        "3. Run: ingen_fab compile-ddl-notebooks --output-mode fabric --generation-mode warehouse"
    )


@app.command()
def delete_workspace_items(
    environment: str = typer.Option(
        "development",
        "--environment",
        "-e",
        help="Environment to use (development, production, etc.)",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Skip confirmation prompt",
    ),
):
    """Delete all items in a workspace except lakehouses and warehouses."""
    console = Console()

    try:
        # Initialize Fabric API utils
        fabric_utils = FabricApiUtils(environment=environment)

        # Confirmation prompt unless --force is used
        if not force:
            console.print(
                f"\n[yellow]Warning: This will delete all items in workspace {fabric_utils.workspace_id} except lakehouses and warehouses.[/yellow]"
            )
            console.print(f"Environment: [cyan]{environment}[/cyan]\n")

            confirm = typer.confirm("Are you sure you want to continue?")
            if not confirm:
                console.print("[red]Operation cancelled.[/red]")
                raise typer.Abort()

        # Perform deletion
        console.print("\n[cyan]Deleting workspace items...[/cyan]")
        result = fabric_utils.delete_all_items_except_lakehouses_and_warehouses()

        # Display results
        deleted_counts = result["deleted_counts"]
        errors = result["errors"]
        total_deleted = result["total_deleted"]

        if total_deleted > 0:
            console.print(f"\n[green]Successfully deleted {total_deleted} items:[/green]")

            # Create table for deleted items
            table = Table(title="Deleted Items by Type")
            table.add_column("Item Type", style="cyan")
            table.add_column("Count", style="green")

            for item_type, count in sorted(deleted_counts.items()):
                table.add_row(item_type, str(count))

            console.print(table)
        else:
            console.print("\n[yellow]No items were deleted.[/yellow]")

        # Display errors if any
        if errors:
            console.print(f"\n[red]Encountered {len(errors)} errors:[/red]")

            error_table = Table(title="Errors")
            error_table.add_column("Item Name", style="cyan")
            error_table.add_column("Type", style="yellow")
            error_table.add_column("Error", style="red")

            for error in errors[:10]:  # Show first 10 errors
                error_table.add_row(
                    error["item_name"],
                    error["item_type"],
                    f"Status {error['status_code']}: {error['error'][:50]}...",
                )

            console.print(error_table)

            if len(errors) > 10:
                console.print(f"[dim]... and {len(errors) - 10} more errors[/dim]")

        console.print("\n[green]Operation completed.[/green]")

    except Exception as e:
        console.print(f"\n[red]Error: {str(e)}[/red]")
        raise typer.Exit(1)


if __name__ == "__cli__":
    app()
