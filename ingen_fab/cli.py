import sys
from pathlib import Path
import typer
import re
import html
import time
from rich.panel import Panel
# Add current file's directory to Python path
current_dir = Path.cwd().resolve()
sys.path.insert(0, str(current_dir))

from ingen_fab.notebook_utils.fabric_cli_notebook import FabricCLINotebook, FabricLivyNotebook
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
def run_simple_notebook(ctx: typer.Context):
    project_cfg = ctx.obj.get("config", {}) if ctx.obj else {}
    fclin = FabricCLINotebook("Trial_Cap")
    try:
        fclin.generate_functional_test_notebook()
        fclin.upload(
            notebook_path=Path.cwd() / "output" / "codex_test_notebook",
            notebook_name="codex_test_notebook",
        )
        r = fclin.run(notebook_name="codex_test_notebook")
        job_id = None
        
        # Extract job_id from the response string
        if r:
            # Decode HTML entities first
            decoded_response = html.unescape(r)
            # Use regex to extract the job ID (UUID format)
            job_id_match = re.search(r"Job instance '([a-f0-9-]{36})'", decoded_response)
            if job_id_match:
                job_id = job_id_match.group(1)
                console.print(f"[info]Notebook started with job ID: {job_id}[/info]")
            else:
                console.print("[warning]Could not extract job ID from response.[/warning]")
                console.print(f"[debug]Response: {r}[/debug]")
        else:
            console.print("[warning]No response returned from notebook execution.[/warning]")
        
        if job_id:
            # Check the status of the notebook execution loop
            console.print("[info]Waiting for notebook execution to complete...[/info]")
            
            # Poll status until completion
            max_wait_time = 300  # 5 minutes
            poll_interval = 10   # 10 seconds
            elapsed_time = 0
            
            while elapsed_time < max_wait_time:
                time.sleep(poll_interval)
                elapsed_time += poll_interval
                
                status_response = fclin.status(notebook_name="codex_test_notebook", job_id=job_id)
                parsed_status = parse_status_response(status_response)
                
                if parsed_status:
                    status = parsed_status['status']
                    console.print(f"[info]Job status: {status} (elapsed: {elapsed_time}s)[/info]")
                    
                    if status in ['Succeeded', 'Failed', 'Cancelled']:
                        if status == 'Succeeded':
                            console.print("[info]✅ Notebook execution completed successfully![/info]")
                        elif status == 'Failed':
                            failure_reason = parsed_status.get('failureReason', 'Unknown')
                            console.print(f"[error]❌ Notebook execution failed: {failure_reason}[/error]")
                        else:
                            console.print("[warning]⚠️ Notebook execution was cancelled[/warning]")
                        break
                else:
                    console.print("[warning]Could not parse status response[/warning]")
                    console.print(f"[debug]Raw response: {status_response}[/debug]")
            
            if elapsed_time >= max_wait_time:
                console.print(f"[warning]⏱️ Timeout after {max_wait_time}s waiting for notebook completion[/warning]")
            
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
  
    apply_replacements: bool = typer.Option(
        False, # Default to False, so it doesn't modify files by default
        "--apply-replacements",
        "-a",
        help="Apply content block replacements from python_libs to notebook-content.py files."
    )
):
    """Scan for notebook-content.py files and display content block summary.
    Optionally applies replacements from python_libs.""" # Updated help text
    finder = NotebookContentFinder(base_dir)
    all_blocks = finder.scan_and_display_blocks() # Store the returned blocks

    if apply_replacements and all_blocks:
        console.print(Panel.fit("[bold cyan]Initiating Block Replacements Process... [/bold cyan]",
                border_style="cyan",
            ))
        finder.apply_replacements(all_blocks)
    elif apply_replacements and not all_blocks:
        console.print("[red]No content blocks found to apply replacements to.[/red]")




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


def parse_status_response(status_text: str) -> dict | None:
    """Parse the status response table and extract job status information."""
    if not status_text:
        return None
    
    lines = status_text.strip().split('\n')
    if len(lines) < 3:  # Need header + separator + data
        return None
    
    # Find the data line (skip header and separator)
    data_line = None
    for line in lines[2:]:  # Skip header and separator
        if line.strip() and not line.startswith('-'):
            data_line = line.strip()
            break
    
    if not data_line:
        return None
    
    # Split by multiple spaces to handle column separation
    fields = re.split(r'\s{2,}', data_line)
    
    if len(fields) >= 5:
        return {
            'id': fields[0].strip(),
            'itemId': fields[1].strip(),
            'jobType': fields[2].strip(),
            'invokeType': fields[3].strip(),
            'status': fields[4].strip(),
            'failureReason': fields[5].strip() if len(fields) > 5 else None,
            'rootActivityId': fields[6].strip() if len(fields) > 6 else None,
            'startTimeUtc': fields[7].strip() if len(fields) > 7 else None,
            'endTimeUtc': fields[8].strip() if len(fields) > 8 else None,
        }
    
    return None


@app.command()
def run_livy_notebook(
    ctx: typer.Context,
    workspace_id: Annotated[
        str,
        typer.Option(
            "--workspace-id",
            "-w",
            help="Fabric workspace ID for Livy endpoint"
        ),
    ],
    lakehouse_id: Annotated[
        str,
        typer.Option(
            "--lakehouse-id",
            "-l",
            help="Fabric lakehouse ID for Spark session"
        ),
    ],
    code: Annotated[
        str,
        typer.Option(
            "--code",
            "-c",
            help="Python code to execute (default: simple test)"
        ),
    ] = "print('Hello from Fabric Livy API!')",
    timeout: Annotated[
        int,
        typer.Option(
            "--timeout",
            "-t",
            help="Timeout in seconds for notebook execution"
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
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id
        )
        
        # Execute the code using the template format with error handling
        console.print(f"[info]Executing code: {code[:50]}{'...' if len(code) > 50 else ''}[/info]")
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
            console.print(f"[warning]⚠️ Unexpected state: {result.get('state', 'unknown')}[/warning]")
            console.print(f"[debug]Full result: {result}[/debug]")
            
    except Exception as e:
        console.print(f"[error]❌ Livy notebook execution failed: {e}[/error]")
        import traceback
        console.print(f"[debug]Traceback: {traceback.format_exc()}[/debug]")


if __name__ == "__cli__":
    app()
