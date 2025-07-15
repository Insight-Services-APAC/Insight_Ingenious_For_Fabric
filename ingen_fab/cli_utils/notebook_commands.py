import html
import os
import re
import sys
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.theme import Theme
from typing_extensions import Annotated

from ingen_fab.cli_utils.console_styles import ConsoleStyles
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


def parse_status_response(status_text: str) -> dict | None:
    if not status_text:
        return None
    lines = status_text.strip().split("\n")
    if len(lines) < 3:
        return None
    data_line = None
    for line in lines[2:]:
        if line.strip() and not line.startswith("-"):
            data_line = line.strip()
            break
    if not data_line:
        return None
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


def compile_ddl_notebooks(
    ctx: typer.Context,
    output_mode: NotebookGenerator.OutputMode = NotebookGenerator.OutputMode.fabric_workspace_repo,
    generation_mode: NotebookGenerator.GenerationMode = None,
    verbose: bool = False,
):
    fabric_workspace_repo_dir = (
        ctx.obj.get("fabric_workspace_repo_dir", None) if ctx.obj else None
    )

    if output_mode is None:
        output_mode = NotebookGenerator.OutputMode.fabric_workspace_repo
    
    generation_modes = []
    if generation_mode is None:
        generation_modes.append(NotebookGenerator.GenerationMode.lakehouse)
        generation_modes.append(NotebookGenerator.GenerationMode.warehouse)
    else:
        generation_modes.append(generation_mode)

    for generation_mode_loop in generation_modes:
        print (f"Running notebook generation for mode: {generation_mode_loop.name}")
        nbg = NotebookGenerator(
            generation_mode=generation_mode_loop,
            output_mode=output_mode,
            fabric_workspace_repo_dir=fabric_workspace_repo_dir,
        )
        nbg.run_all()
    
    # After generating all notebooks, inject variables using variable_lib
    fabric_environment = ctx.obj.get("fabric_environment", "local") if ctx.obj else "local"
    if fabric_workspace_repo_dir:
        try:
            varlib_utils = VariableLibraryUtils(
                project_path=Path(fabric_workspace_repo_dir),
                environment=fabric_environment
            )
            varlib_utils.inject_variables_into_template()
            console.print(f"[green]✓[/green] Variable injection completed for environment: {fabric_environment}")
        except Exception as e:
            console.print(f"[yellow]⚠[/yellow] Variable injection failed: {str(e)}")
            console.print("[dim]This may be expected if no variable library is configured[/dim]")


def test_python_block():
    with open(
        Path(__file__).parent.parent / "python_libs" / "python" / "config_utils.py", "r"
    ) as f:
        code = f.read()
    fct = FabricCodeTester()
    fct.test_code(code=code)


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
        if r:
            decoded_response = html.unescape(r)
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
            console.print("[info]Waiting for notebook execution to complete...[/info]")
            max_wait_time = 300
            poll_interval = 10
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


def find_notebook_content_files(base_dir: Path):
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


def scan_notebook_blocks(base_dir: Path, apply_replacements: bool):
    finder = NotebookContentFinder(base_dir)
    all_blocks = finder.scan_and_display_blocks()
    if apply_replacements and all_blocks:
        console.print(
            Panel.fit(
                "[bold cyan]Initiating Block Replacements Process... [/bold cyan]",
                border_style="cyan",
            )
        )
        finder.apply_replacements(all_blocks)
    elif apply_replacements and not all_blocks:
        ConsoleStyles.print_error(
            console, "No content blocks found to apply replacements to."
        )


def run_livy_notebook(
    ctx: typer.Context, workspace_id: str, lakehouse_id: str, code: str, timeout: int
):
    try:
        console.print("[info]Starting notebook execution via Livy API...[/info]")
        livy_client = FabricLivyNotebook(
            workspace_id=workspace_id, lakehouse_id=lakehouse_id
        )
        console.print(
            f"[info]Executing code: {code[:50]}{'...' if len(code) > 50 else ''}[/info]"
        )
        result = livy_client.run_template_notebook(code, timeout=timeout)
        if result.get("error"):
            console.print(f"[error]❌ Execution failed: {result['error']}[/error]")
        elif result.get("state") == "available":
            console.print("[info]✅ Notebook execution completed successfully![/info]")
            output = result.get("output", {})
            if output.get("data"):
                console.print("[info]Output:[/info]")
                if output["data"].get("text/plain"):
                    console.print(output["data"]["text/plain"])
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
