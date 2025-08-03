"""
Test Commands CLI Utilities

This module contains the command implementations for test-related commands,
consolidating common patterns like environment validation and pytest execution.
"""

import os
from typing import Optional

import pytest
import typer
from rich.console import Console

from ingen_fab.cli_utils.console_styles import ConsoleStyles

console = Console()
console_styles = ConsoleStyles()


def validate_local_environment() -> None:
    """Validate that FABRIC_ENVIRONMENT is set to 'local' for local tests."""
    fabric_env = os.getenv("FABRIC_ENVIRONMENT")
    if fabric_env != "local":
        console.print(
            f"[red]Error: FABRIC_ENVIRONMENT must be set to 'local' for local tests. "
            f"Current value: {fabric_env}[/red]"
        )
        console.print("[yellow]Please set: FABRIC_ENVIRONMENT=local[/yellow]")
        raise typer.Exit(code=1)


def run_pytest_command(base_path: str, lib: Optional[str] = None, verbose: bool = True) -> None:
    """Run pytest with standardized configuration."""
    if lib:
        test_file = f"{base_path}/{lib}_pytest.py"
        if verbose:
            exit_code = pytest.main([test_file, "-v"])
        else:
            exit_code = pytest.main([test_file])
    else:
        if verbose:
            exit_code = pytest.main([base_path, "-v"])
        else:
            exit_code = pytest.main([base_path])
    
    raise typer.Exit(code=exit_code)


# =============================================================================
# TEST LOCAL APP COMMANDS
# =============================================================================

def test_local_pyspark(lib: Optional[str] = None):
    """Run pytest on ingen_fab/python_libs_tests/pyspark or a specific test file if provided."""
    validate_local_environment()
    base_path = "ingen_fab/python_libs_tests/pyspark"
    run_pytest_command(base_path, lib, verbose=True)


def test_local_python(lib: Optional[str] = None):
    """Run pytest on ingen_fab/python_libs_tests/python or a specific test file if provided."""
    validate_local_environment()
    base_path = "ingen_fab/python_libs_tests/python"
    run_pytest_command(base_path, lib, verbose=False)


def test_local_common(lib: Optional[str] = None):
    """Run pytest on ingen_fab/python_libs_tests/common or a specific test file if provided."""
    # Note: common tests don't require local environment validation
    base_path = "ingen_fab/python_libs_tests/common"
    run_pytest_command(base_path, lib, verbose=True)


# =============================================================================
# TEST PLATFORM APP COMMANDS
# =============================================================================

def test_platform_generate(ctx: typer.Context):
    """Generate platform tests using the script in python_libs_tests."""
    from ingen_fab.python_libs_tests import generate_platform_tests

    gpt = generate_platform_tests.GeneratePlatformTests(
        environment=ctx.obj["fabric_environment"],
        project_directory=ctx.obj["fabric_workspace_repo_dir"],
    )
    gpt.generate()