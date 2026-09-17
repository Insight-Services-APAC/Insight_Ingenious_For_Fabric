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


def resolve_test_file(base_path: str, lib: str) -> str:
    """Resolve a library name to its pytest file under ``base_path``.

    Test files are named ``test_<lib>_pytest.py``. The CLI accepts the bare library name
    (``ddl_utils``), the ``test_`` form (``test_ddl_utils``), the stem
    (``test_ddl_utils_pytest``) or the file name itself, so the documented example
    ``ingen_fab test local pyspark ddl_utils`` works.

    Raises:
        typer.Exit: with code 1 when no candidate exists; the available names are listed.
    """
    name = lib.strip()
    if name.endswith(".py"):
        name = name[: -len(".py")]
    if name.endswith("_pytest"):
        name = name[: -len("_pytest")]
    if name.startswith("test_"):
        name = name[len("test_") :]

    candidates = [
        os.path.join(base_path, f"test_{name}_pytest.py"),
        os.path.join(base_path, f"{name}_pytest.py"),
        os.path.join(base_path, f"test_{name}.py"),
        os.path.join(base_path, f"{name}.py"),
    ]
    for candidate in candidates:
        if os.path.isfile(candidate):
            return candidate

    available = (
        sorted(
            f[len("test_") : -len("_pytest.py")]
            for f in os.listdir(base_path)
            if f.startswith("test_") and f.endswith("_pytest.py")
        )
        if os.path.isdir(base_path)
        else []
    )
    console.print(
        f"[red]Error: no test file found for '{lib}' under {base_path}.[/red]"
    )
    if available:
        console.print("[yellow]Available: " + ", ".join(available) + "[/yellow]")
    raise typer.Exit(code=1)


def run_pytest_command(
    base_path: str, lib: Optional[str] = None, verbose: bool = True
) -> None:
    """Run pytest with standardized configuration."""
    target = resolve_test_file(base_path, lib) if lib else base_path
    args = [target, "-v"] if verbose else [target]
    exit_code = pytest.main(args)

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
