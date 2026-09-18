"""
Test Commands CLI Utilities

This module contains the command implementations for test-related commands,
consolidating common patterns like environment validation and pytest execution.
"""

import glob
import os
from typing import Optional

import pytest
import typer
from rich.console import Console

import ingen_fab
from ingen_fab.cli_utils.console_styles import ConsoleStyles

console = Console()
console_styles = ConsoleStyles()

_TEST_FILE_PREFIX = "test_"
_TEST_FILE_SUFFIX = "_pytest.py"

# The library tests ship inside the package, so they are found relative to it rather than to
# the working directory: the commands work from any directory and for pip-installed users.
_TESTS_ROOT = os.path.join(os.path.dirname(ingen_fab.__file__), "python_libs_tests")


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

    Test files are named ``test_<lib>_pytest.py``. As the CLI help text says, the bare
    library name (``ddl_utils``) is enough; the ``test_`` form, the stem
    (``test_ddl_utils_pytest``) and the file name itself are accepted too. Paths are not:
    the name must be a single file name inside ``base_path``.

    Raises:
        typer.Exit: with code 1 when the file does not exist; the available names are listed.
    """
    name = lib.strip()
    if os.path.isabs(name) or os.sep in name or (os.altsep and os.altsep in name):
        console_styles.print_error(
            console, f"Error: '{lib}' is not a library name; pass a name, not a path."
        )
        raise typer.Exit(code=1)
    if not os.path.isdir(base_path):
        console_styles.print_error(
            console, f"Error: test directory not found: {os.path.abspath(base_path)}"
        )
        raise typer.Exit(code=1)

    if name.endswith(".py"):
        name = name[: -len(".py")]
    if name.endswith("_pytest"):
        name = name[: -len("_pytest")]
    if name.startswith(_TEST_FILE_PREFIX):
        name = name[len(_TEST_FILE_PREFIX) :]

    test_file = os.path.join(base_path, f"{_TEST_FILE_PREFIX}{name}{_TEST_FILE_SUFFIX}")
    if os.path.isfile(test_file):
        return test_file

    available = sorted(
        os.path.basename(p)[len(_TEST_FILE_PREFIX) : -len(_TEST_FILE_SUFFIX)]
        for p in glob.glob(
            os.path.join(base_path, f"{_TEST_FILE_PREFIX}*{_TEST_FILE_SUFFIX}")
        )
    )
    # ConsoleStyles renders plain Text, so user-supplied names cannot inject Rich markup.
    console_styles.print_error(
        console, f"Error: no test file found for '{lib}' under {base_path}."
    )
    if available:
        console_styles.print_warning(console, "Available: " + ", ".join(available))
    raise typer.Exit(code=1)


def run_pytest_command(
    base_path: str, lib: Optional[str] = None, verbose: bool = True
) -> None:
    """Run pytest with standardized configuration."""
    target = resolve_test_file(base_path, lib) if lib is not None else base_path
    args = [target, "-v"] if verbose else [target]
    exit_code = pytest.main(args)

    raise typer.Exit(code=exit_code)


# =============================================================================
# TEST LOCAL APP COMMANDS
# =============================================================================


def test_local_pyspark(lib: Optional[str] = None):
    """Run pytest on ingen_fab/python_libs_tests/pyspark or a specific test file if provided."""
    validate_local_environment()
    run_pytest_command(os.path.join(_TESTS_ROOT, "pyspark"), lib, verbose=True)


def test_local_python(lib: Optional[str] = None):
    """Run pytest on ingen_fab/python_libs_tests/python or a specific test file if provided."""
    validate_local_environment()
    run_pytest_command(os.path.join(_TESTS_ROOT, "python"), lib, verbose=False)


def test_local_common(lib: Optional[str] = None):
    """Run pytest on ingen_fab/python_libs_tests/common or a specific test file if provided."""
    # Note: common tests don't require local environment validation
    run_pytest_command(os.path.join(_TESTS_ROOT, "common"), lib, verbose=True)


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
