"""Unit tests for ingen_fab.cli_utils.test_commands.

Covers the library-name resolution used by ``ingen_fab test local {pyspark,python,common} [LIB]``:
test files are named ``test_<lib>_pytest.py`` and the CLI must accept the bare name, the
``test_`` form, the stem and the file name. No Fabric, no Spark: pytest.main is patched.
"""

import os
from unittest import mock

import pytest
import typer

from ingen_fab.cli_utils import test_commands


@pytest.fixture
def tree(tmp_path):
    """A fake python_libs_tests/<flavour> directory with two test files."""
    base = tmp_path / "pyspark"
    base.mkdir()
    (base / "test_lakehouse_utils_pytest.py").write_text("")
    (base / "test_ddl_utils_pytest.py").write_text("")
    return base


@pytest.mark.parametrize(
    "lib",
    [
        "lakehouse_utils",
        "test_lakehouse_utils",
        "test_lakehouse_utils_pytest",
        "test_lakehouse_utils_pytest.py",
        " lakehouse_utils ",
    ],
)
def test_resolve_accepts_every_spelling(tree, lib):
    resolved = test_commands.resolve_test_file(str(tree), lib)
    assert resolved == str(tree / "test_lakehouse_utils_pytest.py")


def test_resolve_unknown_lib_exits_with_available_list(tree, capsys):
    with pytest.raises(typer.Exit) as excinfo:
        test_commands.resolve_test_file(str(tree), "nope")
    assert excinfo.value.exit_code == 1
    out = capsys.readouterr().out
    assert "nope" in out
    assert "ddl_utils" in out and "lakehouse_utils" in out


def test_resolve_missing_base_path_exits_and_names_the_directory(tmp_path, capsys):
    missing = tmp_path / "missing"
    with pytest.raises(typer.Exit) as excinfo:
        test_commands.resolve_test_file(str(missing), "x")
    assert excinfo.value.exit_code == 1
    assert "test directory not found" in capsys.readouterr().out


@pytest.mark.parametrize(
    "lib", ["../python/test_ddl_utils", "sub/lakehouse_utils", "/abs/x"]
)
def test_resolve_rejects_paths(tree, lib, capsys):
    with pytest.raises(typer.Exit) as excinfo:
        test_commands.resolve_test_file(str(tree), lib)
    assert excinfo.value.exit_code == 1
    assert "not a library name" in capsys.readouterr().out


def test_tests_root_is_inside_the_package():
    import ingen_fab

    assert test_commands._TESTS_ROOT.startswith(os.path.dirname(ingen_fab.__file__))
    assert os.path.isdir(os.path.join(test_commands._TESTS_ROOT, "pyspark"))


def test_run_pytest_command_passes_resolved_file_and_verbosity(tree):
    with mock.patch.object(test_commands.pytest, "main", return_value=0) as main:
        with pytest.raises(typer.Exit) as excinfo:
            test_commands.run_pytest_command(str(tree), "ddl_utils", verbose=True)
    assert excinfo.value.exit_code == 0
    main.assert_called_once_with([str(tree / "test_ddl_utils_pytest.py"), "-v"])


def test_run_pytest_command_without_lib_runs_whole_tree_quietly(tree):
    with mock.patch.object(test_commands.pytest, "main", return_value=3) as main:
        with pytest.raises(typer.Exit) as excinfo:
            test_commands.run_pytest_command(str(tree), None, verbose=False)
    assert excinfo.value.exit_code == 3
    main.assert_called_once_with([str(tree)])


def test_resolve_escapes_rich_markup_in_messages(tree, capsys):
    with pytest.raises(typer.Exit):
        test_commands.resolve_test_file(str(tree), "[bold]nope[/bold]")
    out = capsys.readouterr().out
    # The literal brackets are printed, not interpreted as Rich markup.
    assert "[bold]nope[/bold]" in out


def test_run_pytest_command_empty_lib_is_not_the_whole_tree(tree):
    with mock.patch.object(test_commands.pytest, "main", return_value=0) as main:
        with pytest.raises(typer.Exit) as excinfo:
            test_commands.run_pytest_command(str(tree), "", verbose=True)
    assert excinfo.value.exit_code == 1
    main.assert_not_called()


def test_resolve_does_not_accept_non_test_modules(tree):
    """Only test_<lib>_pytest.py files resolve; __init__.py and stray modules do not."""
    (tree / "__init__.py").write_text("")
    (tree / "helper.py").write_text("")
    for lib in ("__init__", "helper"):
        with pytest.raises(typer.Exit) as excinfo:
            test_commands.resolve_test_file(str(tree), lib)
        assert excinfo.value.exit_code == 1
