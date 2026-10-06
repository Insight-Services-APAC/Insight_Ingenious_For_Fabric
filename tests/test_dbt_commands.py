"""`ingen_fab dbt`: the command line it builds, where it finds dbt, what the orchestrator
accepts, and what a project upload includes."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from ingen_fab.az_cli.onelake_utils import DBT_GENERATED_DIRS, collect_upload_files
from ingen_fab.cli_utils import dbt_commands


def test_cli_registers_every_verb_of_the_shared_list() -> None:
    import re

    source = (
        Path(dbt_commands.__file__)
        .parents[1]
        .joinpath("cli.py")
        .read_text(encoding="utf-8")
    )
    block = re.search(r"for _verb in \((.*?)\):", source, re.DOTALL).group(1)
    registered = tuple(re.findall(r'"([a-z]+)"', block))
    assert registered == dbt_commands.DBT_VERBS


def test_command_line_puts_the_docs_subcommand_before_the_options(
    tmp_path: Path,
) -> None:
    line = dbt_commands.dbt_command_line(
        "dbt",
        "docs",
        tmp_path / "p",
        tmp_path / "p" / "profiles",
        "development",
        ["generate", "--select", "x"],
    )
    assert line[:3] == ["dbt", "docs", "generate"]
    assert "--target" not in line
    assert line[-2:] == ["--select", "x"]


def test_command_line_passes_target_and_arguments_for_a_run(tmp_path: Path) -> None:
    line = dbt_commands.dbt_command_line(
        "dbt",
        "build",
        tmp_path / "p",
        tmp_path / "p" / "profiles",
        "development",
        ["--select", "tag:silver"],
    )
    assert line == [
        "dbt",
        "build",
        "--project-dir",
        str(tmp_path / "p"),
        "--profiles-dir",
        str(tmp_path / "p" / "profiles"),
        "--target",
        "development",
        "--select",
        "tag:silver",
    ]


def test_command_line_skips_the_target_for_verbs_without_one(tmp_path: Path) -> None:
    for verb in ("clean", "deps"):
        assert "--target" not in dbt_commands.dbt_command_line(
            "dbt", verb, tmp_path, tmp_path, "development", []
        )


def test_dbt_is_found_next_to_the_interpreter_without_resolving_symlinks(
    tmp_path: Path, monkeypatch
) -> None:
    # a Linux venv: bin/python is a symlink to the base interpreter, dbt lives in bin/
    base = tmp_path / "usr" / "bin"
    base.mkdir(parents=True)
    (base / "python3").write_text("", encoding="utf-8")
    venv_bin = tmp_path / "venv" / "bin"
    venv_bin.mkdir(parents=True)
    link = venv_bin / "python"
    try:
        link.symlink_to(base / "python3")
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not available here")
    (venv_bin / "dbt").write_text("", encoding="utf-8")
    monkeypatch.setattr(sys, "executable", str(link))
    assert dbt_commands.dbt_executable() == str(venv_bin / "dbt")


def test_dbt_falls_back_to_path_when_absent_next_to_the_interpreter(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(sys, "executable", str(tmp_path / "python"))
    monkeypatch.setattr(dbt_commands.shutil, "which", lambda name: "/usr/local/bin/dbt")
    assert dbt_commands.dbt_executable() == "/usr/local/bin/dbt"


def test_orchestrator_refuses_a_command_it_does_not_run() -> None:
    with pytest.raises(
        dbt_commands.profiles.ProfileError, match="not one the orchestrator runs"
    ):
        dbt_commands.write_orchestrator_notebook(
            None, "dbt_project", "nb", "x", command="docs"
        )


def test_orchestrator_template_carries_the_same_allowed_commands() -> None:
    template = (
        Path(dbt_commands.__file__)
        .parents[1]
        .joinpath("templates", "dbt", "orchestrator_notebook.py.jinja")
        .read_text(encoding="utf-8")
    )
    assert "ALLOWED = set({{ allowed_commands | tojson }})" in template
    assert '".gpickle"' in template


def test_upload_keeps_package_files_and_skips_generated_folders(tmp_path: Path) -> None:
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "pkg" / "__pycache__").mkdir()
    (tmp_path / "pkg" / "__pycache__" / "x.pyc").write_text("", encoding="utf-8")
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "a.sql").write_text("select 1", encoding="utf-8")
    files = {
        p.relative_to(tmp_path).as_posix()
        for p in collect_upload_files(tmp_path, exclude_dirs=DBT_GENERATED_DIRS)
    }
    assert files == {"pkg/__init__.py", "models/a.sql"}
