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
    assert "keep_failure(" in template and "publish_run_folder" not in template
    # packages are not uploaded: a project that declares them gets `dbt deps` into scratch
    assert '("packages.yml", "dependencies.yml")' in template
    assert (
        'env["DBT_PACKAGES_INSTALL_PATH"] = str(LOCAL_RUN / "dbt_packages")' in template
    )
    assert 'step("dbt_deps", dbt + ["deps"], env=env)' in template


def test_upload_keeps_package_files_and_skips_generated_folders(tmp_path: Path) -> None:
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    (tmp_path / "pkg" / "__pycache__").mkdir()
    (tmp_path / "pkg" / "__pycache__" / "x.pyc").write_text("", encoding="utf-8")
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "a.sql").write_text("select 1", encoding="utf-8")
    # a generated-folder name below the root is a source folder, not generated output
    (tmp_path / "models" / "logs").mkdir()
    (tmp_path / "models" / "logs" / "b.sql").write_text("select 2", encoding="utf-8")
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "dbt.log").write_text("", encoding="utf-8")
    files = {
        p.relative_to(tmp_path).as_posix()
        for p in collect_upload_files(tmp_path, exclude_dirs=DBT_GENERATED_DIRS)
    }
    assert files == {"pkg/__init__.py", "models/a.sql", "models/logs/b.sql"}


def test_cli_reports_an_orchestrator_validation_error_without_a_traceback(
    tmp_path: Path,
) -> None:
    from typer.testing import CliRunner

    from ingen_fab.cli import app

    project = tmp_path / "p"
    (project / "fabric_workspace_items" / "config").mkdir(parents=True)
    result = CliRunner().invoke(
        app,
        [
            "--fabric-workspace-repo-dir",
            str(project),
            "--fabric-environment",
            "development",
            "dbt",
            "orchestrator",
            "--name",
            "nb",
            "--select",
            "x",
            "--command",
            "docs",
        ],
    )
    assert result.exit_code == 1
    assert "not one the orchestrator runs" in result.output
    assert "Traceback" not in result.output


def test_logging_macro_is_refreshed_and_the_missing_hook_is_reported(
    tmp_path: Path, capsys
) -> None:
    project = tmp_path / "dbt_project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: p\nprofile: p\n", encoding="utf-8")
    target = dbt_commands.refresh_logging_macro(project)
    assert target == project / "macros" / "ingen_fab_logging.sql"
    assert target.read_text(encoding="utf-8") == dbt_commands.LOGGING_MACRO.read_text(
        encoding="utf-8"
    )
    assert not dbt_commands.logging_hook_present(project)
    assert "no on-run-end hook" in capsys.readouterr().out
    (project / "dbt_project.yml").write_text(
        'name: p\nprofile: p\non-run-end:\n  - "{{ ingen_fab_log_run(results) }}"\n',
        encoding="utf-8",
    )
    assert dbt_commands.logging_hook_present(project)
    # an edited copy is overwritten: the macro is generated, not the project's
    target.write_text("edited", encoding="utf-8")
    dbt_commands.refresh_logging_macro(project)
    assert "macro ingen_fab_log_run" in target.read_text(encoding="utf-8")
    assert "no on-run-end hook" not in capsys.readouterr().out


def test_logging_macro_covers_both_adapters_with_the_same_two_tables() -> None:
    macro = dbt_commands.LOGGING_MACRO.read_text(encoding="utf-8")
    for adapter in ("fabricspark", "fabric"):
        assert f"target.type == '{adapter}'" in macro
    for table in ("dbt_batch", "dbt_execution_log"):
        assert macro.count(table) >= 2
    assert (
        "var('log_lakehouse', 'lh_log')" in macro
        and "var('log_warehouse', 'wh_log')" in macro
    )
    assert "USING DELTA" in macro and "IF OBJECT_ID(" in macro
    assert "var('ingen_fab_runner', 'cli')" in macro
    # dbt's Jinja exposes modules.datetime.{date, datetime, time, timedelta, tzinfo} only:
    # `timezone` is not there (a live run failed on it), so timestamps use utcnow()
    assert "modules.datetime.timezone" not in macro and "utcnow()" in macro
    # a local named `log` shadows dbt's log() and made the hook fail after its inserts
    assert "set log =" not in macro
    # a `set` inside a Jinja loop does not survive the loop: node timings go through a namespace
    assert (
        "namespace(started=none, ended=none)" in macro
        and "ns.started = t.started_at" in macro
    )


def test_command_line_keeps_the_callers_target(tmp_path: Path) -> None:
    line = dbt_commands.dbt_command_line(
        "dbt",
        "build",
        tmp_path,
        tmp_path / "profiles",
        "development",
        ["--target", "laptop"],
    )
    assert line.count("--target") == 1 and line[line.index("--target") + 1] == "laptop"
    line = dbt_commands.dbt_command_line(
        "dbt", "build", tmp_path, tmp_path / "profiles", "development", ["-t", "laptop"]
    )
    assert "--target" not in line and "-t" in line


def test_a_hand_written_profile_is_recognised_and_left_alone(tmp_path: Path) -> None:
    project = tmp_path / "dbt_warehouse"
    (project / "profiles").mkdir(parents=True)
    assert not dbt_commands.hand_written_profile(project)
    (project / "profiles" / "profiles.yml").write_text(
        "# Hand-written for dbt-fabric\n'p_warehouse':\n  target: notebook\n",
        encoding="utf-8",
    )
    assert dbt_commands.hand_written_profile(project)
    (project / "profiles" / "profiles.yml").write_text(
        "# Generated by `ingen_fab dbt profile` from the project's Variable Library value sets.\np:\n  target: development\n",
        encoding="utf-8",
    )
    assert not dbt_commands.hand_written_profile(project)
