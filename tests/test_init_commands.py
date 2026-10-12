"""`ingen_fab init new`: template processing of a generated project."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from rich.console import Console

from ingen_fab.cli_utils import init_commands


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _console(tmp_path: Path) -> Console:
    # the console prints check marks; on Windows a default-encoded file cannot take them
    return Console(
        file=open(tmp_path / "console.txt", "w", encoding="utf-8"), force_terminal=False
    )


def test_project_name_is_substituted_in_dbt_project_and_profiles(
    tmp_path: Path,
) -> None:
    project = tmp_path / "sales_dp"
    _write(project / "README.md", "# {project_name}\n")
    _write(
        project / "dbt_project.yml", "name: 'dbt_project'\nprofile: '{project_name}'\n"
    )
    _write(
        project / "dbt_warehouse" / "dbt_project.yml",
        "profile: '{project_name}_warehouse'\n",
    )
    _write(
        project / "dbt_warehouse" / "profiles" / "profiles.yml",
        "{project_name}_warehouse:\n  target: notebook\n",
    )
    _write(
        project
        / "fabric_workspace_items"
        / "dbt_jobs"
        / "job.DataBuildToolJob"
        / "Code"
        / "dbt"
        / "dbt_project.yml",
        "profile: '{project_name}_warehouse'\n",
    )
    _write(
        project / "dbt_project" / "models" / "silver" / "x.sql",
        "select '{project_name}'\n",
    )

    init_commands._process_template_files(project, "sales_dp", _console(tmp_path))

    assert (project / "dbt_project.yml").read_text(
        encoding="utf-8"
    ) == "name: 'dbt_project'\nprofile: 'sales_dp'\n"
    assert (project / "dbt_warehouse" / "dbt_project.yml").read_text(
        encoding="utf-8"
    ) == "profile: 'sales_dp_warehouse'\n"
    assert (
        (project / "dbt_warehouse" / "profiles" / "profiles.yml")
        .read_text(encoding="utf-8")
        .startswith("sales_dp_warehouse:")
    )
    assert "sales_dp_warehouse" in (
        project
        / "fabric_workspace_items"
        / "dbt_jobs"
        / "job.DataBuildToolJob"
        / "Code"
        / "dbt"
        / "dbt_project.yml"
    ).read_text(encoding="utf-8")
    # only dbt_project.yml and profiles.yml are processed; other files keep the literal text
    assert "{project_name}" in (
        project / "dbt_project" / "models" / "silver" / "x.sql"
    ).read_text(encoding="utf-8")


def test_platform_logical_ids_are_regenerated(tmp_path: Path) -> None:
    project = tmp_path / "p"
    platform = (
        project / "fabric_workspace_items" / "lakehouses" / "lh.Lakehouse" / ".platform"
    )
    _write(
        platform,
        json.dumps(
            {
                "metadata": {"type": "Lakehouse", "displayName": "lh"},
                "config": {
                    "version": "2.0",
                    "logicalId": "0f8e688e-6a61-4683-b1f9-79b94fe3b92a",
                },
            }
        ),
    )

    init_commands._process_template_files(project, "p", _console(tmp_path))

    new_id = json.loads(platform.read_text(encoding="utf-8"))["config"]["logicalId"]
    assert new_id != "0f8e688e-6a61-4683-b1f9-79b94fe3b92a" and len(new_id) == 36


def test_default_scaffold_logs_its_runs_like_the_sample(tmp_path: Path) -> None:
    """A project from `init new` without --with-samples carries the run-log hook, the
    log_lakehouse var and the logging macro, named after the project like the sample, so a
    plain project is logged from its first run and `ingen_fab dbt` has nothing to warn about."""
    import shutil

    from ingen_fab.cli_utils import dbt_commands
    from ingen_fab.python_libs.common.utils.path_utils import PathUtils

    scaffold = PathUtils.get_package_resource_path("project_templates") / "dbt_project"
    project = tmp_path / "plain_dp"
    shutil.copytree(scaffold, project / "dbt_project")
    init_commands._process_template_files(project, "plain_dp", _console(tmp_path))

    dbt_project = project / "dbt_project"
    text = (dbt_project / "dbt_project.yml").read_text(encoding="utf-8")
    assert "profile: 'plain_dp'" in text
    assert "{{ ingen_fab_log_run(results) }}" in text
    assert "log_lakehouse: lh_log" in text
    assert dbt_commands.logging_hook_present(dbt_project)
    macro = dbt_project / "macros" / "ingen_fab_logging.sql"
    assert macro.read_text(encoding="utf-8") == dbt_commands.LOGGING_MACRO.read_text(
        encoding="utf-8"
    )
    # and the scaffold declares that lakehouse, so `init storage-config` writes its
    # `lh_log_workspace_id` / `lh_log_lakehouse_id` variables and `dbt orchestrator` passes
    import yaml

    storage = yaml.safe_load(
        (scaffold.parent / "fabric_config" / "storage_config.yaml").read_text(
            encoding="utf-8"
        )
    )
    lakehouses = [
        v
        for item in storage["storage"]
        if "lakehouse" in item
        for k, v in item.items()
        if k != "lakehouse"
    ]
    assert "lh_log" in lakehouses


def test_project_name_must_be_a_plain_identifier(tmp_path: Path, monkeypatch) -> None:
    """The name goes raw into single-quoted YAML scalars (the dbt profile names) and into the
    Livy session name, so an apostrophe or a path separator is refused before anything is
    copied, on both routes (--project-name, and the folder name of FABRIC_WORKSPACE_REPO_DIR);
    a plain name creates the project."""
    import typer

    monkeypatch.setattr(init_commands, "Console", lambda: _console(tmp_path))
    out = tmp_path / "out"
    out.mkdir()
    for bad in ("team's_dp", "../outside", "a b", ""):
        with pytest.raises(typer.Exit):
            init_commands.init_solution(bad, out)
        assert not list(out.iterdir()), bad
    monkeypatch.setenv("FABRIC_WORKSPACE_REPO_DIR", str(out / "team's dp"))
    with pytest.raises(typer.Exit):
        init_commands.init_solution(None, out)
    assert not list(out.iterdir())
    text = (tmp_path / "console.txt").read_text(encoding="utf-8")
    assert "the folder name of FABRIC_WORKSPACE_REPO_DIR" in text
    monkeypatch.delenv("FABRIC_WORKSPACE_REPO_DIR")
    init_commands.init_solution("plain_dp", out)
    text = (out / "plain_dp" / "dbt_project" / "dbt_project.yml").read_text(
        encoding="utf-8"
    )
    assert "profile: 'plain_dp'" in text
