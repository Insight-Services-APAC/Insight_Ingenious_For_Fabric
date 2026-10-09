"""`ingen_fab init new`: template processing of a generated project."""

from __future__ import annotations

import json
from pathlib import Path

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
