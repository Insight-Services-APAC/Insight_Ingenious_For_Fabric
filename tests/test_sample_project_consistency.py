"""The sample project keeps several hand-maintained copies in step; these tests fail when one
drifts: the logging macro (template, scaffold, three sample copies), the warehouse models
(dbt_warehouse versus the dbt job item's embedded copy, and both versus dbt_project's model
SQL), and the two committed orchestrator notebooks versus a fresh render of the template."""

from __future__ import annotations

import filecmp
import json
from pathlib import Path

from ingen_fab.cli_utils import dbt_commands

ROOT = Path(dbt_commands.__file__).resolve().parent.parent
SAMPLE = ROOT / "sample_project"
TEMPLATE_MACRO = ROOT / "templates" / "dbt" / "macros" / "ingen_fab_logging.sql"
JOB_PROJECT = (
    SAMPLE
    / "fabric_workspace_items"
    / "dbt_jobs"
    / "dbtjob_warehouse.DataBuildToolJob"
    / "Code"
    / "dbt"
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n")


def test_every_logging_macro_copy_matches_the_template() -> None:
    copies = [
        ROOT / "project_templates" / "dbt_project" / "macros" / "ingen_fab_logging.sql",
        SAMPLE / "dbt_project" / "macros" / "ingen_fab_logging.sql",
        SAMPLE / "dbt_warehouse" / "macros" / "ingen_fab_logging.sql",
        JOB_PROJECT / "macros" / "ingen_fab_logging.sql",
    ]
    template = _read(TEMPLATE_MACRO)
    for copy in copies:
        assert _read(copy) == template, (
            f"{copy.relative_to(ROOT)} differs from the template"
        )


def test_the_job_item_embeds_the_warehouse_project() -> None:
    """Everything under dbt_warehouse except its profile and requirements is inside the item."""
    skip = {"profiles", "requirements.txt", "target", "logs", "dbt_packages"}
    source = SAMPLE / "dbt_warehouse"
    for path in source.rglob("*"):
        rel = path.relative_to(source)
        if rel.parts[0] in skip or path.is_dir():
            continue
        twin = JOB_PROJECT / rel
        assert twin.is_file(), f"{rel} missing from the job item's Code/dbt"
        assert _read(twin) == _read(path), (
            f"{rel} differs between dbt_warehouse and Code/dbt"
        )
    assert not (JOB_PROJECT / "profiles").exists()
    assert not (JOB_PROJECT / "requirements.txt").exists()
    for path in JOB_PROJECT.rglob("*"):
        rel = path.relative_to(JOB_PROJECT)
        if path.is_dir() or rel.parts[0] in skip:
            continue
        assert (source / rel).is_file(), (
            f"{rel} is in Code/dbt but not in dbt_warehouse"
        )


def test_warehouse_models_carry_the_same_sql_as_the_lakehouse_project() -> None:
    """The model SQL is written once under dbt_project/models and copied to dbt_warehouse."""
    lake = SAMPLE / "dbt_project" / "models"
    wh = SAMPLE / "dbt_warehouse" / "models"
    lake_models = sorted(p.relative_to(lake) for p in lake.rglob("*.sql"))
    wh_models = sorted(p.relative_to(wh) for p in wh.rglob("*.sql"))
    assert lake_models == wh_models and len(lake_models) == 13
    for rel in lake_models:
        assert _read(wh / rel) == _read(lake / rel), f"models/{rel} differs"
    for rel in ("gold/schema.yml", "silver/schema_sales.yml"):
        assert _read(wh / rel) == _read(lake / rel), f"models/{rel} differs"


def test_committed_orchestrator_notebooks_are_a_fresh_render_of_the_template(
    tmp_path,
) -> None:
    """The two notebooks ship pre-generated; `ingen_fab dbt orchestrator` with the README's
    parameters must reproduce them byte for byte, so a template change is never shipped stale."""
    import shutil
    from unittest import mock

    project = tmp_path / "dp"
    shutil.copytree(
        SAMPLE, project, ignore=shutil.ignore_patterns("target", "logs", "__pycache__")
    )
    ctx = mock.Mock()
    ctx.obj = {
        "fabric_workspace_repo_dir": project,
        "fabric_environment": "development",
    }
    renders = {
        "dbtload_lakehouse": dict(
            dbt_project="dbt_project",
            select="path:models",
            dbt_vars="{silver_lakehouse: lh_silver_nb, gold_lakehouse: lh_gold_nb}",
        ),
        "dbtload_warehouse": dict(
            dbt_project="dbt_warehouse",
            select="path:models",
            target="notebook",
            env_variables={"DBT_WAREHOUSE_ENDPOINT": "wh_silver_warehouse_endpoint"},
        ),
    }
    for name, kwargs in renders.items():
        path = dbt_commands.write_orchestrator_notebook(
            ctx, notebook_name=name, **kwargs
        )
        rendered = _read(path / "notebook-content.py")
        committed = _read(
            SAMPLE
            / "fabric_workspace_items"
            / "notebooks"
            / f"{name}.Notebook"
            / "notebook-content.py"
        )
        assert rendered == committed, (
            f"{name} is stale against the template; regenerate it"
        )
        platform = json.loads(
            (
                SAMPLE
                / "fabric_workspace_items"
                / "notebooks"
                / f"{name}.Notebook"
                / ".platform"
            ).read_text(encoding="utf-8")
        )
        assert platform["metadata"]["displayName"] == name


def test_warehouse_project_files_match_between_copies() -> None:
    for rel in ("dbt_project.yml",):
        assert filecmp.cmp(
            SAMPLE / "dbt_warehouse" / rel, JOB_PROJECT / rel, shallow=False
        ), rel
