"""The warehouse DDL notebooks must not install anything inline: `%pip install` is refused in
non-interactive notebook runs (pipeline, schedule, jobs API) unless the run carries the
`_inlineInstallationEnabled` parameter, and pyodbc is already part of the Fabric Spark runtime
(5.0.1 in Runtime 1.3, 5.3.0 in Runtime 2.0). The master orchestrator used to carry such a
cell and failed every scheduled run (12 Oct 2026)."""

from __future__ import annotations

from pathlib import Path

from ingen_fab.cli_utils import dbt_commands

TEMPLATES = Path(dbt_commands.__file__).resolve().parent.parent / "templates" / "ddl"


def test_no_inline_install_in_any_ddl_template() -> None:
    offenders = [
        p.relative_to(TEMPLATES)
        for p in TEMPLATES.rglob("*.jinja")
        if "%pip" in p.read_text(encoding="utf-8")
        or "%conda" in p.read_text(encoding="utf-8")
    ]
    assert offenders == [], f"inline install in DDL templates: {offenders}"


def test_warehouse_notebook_relies_on_the_runtime_pyodbc() -> None:
    content = (TEMPLATES / "warehouse" / "notebook_content.py.jinja").read_text(
        encoding="utf-8"
    )
    assert "import pyodbc" in content
    assert "must be installed in the parent orchestrator" not in content
