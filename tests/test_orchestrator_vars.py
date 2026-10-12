"""The orchestrator notebook hands dbt the merged `--vars` as flow-style YAML: `--vars` is YAML
to dbt, and a typed scalar the caller wrote (`{run_date: 2026-10-12}` loads as a date) must
survive; json.dumps raised TypeError on it before dbt started (Copilot, 12 Oct 2026)."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import pytest
import yaml

from ingen_fab.cli_utils import dbt_commands

TEMPLATE = (
    Path(dbt_commands.__file__).resolve().parent.parent
    / "templates"
    / "dbt"
    / "orchestrator_notebook.py.jinja"
)


def _merge_as_the_notebook_does(dbt_vars: str) -> str:
    """The template's vars block, line for line."""
    run_vars = dict(yaml.safe_load(dbt_vars) or {}) if dbt_vars else {}
    run_vars.setdefault("log_lakehouse", "lh_log")
    run_vars["ingen_fab_runner"] = "notebook/dbtload_test"
    return yaml.safe_dump(run_vars, default_flow_style=True, width=1_000_000).strip()


def test_typed_yaml_scalars_survive_the_merge() -> None:
    given = (
        "{run_date: 2026-10-12, silver_lakehouse: lh_silver_nb, threads: 3, full: true}"
    )
    with pytest.raises(TypeError):
        json.dumps(yaml.safe_load(given))  # what the notebook used to do
    out = _merge_as_the_notebook_does(given)
    assert out.startswith("{")
    assert chr(10) not in out
    back = yaml.safe_load(out)
    assert back["run_date"] == datetime.date(2026, 10, 12)
    assert back["silver_lakehouse"] == "lh_silver_nb"
    assert back["threads"] == 3 and back["full"] is True
    assert back["log_lakehouse"] == "lh_log"
    assert back["ingen_fab_runner"] == "notebook/dbtload_test"


def test_the_template_serialises_vars_as_yaml_not_json() -> None:
    text = TEMPLATE.read_text(encoding="utf-8")
    assert "json.dumps(run_vars)" not in text
    assert "yaml.safe_dump(run_vars, default_flow_style=True" in text
