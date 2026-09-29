"""Semantic models and reports as first-class ingen_fab items.

The convention under test: a Direct Lake on SQL model reads its warehouse endpoint and
database from ``{{varlib:...}}`` tokens in ``expressions.tmdl``; a report references its model
by relative path (``byPath``) and fabric-cicd rewrites that to the deployed model id at
publish. The sample project ships one of each.
"""

import json
import re
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from ingen_fab.config_utils.variable_lib import VariableLibraryUtils
from ingen_fab.fabric_cicd.promotion_utils import PublishResult, SyncToFabricEnvironment

MODULE = "ingen_fab.fabric_cicd.promotion_utils"
SAMPLE = Path(__file__).resolve().parents[1] / "ingen_fab" / "sample_project"
ITEMS = SAMPLE / "fabric_workspace_items"
MODEL = ITEMS / "semantic_models" / "sm_gold_cities.SemanticModel"
REPORT = ITEMS / "reports" / "rpt_gold_cities.Report"
TOKEN = re.compile(r"\{\{varlib:([a-zA-Z0-9_]+)\}\}")


def _platform(item: Path) -> dict:
    return json.loads((item / ".platform").read_text(encoding="utf-8"))


# --- the sample items -----------------------------------------------------------------


def test_sample_model_and_report_are_declared_as_their_types():
    assert _platform(MODEL)["metadata"] == {
        "type": "SemanticModel",
        "displayName": "sm_gold_cities",
    }
    assert _platform(REPORT)["metadata"] == {
        "type": "Report",
        "displayName": "rpt_gold_cities",
    }


def test_sample_items_have_distinct_real_logical_ids():
    ids = {
        _platform(p)["config"]["logicalId"]
        for p in ITEMS.rglob(".platform")
        for p in [p.parent]
    }
    assert len(ids) == len(list(ITEMS.rglob(".platform")))
    assert "00000000-0000-0000-0000-000000000000" not in ids


def test_sample_report_references_its_model_by_relative_path():
    pbir = json.loads((REPORT / "definition.pbir").read_text(encoding="utf-8"))
    rel = pbir["datasetReference"]["byPath"]["path"]
    assert (REPORT / rel).resolve() == MODEL.resolve()


def test_sample_model_source_is_direct_lake_over_the_gold_warehouse():
    expressions = (MODEL / "definition" / "expressions.tmdl").read_text(
        encoding="utf-8"
    )
    assert "Sql.Database(" in expressions
    assert TOKEN.findall(expressions) == [
        "wh_gold_warehouse_endpoint",
        "wh_gold_warehouse_name",
    ]
    table = (MODEL / "definition" / "tables" / "vDim_Cities.tmdl").read_text(
        encoding="utf-8"
    )
    assert "mode: directLake" in table
    assert "expressionSource: DatabaseQuery" in table


def test_every_token_in_the_sample_items_is_a_declared_variable():
    declared = {
        v["name"]
        for v in json.loads(
            (ITEMS / "config" / "var_lib.VariableLibrary" / "variables.json").read_text(
                encoding="utf-8"
            )
        )["variables"]
    }
    used = set()
    for item in (MODEL, REPORT):
        for f in item.rglob("*"):
            if f.is_file():
                used.update(
                    TOKEN.findall(f.read_text(encoding="utf-8", errors="ignore"))
                )
    assert used and used <= declared


def test_sample_report_ships_its_base_theme():
    report = json.loads(
        (REPORT / "definition" / "report.json").read_text(encoding="utf-8")
    )
    theme = report["themeCollection"]["baseTheme"]["name"]
    assert (
        REPORT / "StaticResources" / "SharedResources" / "BaseThemes" / f"{theme}.json"
    ).is_file()


# --- deploy-time substitution ---------------------------------------------------------


def test_model_tokens_resolve_from_the_value_set():
    """The same substitution ``sync_environment`` applies to every ``*.tmdl`` in ./output."""
    vlu = VariableLibraryUtils(project_path=SAMPLE, environment="test")
    rendered = vlu.perform_code_replacements(
        (MODEL / "definition" / "expressions.tmdl").read_text(encoding="utf-8"),
        replace_placeholders=True,
        inject_code=True,
    )
    assert not TOKEN.search(rendered)
    assert 'Sql.Database("REPLACE_WITH_WH_GOLD_SQL_ENDPOINT", "wh_gold")' in rendered


# --- manifest tracking ------------------------------------------------------------------


def _sync(tmp_path):
    return SyncToFabricEnvironment(project_path=str(tmp_path), console=mock.Mock())


def test_manifest_scan_tracks_both_item_types(tmp_path):
    shutil.copytree(ITEMS, tmp_path / "fabric_workspace_items")
    found = {
        (i.name, i.status)
        for i in _sync(tmp_path).find_platform_folders(
            tmp_path / "fabric_workspace_items"
        )
    }
    assert ("sm_gold_cities.SemanticModel", "new") in found
    assert ("rpt_gold_cities.Report", "new") in found


def test_power_bi_desktop_cache_does_not_change_the_item_hash(tmp_path):
    item = tmp_path / "m.SemanticModel"
    shutil.copytree(MODEL, item)
    sync = _sync(tmp_path)
    before = sync.calculate_folder_hash(item)
    (item / ".pbi").mkdir()
    (item / ".pbi" / "cache.abf").write_bytes(b"\x00" * 64)
    (item / ".pbi" / "localSettings.json").write_text("{}", encoding="utf-8")
    assert sync.calculate_folder_hash(item) == before
    (item / "definition" / "model.tmdl").write_text("model Model\n", encoding="utf-8")
    assert sync.calculate_folder_hash(item) != before


def test_report_ids_are_written_back_by_convention(tmp_path):
    vs_dir = (
        tmp_path
        / "fabric_workspace_items"
        / "config"
        / "var_lib.VariableLibrary"
        / "valueSets"
    )
    vs_dir.mkdir(parents=True)
    vs = vs_dir / "development.json"
    vs.write_text(
        json.dumps(
            {
                "variableOverrides": [
                    {"name": "sm_semanticmodel_id", "value": "old"},
                    {"name": "rpt_report_id", "value": "old"},
                ]
            }
        ),
        encoding="utf-8",
    )
    api = mock.Mock()
    api.list_workspace_items.return_value = [
        {"displayName": "sm", "type": "SemanticModel", "id": "sm-id"},
        {"displayName": "rpt", "type": "Report", "id": "rpt-id"},
    ]
    with (
        mock.patch(f"{MODULE}.FabricApiUtils", return_value=api),
        mock.patch(f"{MODULE}.get_token_credential", return_value="cred"),
    ):
        _sync(tmp_path)._update_variables_with_item_ids_after_deployment(
            [
                PublishResult("sm", "SemanticModel", True),
                PublishResult("rpt", "Report", True),
            ],
            workspace_id="ws1",
            environment="development",
        )
    values = {
        v["name"]: v["value"]
        for v in json.loads(vs.read_text(encoding="utf-8"))["variableOverrides"]
    }
    assert values == {"sm_semanticmodel_id": "sm-id", "rpt_report_id": "rpt-id"}


# --- what fabric-cicd does with the report ------------------------------------------------


def _report_file(item_path: Path):
    """Stand-ins for fabric-cicd's Item and File as ``func_process_file`` reads them."""
    pbir = (REPORT / "definition.pbir").read_text(encoding="utf-8")
    return (
        SimpleNamespace(path=item_path),
        SimpleNamespace(name="definition.pbir", contents=pbir),
    )


def _repository_with_model(guid: str):
    """A workspace stand-in whose repository index holds the sample model, as fabric-cicd's
    scan builds it: the logical id from .platform, the guid from the deployed-items index
    (empty when the model is not in the workspace yet)."""
    from fabric_cicd._common._item import Item

    model_path = (ITEMS / "semantic_models" / "sm_gold_cities.SemanticModel").resolve()
    model = Item(
        type="SemanticModel",
        name="sm_gold_cities",
        description="",
        guid=guid,
        logical_id=_platform(MODEL)["config"]["logicalId"],
        path=model_path,
    )
    workspace = mock.Mock()
    workspace.repository_items = {"SemanticModel": {"sm_gold_cities": model}}
    workspace._convert_path_to_id.return_value = model.logical_id
    return workspace


def test_fabric_cicd_rewrites_by_path_to_the_deployed_model_id(tmp_path):
    """The two steps the convention relies on, pinned against the installed library: the
    report processor turns byPath into byConnection carrying the model's *logical* id, and
    the logical-id pass then swaps it for the model's workspace guid from the repository
    index (filled from the deployed items, whether published in this run or already there)."""
    from fabric_cicd import FabricWorkspace
    from fabric_cicd._items._report import func_process_file

    item, file = _report_file(tmp_path / "reports" / "rpt.Report")
    workspace = _repository_with_model(guid="deployed-model-guid")
    after_report_step = func_process_file(workspace, item, file)

    item_type, resolved = workspace._convert_path_to_id.call_args.args
    assert item_type == "SemanticModel"
    assert (
        Path(resolved)
        == (tmp_path / "semantic_models" / "sm_gold_cities.SemanticModel").resolve()
    )
    ref = json.loads(after_report_step)["datasetReference"]
    assert "byPath" not in ref
    assert (
        ref["byConnection"]["pbiModelDatabaseName"]
        == _platform(MODEL)["config"]["logicalId"]
    )

    final = FabricWorkspace._replace_logical_ids(workspace, after_report_step)
    assert (
        json.loads(final)["datasetReference"]["byConnection"]["pbiModelDatabaseName"]
        == "deployed-model-guid"
    )


def test_fabric_cicd_refuses_a_report_whose_model_is_not_deployed_yet(tmp_path):
    """Model in the repository but not in the workspace and not published in this run (for
    example out of scope on a first deploy): the logical id cannot be resolved."""
    from fabric_cicd import FabricWorkspace
    from fabric_cicd._common._exceptions import ParsingError
    from fabric_cicd._items._report import func_process_file

    item, file = _report_file(tmp_path / "reports" / "rpt.Report")
    workspace = _repository_with_model(guid="")
    rewritten = func_process_file(workspace, item, file)
    with pytest.raises(ParsingError, match="not yet deployed"):
        FabricWorkspace._replace_logical_ids(workspace, rewritten)


def test_fabric_cicd_refuses_a_report_whose_model_is_not_in_the_repository(tmp_path):
    from fabric_cicd._common._exceptions import ItemDependencyError
    from fabric_cicd._items._report import func_process_file

    item, file = _report_file(tmp_path / "reports" / "rpt.Report")
    workspace = mock.Mock()
    workspace._convert_path_to_id.return_value = None
    with pytest.raises(ItemDependencyError):
        func_process_file(workspace, item, file)


def test_rebinding_a_report_changes_only_the_report_hash(tmp_path):
    """Pointing a report at another model is a one-line edit that only republishes the report."""
    shutil.copytree(ITEMS, tmp_path / "fabric_workspace_items")
    other = (
        tmp_path
        / "fabric_workspace_items"
        / "semantic_models"
        / "sm_other.SemanticModel"
    )
    shutil.copytree(MODEL, other)
    sync = _sync(tmp_path)
    before = {
        i.name: i.hash
        for i in sync.find_platform_folders(tmp_path / "fabric_workspace_items")
    }

    pbir = (
        tmp_path
        / "fabric_workspace_items"
        / "reports"
        / "rpt_gold_cities.Report"
        / "definition.pbir"
    )
    body = json.loads(pbir.read_text(encoding="utf-8"))
    body["datasetReference"]["byPath"]["path"] = (
        "../../semantic_models/sm_other.SemanticModel"
    )
    pbir.write_text(json.dumps(body, indent=2), encoding="utf-8")

    after = {
        i.name: i.hash
        for i in sync.find_platform_folders(tmp_path / "fabric_workspace_items")
    }
    changed = {n for n in before if before[n] != after[n]}
    assert changed == {"rpt_gold_cities.Report"}


def test_sample_report_theme_declares_the_import_version():
    """Fabric rejects a report whose base theme lacks ``reportVersionAtImport`` (seen live, 24 Sep)."""
    report = json.loads(
        (REPORT / "definition" / "report.json").read_text(encoding="utf-8")
    )
    version = report["themeCollection"]["baseTheme"]["reportVersionAtImport"]
    assert set(version) == {"visual", "report", "page"}
