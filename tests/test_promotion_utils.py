"""ingen_fab.fabric_cicd.promotion_utils under fabric-cicd 1.x.

The library calls are patched; what is tested is how ingen_fab drives them: an explicit
credential on every FabricWorkspace, keyword-only construction, `items_to_include`
semantics, and how collected responses become per-item manifest results.
"""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from ingen_fab.fabric_cicd import promotion_utils as pu_module
from ingen_fab.fabric_cicd.promotion_utils import (
    PublishResult,
    SyncToFabricEnvironment,
    WorkspaceSettings,
    promotion_utils,
    publish_items,
    publish_results_from_responses,
)

MODULE = "ingen_fab.fabric_cicd.promotion_utils"


def _build_workspace(**overrides):
    base = dict(
        workspace_id="ws1",
        repository_directory="repo",
        environment="development",
        item_type_in_scope=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


@pytest.fixture
def library():
    """Patch the three fabric-cicd entry points and the credential factory."""
    cred = mock.Mock(name="credential")
    with (
        mock.patch(f"{MODULE}.FabricWorkspace") as fw,
        mock.patch(f"{MODULE}.publish_all_items", return_value=None) as pub,
        mock.patch(f"{MODULE}.unpublish_all_orphan_items") as unpub,
        mock.patch(f"{MODULE}.get_token_credential", return_value=cred) as get_cred,
    ):
        fw.return_value = mock.Mock(name="workspace", responses=None)
        yield SimpleNamespace(fw=fw, pub=pub, unpub=unpub, get_cred=get_cred, cred=cred)


# --- promotion_utils ---------------------------------------------------------------


def test_promote_calls_publish_only(library):
    promoter = promotion_utils(_build_workspace(), mock.Mock())
    promoter.promote(delete_orphans=False)
    library.pub.assert_called_once()
    library.unpub.assert_not_called()


def test_promote_with_unpublish(library):
    promoter = promotion_utils(_build_workspace(), mock.Mock())
    promoter.promote(delete_orphans=True)
    library.pub.assert_called_once()
    library.unpub.assert_called_once()


def test_workspace_is_built_keyword_only_with_a_credential(library):
    promoter = promotion_utils(_build_workspace(), mock.Mock())
    promoter.publish_all()

    library.fw.assert_called_once()
    args, kwargs = library.fw.call_args
    assert args == (), "fabric-cicd 1.x FabricWorkspace is keyword-only"
    assert kwargs["token_credential"] is library.cred
    assert kwargs["workspace_id"] == "ws1"
    assert kwargs["repository_directory"] == "repo"
    assert kwargs["environment"] == "development"


def test_explicit_credential_wins_over_the_factory(library):
    mine = mock.Mock(name="mine")
    library.get_cred.side_effect = lambda c=None: c if c is not None else library.cred
    promoter = promotion_utils(_build_workspace(), mock.Mock(), credential=mine)
    promoter.publish_all()
    assert library.fw.call_args.kwargs["token_credential"] is mine


def test_default_scope_is_the_library_accepted_item_types(library):
    promoter = promotion_utils(_build_workspace(item_type_in_scope=None), mock.Mock())
    assert promoter.item_type_in_scope == list(pu_module.constants.ACCEPTED_ITEM_TYPES)
    assert "DataBuildToolJob" in promoter.item_type_in_scope


def test_explicit_scope_is_kept(library):
    promoter = promotion_utils(
        _build_workspace(item_type_in_scope=["Notebook", "Lakehouse"]), mock.Mock()
    )
    assert promoter.item_type_in_scope == ["Notebook", "Lakehouse"]


def test_empty_include_list_publishes_everything(library):
    """Since fabric-cicd 1.1.0 an empty items_to_include publishes nothing, so ingen_fab
    passes None when it means everything."""
    promoter = promotion_utils(_build_workspace(), mock.Mock())
    promoter.publish_all([])
    assert library.pub.call_args.kwargs["items_to_include"] is None
    promoter.publish_all(None)
    assert library.pub.call_args.kwargs["items_to_include"] is None


def test_include_list_is_passed_through(library):
    promoter = promotion_utils(_build_workspace(), mock.Mock())
    promoter.publish_all(["a.Notebook", "b.Lakehouse"])
    assert library.pub.call_args.kwargs["items_to_include"] == [
        "a.Notebook",
        "b.Lakehouse",
    ]


def test_publish_enables_the_flags_it_relies_on(library):
    promotion_utils(_build_workspace(), mock.Mock()).publish_all(["a.Notebook"])
    flags = pu_module.constants.FEATURE_FLAG
    for flag in (
        "enable_shortcut_publish",
        "enable_response_collection",
        "enable_items_to_include",
        "enable_experimental_features",
    ):
        assert flag in flags


def test_workspace_settings_carry_a_credential(library):
    mine = mock.Mock(name="mine")
    library.get_cred.side_effect = lambda c=None: c if c is not None else library.cred
    settings = WorkspaceSettings(
        workspace_id="ws1", repository_directory=Path("repo"), credential=mine
    )
    promoter = promotion_utils(settings)
    assert promoter.credential is mine
    assert promoter.environment == "N/A"


# --- responses -> results ----------------------------------------------------------


def test_results_from_collected_responses():
    responses = {
        "Notebook": {
            "nb_ok": {"status_code": 200, "body": {}, "header": {}},
            "nb_moved": {
                "publish_response": {"status_code": 201, "body": {}},
                "move_response": {"status_code": 200},
            },
            "nb_bad": {"status_code": 400, "body": {"error": "x"}},
        },
        "Lakehouse": {"lh": {"status_code": 200}},
    }
    results = {r.key: r for r in publish_results_from_responses(responses)}
    assert results["nb_ok.notebook"].success
    assert results["nb_moved.notebook"].success
    assert results["nb_moved.notebook"].status_code == 201
    assert not results["nb_bad.notebook"].success
    assert results["nb_bad.notebook"].error == "HTTP 400"
    assert results["lh.lakehouse"].success


def test_results_without_status_code_count_as_success():
    # bulk publish stores some responses without a usable code; absence is not failure
    results = publish_results_from_responses({"Notebook": {"n": {"body": {}}}})
    assert results[0].success and results[0].status_code is None


def test_results_on_exception_mark_missing_attempted_items_failed():
    responses = {"Notebook": {"first": {"status_code": 200}}}
    err = RuntimeError("boom")
    results = {
        r.key: r
        for r in publish_results_from_responses(
            responses, ["first.Notebook", "second.Notebook", "lh.Lakehouse"], error=err
        )
    }
    assert results["first.notebook"].success
    assert not results["second.notebook"].success
    assert results["second.notebook"].error == "boom"
    assert results["lh.lakehouse"].item_type == "Lakehouse"


def test_results_without_error_ignore_attempted_list():
    assert publish_results_from_responses(None, ["a.Notebook"]) == []


def test_publish_items_maps_the_library_return(library):
    library.pub.return_value = {"Notebook": {"n": {"status_code": 200}}}
    results = publish_items(mock.Mock(), ["n.Notebook"])
    assert [r.key for r in results] == ["n.notebook"]
    assert results[0].success


# --- manifest update -------------------------------------------------------------


def _manifest_items(*names):
    return [
        SyncToFabricEnvironment.manifest_item(
            name=n, path=n, hash="h", status="updated", environment="development"
        )
        for n in names
    ]


def _sync(tmp_path):
    return SyncToFabricEnvironment(project_path=str(tmp_path), console=mock.Mock())


def test_manifest_updated_from_results(tmp_path):
    sync = _sync(tmp_path)
    items = _manifest_items("a.Notebook", "b.Notebook", "c.Lakehouse")
    entries = [
        PublishResult("a", "Notebook", True),
        PublishResult("b", "Notebook", False, error="HTTP 400"),
    ]
    with mock.patch.object(sync, "save_platform_manifest") as save:
        out = sync._update_manifest_with_results(
            items,
            entries,
            tmp_path / "m.yml",
            attempted_item_names={"a.Notebook", "b.Notebook"},
        )
    assert [i["name"] for i in out["deployed"]] == ["a.Notebook"]
    assert out["failed"] == [{"name": "b.Notebook", "error": "HTTP 400"}]
    by_name = {i.name: i.status for i in items}
    assert by_name == {
        "a.Notebook": "deployed",
        "b.Notebook": "failed",
        "c.Lakehouse": "updated",
    }
    save.assert_called_once()


def test_manifest_fallback_when_nothing_was_collected(tmp_path):
    sync = _sync(tmp_path)
    items = _manifest_items("a.Notebook")
    with mock.patch.object(sync, "save_platform_manifest"):
        out = sync._update_manifest_with_results(
            items, [], tmp_path / "m.yml", attempted_item_names={"a.Notebook"}
        )
    assert out["deployed"] == [{"name": "a.Notebook"}]
    assert items[0].status == "deployed"


def test_manifest_exception_without_results_marks_attempted_failed(tmp_path):
    sync = _sync(tmp_path)
    items = _manifest_items("a.Notebook")
    with mock.patch.object(sync, "save_platform_manifest"):
        out = sync._update_manifest_with_results(
            items,
            [],
            tmp_path / "m.yml",
            attempted_item_names={"a.Notebook"},
            exception_occurred=True,
        )
    assert out["failed"][0]["name"] == "a.Notebook"
    assert items[0].status == "failed"


def test_deleted_item_with_unchanged_hash_returns_to_deployed(tmp_path):
    """Regression for 963f6e6: a manifest entry previously marked deleted whose folder
    hash has not changed is set back to deployed on the next save."""
    sync = _sync(tmp_path)
    manifest_path = tmp_path / "platform_manifest_development.yml"
    existing = SyncToFabricEnvironment.manifest_item(
        name="a.Notebook",
        path="a.Notebook",
        hash="same",
        status="deleted",
        environment="development",
    )
    on_disk = SimpleNamespace(platform_folders=[existing])
    with mock.patch.object(sync, "_read_local_manifest_file", return_value=on_disk):
        in_memory = SyncToFabricEnvironment.manifest_item(
            name="a.Notebook",
            path="a.Notebook",
            hash="same",
            status="new",
            environment="development",
        )
        sync.save_platform_manifest([in_memory], manifest_path, perform_hash_check=True)
    assert in_memory.status == "deployed"


def test_sync_credential_is_resolved_once(tmp_path):
    sync = _sync(tmp_path)
    with mock.patch(f"{MODULE}.get_token_credential", return_value="cred") as factory:
        assert sync._get_credential() == "cred"
        assert sync._get_credential() == "cred"
    factory.assert_called_once()


def test_valueset_item_ids_updated_from_results(tmp_path):
    """AUTO_UPDATE_ITEM_IDS: variables named <name>_<type>_id take the live item id."""
    sync = _sync(tmp_path)
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
                    {"name": "lh_lakehouse_id", "value": "old"},
                    {"name": "untouched", "value": "x"},
                ]
            }
        ),
        encoding="utf-8",
    )
    api = mock.Mock()
    api.list_workspace_items.return_value = [
        {"displayName": "lh", "type": "Lakehouse", "id": "new-id"}
    ]
    with (
        mock.patch(f"{MODULE}.FabricApiUtils", return_value=api) as api_cls,
        mock.patch(f"{MODULE}.get_token_credential", return_value="cred"),
    ):
        sync._update_variables_with_item_ids_after_deployment(
            [
                PublishResult("lh", "Lakehouse", True),
                PublishResult("nb", "Notebook", False),
            ],
            workspace_id="ws1",
            environment="development",
        )
    assert api_cls.call_args.kwargs["credential"] == "cred"
    data = json.loads(vs.read_text(encoding="utf-8"))
    values = {v["name"]: v["value"] for v in data["variableOverrides"]}
    assert values == {"lh_lakehouse_id": "new-id", "untouched": "x"}
