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
    DeployScopeError,
    PublishResult,
    SyncToFabricEnvironment,
    WorkspaceSettings,
    promotion_utils,
    publish_items,
    publish_results_from_responses,
    resolve_deploy_scope,
    split_by_scope,
    unsupported_results,
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


def test_attempted_item_without_response_is_not_published():
    """Seen live: fabric-cicd 1.3 silently skips an item whose type it does not accept
    (a misspelt type in .platform), returning no response and no error. The item must
    not vanish from the summary; it is reported as failed with a clear reason."""
    results = publish_results_from_responses(
        {"Notebook": {"a": {"status_code": 200}}}, ["a.Notebook", "b.Notebok"]
    )
    by_key = {r.key: r for r in results}
    assert by_key["a.notebook"].success
    assert not by_key["b.notebok"].success
    assert "not published" in by_key["b.notebok"].error
    assert "Notebok" in by_key["b.notebok"].error


def test_no_responses_and_no_attempted_gives_no_results():
    assert publish_results_from_responses(None) == []


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


def test_manifest_with_no_results_marks_attempted_items_failed(tmp_path):
    """With response collection on, a published item always has a response; an empty
    result set means nothing was published, never a silent success."""
    sync = _sync(tmp_path)
    items = _manifest_items("a.Notebook", "b.Notebook")
    with mock.patch.object(sync, "save_platform_manifest"):
        out = sync._update_manifest_with_results(
            items, [], tmp_path / "m.yml", attempted_item_names={"a.Notebook"}
        )
    assert out["deployed"] == []
    assert out["failed"] == [
        {"name": "a.Notebook", "error": "not published: no result recorded"}
    ]
    assert {i.name: i.status for i in items} == {
        "a.Notebook": "failed",
        "b.Notebook": "updated",
    }


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


def test_manifest_marks_attempted_item_without_result_failed(tmp_path):
    """Belt to the results mapping: an attempted item with no entry at all is failed."""
    sync = _sync(tmp_path)
    items = _manifest_items("a.Notebook", "b.Notebook")
    with mock.patch.object(sync, "save_platform_manifest"):
        out = sync._update_manifest_with_results(
            items,
            [PublishResult("a", "Notebook", True)],
            tmp_path / "m.yml",
            attempted_item_names={"a.Notebook", "b.Notebook"},
        )
    assert [i["name"] for i in out["deployed"]] == ["a.Notebook"]
    assert out["failed"] == [
        {"name": "b.Notebook", "error": "not published: no result recorded"}
    ]
    assert {i.name: i.status for i in items} == {
        "a.Notebook": "deployed",
        "b.Notebook": "failed",
    }


# --- deploy scope: ingen_fab's decision, applied before fabric-cicd is called ------------


def test_unset_or_empty_scope_is_every_accepted_type():
    from fabric_cicd import constants

    assert resolve_deploy_scope(None) == list(constants.ACCEPTED_ITEM_TYPES)
    assert resolve_deploy_scope("") == list(constants.ACCEPTED_ITEM_TYPES)
    assert resolve_deploy_scope("  ") == list(constants.ACCEPTED_ITEM_TYPES)


def test_separator_only_scope_is_every_accepted_type_not_nothing():
    """`ITEM_TYPES_TO_DEPLOY=","` must not become "skip everything, exit 0"."""
    from fabric_cicd import constants

    assert resolve_deploy_scope(",") == list(constants.ACCEPTED_ITEM_TYPES)
    assert resolve_deploy_scope(" , ,") == list(constants.ACCEPTED_ITEM_TYPES)


def test_scope_list_is_parsed_and_kept_in_order():
    assert resolve_deploy_scope(" Report, SemanticModel ,Notebook,") == [
        "Report",
        "SemanticModel",
        "Notebook",
    ]


def test_misspelt_scope_type_stops_before_publishing():
    """The live S6d case: 'Notebok' used to be ignored by the library and the notebook
    reported as not published; now the deploy refuses the scope up front."""
    with pytest.raises(DeployScopeError, match="Notebok"):
        resolve_deploy_scope("Lakehouse,Notebok")


def test_changed_items_outside_the_scope_are_split_off_not_attempted():
    items = _manifest_items(
        "a.Notebook", "m.SemanticModel", "r.Report", "p.DataPipeline"
    )
    in_scope, skipped, unsupported = split_by_scope(
        items,
        ["SemanticModel", "Report"],
        ["Notebook", "SemanticModel", "Report", "DataPipeline"],
    )
    assert [i.name for i in in_scope] == ["m.SemanticModel", "r.Report"]
    assert [i.name for i in skipped] == ["a.Notebook", "p.DataPipeline"]
    assert unsupported == []
    assert {i.status for i in skipped} == {"updated"}, "manifest status is left alone"


def test_unsupported_item_type_is_never_merely_out_of_scope():
    """A typo in a .platform type ('Notebok') must not hide behind the scope filter and let
    the deploy exit 0; it is split off as unsupported and reported as a failed item."""
    from fabric_cicd import constants

    items = _manifest_items("a.Notebook", "b.Notebok")
    in_scope, skipped, unsupported = split_by_scope(
        items, list(constants.ACCEPTED_ITEM_TYPES), list(constants.ACCEPTED_ITEM_TYPES)
    )
    assert [i.name for i in in_scope] == ["a.Notebook"]
    assert skipped == []
    assert [i.name for i in unsupported] == ["b.Notebok"]

    (result,) = unsupported_results(unsupported)
    assert result.key == "b.notebok" and not result.success
    assert "not published" in result.error and "Notebok" in result.error


def test_unsupported_item_is_marked_failed_in_the_manifest(tmp_path):
    sync = _sync(tmp_path)
    items = _manifest_items("a.Notebook", "b.Notebok")
    entries = [PublishResult("a", "Notebook", True)] + unsupported_results(items[1:])
    with mock.patch.object(sync, "save_platform_manifest"):
        out = sync._update_manifest_with_results(
            items,
            entries,
            tmp_path / "m.yml",
            attempted_item_names={i.name for i in items},
        )
    assert [i["name"] for i in out["deployed"]] == ["a.Notebook"]
    assert [i["name"] for i in out["failed"]] == ["b.Notebok"]
    assert items[1].status == "failed"


def _project(tmp_path, items, environment="development"):
    """A minimal project: one value set and one folder per ``name.Type`` with a .platform
    file and a notebook-content.py, so sync_environment can copy, substitute, hash and
    (with publishing mocked) publish it."""
    root = tmp_path / "fabric_workspace_items"
    vs_dir = root / "config" / "var_lib.VariableLibrary" / "valueSets"
    vs_dir.mkdir(parents=True)
    (vs_dir / f"{environment}.json").write_text(
        json.dumps(
            {
                "variableOverrides": [
                    {"name": "fabric_deployment_workspace_id", "value": "ws1"}
                ]
            }
        ),
        encoding="utf-8",
    )
    for i, full_name in enumerate(items):
        name, _, item_type = full_name.rpartition(".")
        folder = root / full_name
        folder.mkdir()
        (folder / ".platform").write_text(
            json.dumps(
                {
                    "metadata": {"type": item_type, "displayName": name},
                    "config": {
                        "version": "2.0",
                        "logicalId": f"0000000{i}-0000-0000-0000-000000000000",
                    },
                }
            ),
            encoding="utf-8",
        )
        (folder / "notebook-content.py").write_text(f"# {name}\n", encoding="utf-8")
    return tmp_path


def _manifest_statuses(tmp_path, environment="development"):
    import yaml

    data = yaml.safe_load(
        (tmp_path / f"platform_manifest_{environment}.yml").read_text(encoding="utf-8")
    )
    return {i["name"]: i["status"] for i in data["platform_folders"]}


def _run_sync(tmp_path, monkeypatch, responses):
    """Run sync_environment against the project in tmp_path with the library mocked.
    Returns (sync, FabricWorkspace mock, publish mock)."""
    monkeypatch.chdir(tmp_path)
    sync = _sync(tmp_path)
    with (
        mock.patch(f"{MODULE}.FabricWorkspace") as fw,
        mock.patch(f"{MODULE}.publish_all_items", return_value=responses) as pub,
        mock.patch(f"{MODULE}.get_token_credential", return_value="cred"),
    ):
        fw.return_value = mock.Mock(name="workspace", responses=responses)
        sync.sync_environment()
    return sync, fw, pub


def test_sync_publishes_only_in_scope_items_and_fails_on_unsupported_types(
    tmp_path, monkeypatch
):
    """End to end with publishing mocked: a manifest with an in-scope item, an accepted
    type outside the scope, and a type fabric-cicd does not accept. Only the in-scope
    item reaches the library; the skipped one keeps its status; the unsupported one is
    failed and the deploy exits 1."""
    _project(tmp_path, ["a.Notebook", "p.DataPipeline", "b.Notebok"])
    monkeypatch.setenv("ITEM_TYPES_TO_DEPLOY", "Notebook")

    with pytest.raises(SystemExit) as exc:
        sync, fw, pub = _run_sync(
            tmp_path, monkeypatch, {"Notebook": {"a": {"status_code": 200}}}
        )
    assert exc.value.code == 1

    statuses = _manifest_statuses(tmp_path)
    assert statuses == {
        "a.Notebook": "deployed",
        "p.DataPipeline": "new",  # skipped: status kept for a later, wider deploy
        "b.Notebok": "failed",  # unsupported type: never silently skipped
    }


def test_sync_call_arguments_and_summary_for_a_scoped_deploy(tmp_path, monkeypatch):
    """Same scenario without the unsupported item, so the run completes: the library gets
    the in-scope include list and the parsed scope, and the summary counts the skip."""
    _project(tmp_path, ["a.Notebook", "p.DataPipeline"])
    monkeypatch.setenv("ITEM_TYPES_TO_DEPLOY", "Notebook")

    sync, fw, pub = _run_sync(
        tmp_path, monkeypatch, {"Notebook": {"a": {"status_code": 200}}}
    )

    assert fw.call_args.kwargs["item_type_in_scope"] == ["Notebook"]
    assert pub.call_args.kwargs["items_to_include"] == ["a.Notebook"]
    assert _manifest_statuses(tmp_path) == {
        "a.Notebook": "deployed",
        "p.DataPipeline": "new",
    }
    printed = " ".join(
        str(a) for call in sync.console.print.call_args_list for a in call.args
    )
    assert "1 deployed, 0 failed, 0 unchanged, 1 skipped (out of scope)" in printed


def test_sync_with_only_unsupported_changes_builds_no_workspace(tmp_path, monkeypatch):
    """Nothing publishable: the library is not even constructed, the item is failed."""
    _project(tmp_path, ["b.Notebok"])
    monkeypatch.delenv("ITEM_TYPES_TO_DEPLOY", raising=False)

    with pytest.raises(SystemExit):
        _run_sync(tmp_path, monkeypatch, None)

    assert _manifest_statuses(tmp_path) == {"b.Notebok": "failed"}


def test_sync_rejects_an_unknown_scope_name_before_publishing(tmp_path, monkeypatch):
    """A typo in ITEM_TYPES_TO_DEPLOY stops the deploy: no workspace is built, nothing is
    published, and the manifest still shows the item as new."""
    _project(tmp_path, ["a.Notebook"])
    monkeypatch.setenv("ITEM_TYPES_TO_DEPLOY", "Notebok")
    monkeypatch.chdir(tmp_path)
    sync = _sync(tmp_path)
    with (
        mock.patch(f"{MODULE}.FabricWorkspace") as fw,
        mock.patch(f"{MODULE}.publish_all_items") as pub,
        mock.patch(f"{MODULE}.get_token_credential", return_value="cred"),
        pytest.raises(SystemExit) as exc,
    ):
        sync.sync_environment()
    assert exc.value.code == 1
    fw.assert_not_called()
    pub.assert_not_called()
    assert _manifest_statuses(tmp_path) == {"a.Notebook": "new"}


def test_summary_counts_skipped_items(tmp_path):
    sync = _sync(tmp_path)
    sync._print_deployment_summary(
        {"deployed": [], "failed": []}, unchanged=3, skipped=2
    )
    text = " ".join(
        str(a) for call in sync.console.print.call_args_list for a in call.args
    )
    assert "3 unchanged, 2 skipped (out of scope)" in text


def test_config_lakehouse_manifest_reads_and_writes_reuse_the_sync_credential(tmp_path):
    """The manifest download/upload through OneLake use the same credential the sync
    hands to fabric-cicd and the API helper; no second credential chain."""
    sync = _sync(tmp_path)
    sync.workspace_manifest_location = "config_lakehouse"
    ol = mock.Mock()
    ol.download_manifest_file_from_config_lakehouse.return_value = None
    with (
        mock.patch(f"{MODULE}.OneLakeUtils", return_value=ol) as ol_cls,
        mock.patch(f"{MODULE}.get_token_credential", return_value="cred"),
    ):
        sync.read_platform_manifest(tmp_path / "missing.yml")
        sync._upload_manifest_to_remote(tmp_path / "m.yml")
    assert ol_cls.call_count == 2
    assert all(c.kwargs["credential"] == "cred" for c in ol_cls.call_args_list)


# --- deploy-time variable substitution in dbt job and Ontology items ------------------------


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_environment_specific_json_files_finds_dbt_jobs_and_ontologies(tmp_path):
    job = _write(tmp_path / "dbt_jobs/j.DataBuildToolJob/dbt-content.json", "{}")
    binding = _write(
        tmp_path / "ontologies/o.Ontology/EntityTypes/1/DataBindings/b.json", "{}"
    )
    entity = _write(
        tmp_path / "ontologies/o.Ontology/EntityTypes/1/definition.json", "{}"
    )
    _write(
        tmp_path / "notebooks/n.Notebook/other.json", "{}"
    )  # not an environment-specific kind
    found = pu_module.environment_specific_json_files(tmp_path)
    assert found["dbt job"] == [job]
    assert found["ontology"] == sorted([binding, entity])


def test_substitute_variables_in_files_rewrites_only_what_changes(tmp_path):
    with_token = _write(
        tmp_path / "a.json", '{"itemId": "{{varlib:lh_gold_lakehouse_id}}"}'
    )
    without = _write(tmp_path / "b.json", '{"name": "City"}')
    before = without.stat().st_mtime_ns
    changed = pu_module.substitute_variables_in_files(
        [with_token, without],
        lambda text: text.replace("{{varlib:lh_gold_lakehouse_id}}", "abc"),
    )
    assert changed == 1
    assert with_token.read_text(encoding="utf-8") == '{"itemId": "abc"}'
    assert without.stat().st_mtime_ns == before


def test_ontology_binding_tokens_resolve_from_the_value_set(tmp_path):
    """End to end with the real VariableLibraryUtils: a data binding's lakehouse reference."""
    from ingen_fab.config_utils.variable_lib import VariableLibraryUtils

    vs = (
        tmp_path
        / "fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json"
    )
    _write(
        vs,
        json.dumps(
            {
                "variableOverrides": [
                    {"name": "fabric_environment", "value": "development"},
                    {
                        "name": "lh_gold_workspace_id",
                        "value": "11111111-1111-1111-1111-111111111111",
                    },
                    {
                        "name": "lh_gold_lakehouse_id",
                        "value": "22222222-2222-2222-2222-222222222222",
                    },
                ]
            }
        ),
    )
    out = tmp_path / "output"
    binding = _write(
        out / "ontologies/o.Ontology/EntityTypes/1/DataBindings/b.json",
        json.dumps(
            {
                "sourceTableProperties": {
                    "sourceType": "LakehouseTable",
                    "workspaceId": "{{varlib:lh_gold_workspace_id}}",
                    "itemId": "{{varlib:lh_gold_lakehouse_id}}",
                    "sourceTableName": "dim_cities",
                }
            }
        ),
    )
    vlu = VariableLibraryUtils(project_path=tmp_path, environment="development")
    changed = pu_module.substitute_variables_in_files(
        pu_module.environment_specific_json_files(out)["ontology"],
        lambda content: vlu.perform_code_replacements(
            content, replace_placeholders=True, inject_code=True
        ),
    )
    props = json.loads(binding.read_text(encoding="utf-8"))["sourceTableProperties"]
    assert changed == 1
    assert props["workspaceId"] == "11111111-1111-1111-1111-111111111111"
    assert props["itemId"] == "22222222-2222-2222-2222-222222222222"


def test_valueset_item_workspace_ids_filled_from_the_deployment_workspace(tmp_path):
    """AUTO_UPDATE_ITEM_IDS also fills <name>_workspace_id when it exists and still holds a
    placeholder (or nothing); a real value there is left alone."""
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
                    {
                        "name": "lh_lakehouse_id",
                        "value": "REPLACE_WITH_LH_LAKEHOUSE_ID",
                    },
                    {
                        "name": "lh_workspace_id",
                        "value": "REPLACE_WITH_LH_WORKSPACE_ID",
                    },
                    {"name": "wh_warehouse_id", "value": ""},
                    {"name": "wh_workspace_id", "value": "other-ws"},
                ]
            }
        ),
        encoding="utf-8",
    )
    api = mock.Mock()
    api.list_workspace_items.return_value = [
        {"displayName": "lh", "type": "Lakehouse", "id": "lh-id"},
        {"displayName": "wh", "type": "Warehouse", "id": "wh-id"},
    ]
    with (
        mock.patch(f"{MODULE}.FabricApiUtils", return_value=api),
        mock.patch(f"{MODULE}.get_token_credential", return_value="cred"),
    ):
        sync._update_variables_with_item_ids_after_deployment(
            [
                PublishResult("lh", "Lakehouse", True),
                PublishResult("wh", "Warehouse", True),
            ],
            workspace_id="ws1",
            environment="development",
        )
    data = json.loads(vs.read_text(encoding="utf-8"))
    values = {v["name"]: v["value"] for v in data["variableOverrides"]}
    assert values == {
        "lh_lakehouse_id": "lh-id",
        "lh_workspace_id": "ws1",
        "wh_warehouse_id": "wh-id",
        "wh_workspace_id": "other-ws",
    }
