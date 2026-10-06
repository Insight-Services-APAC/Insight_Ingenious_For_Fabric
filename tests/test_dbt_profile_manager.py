"""dbt profile generation for the native dbt-fabricspark adapter, and the orchestrator notebook.

Everything runs offline: the profile is built from a value set on disk and validated by the
installed adapter's own credentials class; the notebook is rendered from the template; the
CLI proxy is exercised with the dbt process patched.
"""

import json
from unittest import mock

import pytest
import yaml

from ingen_fab.cli_utils import dbt_commands
from ingen_fab.cli_utils import dbt_profile_manager as pm

WS = "11111111-1111-1111-1111-111111111111"
LH_BRONZE = "22222222-2222-2222-2222-222222222222"
LH_SILVER = "33333333-3333-3333-3333-333333333333"
LOG_VARS = {
    "lh_log_workspace_id": WS,
    "lh_log_lakehouse_id": "55555555-5555-5555-5555-555555555555",
}


def _project(tmp_path, environments=("development",), extra=None, profile="if_demo"):
    """A project with a value set per environment and a dbt project declaring ``profile``."""
    vs_dir = (
        tmp_path / "fabric_workspace_items/config/var_lib.VariableLibrary/valueSets"
    )
    vs_dir.mkdir(parents=True)
    for env in environments:
        overrides = [
            {"name": "fabric_environment", "value": env},
            {"name": "fabric_deployment_workspace_id", "value": WS},
            {"name": "config_lakehouse_name", "value": "config"},
            {"name": "lh_bronze_workspace_id", "value": WS},
            {"name": "lh_bronze_lakehouse_name", "value": "lh_bronze"},
            {"name": "lh_bronze_lakehouse_id", "value": LH_BRONZE},
            {"name": "lh_silver_workspace_id", "value": WS},
            {"name": "lh_silver_lakehouse_name", "value": "lh_silver"},
            {"name": "lh_silver_lakehouse_id", "value": LH_SILVER},
            {
                "name": "lh_gold_lakehouse_id",
                "value": "REPLACE_WITH_LH_GOLD_LAKEHOUSE_GUID",
            },
        ] + [{"name": k, "value": v} for k, v in (extra or {}).items()]
        (vs_dir / f"{env}.json").write_text(
            json.dumps({"variableOverrides": overrides}), encoding="utf-8"
        )
    dbt_dir = tmp_path / "dbt_project"
    dbt_dir.mkdir()
    (dbt_dir / "dbt_project.yml").write_text(
        f"name: 'dbt_project'\nversion: '1.0.0'\nconfig-version: 2\nprofile: '{profile}'\n",
        encoding="utf-8",
    )
    return tmp_path


# --- inputs -------------------------------------------------------------------------------


def test_profile_name_comes_from_dbt_project_yml(tmp_path):
    """Issue #23: no hardcoded profile name."""
    _project(tmp_path, profile="my_profile")
    assert pm.read_profile_name(tmp_path / "dbt_project") == "my_profile"


def test_missing_profile_key_is_an_error(tmp_path):
    (tmp_path / "dbt_project").mkdir()
    (tmp_path / "dbt_project" / "dbt_project.yml").write_text(
        "name: x\n", encoding="utf-8"
    )
    with pytest.raises(pm.ProfileError, match="'profile' is not set"):
        pm.read_profile_name(tmp_path / "dbt_project")


def test_lakehouses_are_discovered_by_prefix_and_placeholders_skipped(tmp_path):
    _project(tmp_path)
    values = pm.read_value_set(tmp_path, "development")
    found = pm.discover_lakehouses(values)
    assert list(found) == ["lh_bronze", "lh_silver"], "lh_gold has a placeholder id"
    assert found["lh_bronze"] == pm.LakehouseRef(
        "lh_bronze", "lh_bronze", LH_BRONZE, WS
    )


def test_default_lakehouse_selection_rules(tmp_path):
    _project(tmp_path)
    values = pm.read_value_set(tmp_path, "development")
    found = pm.discover_lakehouses(values)

    with pytest.raises(pm.ProfileError, match="several lakehouses"):
        pm.choose_default_lakehouse(values, found)  # no prompt: fail with the prefixes

    assert pm.choose_default_lakehouse(values, found, "lh_silver").name == "lh_silver"
    assert (
        pm.choose_default_lakehouse(
            {**values, "dbt_default_lakehouse": "lh_bronze"}, found
        ).name
        == "lh_bronze"
    )
    assert (
        pm.choose_default_lakehouse(
            {**values, "dbt_default_lakehouse": "lh_silver"}, found
        ).prefix
        == "lh_silver"
    )
    with pytest.raises(pm.ProfileError, match="not declared"):
        pm.choose_default_lakehouse(values, found, "lh_nope")
    with pytest.raises(pm.ProfileError, match="declared but has no id yet"):
        pm.choose_default_lakehouse(values, found, "lh_gold")  # placeholder id

    single = {"lh_bronze": found["lh_bronze"]}
    assert pm.choose_default_lakehouse(values, single).prefix == "lh_bronze"


# --- the generated profile ----------------------------------------------------------------


def test_profile_has_two_targets_per_environment_and_the_adapter_accepts_them(tmp_path):
    _project(
        tmp_path,
        environments=("development", "test"),
        extra={"dbt_default_lakehouse": "lh_bronze"},
    )
    profile = pm.build_profile(
        tmp_path,
        tmp_path / "dbt_project",
        ["development", "test"],
        default_environment="test",
    )

    assert list(profile) == ["if_demo"]
    body = profile["if_demo"]
    assert body["target"] == "test"
    assert list(body["outputs"]) == [
        "development",
        "development-notebook",
        "test",
        "test-notebook",
    ]

    dev = body["outputs"]["development"]
    assert dev["type"] == "fabricspark" and dev["method"] == "livy"
    assert dev["workspaceid"] == WS and dev["lakehouseid"] == LH_BRONZE
    assert dev["lakehouse"] == "lh_bronze" and dev["schema"] == "lh_bronze"
    assert dev["authentication"] == "token_credential"
    assert dev["credential_class"] == "azure.identity.DefaultAzureCredential"
    assert dev["credential_kwargs"] == {"exclude_interactive_browser_credential": True}
    assert dev["spark_config"] == {"name": "dbt-if_demo-development"}
    assert dev["reuse_session"] is False and dev["threads"] == 4

    nb = body["outputs"]["development-notebook"]
    assert nb["authentication"] == "int_tests", "the adapter's raw-token mode"
    assert nb["accessToken"] == "{{ env_var('DBT_FABRIC_TOKEN') }}"
    assert "credential_class" not in nb

    pm.validate_with_adapter(profile)  # the installed adapter's own checks, no network


def test_schema_and_threads_come_from_the_value_set(tmp_path):
    _project(
        tmp_path,
        extra={
            "dbt_default_lakehouse": "lh_bronze",
            "dbt_schema": "dbo",
            "dbt_threads": "8",
        },
    )
    profile = pm.build_profile(
        tmp_path,
        tmp_path / "dbt_project",
        ["development"],
        default_environment="development",
    )
    target = profile["if_demo"]["outputs"]["development"]
    assert target["schema"] == "dbo", (
        "schema-enabled lakehouse: schema differs from the lakehouse name"
    )
    assert target["threads"] == 8
    pm.validate_with_adapter(profile)


def test_adapter_rejects_a_bad_target(tmp_path):
    _project(tmp_path, extra={"dbt_default_lakehouse": "lh_bronze"})
    profile = pm.build_profile(
        tmp_path,
        tmp_path / "dbt_project",
        ["development"],
        default_environment="development",
    )
    profile["if_demo"]["outputs"]["development"]["workspaceid"] = "not-a-guid"
    with pytest.raises(pm.ProfileError, match="rejected by dbt-fabricspark"):
        pm.validate_with_adapter(profile)


def test_ensure_profile_writes_into_the_dbt_project(tmp_path):
    _project(
        tmp_path,
        environments=("development", "test"),
        extra={"dbt_default_lakehouse": "lh_bronze"},
    )
    folder = pm.ensure_profile(tmp_path, "dbt_project", "development")
    assert folder == tmp_path / "dbt_project" / "profiles"
    text = (folder / "profiles.yml").read_text(encoding="utf-8")
    assert text.startswith("# Generated by `ingen_fab dbt profile`")
    loaded = yaml.safe_load(text)
    assert loaded["if_demo"]["target"] == "development"
    assert set(loaded["if_demo"]["outputs"]) == {
        "development",
        "development-notebook",
        "test",
        "test-notebook",
    }


def test_golden_profile_matches(tmp_path):
    """The exact file a project gets, so a change in shape is a deliberate diff."""
    _project(
        tmp_path, extra={"dbt_default_lakehouse": "lh_bronze", "dbt_schema": "dbo"}
    )
    folder = pm.ensure_profile(
        tmp_path, "dbt_project", "development", all_environments=False
    )
    expected = pm.GENERATED_HEADER + yaml.safe_dump(
        {
            "if_demo": {
                "target": "development",
                "outputs": {
                    "development": {
                        "type": "fabricspark",
                        "method": "livy",
                        "endpoint": "https://api.fabric.microsoft.com/v1",
                        "workspaceid": WS,
                        "lakehouseid": LH_BRONZE,
                        "lakehouse": "lh_bronze",
                        "schema": "dbo",
                        "threads": 4,
                        "connect_retries": 2,
                        "connect_timeout": 30,
                        "reuse_session": False,
                        "spark_config": {"name": "dbt-if_demo-development"},
                        "authentication": "token_credential",
                        "credential_class": "azure.identity.DefaultAzureCredential",
                        "credential_kwargs": {
                            "exclude_interactive_browser_credential": True
                        },
                    },
                    "development-notebook": {
                        "type": "fabricspark",
                        "method": "livy",
                        "endpoint": "https://api.fabric.microsoft.com/v1",
                        "workspaceid": WS,
                        "lakehouseid": LH_BRONZE,
                        "lakehouse": "lh_bronze",
                        "schema": "dbo",
                        "threads": 4,
                        "connect_retries": 2,
                        "connect_timeout": 30,
                        "reuse_session": False,
                        "spark_config": {"name": "dbt-if_demo-development-notebook"},
                        "authentication": "int_tests",
                        "accessToken": "{{ env_var('DBT_FABRIC_TOKEN') }}",
                    },
                },
            }
        },
        sort_keys=False,
        default_flow_style=False,
    )
    assert (folder / "profiles.yml").read_text(encoding="utf-8") == expected


# --- the CLI proxy --------------------------------------------------------------------------


def _ctx(tmp_path, environment="development"):
    return mock.Mock(
        obj={
            "fabric_workspace_repo_dir": str(tmp_path),
            "fabric_environment": environment,
        }
    )


def test_run_dbt_regenerates_the_profile_and_calls_dbt_with_it(tmp_path):
    _project(tmp_path, extra={"dbt_default_lakehouse": "lh_bronze"})
    with (
        mock.patch.object(dbt_commands, "dbt_executable", return_value="/venv/bin/dbt"),
        mock.patch.object(
            dbt_commands.subprocess, "run", return_value=mock.Mock(returncode=0)
        ) as run,
    ):
        rc = dbt_commands.run_dbt(
            _ctx(tmp_path), "build", "dbt_project", ["--select", "tag:silver"]
        )
    assert rc == 0
    command = run.call_args.args[0]
    assert command[:2] == ["/venv/bin/dbt", "build"]
    assert command[command.index("--project-dir") + 1] == str(tmp_path / "dbt_project")
    assert command[command.index("--profiles-dir") + 1] == str(
        tmp_path / "dbt_project" / "profiles"
    )
    assert command[command.index("--target") + 1] == "development"
    assert command[-2:] == ["--select", "tag:silver"]
    assert run.call_args.kwargs["env"]["DBT_PROFILES_DIR"] == str(
        tmp_path / "dbt_project" / "profiles"
    )
    assert (tmp_path / "dbt_project" / "profiles" / "profiles.yml").is_file()


def test_run_dbt_without_dbt_on_path_fails_clearly(tmp_path):
    import typer

    _project(tmp_path, extra={"dbt_default_lakehouse": "lh_bronze"})
    with (
        mock.patch.object(dbt_commands, "dbt_executable", return_value=None),
        pytest.raises(typer.Exit),
    ):
        dbt_commands.run_dbt(_ctx(tmp_path), "run", "dbt_project", [])


# --- the orchestrator notebook -------------------------------------------------------------


def test_orchestrator_notebook_is_written_as_a_deployable_item(tmp_path):
    _project(tmp_path, extra={"dbt_default_lakehouse": "lh_bronze", **LOG_VARS})
    path = dbt_commands.write_orchestrator_notebook(
        _ctx(tmp_path), "dbt_project", "dbtload_silver", "+tag:silver"
    )
    assert (
        path
        == tmp_path / "fabric_workspace_items" / "notebooks" / "dbtload_silver.Notebook"
    )
    platform = json.loads((path / ".platform").read_text(encoding="utf-8"))
    assert platform["metadata"] == {
        "type": "Notebook",
        "displayName": "dbtload_silver",
        "description": "dbt build --select +tag:silver for dbt_project",
    }
    content = (path / "notebook-content.py").read_text(encoding="utf-8")
    # a Python notebook: it hosts the dbt process and holds no Spark session of its own
    assert (
        '"name": "jupyter"' in content
        and '"jupyter_kernel_name": "python3.11"' in content
    )
    assert "synapse_pyspark" not in content
    assert '"defaultLakehouse": {' in content and '"name": "config"' in content
    assert (
        'dbt_command = "build"' in content and 'dbt_select = "+tag:silver"' in content
    )
    assert 'dbt_vars = ""' in content and 'dbt_target = ""' in content
    assert "ENV_FROM_VARIABLES = {}" in content
    assert 'variable("fabric_environment")' in content
    assert 'variableLibrary.get(f"$(/**/var_lib/{name})")' in content
    assert 'target = dbt_target or f"{environment}-notebook"' in content
    # install and run go through subprocess with a failure check
    assert (
        'step("pip_install", [sys.executable, "-m", "pip", "install", "-q", "-r"'
        in content
    )
    assert "raise RuntimeError(" in content
    # logs go to the log lakehouse (lh_log by default), not to the config or a data lakehouse
    assert (
        "variable('lh_log_workspace_id')" in content
        and "variable('lh_log_lakehouse_id')" in content
    )
    assert "/Files/dbt_runs/dbt_project/dbtload_silver/{STAMP}" in content
    assert "/lakehouse/default/Files/dbt_runs" not in content
    # dbt writes to local scratch; the run folder is published even on failure
    assert '"DBT_LOG_PATH": str(LOCAL_RUN / "dbt_logs")' in content
    assert "notebook_error.log" in content and 'publish_run_folder("failed"' in content
    assert "run_summary.json" in content
    assert 'notebookutils.credentials.getToken("pbi")' in content
    assert (
        '"DBT_FABRIC_TOKEN": fabric_token' in content
        and '"PYTHONUNBUFFERED": "1"' in content
    )
    assert "%%bash" not in content and "!pip" not in content
    assert "{{" not in content, "every template variable rendered"


def test_orchestrator_notebook_log_lakehouse_and_vars(tmp_path):
    """`log_lakehouse` from the value set, or the option, replaces lh_log; --vars reaches dbt."""
    other = {
        "lh_audit_workspace_id": WS,
        "lh_audit_lakehouse_id": "66666666-6666-6666-6666-666666666666",
    }
    _project(
        tmp_path,
        extra={
            "dbt_default_lakehouse": "lh_bronze",
            "log_lakehouse": "lh_audit",
            **other,
        },
    )
    path = dbt_commands.write_orchestrator_notebook(
        _ctx(tmp_path),
        "dbt_project",
        "dbtload_nb",
        "silver gold",
        dbt_vars="{silver_lakehouse: lh_silver_nb}",
    )
    content = (path / "notebook-content.py").read_text(encoding="utf-8")
    assert "variable('lh_audit_lakehouse_id')" in content and "lh_log" not in content
    assert 'dbt_vars = "{silver_lakehouse: lh_silver_nb}"' in content
    assert 'arguments += ["--vars", dbt_vars]' in content


def test_orchestrator_notebook_target_and_env_for_another_adapter(tmp_path):
    """A warehouse project with a hand-written profile: its own target and a SQL endpoint that
    the notebook reads from the Variable Library and exports to the dbt process."""
    _project(
        tmp_path,
        extra={
            "dbt_default_lakehouse": "lh_bronze",
            "wh_silver_warehouse_endpoint": "x.datawarehouse.fabric.microsoft.com",
            **LOG_VARS,
        },
    )
    path = dbt_commands.write_orchestrator_notebook(
        _ctx(tmp_path),
        "dbt_warehouse",
        "dbtload_warehouse",
        "path:models",
        target="notebook",
        env_variables={"DBT_WAREHOUSE_ENDPOINT": "wh_silver_warehouse_endpoint"},
    )
    content = (path / "notebook-content.py").read_text(encoding="utf-8")
    assert 'dbt_target = "notebook"' in content
    assert 'target = dbt_target or f"{environment}-notebook"' in content
    assert (
        'ENV_FROM_VARIABLES = {"DBT_WAREHOUSE_ENDPOINT": "wh_silver_warehouse_endpoint"}'
        in content
    )
    assert (
        "**{name: variable(source) for name, source in ENV_FROM_VARIABLES.items()}"
        in content
    )
    assert "/lakehouse/default/Files/dbt_warehouse" in content

    with pytest.raises(pm.ProfileError, match="does_not_exist"):
        dbt_commands.write_orchestrator_notebook(
            _ctx(tmp_path),
            "dbt_warehouse",
            "dbtload_x",
            "path:models",
            env_variables={"X": "does_not_exist"},
        )


def test_orchestrator_notebook_needs_a_declared_log_lakehouse(tmp_path):
    """No lh_log in the Variable Library and nothing defined: fail at generation, with the fix."""
    _project(tmp_path, extra={"dbt_default_lakehouse": "lh_bronze"})
    with pytest.raises(pm.ProfileError, match="log lakehouse 'lh_log' is not declared"):
        dbt_commands.write_orchestrator_notebook(
            _ctx(tmp_path), "dbt_project", "dbtload_x", "silver"
        )


def test_dbt_executable_is_found_next_to_the_interpreter(tmp_path, monkeypatch):
    """Running `python -m ingen_fab.cli` without activating the venv must still find dbt."""
    scripts = tmp_path / "Scripts"
    scripts.mkdir()
    (scripts / "python.exe").write_text("", encoding="utf-8")
    (scripts / "dbt.exe").write_text("", encoding="utf-8")
    monkeypatch.setattr(dbt_commands.sys, "executable", str(scripts / "python.exe"))
    monkeypatch.setattr(dbt_commands.shutil, "which", lambda name: None)
    assert dbt_commands.dbt_executable() == str(scripts / "dbt.exe")


# --- project upload -------------------------------------------------------------------------


def test_dbt_upload_skips_generated_folders_and_dunder_dirs(tmp_path):
    """Seen live: the upload sent target/ (with binary partial_parse.msgpack) and failed."""
    from ingen_fab.az_cli.onelake_utils import DBT_GENERATED_DIRS, collect_upload_files

    for rel in (
        "dbt_project.yml",
        "models/silver/a.sql",
        "profiles/profiles.yml",
        "requirements.txt",
        "target/partial_parse.msgpack",
        "target/run/x.sql",
        "logs/dbt.log",
        "dbt_packages/pkg/macros/m.sql",
        "__pycache__/x.pyc",
    ):
        f = tmp_path / rel
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"\x80" if rel.endswith(".msgpack") else b"x")
    files = {
        p.relative_to(tmp_path).as_posix()
        for p in collect_upload_files(tmp_path, None, DBT_GENERATED_DIRS)
    }
    assert files == {
        "dbt_project.yml",
        "models/silver/a.sql",
        "profiles/profiles.yml",
        "requirements.txt",
    }

    other = tmp_path / "bad"
    other.mkdir()
    _project(
        other,
        extra={"dbt_default_lakehouse": "lh_bronze", "dbt_reuse_session": "maybe"},
    )
    with pytest.raises(pm.ProfileError, match="dbt_reuse_session"):
        pm.build_profile(
            other,
            other / "dbt_project",
            ["development"],
            default_environment="development",
        )


def test_other_environments_with_placeholders_are_skipped_not_fatal(tmp_path):
    """The sample project ships `local` and `test` value sets full of placeholders; the
    current environment must resolve, the others are left out and named."""
    _project(
        tmp_path,
        environments=("development",),
        extra={"dbt_default_lakehouse": "lh_bronze"},
    )
    vs_dir = (
        tmp_path / "fabric_workspace_items/config/var_lib.VariableLibrary/valueSets"
    )
    (vs_dir / "test.json").write_text(
        json.dumps(
            {
                "variableOverrides": [
                    {"name": "lh_bronze_lakehouse_id", "value": "REPLACE_WITH_GUID"}
                ]
            }
        ),
        encoding="utf-8",
    )
    skipped: dict = {}
    folder = pm.ensure_profile(tmp_path, "dbt_project", "development", skipped=skipped)
    outputs = yaml.safe_load((folder / "profiles.yml").read_text(encoding="utf-8"))[
        "if_demo"
    ]["outputs"]
    assert set(outputs) == {"development", "development-notebook"}
    assert list(skipped) == ["test"]

    with pytest.raises(pm.ProfileError, match="environment 'test'"):
        pm.ensure_profile(tmp_path, "dbt_project", "test", skipped=skipped)


def test_missing_adapter_is_a_clear_profile_error(tmp_path, monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("dbt.adapters.fabricspark"):
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(pm.ProfileError, match="uv sync --group dbt"):
        pm.validate_with_adapter({"p": {"outputs": {"t": {"type": "fabricspark"}}}})
