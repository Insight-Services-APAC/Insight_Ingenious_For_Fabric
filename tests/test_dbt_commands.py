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
    # the default store is checked at run time; a missing one falls back to the project's own
    assert (
        "SHOW DATABASES LIKE" in macro
        and "set fallback = target.lakehouse or target.schema" in macro
    )
    assert (
        "FROM sys.databases WHERE name" in macro and "return(target.database)" in macro
    )
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


def test_command_line_injects_no_target_for_a_hand_written_profile(
    tmp_path: Path,
) -> None:
    """A dbt-fabric profile names its own targets and default: `ingen_fab dbt build` must not
    pass the Fabric environment as the target (the sample's has notebook and laptop only)."""
    line = dbt_commands.dbt_command_line(
        "dbt",
        "build",
        tmp_path,
        tmp_path / "profiles",
        "development",
        [],
        generated_profile=False,
    )
    assert "--target" not in line
    line = dbt_commands.dbt_command_line(
        "dbt",
        "build",
        tmp_path,
        tmp_path / "profiles",
        "development",
        ["--target", "laptop"],
        generated_profile=False,
    )
    assert line[line.index("--target") + 1] == "laptop"
    line = dbt_commands.dbt_command_line(
        "dbt",
        "build",
        tmp_path,
        tmp_path / "profiles",
        "development",
        [],
        generated_profile=True,
    )
    assert line[line.index("--target") + 1] == "development"


def test_names_that_could_leave_their_folder_are_refused() -> None:
    import typer

    for bad in ("../outside", "a/b", "a\b", ".hidden", "", "x..y/"):
        with pytest.raises(typer.Exit):
            dbt_commands.check_name(bad, "notebook name", dbt_commands.ITEM_NAME)
    assert (
        dbt_commands.check_name(
            "dbtload_silver", "notebook name", dbt_commands.ITEM_NAME
        )
        == "dbtload_silver"
    )
    assert (
        dbt_commands.check_name(
            "dbt load silver", "notebook name", dbt_commands.ITEM_NAME
        )
        == "dbt load silver"
    )
    with pytest.raises(typer.Exit):
        dbt_commands.check_name("dbt project", "dbt project", dbt_commands.PROJECT_NAME)
    assert (
        dbt_commands.check_name(
            "dbt_warehouse", "dbt project", dbt_commands.PROJECT_NAME
        )
        == "dbt_warehouse"
    )


def test_logging_macro_renders_safe_values_for_both_dialects() -> None:
    """The VALUES rows are rendered with plain Jinja (no dbt context needed for these macros):
    quotes are doubled in both dialects, backslashes only for Spark SQL, which reads them as
    escapes; NULL for a missing value; timestamps cast to the dialect's type."""
    import datetime as dt

    import jinja2

    module = (
        jinja2.Environment(extensions=["jinja2.ext.do"])
        .from_string(dbt_commands.LOGGING_MACRO.read_text(encoding="utf-8"))
        .module
    )
    rows = [
        {
            "unique_id": "model.p.m",
            "resource_type": "model",
            "name": "m",
            "status": "error",
            "message": "It's broken: path C:\\temp\\",
            "execution_time": 1.5,
            "started_at": dt.datetime(2026, 10, 7, 3, 26, 46),
            "ended_at": None,
            "failures": 0,
        }
    ]
    spark = module.ingen_fab_node_values(rows, "b1", "TIMESTAMP", True)
    assert "'It''s broken: path C:\\\\temp\\\\'" in spark
    assert "CAST('2026-10-07 03:26:46' AS TIMESTAMP), NULL, 0)" in spark
    tsql = module.ingen_fab_node_values(rows, "b1", "DATETIME2(3)", False)
    assert "'It''s broken: path C:\\temp\\'" in tsql
    assert "CAST('2026-10-07 03:26:46' AS DATETIME2(3)), NULL, 0)" in tsql
    assert module.ingen_fab_sql_string(None) == "NULL"
    # dbt runs the hook only after build/run/test/seed/snapshot with at least one node
    macro = dbt_commands.LOGGING_MACRO.read_text(encoding="utf-8")
    assert "{% if execute %}" in macro


def test_log_store_macros_pick_the_store_or_fall_back(monkeypatch) -> None:
    """The store macros are executed with a stub dbt context: a store the workspace has is
    used; a missing one falls back to the project's own lakehouse or warehouse, with a log
    line; the lookup literal escapes a quote."""
    import jinja2

    class Found:
        def __init__(self, names):
            self.rows = [(n,) for n in names]

    class Return(Exception):
        def __init__(self, value):
            self.value = value

    def run(macro_name, *, present, wanted, target, queries, logs):
        env = jinja2.Environment(extensions=["jinja2.ext.do"])

        def run_query(sql):
            queries.append(sql)
            return Found([n for n in present if n in sql])

        def _return(value):
            raise Return(value)

        env.globals.update(
            run_query=run_query,
            var=lambda name, default=None: wanted if wanted is not None else default,
            target=target,
            log=lambda msg, info=False: logs.append(msg),
            return_=_return,
        )
        env.globals["return"] = _return
        module = env.from_string(
            dbt_commands.LOGGING_MACRO.read_text(encoding="utf-8")
        ).module
        try:
            getattr(module, macro_name)()
        except Return as r:
            return r.value
        raise AssertionError("the macro did not return")

    spark_target = {"lakehouse": "lh_silver", "schema": "lh_silver", "database": None}
    q, logs = [], []
    assert (
        run(
            "ingen_fab_log_store_spark",
            present=["lh_log"],
            wanted=None,
            target=spark_target,
            queries=q,
            logs=logs,
        )
        == "lh_log"
    )
    assert q == ["SHOW DATABASES LIKE 'lh_log'"] and logs == []
    q, logs = [], []
    assert (
        run(
            "ingen_fab_log_store_spark",
            present=[],
            wanted="nope",
            target=spark_target,
            queries=q,
            logs=logs,
        )
        == "lh_silver"
    )
    assert "nope not found" in logs[0] and "lh_silver" in logs[0]
    q, logs = [], []
    run(
        "ingen_fab_log_store_spark",
        present=[],
        wanted="it's",
        target=spark_target,
        queries=q,
        logs=logs,
    )
    assert q == ["SHOW DATABASES LIKE 'it''s'"]

    wh_target = {"database": "wh_silver", "schema": "dbo"}
    q, logs = [], []
    assert (
        run(
            "ingen_fab_log_store_warehouse",
            present=["wh_log"],
            wanted=None,
            target=wh_target,
            queries=q,
            logs=logs,
        )
        == "wh_log"
    )
    assert q == ["SELECT name FROM sys.databases WHERE name = 'wh_log'"]
    q, logs = [], []
    assert (
        run(
            "ingen_fab_log_store_warehouse",
            present=[],
            wanted=None,
            target=wh_target,
            queries=q,
            logs=logs,
        )
        == "wh_silver"
    )
    assert "wh_log not found" in logs[0]


def test_check_name_refuses_a_trailing_newline() -> None:
    """`$` would let "dp\n" through `match`; the check is a full match, so a name that would
    end the notebook's header comment is refused."""
    import typer

    assert dbt_commands.check_name("dp", "x", dbt_commands.PROJECT_NAME) == "dp"
    with pytest.raises(typer.Exit):
        dbt_commands.check_name("dp\n", "x", dbt_commands.PROJECT_NAME)


def _macro_module(run_query, logs):
    """The logging macro as a plain Jinja module with a stub dbt context: run_query and log
    from the caller, raise_compiler_error and return as exceptions the test can catch."""
    import jinja2

    class Compiler(Exception):
        pass

    class Return(Exception):
        def __init__(self, value):
            self.value = value

    def raise_(exc):
        raise exc

    env = jinja2.Environment(extensions=["jinja2.ext.do"])
    env.globals.update(
        run_query=run_query,
        log=lambda msg, info=False: logs.append(msg),
        exceptions=type(
            "E",
            (),
            {"raise_compiler_error": staticmethod(lambda m: raise_(Compiler(m)))},
        )(),
    )
    env.globals["return"] = lambda value: raise_(Return(value))
    module = env.from_string(
        dbt_commands.LOGGING_MACRO.read_text(encoding="utf-8")
    ).module
    return module, Compiler, Return


def test_logging_macro_retires_a_legacy_table_before_creating_the_new_one() -> None:
    """The retired adapter left dbt_batch and dbt_execution_log with other columns, which
    CREATE TABLE IF NOT EXISTS would keep. On Spark the macro asks SHOW TABLES first (SHOW
    COLUMNS on a missing table raises), renames a table of another shape to <name>_v1 and
    creates the new one; a matching table is left alone; the inserts name their columns so a
    mismatch fails on a column, never on position."""

    class Found:
        def __init__(self, rows):
            self.rows = rows

    legacy = [("batch_id",), ("start_time",), ("status",), ("master_notebook",)]
    EXISTS = "SHOW TABLES IN lh_log LIKE 'dbt_batch'"
    COLUMNS = "SHOW COLUMNS IN lh_log.dbt_batch"
    RENAME = "ALTER TABLE lh_log.dbt_batch RENAME TO lh_log.dbt_batch_v1"

    def run(answers):
        queries, logs = [], []
        module, Compiler, Return = _macro_module(
            lambda sql: queries.append(sql) or Found(answers.get(sql, [])), logs
        )
        expected = module.ingen_fab_batch_columns().replace(" ", "").split(",")
        try:
            module.ingen_fab_legacy_table(
                "lh_log.dbt_batch", expected, COLUMNS, "lh_log.dbt_batch_v1", EXISTS
            )
        except Return:
            pass
        return queries, logs, expected

    # a legacy table: SHOW TABLES, SHOW COLUMNS, then the rename
    queries, logs, expected = run(
        {EXISTS: [("lh_log", "dbt_batch", False)], COLUMNS: legacy}
    )
    assert queries == [EXISTS, COLUMNS, RENAME]
    assert "renamed to lh_log.dbt_batch_v1" in logs[0]

    # a table of the right shape: nothing renamed
    queries, _, _ = run(
        {EXISTS: [("lh_log", "dbt_batch", False)], COLUMNS: [(c,) for c in expected]}
    )
    assert queries == [EXISTS, COLUMNS]

    # no table yet (a fresh log lakehouse): SHOW TABLES only, SHOW COLUMNS never asked
    queries, _, _ = run({})
    assert queries == [EXISTS]

    # a warehouse table of another shape stops the run with the table named
    module, Compiler, _ = _macro_module(lambda sql: Found(legacy), [])
    with pytest.raises(Compiler, match="wh_log.dbo.dbt_batch exists with columns"):
        module.ingen_fab_legacy_table(
            "wh_log.dbo.dbt_batch", expected, "SELECT ...", None
        )

    macro = dbt_commands.LOGGING_MACRO.read_text(encoding="utf-8")
    assert macro.count('" (" ~ ingen_fab_batch_columns() ~ ") VALUES "') == 2
    assert macro.count('" (" ~ ingen_fab_node_columns() ~ ") VALUES "') == 2
    assert 'INSERT INTO " ~ batch ~ " VALUES' not in macro
    assert "SHOW TABLES IN \" ~ db ~ \" LIKE 'dbt_batch'" in macro
    assert module.ingen_fab_node_columns().replace(" ", "").split(",") == [
        "batch_id",
        "unique_id",
        "resource_type",
        "name",
        "status",
        "message",
        "execution_time",
        "started_at",
        "ended_at",
        "failures",
    ]
