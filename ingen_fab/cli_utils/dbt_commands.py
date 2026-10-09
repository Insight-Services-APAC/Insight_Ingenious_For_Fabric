"""dbt on Fabric Spark through the native ``dbt-fabricspark`` adapter.

Three things live here: the proxy that runs ``dbt`` with the project's generated profile,
the orchestrator notebook that runs the same dbt command inside Fabric against the project
uploaded to the config lakehouse, and the ``schema.yml`` generator from lakehouse metadata.
"""

import csv
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from ingen_fab.cli_utils import dbt_profile_manager as profiles
from ingen_fab.notebook_utils.notebook_utils import NotebookUtils

console = Console()

DBT_VERBS = (
    "build",
    "run",
    "test",
    "seed",
    "snapshot",
    "compile",
    "parse",
    "debug",
    "docs",
    "ls",
    "list",
    "clean",
    "deps",
    "show",
)
DEFAULT_VARIABLE_LIBRARY = "var_lib"
# Logs go to the log lakehouse; this is its name unless the project defines `log_lakehouse`.
DEFAULT_LOG_LAKEHOUSE = "lh_log"


def dbt_executable() -> Optional[str]:
    """The ``dbt`` entry point of the environment ingen_fab runs in.

    Looked up next to the running interpreter first (the venv's Scripts/bin folder, which is
    not on PATH when the interpreter is invoked without activating the venv), then on PATH.
    The interpreter path is taken as is: in a Linux or macOS venv ``sys.executable`` is a
    symlink to the base interpreter, and resolving it would look next to the wrong python.
    """
    for scripts in (Path(sys.executable).parent, Path(sys.executable).resolve().parent):
        for name in ("dbt.exe", "dbt"):
            candidate = scripts / name
            if candidate.is_file():
                return str(candidate)
    return shutil.which("dbt")


def dbt_command_line(
    exe: str,
    verb: str,
    dbt_project_dir: Path,
    profiles_dir: Path,
    environment: str,
    args: list[str],
    generated_profile: bool = True,
) -> list[str]:
    """The ``dbt`` command for a verb: explicit project and profiles directories, the
    environment as target where dbt takes one, then the pass-through arguments. ``dbt docs``
    is a command group whose subcommand (``generate``, ``serve``) must come before any option,
    so a leading positional argument is placed right after the verb."""
    args = list(args)
    command = [exe, verb]
    if verb == "docs" and args and not args[0].startswith("-"):
        command.append(args.pop(0))
    command += [
        "--project-dir",
        str(dbt_project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    # A generated profile has one target per environment, so the environment is the target
    # unless the caller chose one (`-- --target laptop`). A hand-written profile names its own
    # targets and its own default: nothing is injected.
    caller_chose_target = any(
        a in ("--target", "-t") or a.startswith("--target=") for a in args
    )
    if (
        generated_profile
        and verb not in ("clean", "deps", "docs")
        and not caller_chose_target
    ):
        command += ["--target", environment]
    return command + args


# Verbs that touch no lakehouse: they run even when the value set cannot yet yield a profile
# (ids still placeholders), for example to fetch packages before any workspace exists.
OFFLINE_VERBS = ("clean", "deps")

# What the orchestrator notebook may run: the verbs that take a selector (the notebook always
# passes --select; `dbt parse` does not accept one). The template carries the same set,
# rendered from here, and refuses anything else at run time.
ORCHESTRATOR_COMMANDS = ("build", "run", "test", "seed", "snapshot", "compile")


# The run-logging macro every project gets: dbt writes dbt_batch and dbt_execution_log into the
# log store from its on-run-end hook (the tables the Spark adapter's projects always had).
LOGGING_MACRO = (
    Path(__file__).resolve().parent.parent
    / "templates"
    / "dbt"
    / "macros"
    / "ingen_fab_logging.sql"
)
LOGGING_HOOK = "{{ ingen_fab_log_run(results) }}"


def refresh_logging_macro(dbt_project_dir: Path) -> Path:
    """Copy the generated logging macro into ``<dbt_project>/macros/`` (overwriting the previous
    copy) and warn when ``dbt_project.yml`` does not call it from ``on-run-end``."""
    target = dbt_project_dir / "macros" / LOGGING_MACRO.name
    target.parent.mkdir(parents=True, exist_ok=True)
    text = LOGGING_MACRO.read_text(encoding="utf-8")
    if not target.is_file() or target.read_text(encoding="utf-8") != text:
        target.write_text(text, encoding="utf-8", newline="\n")
    if not logging_hook_present(dbt_project_dir):
        console.print(
            f"[yellow]{dbt_project_dir.name}/dbt_project.yml has no on-run-end hook calling "
            f'ingen_fab_log_run: runs are not logged. Add:\n  on-run-end:\n    - "{LOGGING_HOOK}"[/yellow]'
        )
    return target


def logging_hook_present(dbt_project_dir: Path) -> bool:
    import yaml

    project_file = dbt_project_dir / "dbt_project.yml"
    if not project_file.is_file():
        return False
    data = yaml.safe_load(project_file.read_text(encoding="utf-8")) or {}
    hooks = data.get("on-run-end") or []
    if isinstance(hooks, str):
        hooks = [hooks]
    return any("ingen_fab_log_run" in str(h) for h in hooks)


# Names that become path components and Fabric item names: no separators, no dot segments.
PROJECT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
ITEM_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _-]*$")


def check_name(value: str, what: str, pattern: re.Pattern) -> str:
    """Stop with a message when a name could leave its folder or is not a Fabric item name."""
    if not pattern.fullmatch(value or ""):
        console.print(
            f"[red]{what} {value!r} is not valid: letters, digits, underscore and hyphen"
            f"{' and space' if pattern is ITEM_NAME else ''}, starting with a letter or digit[/red]"
        )
        raise typer.Exit(code=1)
    return value


def hand_written_profile(dbt_project_dir: Path) -> bool:
    """True when the project carries its own profiles.yml that ingen_fab did not generate
    (a dbt-fabric warehouse project, for example): never overwrite it."""
    path = dbt_project_dir / "profiles" / "profiles.yml"
    if not path.is_file():
        return False
    head = path.read_text(encoding="utf-8")[:400]
    return "Generated by `ingen_fab dbt profile`" not in head


def _project_context(ctx: typer.Context) -> tuple[Path, str]:
    project_path = ctx.obj.get("fabric_workspace_repo_dir") if ctx.obj else None
    environment = str(ctx.obj.get("fabric_environment")) if ctx.obj else ""
    if not project_path or not environment:
        console.print(
            "[red]Fabric workspace repo dir and environment must be set.[/red]"
        )
        raise typer.Exit(code=1)
    return Path(project_path), environment


def _report_skipped(skipped: dict[str, str]) -> None:
    for env_name, reason in skipped.items():
        console.print(
            f"[yellow]environment '{env_name}' left out of the profile: {reason}[/yellow]"
        )


def write_profile(
    ctx: typer.Context,
    dbt_project: str,
    lakehouse: Optional[str] = None,
    show: bool = True,
) -> Path:
    """``ingen_fab dbt profile``: (re)generate ``<dbt_project>/profiles/profiles.yml``."""
    project_path, environment = _project_context(ctx)
    check_name(dbt_project, "dbt project", PROJECT_NAME)
    if hand_written_profile(project_path / dbt_project):
        console.print(
            f"[yellow]{dbt_project}/profiles/profiles.yml is hand-written (not generated by "
            "ingen_fab, for example a dbt-fabric warehouse profile): left as it is.[/yellow]"
        )
        refresh_logging_macro(project_path / dbt_project)
        return project_path / dbt_project / "profiles" / "profiles.yml"
    skipped: dict[str, str] = {}
    try:
        folder = profiles.ensure_profile(
            project_path, dbt_project, environment, lakehouse=lakehouse, skipped=skipped
        )
    except profiles.ProfileError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)
    _report_skipped(skipped)
    refresh_logging_macro(project_path / dbt_project)
    path = folder / "profiles.yml"
    console.print(f"[green]✓[/green] Wrote {path}")
    if show:
        console.print(path.read_text(encoding="utf-8"))
    return path


def run_dbt(
    ctx: typer.Context,
    verb: str,
    dbt_project: str,
    args: list[str],
    lakehouse: Optional[str] = None,
) -> int:
    """``ingen_fab dbt <verb> ...``: regenerate the profile, then run ``dbt`` with it.

    The project directory and profiles directory are passed explicitly and the target is
    the current environment, so plain ``dbt`` behaviour is unchanged otherwise; every extra
    argument goes through untouched (``--select``, ``--full-refresh``, ...).
    """
    project_path, environment = _project_context(ctx)
    check_name(dbt_project, "dbt project", PROJECT_NAME)
    dbt_project_dir = project_path / dbt_project
    if not (dbt_project_dir / "dbt_project.yml").is_file():
        console.print(
            f"[red]dbt project not found: {dbt_project_dir / 'dbt_project.yml'}[/red]"
        )
        raise typer.Exit(code=1)
    skipped: dict[str, str] = {}
    if hand_written_profile(dbt_project_dir):
        # another adapter's profile (dbt-fabric for a warehouse): the project's own file is
        # the profile, ingen_fab neither generates nor touches it
        profiles_dir = dbt_project_dir / "profiles"
        if lakehouse:
            console.print(
                "[yellow]--lakehouse applies to a generated profile only; this project's "
                "hand-written profile is used as it is[/yellow]"
            )
    else:
        try:
            profiles_dir = profiles.ensure_profile(
                project_path,
                dbt_project,
                environment,
                lakehouse=lakehouse,
                skipped=skipped,
            )
        except profiles.ProfileError as e:
            if verb not in OFFLINE_VERBS:
                console.print(f"[red]{e}[/red]")
                raise typer.Exit(code=1)
            # clean and deps touch no lakehouse: run them with whatever profiles folder exists
            console.print(
                f"[yellow]profile not generated ({e}); `dbt {verb}` needs none[/yellow]"
            )
            profiles_dir = dbt_project_dir / "profiles"
            profiles_dir.mkdir(parents=True, exist_ok=True)
    _report_skipped(skipped)
    refresh_logging_macro(dbt_project_dir)

    exe = dbt_executable()
    if not exe:
        console.print(
            "[red]'dbt' was not found next to this Python interpreter nor on PATH. "
            "Install the dbt dependency group (uv sync --group dbt).[/red]"
        )
        raise typer.Exit(code=1)

    command = dbt_command_line(
        exe,
        verb,
        dbt_project_dir,
        profiles_dir,
        environment,
        args,
        generated_profile=not hand_written_profile(dbt_project_dir),
    )
    console.print(f"[dim]{' '.join(command)}[/dim]")
    env = {**os.environ, "DBT_PROFILES_DIR": str(profiles_dir)}
    result = subprocess.run(command, check=False, env=env)
    return result.returncode


def write_orchestrator_notebook(
    ctx: typer.Context,
    dbt_project: str,
    notebook_name: str,
    select: str,
    command: str = "build",
    config_lakehouse: Optional[str] = None,
    threads: int = 4,
    variable_library: str = DEFAULT_VARIABLE_LIBRARY,
    log_lakehouse: Optional[str] = None,
    dbt_vars: str = "",
    target: str = "",
    env_variables: Optional[dict[str, str]] = None,
    indirect_selection: str = "",
    log_warehouse: str = "",
) -> Path:
    """``ingen_fab dbt orchestrator``: a Python notebook item that runs ``dbt <command> --select
    <select>`` inside Fabric against the uploaded project, deployable with ``deploy deploy``.

    The run is logged by dbt's on-run-end hook into the log store (``dbt_batch`` and
    ``dbt_execution_log``); the log lakehouse is ``log_lakehouse`` if given, else the value
    set's ``log_lakehouse`` variable, else ``lh_log``, and a failed run's log files are kept
    there under ``Files/dbt_failures/``. That lakehouse must be declared in the project
    (``<name>_workspace_id`` and ``<name>_lakehouse_id`` in the Variable Library), because the
    notebook resolves it there. ``log_warehouse`` names the log store of a dbt-fabric project.

    ``target`` overrides the profile target (default ``<environment>-notebook``, the one
    ``ingen_fab dbt profile`` writes). ``env_variables`` maps an environment variable for the
    dbt process to a Variable Library variable, read at run time: a hand-written profile for
    another adapter can then use ``env_var()``, for example for a warehouse SQL endpoint."""
    check_name(notebook_name, "notebook name", ITEM_NAME)
    if log_warehouse:
        check_name(log_warehouse, "log warehouse", PROJECT_NAME)
    if any(ord(ch) < 32 for ch in select):
        # the selector is rendered into the notebook's header comment as well as its code
        raise profiles.ProfileError(
            "--select must be one line without control characters"
        )
    if command not in ORCHESTRATOR_COMMANDS:
        raise profiles.ProfileError(
            f"--command {command!r} is not one the orchestrator runs; choose one of "
            f"{', '.join(ORCHESTRATOR_COMMANDS)}"
        )
    project_path, environment = _project_context(ctx)
    check_name(dbt_project, "dbt project", PROJECT_NAME)
    if not (project_path / dbt_project / "dbt_project.yml").is_file():
        raise profiles.ProfileError(
            f"dbt project not found: {project_path / dbt_project / 'dbt_project.yml'}"
        )
    if dbt_vars:
        # a bad --vars would fail only inside the Fabric run, twenty seconds in
        import yaml

        try:
            parsed = yaml.safe_load(dbt_vars)
        except yaml.YAMLError as e:
            raise profiles.ProfileError(f"--vars is not valid YAML or JSON: {e}")
        if not isinstance(parsed, dict):
            raise profiles.ProfileError(
                "--vars must be a mapping, for example '{silver_lakehouse: lh_silver_nb}'"
            )
    refresh_logging_macro(project_path / dbt_project)
    values = profiles.read_value_set(project_path, environment)
    if config_lakehouse is None:
        config_lakehouse = values.get("config_lakehouse_name") or "config"
    log_lakehouse = (
        log_lakehouse or values.get("log_lakehouse") or DEFAULT_LOG_LAKEHOUSE
    )
    missing = [
        name
        for name in (f"{log_lakehouse}_workspace_id", f"{log_lakehouse}_lakehouse_id")
        if name not in values
    ]
    if missing:
        raise profiles.ProfileError(
            f"log lakehouse '{log_lakehouse}' is not declared in the Variable Library "
            f"(missing {', '.join(missing)}). Add it to fabric_config/storage_config.yaml and run "
            "`ingen_fab init storage-config`, or name another one with --log-lakehouse or the "
            "`log_lakehouse` variable."
        )
    unknown = sorted(v for v in (env_variables or {}).values() if v not in values)
    if unknown:
        raise profiles.ProfileError(
            f"--env names Variable Library variable(s) that the '{environment}' value set does not "
            f"have: {', '.join(unknown)}"
        )
    utils = NotebookUtils(
        templates_dir=Path(__file__).resolve().parent.parent / "templates",
        fabric_workspace_repo_dir=project_path,
        output_dir=project_path / "fabric_workspace_items" / "notebooks",
        enable_console=False,
    )
    rendered = utils.render_template(
        "dbt/orchestrator_notebook.py.jinja",
        notebook_name=notebook_name,
        dbt_project=dbt_project,
        dbt_command=command,
        allowed_commands=sorted(ORCHESTRATOR_COMMANDS),
        dbt_select=select,
        dbt_threads=threads,
        config_lakehouse_name=config_lakehouse,
        variable_library=variable_library,
        log_lakehouse=log_lakehouse,
        dbt_vars=dbt_vars,
        dbt_target=target,
        env_variables=env_variables or {},
        dbt_indirect_selection=indirect_selection,
        log_warehouse=log_warehouse,
    )
    path = utils.create_notebook_with_platform(
        notebook_name=notebook_name,
        rendered_content=rendered,
        display_name=notebook_name,
        description=f"dbt {command} --select {select} for {dbt_project}",
    )
    console.print(f"[green]✓[/green] Created {path}")
    return path


def convert_tsql_to_spark_type(tsql_type: str) -> str:
    """Convert T-SQL/SQL Server data types to Spark SQL data types."""

    # Normalize the input type (remove size specifications, make lowercase)
    base_type = tsql_type.lower().strip()

    # Remove size/precision specifications like (10) or (10,2)
    if "(" in base_type:
        base_type = base_type.split("(")[0].strip()

    # Type mapping from T-SQL to Spark SQL
    type_mapping = {
        # String types
        "varchar": "string",
        "nvarchar": "string",
        "char": "string",
        "nchar": "string",
        "text": "string",
        "ntext": "string",
        # Numeric types
        "int": "int",
        "bigint": "bigint",
        "smallint": "smallint",
        "tinyint": "tinyint",
        "bit": "boolean",
        "decimal": "decimal",
        "numeric": "decimal",
        "float": "float",
        "real": "float",
        "money": "decimal(19,4)",
        "smallmoney": "decimal(10,4)",
        # Date/Time types
        "datetime": "timestamp",
        "datetime2": "timestamp",
        "date": "date",
        "time": "string",  # Spark doesn't have a time-only type
        "datetimeoffset": "timestamp",
        "smalldatetime": "timestamp",
        # Binary types
        "binary": "binary",
        "varbinary": "binary",
        "image": "binary",
        # Other types
        "uniqueidentifier": "string",
        "xml": "string",
        "sql_variant": "string",
        "hierarchyid": "string",
        "geometry": "string",
        "geography": "string",
    }

    # Return mapped type or original if not found
    return type_mapping.get(base_type, base_type)


def create_schema_yml_from_metadata(
    ctx: typer.Context,
    dbt_project: str,
    lakehouse: str,
    layer: str,
    dbt_type: str,
) -> None:
    """Convert cached lakehouse metadata CSV to dbt schema.yml format.

    Reads from {workspace}/metadata/lakehouse_metadata_all.csv and creates schema.yml:
    Only includes tables from the specified lakehouse.
    dbt_type determines the format: 'source', 'model', or 'snapshot'.
    """
    console.print(
        f"Creating [bold]{dbt_type}[/bold] schema.yml for layer [bold]{layer}[/bold] and lakehouse [bold]{lakehouse}[/bold] "
    )

    workspace_dir = ctx.obj.get("fabric_workspace_repo_dir") if ctx.obj else None
    if not workspace_dir:
        console.print("[red]Fabric workspace repo dir not provided.[/red]")
        raise typer.Exit(code=1)

    workspace_dir = Path(workspace_dir)

    # Source CSV file
    metadata_csv = workspace_dir / "metadata" / "lakehouse_metadata_all.csv"
    if not metadata_csv.exists():
        console.print(f"[red]Metadata CSV not found:[/red] {metadata_csv}")
        console.print(
            "[yellow]Run 'ingen_fab deploy get-metadata --target lakehouse' first to generate metadata.[/yellow]"
        )
        raise typer.Exit(code=1)

    # Target directory for dbt schema yml (always under schema_yml folder)
    target_dir = workspace_dir / dbt_project / "schema_yml" / layer
    target_dir.mkdir(parents=True, exist_ok=True)
    schema_yml_path = target_dir / "schema.yml"

    # Collect metadata for dbt sources/models/snapshots
    tables = {}
    try:
        with metadata_csv.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                lakehouse_name = row.get("lakehouse_name", "").strip()
                table_name = row.get("table_name", "").strip()
                column_name = row.get("column_name", "").strip()
                tsql_type = row.get("data_type", "").strip()
                schema_name = row.get("schema_name", "").strip().lower()

                # Filter by lakehouse parameter
                if lakehouse_name != lakehouse:
                    continue

                # Exclude tables with schema_name of 'sys' or 'queryinsights'
                if schema_name in ["sys", "queryinsights"]:
                    continue

                if not lakehouse_name or not table_name or not column_name:
                    continue

                spark_type = convert_tsql_to_spark_type(tsql_type)

                if table_name not in tables:
                    tables[table_name] = []

                tables[table_name].append(
                    {
                        "name": column_name,
                        "data_type": spark_type,
                        "description": "",
                    }
                )

        # Build dbt schema.yml structure based on dbt_type
        schema_yml = {"version": 2}
        if dbt_type == "source":
            dbt_sources = [
                {
                    "name": layer,
                    "description": "",
                    "schema": lakehouse,
                    "tables": [
                        {
                            "name": table_name,
                            "description": "",
                            "columns": columns,
                        }
                        for table_name, columns in tables.items()
                    ],
                }
            ]
            schema_yml["sources"] = dbt_sources
        elif dbt_type == "model":
            dbt_models = [
                {
                    "name": table_name,
                    "description": "",
                    "columns": columns,
                }
                for table_name, columns in tables.items()
            ]
            schema_yml["models"] = dbt_models
        elif dbt_type == "snapshot":
            dbt_snapshots = [
                {
                    "name": table_name,
                    "description": "",
                    "columns": columns,
                }
                for table_name, columns in tables.items()
            ]
            schema_yml["snapshots"] = dbt_snapshots
        else:
            console.print(
                f"[red]Unknown dbt_type: {dbt_type}. Must be 'source', 'model', or 'snapshot'.[/red]"
            )
            raise typer.Exit(code=1)

        # Write schema.yml as YAML
        import yaml

        with schema_yml_path.open("w", encoding="utf-8") as f:
            yaml.dump(
                schema_yml,
                f,
                sort_keys=False,
                default_flow_style=False,
                allow_unicode=True,
            )

        console.print(f"[green]✓[/green] Created {schema_yml_path}")

    except Exception as e:
        console.print(f"[red]Error creating schema.yml: {e}[/red]")
        raise typer.Exit(code=1)
