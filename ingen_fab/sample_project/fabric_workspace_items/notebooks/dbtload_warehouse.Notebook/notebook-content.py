# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## dbtload_warehouse: dbt build `path:models`
#
# A Python notebook (no Spark session of its own) that runs dbt against the project uploaded to
# the config lakehouse (`ingen_fab deploy upload-dbt-project --dbt-project dbt_warehouse`),
# with the adapter the project's `requirements.txt` names. Where the models execute depends on
# that adapter: dbt-fabricspark opens a Spark (Livy) session on the profile's lakehouse,
# dbt-fabric runs T-SQL in the warehouse. A Fabric token is fetched in this notebook and passed
# to dbt as DBT_FABRIC_TOKEN for profiles that use it.
# The profile target is `notebook`, from the project's own profiles.yml.
#
# Every step runs through Python with a failure check, so a failed install or a failed dbt run
# fails the notebook job. dbt writes its `target/` and logs to local scratch space. At the end,
# and on failure, the run folder is published to the log lakehouse (`lh_log`):
# `Files/dbt_runs/dbt_warehouse/dbtload_warehouse/<timestamp>/` with `pip_install.log`,
# `dbt_<command>.log`, dbt's own logs and `target/`, a `run_summary.json`, and
# `notebook_error.log` if the notebook itself failed.

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": "config"
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# PARAMETERS CELL ********************

dbt_command = "build"
dbt_select = "path:models"
dbt_threads = 4
dbt_vars = ""
dbt_target = "notebook"  # empty: <environment>-notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import json
import os
import subprocess
import sys
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

ALLOWED = set(["build", "compile", "run", "seed", "snapshot", "test"])
if dbt_command not in ALLOWED:
    raise ValueError(f"Invalid dbt_command {dbt_command!r}. Allowed: {sorted(ALLOWED)}")

# Names rendered as JSON strings and joined as path parts: a quote or backslash in a project,
# notebook or lakehouse name cannot break this code
DBT_PROJECT = "dbt_warehouse"
NOTEBOOK_NAME = "dbtload_warehouse"
LOG_LAKEHOUSE = "lh_log"
PROJECT_DIR = Path("/lakehouse/default/Files") / DBT_PROJECT
# one folder per run: the UTC stamp orders them, the short id keeps two runs that start in
# the same second apart
STAMP = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
LOCAL_RUN = Path("/tmp/dbt_runs") / NOTEBOOK_NAME / STAMP
LOCAL_RUN.mkdir(parents=True, exist_ok=True)
STARTED = time.time()
STEPS = []
# Environment variables for the dbt process, each read from the Variable Library at run time
# (for example a warehouse SQL endpoint that the profile reads with env_var()).
ENV_FROM_VARIABLES = {"DBT_WAREHOUSE_ENDPOINT": "wh_silver_warehouse_endpoint"}


def variable(name):
    """A value of the Variable Library deployed with this workspace."""
    return notebookutils.variableLibrary.get(f"$(/**/var_lib/{name})")  # noqa: F821


# Logs go to the log lakehouse, never to a data lakehouse or the config lakehouse.
LOG_RUN = (
    f"abfss://{variable(LOG_LAKEHOUSE + '_workspace_id')}@onelake.dfs.fabric.microsoft.com/"
    f"{variable(LOG_LAKEHOUSE + '_lakehouse_id')}/Files/dbt_runs/{DBT_PROJECT}/{NOTEBOOK_NAME}/{STAMP}"
)


def publish_run_folder(status, error=None):
    """Write run_summary.json and copy the local run folder to the log lakehouse."""
    summary = {
        "notebook": NOTEBOOK_NAME,
        "dbt_project": DBT_PROJECT,
        "command": dbt_command,
        "select": dbt_select,
        "vars": dbt_vars,
        "target": dbt_target,
        "status": status,
        "error": error,
        "started_utc": STAMP,
        "seconds": round(time.time() - STARTED, 1),
        "steps": STEPS,
    }
    (LOCAL_RUN / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    published = 0
    for path in sorted(LOCAL_RUN.rglob("*")):
        if not path.is_file() or path.suffix in {".msgpack", ".pyc", ".gpickle"}:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
            notebookutils.fs.put(f"{LOG_RUN}/{path.relative_to(LOCAL_RUN).as_posix()}", text, True)  # noqa: F821
            published += 1
        except Exception as e:  # one unreadable file must not hide the run's real outcome
            print(f"could not publish {path.name}: {type(e).__name__}: {e}")
    print(f"{published} file(s) published to {LOG_RUN}")


def step(name, command, env=None):
    """Run a command, write its output to <name>.log, fail the notebook on a non-zero exit."""
    log = LOCAL_RUN / f"{name}.log"
    began = time.time()
    with log.open("w", encoding="utf-8") as f:
        proc = subprocess.run(command, cwd=str(PROJECT_DIR), env=env, stdout=f, stderr=subprocess.STDOUT, text=True)
    STEPS.append({"step": name, "exit": proc.returncode, "seconds": round(time.time() - began, 1)})
    tail = log.read_text(encoding="utf-8", errors="replace").splitlines()[-40:]
    print(f"--- {name}: exit {proc.returncode}")
    print("\n".join(tail))
    if proc.returncode != 0:
        raise RuntimeError(f"{name} failed with exit code {proc.returncode}; see {LOG_RUN}/{name}.log")


try:
    # The environment comes from the Variable Library, so the same notebook selects the right
    # profile target in development, test and production.
    environment = variable("fabric_environment")
    target = dbt_target or f"{environment}-notebook"
    # The token is fetched here and handed to the dbt child process: the profile's notebook
    # target reads it from DBT_FABRIC_TOKEN (notebookutils is not available in a child process).
    fabric_token = notebookutils.credentials.getToken("pbi")  # noqa: F821

    step("pip_install", [sys.executable, "-m", "pip", "install", "-q", "-r", str(PROJECT_DIR / "requirements.txt")])

    dbt_exe = Path(sys.executable).parent / "dbt"
    dbt = [str(dbt_exe)] if dbt_exe.is_file() else [sys.executable, "-m", "dbt.cli.main"]
    env = {
        **os.environ,
        "DBT_PROJECT_DIR": str(PROJECT_DIR),
        "DBT_PROFILES_DIR": str(PROJECT_DIR / "profiles"),
        "DBT_TARGET_PATH": str(LOCAL_RUN / "target"),
        "DBT_LOG_PATH": str(LOCAL_RUN / "dbt_logs"),
        "DBT_USE_COLORS": "false",
        "DBT_FABRIC_TOKEN": fabric_token,
        "PYTHONUNBUFFERED": "1",
        "PATH": f"{Path(sys.executable).parent}{os.pathsep}{os.environ.get('PATH', '')}",
        **{name: variable(source) for name, source in ENV_FROM_VARIABLES.items()},
    }
    arguments = [dbt_command, "--select", dbt_select, "--target", target, "--threads", str(dbt_threads), "--indirect-selection", "cautious"]
    if dbt_vars:
        arguments += ["--vars", dbt_vars]
    # Packages are not part of the upload: a project that declares them gets `dbt deps` first,
    # installed into this run's scratch folder so the uploaded project stays untouched
    if any((PROJECT_DIR / name).is_file() for name in ("packages.yml", "dependencies.yml")):
        env["DBT_PACKAGES_INSTALL_PATH"] = str(LOCAL_RUN / "dbt_packages")
        step("dbt_deps", dbt + ["deps"], env=env)
    print("dbt " + " ".join(arguments))
    step("dbt_" + dbt_command, dbt + arguments, env=env)
except BaseException as e:
    (LOCAL_RUN / "notebook_error.log").write_text(traceback.format_exc(), encoding="utf-8")
    publish_run_folder("failed", f"{type(e).__name__}: {e}")
    raise
else:
    publish_run_folder("succeeded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }