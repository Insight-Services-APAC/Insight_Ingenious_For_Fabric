# spark_minimal dev container

A ready-to-use development environment for `ingen_fab`: Python 3.12, a JVM for local Spark
sessions, `uv`, git and the SQL Server ODBC driver, with the project's virtual environment
created automatically. Open the repository in VS Code and choose **Dev Containers: Reopen in
Container**, then pick `spark_minimal`.

## What you get

| Component | Detail |
| --- | --- |
| Base image | `python:3.12-slim-bookworm` (Debian 12) |
| Python | 3.12, the version `ingen_fab` requires (`>=3.12,<3.14`) |
| Java | OpenJDK 17 (headless). PySpark 4.0.0 from the `dev` dependency group bundles Spark itself, so no separate Spark install is needed |
| Package manager | `uv` (pinned). The venv lives at `/opt/venv` inside the container, not on the bind mount, and is on `PATH` |
| Tools | git, curl, ODBC Driver 18 for SQL Server (for `pyodbc`) |
| Volumes | `ingen-fab-uv-cache` (package cache, survives rebuilds) and `ingen-fab-azure` (`az login` state, survives restarts) |
| Environment | `FABRIC_ENVIRONMENT=local`, `FABRIC_WORKSPACE_REPO_DIR=ingen_fab/sample_project` |

On first start `postCreateCommand` runs `uv sync --all-extras`, which installs the project,
its dev group (pytest, ruff, pyspark, delta-spark) and the optional extras. It takes a few
minutes the first time and seconds afterwards thanks to the cache volume.

## Verify

```bash
ingen_fab --help
pytest tests/test_promotion_utils.py -q
ingen_fab test local pyspark lakehouse_utils              # starts a local Spark session with Delta
bash .devcontainer/spark_minimal/verify.sh                # all of the above in one go
bash .devcontainer/spark_minimal/verify.sh quick          # same, minus the Spark-backed library tests (the Spark + Delta round trip still runs)
```

The first Spark session downloads the Delta jars from Maven, so it needs internet access once.

## Azure login

The Azure CLI is installed into the venv as a dependency of `ingen_fab` (`az` is on `PATH`
once the venv is active). Log in inside the container; the token cache persists in the
`ingen-fab-azure` volume:

```bash
az login --use-device-code
```

## SQL Server (optional)

The dev container has no Docker CLI, so a SQL Server container is started **on the host**,
not from inside the dev container:

```bash
# on the host
docker run -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=YourStrong!Passw0rd' \
    -p 1433:1433 --name sql_server \
    -d mcr.microsoft.com/mssql/server:2022-latest
```

Single quotes matter: `!` inside double quotes triggers history expansion in an interactive
Bash shell.

A port published on the host is not `localhost` inside the dev container. From inside, the
host is reachable as `host.docker.internal` (Docker Desktop provides the name; `runArgs` in
`devcontainer.json` adds it on Linux hosts too). The library's default SQL Server connection
string is hard-coded to `SERVER=localhost,1433` and only the password is configurable
(`SQL_SERVER_PASSWORD`), so point it at the host explicitly. The host-side `docker run` does
not set any variable inside the dev container; export the password there first:

```bash
# inside the dev container
export SQL_SERVER_PASSWORD='YourStrong!Passw0rd'
```

```python
import os
from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils

password = os.environ.get("SQL_SERVER_PASSWORD", "YourStrong!Passw0rd")  # the library's default
wh = warehouse_utils(
    dialect="sql_server",
    connection_string=(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=host.docker.internal,1433;"
        f"UID=sa;PWD={password};TrustServerCertificate=yes;"
    ),
)
```

Note that with `FABRIC_ENVIRONMENT=local` the warehouse library selects the PostgreSQL dialect
by default; SQL Server is used only when a caller asks for `dialect="sql_server"`.

## Optional extras

The scripts under `scripts/dev_container_scripts/spark_minimal/` are no longer required. They
remain for developers who want them:

- `pwsh_install.sh` and `dev_tools.ps1`: PowerShell, oh-my-posh, GitHub CLI, npm.
- `sql_install_4_linux.sh`: a SQL Server installed inside the container itself, which avoids
  the networking above at the cost of a manual, non-reproducible install.
- `postgres_metastore_setup.sh`: a PostgreSQL-backed Hive metastore for Spark.

## Why the base image changed (issue #39)

The container was `FROM bitnami/spark:4.0-debian-12`. Bitnami withdrew its free image catalog
from Docker Hub in 2025, so that tag no longer resolves and the container failed to build.
The legacy tag `bitnamilegacy/spark:4.0.0-debian-12-r20` still builds and carries Python
3.12.11, Java 17 and Spark 4.0.0, but it receives no updates, weighs 2 GB, runs as an
unprivileged user without git, sudo or writable apt, and left every developer tool to be
installed by hand. A slim Python image plus a JDK gives the same capability at 0.7 GB with
the tooling baked in.
