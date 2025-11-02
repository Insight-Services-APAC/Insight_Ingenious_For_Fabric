# Common Tasks

[Home](../index.md) > [Guides](../user_guide/index.md) > Common Tasks

Quick, task-oriented commands with links to deeper docs.

| Task | Command | Notes | Links |
|------|---------|-------|-------|
| Initialize a new project | `ingen_fab init new --project-name "My Project"` | Creates workspace repo layout and starter templates | Quick Start, Workspace Layout |
| Generate DDL notebooks (Warehouse) | `ingen_fab ddl compile -o fabric_workspace_repo -g Warehouse` | Generates notebooks from DDL scripts | CLI Reference → ddl |
| Generate DDL notebooks (Lakehouse) | `ingen_fab ddl compile -o fabric_workspace_repo -g Lakehouse` | Uses PySpark notebooks | CLI Reference → ddl |
| Deploy to an environment | `ingen_fab deploy deploy` | Requires `FABRIC_WORKSPACE_REPO_DIR`, `FABRIC_ENVIRONMENT` | Deploy Guide, CLI Reference → deploy |
| Upload python_libs to OneLake | `ingen_fab deploy upload-python-libs` | Performs variable injection during upload | Deploy Guide, CLI Reference → deploy |
| Extract lakehouse/warehouse metadata | `ingen_fab deploy get-metadata --target both -f csv -o ./artifacts/meta.csv` | Flexible filters via `--schema`, `--table` | Deploy Guide, CLI Reference → deploy |
| Run local tests (python) | `export FABRIC_ENVIRONMENT=local && ingen_fab test local python` | Set environment to `local` | CLI Reference → test |
| Run local tests (pyspark) | `export FABRIC_ENVIRONMENT=local && ingen_fab test local pyspark` | Requires local Spark | CLI Reference → test |
| Configure workspace by name | `ingen_fab init workspace --workspace-name "My Workspace"` | Optionally create if missing with `-c` | CLI Reference → init |

Tip: set environment variables once to simplify commands.

--8<-- "_includes/environment_setup.md"

