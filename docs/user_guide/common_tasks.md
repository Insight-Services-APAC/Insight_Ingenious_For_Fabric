# Common Tasks

[Home](../index.md) > [User Guide](index.md) > Common Tasks

## Common Ingenious commands

Quick, task-oriented commands with links to deeper docs.

| Task | Command | Notes | Links |
|----------|-------------|-----------|-----------|
| Initialize a new project | `ingen_fab init new --project-name "My Project"` | Creates workspace repo layout and starter templates. Add `--with-samples` for sample project template | [Quick Start](quick_start.md), [Workspace Layout](../reference/workspace-layout.md) |
| Configure workspace by name | `ingen_fab init workspace --workspace-name "My Workspace"` | Optionally create if missing with `-c` | [CLI Reference → init](cli_reference.md#init) |
| Extract lakehouse/warehouse metadata | `ingen_fab deploy get-metadata --target both -f csv -o ./artifacts/meta.csv` | Flexible filters via `--schema`, `--table` | [Deploy Guide](deploy_guide.md), [CLI Reference → deploy](cli_reference.md#deploy) |
| Convert metadata for dbt project | `Ingen_fab dbt convert-metadata --dbt-project dbt_project` | Converts metadata into dbt wrapper format | [DBT Integration](dbt_integration.md) |
| Generate dbt notebooks | `ingen_fab dbt create-notebooks -p my_dbt_project` | Automatically configures dbt profile, prompts for lakehouse selection | [DBT Integration](dbt_integration.md) |
| Generate dbt schema.yml | `ingen_fab dbt generate-schema-yml -p my_dbt_project --lakehouse lh_bronze --layer staging --dbt-type model` | Converts metadata to dbt schema.yml format for specific lakehouse and layer | [DBT Integration](dbt_integration.md) |
| Build dbt models and snapshots | `Ingen_fab dbt exec -- stage run build --project-dir dbt_project` | Proxy command to build dbt models and snapshots with automatic profile management | [DBT Integration](dbt_integration.md) |
| Build dbt master notebooks | `Ingen_fab dbt exec -- stage run post-scripts --project-dir dbt_project` | Proxy command to build dbt master notebooks with automatic profile management | [DBT Integration](dbt_integration.md) |
| Set up dbt profile | Run any `ingen_fab dbt` command | Interactive lakehouse selection on first run, saves preference | [DBT Integration](dbt_integration.md) |
| Generate DDL scripts from metadata | `ingen_fab ddl ddls-from-metadata --lakehouse lh_silver` | Generates DDL scripts from metadata (optional helper functionality) | [CLI Reference → ddl](cli_reference.md#ddl) |
| Generate DDL notebooks (Warehouse) | `ingen_fab ddl compile -o fabric_workspace_repo -g Warehouse` | Generates notebooks from DDL scripts | [CLI Reference → ddl](cli_reference.md#ddl) |
| Generate DDL notebooks (Lakehouse) | `ingen_fab ddl compile -o fabric_workspace_repo -g Lakehouse` | Uses PySpark notebooks | [CLI Reference → ddl](cli_reference.md#ddl) |
| Download Fabric artefacts from workspace | `ingen_fab deploy download-artefact --artefact-name "rp_test" --artefact-type Report` | For artefacts that are developed in Fabric | [CLI Reference → deploy download-artefact](cli_reference.md#deploy-download-artefact) |
| Compare metadata files | `ingen_fab deploy compare-metadata -f1 before.csv -f2 after.csv -o diff.json --format json` | Detects missing tables/columns, data type changes | [Deploy Guide](deploy_guide.md), [CLI Reference → deploy](cli_reference.md#deploy) |
| Deploy to an environment | `ingen_fab deploy deploy` | Requires `FABRIC_WORKSPACE_REPO_DIR`, `FABRIC_ENVIRONMENT` | [Deploy Guide](deploy_guide.md), [CLI Reference → deploy](cli_reference.md#deploy) |
| Upload python_libs to OneLake | `ingen_fab deploy upload-python-libs` | Performs variable injection during upload | [Deploy Guide](deploy_guide.md), [CLI Reference → deploy](cli_reference.md#deploy) |
| Run local tests (python) | `export FABRIC_ENVIRONMENT=local && ingen_fab test local python` | Set environment to `local` | [CLI Reference → test](cli_reference.md#test) |
| Run local tests (pyspark) | `export FABRIC_ENVIRONMENT=local && ingen_fab test local pyspark` | Requires local Spark | [CLI Reference → test](cli_reference.md#test) |

## DBT Profile Setup

The first time you run any dbt command, you'll be prompted to select a target lakehouse:

=== "Multiple Lakehouses Available"

    ```bash
    # First run - shows interactive selection
    ingen_fab dbt create-notebooks -p my_project
    
    # Output:
    # Available Lakehouse Configurations:
    # 
    # ┏━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┓
    # ┃ # ┃ Prefix    ┃ Lakehouse Name       ┃ Workspace Name    ┃
    # ┡━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━┩
    # │ 1 │ bronze    │ Bronze Layer         │ Analytics         │
    # │ 2 │ silver    │ Silver Layer         │ Analytics         │
    # │ 3 │ gold      │ Gold Layer           │ Analytics         │
    # └───┴───────────┴──────────────────────┴───────────────────┘
    # 
    # Select a lakehouse configuration by number [1]: 2
    ```

=== "Single Lakehouse Available"

    ```bash
    # Automatically uses the only available lakehouse
    ingen_fab dbt create-notebooks -p my_project
    
    # Output:
    # Using the only available lakehouse: Sample Lakehouse
    ```

Your selection is saved and reused automatically for future commands in the same environment.


