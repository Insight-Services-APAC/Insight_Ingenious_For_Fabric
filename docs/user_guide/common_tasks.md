# Common Tasks

[Home](../index.md) > [User Guide](index.md) > Common Tasks

## Common Ingenious commands

Quick, task-oriented commands with links to deeper docs.

| Task | Command | Notes | Links |
|----------|-------------|-----------|-----------|
| Initialize a new project | `ingen_fab init new --project-name "dp"` | Creates workspace repo layout and starter templates. Add `--with-samples` for sample project template | [Quick Start](quick_start.md), [Workspace Layout](workspace_layout.md) |
| Configure workspace by name | `ingen_fab init workspace --workspace-name "dp_dev"` | Optionally create if missing with `-c` | [CLI Reference → init](cli_reference.md#init) |
| Generate storage artifacts | `ingen_fab init storage-config` | Generates lakehouse, warehouse, and SQL database artifacts from storage_config.yaml. Creates folders, updates variables | [Quick Start](quick_start.md), [CLI Reference → init storage-config](cli_reference.md#init-storage-config) |
| Extract lakehouse/warehouse metadata | `ingen_fab deploy get-metadata --target both -f csv -o ./artifacts/meta.csv` | Flexible filters via `--schema`, `--table` | [Deploy Guide](deploy_guide.md), [CLI Reference → deploy](cli_reference.md#deploy) |
| Write the dbt profile | `ingen_fab dbt profile -p my_dbt_project` | `profiles/profiles.yml` from the value sets; `-l <prefix>` picks the default lakehouse | [DBT Integration](dbt_integration.md) |
| Generate dbt schema.yml | `ingen_fab dbt generate-schema-yml -p my_dbt_project --lakehouse lh_bronze --layer staging --dbt-type model` | Converts metadata to dbt schema.yml format for specific lakehouse and layer | [DBT Integration](dbt_integration.md) |
| Build dbt models | `ingen_fab dbt build -- --select tag:silver` | Runs `dbt build` over Livy with the generated profile; any dbt verb works the same way | [DBT Integration](dbt_integration.md) |
| Create a dbt orchestrator notebook | `ingen_fab dbt orchestrator --name dbtload_silver --select +tag:silver` | Notebook that runs dbt inside Fabric against the uploaded project (`deploy upload-dbt-project`) | [DBT Integration](dbt_integration.md) |
| Generate DDL scripts from metadata | `ingen_fab ddl ddls-from-metadata --lakehouse lh_silver` | Generates DDL scripts from metadata (optional helper functionality) | [CLI Reference → ddl](cli_reference.md#ddl) |
| Generate DDL notebooks (Warehouse) | `ingen_fab ddl compile -o fabric_workspace_repo -g Warehouse` | Generates notebooks from DDL scripts | [CLI Reference → ddl](cli_reference.md#ddl) |
| Generate DDL notebooks (Lakehouse) | `ingen_fab ddl compile -o fabric_workspace_repo -g Lakehouse` | Uses PySpark notebooks | [CLI Reference → ddl](cli_reference.md#ddl) |
| Download Fabric artefacts from workspace | `ingen_fab deploy download-artefact --artefact-name "rp_test" --artefact-type Report` | For artefacts that are developed in Fabric | [CLI Reference → deploy download-artefact](cli_reference.md#deploy-download-artefact) |
| Compare metadata files | `ingen_fab deploy compare-metadata -f1 before.csv -f2 after.csv -o diff.json --format json` | Detects missing tables/columns, data type changes | [Deploy Guide](deploy_guide.md), [CLI Reference → deploy](cli_reference.md#deploy) |
| Deploy to an environment | `ingen_fab deploy deploy` | Requires `FABRIC_WORKSPACE_REPO_DIR`, `FABRIC_ENVIRONMENT` | [Deploy Guide](deploy_guide.md), [CLI Reference → deploy](cli_reference.md#deploy) |
| Upload python_libs to OneLake | `ingen_fab deploy upload-python-libs` | Performs variable injection during upload | [Deploy Guide](deploy_guide.md), [CLI Reference → deploy](cli_reference.md#deploy) |

## DBT Profile Setup

The dbt profile is generated from the value set, never prompted for: `ingen_fab dbt profile`
writes `<dbt_project>/profiles/profiles.yml`, and every `ingen_fab dbt <verb>` regenerates it
first. With one lakehouse in the value set it is the default; with several, pass
`--lakehouse <prefix>` or set the `dbt_default_lakehouse` variable. See
[DBT Integration](dbt_integration.md).
