# Deploy Guide

[Home](../index.md) > [User Guide](index.md) > Deploy Guide

Practical guide to deploy changes, upload Python libraries with variable injection, and extract metadata from Fabric assets.

## Summary

| Action | Command | Common flags |
|--------|---------|--------------|
| Deploy items | `ingen_fab deploy deploy` | Uses `FABRIC_WORKSPACE_REPO_DIR`, `FABRIC_ENVIRONMENT` |
| Upload `python_libs` | `ingen_fab deploy upload-python-libs` | Injects variables during upload |
| Upload dbt project to config lakehouse| `ingen_fab deploy upload-dbt-project` | `--dbt-project` (required)|
| Extract metadata | `ingen_fab deploy get-metadata` | `--target`, `--schema`, `--table`, `--format`, `--output` |
| Delete all items | `ingen_fab deploy delete-all` | `--force` |

## Prerequisites

--8<-- "_includes/environment_setup.md"

## Deploy artifacts

Deploy all items under `fabric_workspace_items` to the selected environment.

```bash
ingen_fab deploy deploy
```

Tips:
- Use semantic, ordered DDL under `ddl_scripts` and generate notebooks with `ingen_fab ddl compile ...` before deploying.
- Validate your variable library value set for the target environment.

## Upload Python libraries to OneLake

Upload `python_libs` to the config lakehouse with variable injection during upload.

```bash
ingen_fab deploy upload-python-libs
```

Notes:
- Code between injection markers is resolved using the active value set.
- Upload progress and failures are shown in the console.

## Upload dbt project to OneLake/Config Lakehouse

Upload dbt project to the /Files section of the Config Lakehouse. Useful for Fabric Warehouse based projects to mount to Fabric Python notebooks

```bash
ingen_fab deploy upload-dbt-project --dbt-project <project_name>
```

## Extract metadata (Lakehouse/Warehouse)

Discover schemas, tables, and columns via Fabric SQL endpoints.

```bash
# Lakehouse example (write CSV)
ingen_fab deploy get-metadata \
  --workspace-name "Analytics Workspace" \
  --lakehouse-name "Config Lakehouse" \
  --schema config --table meta \
  --format csv --output ./artifacts/lakehouse_metadata.csv

# Warehouse example (pretty table)
ingen_fab deploy get-metadata \
  --workspace-name "Analytics Workspace" \
  --warehouse-name "EDW" \
  --target warehouse --format table

# Both lakehouse and warehouse (filter by schema)
ingen_fab deploy get-metadata --workspace-name "Analytics Workspace" --schema sales --target both
```

Common flags:
- `--target / -tgt`: `lakehouse` (default), `warehouse`, `both`
- `--schema / -s`, `--table / -t`: filters
- `--format / -f`: `csv`, `json`, `table`
- `--output / -o`: write to file

## Clean up

Delete all workspace items in an environment (use with care):

```bash
ingen_fab deploy delete-all --force
```

!!! warning
    `deploy delete-all` removes all items in the resolved workspace for the active environment. Use the `--force` flag only when you are certain.

## Troubleshooting

- Ensure `FABRIC_WORKSPACE_REPO_DIR` points to your workspace repo root.
- `FABRIC_ENVIRONMENT` must map to a value set in your variable library.
- Validate Azure authentication and permissions for the target workspace.
