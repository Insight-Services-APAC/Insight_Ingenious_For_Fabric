# Deploy Guide

Practical guide to deploy changes, upload Python libraries with variable injection, and extract metadata from Fabric assets.

## Summary

| Action | Command | Common flags |
|--------|---------|--------------|
| Deploy items | `ingen_fab deploy deploy` | Uses `FABRIC_WORKSPACE_REPO_DIR`, `FABRIC_ENVIRONMENT` |
| Upload `python_libs` | `ingen_fab deploy upload-python-libs` | Injects variables during upload |
| Extract metadata | `ingen_fab deploy get-metadata` | `--target`, `--schema`, `--table`, `--format`, `--output` |
| Delete all items | `ingen_fab deploy delete-all` | `--force` |

## Prerequisites

- Set workspace repo and target environment:

```bash
export FABRIC_WORKSPACE_REPO_DIR="./sample_project"
export FABRIC_ENVIRONMENT="development"
```

- For deploys, ensure Azure auth is configured (e.g., `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` when required).

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
