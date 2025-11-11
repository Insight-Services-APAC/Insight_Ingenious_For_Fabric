# Workspace Layout

[Home](../index.md) > [User Guide](index.md) > Workspace Layout

Overview of the sample Fabric workspace repository and how commands interact with it.

```
dp/
├── dbt_project*/                              # DBT project sources
├── ddl_scripts/                               # Jinja-based DDL sources
│   ├── Lakehouses/
│   └── Warehouses/
├── deployment/                              # DevOps yml deployment scripts
├── fabric_workspace_items/
│   ├── config/
│   │   └── var_lib.VariableLibrary/           # Value sets per environment
│   ├── lakehouses/                            # Lakehouse definitions (.Lakehouse)
│   ├── warehouses/                            # Warehouse definitions (.Warehouse)
│   ├── ddl_scripts/                           # Generated DDL notebooks
│   └── dbt_project*/                          # DBT project notebooks
├── metadata/                                  # Extracted metadata cache
└── platform_manifest_*.yml                    # Platform/environment manifests
```

Command mapping:

| Area | Typical Commands | Notes |
|------|-------------------|-------|
| DDL scripts (`ddl_scripts/*`) | `ingen_fab ddl compile -o fabric_workspace_repo -g <Lakehouse|Warehouse>` | Compiles to notebooks under `fabric_workspace_items/*` |
| Fabric workspace items (`fabric_workspace_items/*`) | `ingen_fab deploy deploy` | Deployed to the target Fabric workspace |
| Variable library (`var_lib.VariableLibrary`) | Used implicitly by compile/deploy | Selects value set by `FABRIC_ENVIRONMENT` |
| Python libs (`python_libs/*`) | `ingen_fab deploy upload-python-libs` | Injects variables during OneLake upload |

Tips:
- Keep DDL scripts numbered and idempotent; compile before deployment.
- Maintain separate value sets for each environment under `valueSets/*.json`.
- Store artifacts (e.g., metadata extracts) under `./artifacts/` to keep the repo tidy.

