# Workspace Layout

Overview of the sample Fabric workspace repository and how commands interact with it.

```
sample_project/
├── fabric_workspace_items/
│   ├── config/
│   │   └── var_lib.VariableLibrary/           # Value sets per environment
│   ├── Lakehouses/                            # Lakehouse items (notebooks, artifacts)
│   └── Warehouses/                            # Warehouse items (notebooks, artifacts)
├── ddl_scripts/                               # Jinja-based DDL sources
│   ├── Lakehouses/
│   └── Warehouses/
├── packages/                                  # Package configs & outputs
├── python_libs/                               # Shared Python/PySpark libraries
├── python_libs_tests/                         # Library tests
└── platform_manifest_*.yml                    # Platform/environment manifests
```

Command mapping:

| Area | Typical Commands | Notes |
|------|-------------------|-------|
| DDL sources (`ddl_scripts/*`) | `ingen_fab ddl compile -o fabric_workspace_repo -g <Lakehouse|Warehouse>` | Compiles to notebooks under `fabric_workspace_items/*` |
| Notebooks (`fabric_workspace_items/*`) | `ingen_fab deploy deploy` | Deployed to the target Fabric workspace |
| Variable library (`var_lib.VariableLibrary`) | Used implicitly by compile/deploy | Selects value set by `FABRIC_ENVIRONMENT` |
| Python libs (`python_libs/*`) | `ingen_fab deploy upload-python-libs` | Injects variables during OneLake upload |
| Packages (`packages/*`) | `ingen_fab package …` | Package-specific compile/run workflows |
| Tests (`python_libs_tests/*`) | `ingen_fab test local <python|pyspark|common>` | Use `FABRIC_ENVIRONMENT=local` |

Tips:
- Keep DDL scripts numbered and idempotent; compile before deployment.
- Maintain separate value sets for each environment under `valueSets/*.json`.
- Store artifacts (e.g., metadata extracts) under `./artifacts/` to keep the repo tidy.

