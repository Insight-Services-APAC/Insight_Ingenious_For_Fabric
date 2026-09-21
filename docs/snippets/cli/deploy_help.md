```text
Falling back to FABRIC_WORKSPACE_REPO_DIR environment variable.
Falling back to FABRIC_ENVIRONMENT environment variable.
Using Fabric workspace repo directory: ingen_fab/sample_project
Using Fabric environment: local

 Usage: python -m ingen_fab.cli deploy [OPTIONS] COMMAND [ARGS]...

 Commands for deploying to environments and managing workspace items.


╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                  │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────╮
│ deploy               Deploy Fabric artefacts to the target environment.      │
│ cleanup              Remove workspace items that are not in                  │
│                      fabric_workspace_items.                                 │
│ delete-all           Delete all workspace items in the target environment.   │
│ upload-python-libs   Inject code into python_libs (in-place) and upload to   │
│                      Fabric config lakehouse.                                │
│ upload-dbt-project   Sync a dbt project's files to the Fabric config         │
│                      lakehouse.                                              │
│ get-metadata         Get schema/table/column metadata for                    │
│                      lakehouse/warehouse/both.                               │
│ compare-metadata     Compare two metadata CSV files and report differences.  │
│ download-artefact    Download a specific Fabric artefact from workspace      │
│                      using Fabric API.                                       │
╰──────────────────────────────────────────────────────────────────────────────╯
```
