```text
Falling back to FABRIC_WORKSPACE_REPO_DIR environment variable.
Falling back to FABRIC_ENVIRONMENT environment variable.
Using Fabric workspace repo directory: sample_project
Using Fabric environment: local
                                                                                
 Usage: python -m ingen_fab.cli dbt [OPTIONS] COMMAND [ARGS]...                 
                                                                                
 Proxy commands to dbt_wrapper inside the Fabric workspace repo.                
                                                                                
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                  │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────╮
│ create-notebooks   Create Fabric notebooks from dbt-generated Python         │
│                    notebooks.                                                │
│ convert-metadata   Convert cached lakehouse metadata to dbt metaextracts     │
│                    format.                                                   │
│ exec               Run dbt_wrapper from within the Fabric workspace repo,    │
│                    then return to the original directory.                    │
╰──────────────────────────────────────────────────────────────────────────────╯
```
