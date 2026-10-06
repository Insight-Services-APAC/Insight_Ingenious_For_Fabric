```text
Falling back to FABRIC_WORKSPACE_REPO_DIR environment variable.
Falling back to FABRIC_ENVIRONMENT environment variable.
Using Fabric workspace repo directory: ingen_fab/sample_project
Using Fabric environment: local
                                                                                
 Usage: python -m ingen_fab.cli dbt [OPTIONS] COMMAND [ARGS]...                 
                                                                                
 dbt on Fabric (lakehouses and warehouses): profile from the value set, dbt     
 commands, orchestrator notebook.                                               
                                                                                
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help          Show this message and exit.                                  │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────╮
│ profile               Generate <dbt_project>/profiles/profiles.yml from the  │
│                       Variable Library value sets.                           │
│ build                 Run `dbt build` for the project with the generated     │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt build -- --select tag:silver`.                     │
│ run                   Run `dbt run` for the project with the generated       │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt run -- --select tag:silver`.                       │
│ test                  Run `dbt test` for the project with the generated      │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt test -- --select tag:silver`.                      │
│ seed                  Run `dbt seed` for the project with the generated      │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt seed -- --select tag:silver`.                      │
│ snapshot              Run `dbt snapshot` for the project with the generated  │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt snapshot -- --select tag:silver`.                  │
│ compile               Run `dbt compile` for the project with the generated   │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt compile -- --select tag:silver`.                   │
│ parse                 Run `dbt parse` for the project with the generated     │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt parse -- --select tag:silver`.                     │
│ debug                 Run `dbt debug` for the project with the generated     │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt debug -- --select tag:silver`.                     │
│ docs                  Run `dbt docs` for the project with the generated      │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt docs -- --select tag:silver`.                      │
│ ls                    Run `dbt ls` for the project with the generated        │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt ls -- --select tag:silver`.                        │
│ list                  Run `dbt list` for the project with the generated      │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt list -- --select tag:silver`.                      │
│ clean                 Run `dbt clean` for the project with the generated     │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt clean -- --select tag:silver`.                     │
│ deps                  Run `dbt deps` for the project with the generated      │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt deps -- --select tag:silver`.                      │
│ show                  Run `dbt show` for the project with the generated      │
│                       profile and the current environment as target. Extra   │
│                       arguments are passed to dbt unchanged, e.g. `ingen_fab │
│                       dbt show -- --select tag:silver`.                      │
│ orchestrator          Create an orchestrator notebook that runs dbt inside   │
│                       Fabric against the uploaded project.                   │
│ generate-schema-yml   Convert cached lakehouse metadata to dbt schema.yml    │
│                       format for a lakehouse and layer.                      │
╰──────────────────────────────────────────────────────────────────────────────╯
```
