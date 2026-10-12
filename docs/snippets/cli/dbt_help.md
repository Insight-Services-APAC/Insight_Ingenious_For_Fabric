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
│ build                 Run `dbt build` for the project. With a generated      │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt build -- --threads 8`   │
│                       (only the options `dbt build` accepts).                │
│ run                   Run `dbt run` for the project. With a generated        │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt run -- --threads 8`     │
│                       (only the options `dbt run` accepts).                  │
│ test                  Run `dbt test` for the project. With a generated       │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt test -- --threads 8`    │
│                       (only the options `dbt test` accepts).                 │
│ seed                  Run `dbt seed` for the project. With a generated       │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt seed -- --threads 8`    │
│                       (only the options `dbt seed` accepts).                 │
│ snapshot              Run `dbt snapshot` for the project. With a generated   │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt snapshot -- --threads   │
│                       8` (only the options `dbt snapshot` accepts).          │
│ compile               Run `dbt compile` for the project. With a generated    │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt compile -- --threads 8` │
│                       (only the options `dbt compile` accepts).              │
│ parse                 Run `dbt parse` for the project. With a generated      │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt parse -- --threads 8`   │
│                       (only the options `dbt parse` accepts).                │
│ debug                 Run `dbt debug` for the project. With a generated      │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt debug -- --threads 8`   │
│                       (only the options `dbt debug` accepts).                │
│ docs                  Run `dbt docs` for the project. With a generated       │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt docs -- --threads 8`    │
│                       (only the options `dbt docs` accepts).                 │
│ ls                    Run `dbt ls` for the project. With a generated profile │
│                       the current environment is the target unless you pass  │
│                       one; a hand-written profile keeps its own default      │
│                       target. Arguments after `--` go to dbt unchanged, e.g. │
│                       `ingen_fab dbt ls -- --threads 8` (only the options    │
│                       `dbt ls` accepts).                                     │
│ list                  Run `dbt list` for the project. With a generated       │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt list -- --threads 8`    │
│                       (only the options `dbt list` accepts).                 │
│ clean                 Run `dbt clean` for the project. With a generated      │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt clean -- --threads 8`   │
│                       (only the options `dbt clean` accepts).                │
│ deps                  Run `dbt deps` for the project. With a generated       │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt deps -- --threads 8`    │
│                       (only the options `dbt deps` accepts).                 │
│ show                  Run `dbt show` for the project. With a generated       │
│                       profile the current environment is the target unless   │
│                       you pass one; a hand-written profile keeps its own     │
│                       default target. Arguments after `--` go to dbt         │
│                       unchanged, e.g. `ingen_fab dbt show -- --threads 8`    │
│                       (only the options `dbt show` accepts).                 │
│ orchestrator          Create an orchestrator notebook that runs dbt inside   │
│                       Fabric against the uploaded project.                   │
│ generate-schema-yml   Convert cached lakehouse metadata to dbt schema.yml    │
│                       format for a lakehouse and layer.                      │
╰──────────────────────────────────────────────────────────────────────────────╯
```
