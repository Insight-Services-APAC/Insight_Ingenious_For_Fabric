```text
Falling back to FABRIC_WORKSPACE_REPO_DIR environment variable.
Falling back to FABRIC_ENVIRONMENT environment variable.
Using Fabric workspace repo directory: sample_project
Using Fabric environment: local

 Usage: python -m ingen_fab.cli deploy get-metadata [OPTIONS]

 Get schema/table/column metadata for lakehouse/warehouse/both.

 This consolidates prior extract commands under deploy for easier access.

╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --workspace-id                   TEXT  Workspace ID [default: None]          │
│ --workspace-name                 TEXT  Workspace name [default: None]        │
│ --lakehouse-id                   TEXT  Lakehouse ID [default: None]          │
│ --lakehouse-name                 TEXT  Lakehouse name [default: None]        │
│ --warehouse-id                   TEXT  Warehouse ID [default: None]          │
│ --warehouse-name                 TEXT  Warehouse name [default: None]        │
│ --schema               -s        TEXT  Schema name filter [default: None]    │
│ --table                -t        TEXT  Table name filter (substring match)   │
│                                        [default: None]                       │
│ --method               -m        TEXT  Extraction method: 'sql-endpoint'     │
│                                        (default) or 'sql-endpoint-odbc'      │
│                                        [default: sql-endpoint]               │
│ --sql-endpoint-id                TEXT  Explicit SQL endpoint ID to use       │
│                                        [default: None]                       │
│ --sql-endpoint-server            TEXT  SQL endpoint server prefix (without   │
│                                        domain), e.g., 'myws-abc123' to form  │
│                                        myws-abc123.datawarehouse.fabric.mic… │
│                                        [default: None]                       │
│ --format               -f        TEXT  Output format: csv (default), json,   │
│                                        or table                              │
│                                        [default: csv]                        │
│ --output               -o        PATH  Write output to file (defaults to     │
│                                        metadata cache)                       │
│                                        [default: None]                       │
│ --all                                  For lakehouse target, extract all     │
│                                        lakehouses in workspace               │
│ --target               -tgt      TEXT  Target asset type: 'lakehouse',       │
│                                        'warehouse', or 'both' (default       │
│                                        lakehouse)                            │
│                                        [default: lakehouse]                  │
│ --help                                 Show this message and exit.           │
╰──────────────────────────────────────────────────────────────────────────────╯
```
