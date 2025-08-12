```text
Usage: python -m ingen_fab.cli [OPTIONS] COMMAND [ARGS]...                     
                                                                                
╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --fabric-workspace-repo-dir  -fwd      PATH  Directory containing fabric     │
│                                              workspace repository files      │
│                                              [default: None]                 │
│ --fabric-environment         -fe       PATH  The name of your fabric         │
│                                              environment (e.g., development, │
│                                              production). This must match    │
│                                              one of the valuesets in your    │
│                                              variable library.               │
│                                              [default: None]                 │
│ --install-completion                         Install completion for the      │
│                                              current shell.                  │
│ --show-completion                            Show completion for the current │
│                                              shell, to copy it or customize  │
│                                              the installation.               │
│ --help                                       Show this message and exit.     │
╰──────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────╮
│ deploy     Commands for deploying to environments and managing workspace     │
│            items.                                                            │
│ init       Commands for initializing solutions and projects.                 │
│ ddl        Commands for compiling DDL notebooks.                             │
│ test       Commands for testing notebooks and Python blocks.                 │
│ notebook   Commands for managing and scanning notebook content.              │
│ package    Commands for running extension packages.                          │
│ libs       Commands for compiling and managing Python libraries.             │
│ dbt        Proxy commands to dbt_wrapper inside the Fabric workspace repo.   │
│ extract    Data extraction and package commands (keep compile, extract-run). │
╰──────────────────────────────────────────────────────────────────────────────╯
```
