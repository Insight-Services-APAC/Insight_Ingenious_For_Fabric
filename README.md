# Ingenious Fabric Accelerator

Ingenious for Fabric is a small command line tool built with [Typer](https://typer.tiangolo.com/) that helps create and manage Microsoft Fabric assets. It generates notebooks for lakehouse or warehouse DDL, builds orchestrator notebooks and provides utilities for exploring existing notebook code.

## Features

- Generate DDL notebooks from Jinja templates.
- Create orchestrator notebooks to run generated notebooks in sequence.
- Locate `notebook-content.py` files and summarise embedded code blocks.
- Includes a `sample_project` folder that demonstrates the expected repository layout.

## Requirements

- Python 3.12+
- Dependencies listed in `pyproject.toml`.

Create and activate a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

You can also manage the environment using [uv](https://github.com/astral-sh/uv):

```bash
uv sync
```

## Usage

The main entry point is the `ingen_fab` command. Use `--help` to view all commands:

```bash
ingen_fab --help
```

### Compile DDL notebooks

```bash
ingen_fab compile-ddl-notebooks \
    --output-mode local \
    --generation-mode warehouse
```

### Find notebook-content files

```bash
ingen_fab find-notebook-content-files --base-dir path/to/workspace
```

### Scan notebook blocks

```bash
ingen_fab scan-notebook-blocks --base-dir path/to/workspace
```

## Running the tests

Execute the unit tests using `pytest`:

```bash
pytest
```

The tests run entirely offline. A few end-to-end tests are skipped unless the required environment variables are present.

## Sample project

See [sample_project/README.md](sample_project/README.md) for a tour of the example Fabric workspace used by the CLI.

## Folder structure

```
ingen_fab/
├── ddl_scripts/          # Jinja templates used to generate DDL notebooks
├── notebook_utils/       # Notebook scanning and injection helpers
├── python_libs/          # Shared Python libraries
sample_project/           # Example workspace with config and generated notebooks
scripts/                  # Helper scripts such as SQL Server setup
tests/                    # Unit tests
```

Additional documentation is available in the subdirectories:

- [ingen_fab/python_libs/README.md](ingen_fab/python_libs/README.md) – details of the helper libraries.
- [ingen_fab/ddl_scripts/README.md](ingen_fab/ddl_scripts/README.md) – how DDL notebooks are generated.

### Example Usage

``` pwsh 
# Set up environment variables for your project location and fabric environment
# Note: You can set these on each cli invocation or set them in the environment variables.

$env:FABRIC_WORKSPACE_REPO_DIR="sample_project"
$env:FABRIC_ENVIRONMENT="development"

# Compile DDL notebooks for Warehouse
typer ./ingen_fab/cli.py run compile-ddl-notebooks --output-mode fabric_workspace_repo --generation-mode Warehouse

# Compile DDL notebooks for Lakehouse
typer ./ingen_fab/cli.py run compile-ddl-notebooks --output-mode fabric_workspace_repo --generation-mode Lakehouse


```


## License

This project is provided for demonstration purposes and has no specific license.
