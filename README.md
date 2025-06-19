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

| Folder | Purpose |
| ------ | ------- |
| `ingen_fab` | Source code for the CLI and utilities |
| `&nbsp;&nbsp;ddl_scripts` | Jinja templates used to generate DDL notebooks |
| `&nbsp;&nbsp;notebook_utils` | Notebook scanning and injection helpers |
| `&nbsp;&nbsp;python_libs` | Shared Python libraries |
| `sample_project` | Example workspace with config and generated notebooks |
| `scripts` | Helper scripts such as SQL Server setup |
| `tests` | Unit tests |

## Environment setup

Run `scripts/install_sql_server.sh` to install SQL Server on Ubuntu. Set the `MSSQL_SA_PASSWORD` environment variable before running the script:

```bash
export MSSQL_SA_PASSWORD='<YourStrong!Passw0rd>'
./scripts/install_sql_server.sh
```

## License

This project is provided for demonstration purposes and has no specific license.
