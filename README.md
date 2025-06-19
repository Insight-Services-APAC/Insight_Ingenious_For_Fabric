# Ingenious Fabric Accelerator

This repository contains the source code for **Ingenious for Fabric**, a command line application built with [Typer](https://typer.tiangolo.com/) that streamlines the creation of Microsoft Fabric assets. The CLI helps generate data definition notebooks, orchestrator notebooks and provides utilities for exploring existing notebook content.

## Features

- Generation of DDL notebooks for lakehouses or warehouses using Jinja templates.
- Creation of orchestrator notebooks to run generated notebooks in sequence.
- Scanning utilities to locate `notebook-content.py` files and summarise embedded content blocks.
- Example project under `sample_project` showcasing configuration files and generated assets.

## Requirements

- Python 3.12+
- The packages listed in `pyproject.toml` under `[project]` and `[dependency-groups.dev]` for development.

Create and activate a virtual environment, then install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Usage

The CLI exposes the command `ingen_fab` (configured in `pyproject.toml`). Run `--help` to view available commands:

```bash
ingen_fab --help
```

### Compile DDL notebooks

Generate notebooks from configuration folders:

```bash
ingen_fab compile-ddl-notebooks \
    --output-mode local \
    --generation-mode warehouse
```

### Find notebook-content files

Recursively search a folder (default `fabric_workspace_items`) for notebook content files:

```bash
ingen_fab find-notebook-content-files --base-dir path/to/workspace
```

### Scan notebook blocks

Display a summary of code blocks marked with `_blockstart:` and `_blockend:` within notebook files:

```bash
ingen_fab scan-notebook-blocks --base-dir path/to/workspace
```

## Sample project

The `sample_project` directory contains a fabricated workspace with example DDL scripts, generated notebooks and supporting utilities. It can be used as a reference when structuring your own Fabric projects.

## Environment setup

Run `scripts/install_sql_server.sh` during environment provisioning to install SQL Server on Ubuntu. The script requires the `SA_PASSWORD` environment variable to be set:

```bash
export SA_PASSWORD='<YourStrongPassword>'
./scripts/install_sql_server.sh
```

## License

This project is provided for demonstration purposes and has no specific license.
