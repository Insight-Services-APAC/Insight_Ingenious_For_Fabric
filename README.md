# Ingenious Fabric Accelerator

Ingenious for Fabric is a comprehensive command line tool built with [Typer](https://typer.tiangolo.com/) that helps create and manage Microsoft Fabric assets. It provides a complete development workflow for Fabric workspaces, including project initialization, DDL notebook generation, environment management, and deployment automation.

## Features

- **Project Initialization**: Create new Fabric workspace projects with proper structure and templates
- **DDL Notebook Generation**: Generate DDL notebooks from Jinja templates for both lakehouses and warehouses
- **Environment Management**: Deploy and manage artifacts across multiple environments (development, test, production)
- **Orchestrator Notebooks**: Create orchestrator notebooks to run generated notebooks in sequence
- **Notebook Utilities**: Scan and analyze existing notebook code and content
- **Testing Framework**: Test notebooks both locally and on the Fabric platform
- **Python Libraries**: Reusable Python and PySpark libraries for common Fabric operations

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

## Quick Start

### Initialize a New Project

```bash
# Create a new Fabric workspace project
ingen_fab init new --project-name "My Fabric Project"
```

### Generate DDL Notebooks

```bash
# Generate DDL notebooks for warehouses
ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Warehouse

# Generate DDL notebooks for lakehouses  
ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Lakehouse
```

### Deploy to Environment

```bash
# Deploy to development environment
ingen_fab deploy deploy \
    --fabric-workspace-repo-dir . \
    --fabric-environment development
```

## Command Reference

The main entry point is the `ingen_fab` command. Use `--help` to view all commands:

```bash
ingen_fab --help
```

### Core Command Groups

- **`init`** - Initialize solutions and projects
- **`ddl`** - Compile DDL notebooks from templates
- **`deploy`** - Deploy to environments and manage workspace items
- **`notebook`** - Manage and scan notebook content
- **`test`** - Test notebooks and Python blocks (local and platform)
- **`package`** - Compile and run extension packages (e.g., flat file ingestion, synapse sync)
- **`libs`** - Compile and manage Python libraries

### Common Commands

```bash
# Initialize new solution
ingen_fab init new --project-name "Project Name"

# Compile DDL notebooks
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse

# Deploy to environment
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development

# Find notebook content files
ingen_fab notebook find-notebook-content-files --base-dir path/to/workspace

# Scan notebook blocks
ingen_fab notebook scan-notebook-blocks --base-dir path/to/workspace

# Test Python libraries locally
ingen_fab test local python  # Tests python implementations
ingen_fab test local pyspark  # Tests pyspark implementations

# Generate platform tests
ingen_fab test platform generate

# Upload Python libraries to Fabric
ingen_fab deploy upload-python-libs

# Delete all workspace items (use with caution!)
ingen_fab deploy delete-all --force

# Compile flat file ingestion package
ingen_fab package ingest compile --include-samples

# Compile synapse sync package
ingen_fab package synapse compile --include-samples

# Compile Python libraries with variable injection
ingen_fab libs compile --target-file path/to/file.py
```

## Running the tests

Execute the unit tests using `pytest`:

```bash
pytest
```

The tests run entirely offline. A few end-to-end tests are skipped unless the required environment variables are present.

## Sample project

See [sample_project/README.md](sample_project/README.md) for a tour of the example Fabric workspace used by the CLI.

## Project Structure

```
ingen_fab/
├── cli_utils/            # CLI command implementations
├── ddl_scripts/          # Jinja templates for DDL notebook generation
├── notebook_utils/       # Notebook scanning and injection helpers
├── python_libs/          # Shared Python and PySpark libraries
│   ├── common/          # Common utilities (config, data, workflow)
│   ├── interfaces/      # Abstract interfaces
│   ├── python/          # CPython/Fabric runtime libraries
│   └── pyspark/         # PySpark-specific implementations
├── python_libs_tests/   # Test suites for Python libraries
sample_project/          # Example workspace demonstrating project layout
project_templates/       # Templates for new project initialization
scripts/                 # Helper scripts (SQL Server setup, etc.)
tests/                   # Unit tests for core functionality
```

## Environment Variables

You can set these environment variables to avoid specifying them on each command:

```bash
# Project location
export FABRIC_WORKSPACE_REPO_DIR="./sample_project"

# Target environment
export FABRIC_ENVIRONMENT="development"

# Authentication (for deployment)
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

## Workflow Example

```bash
# 1. Initialize a new project
ingen_fab init new --project-name "My Data Platform"

# 2. Configure variables in fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/
# Edit development.json, production.json, etc.

# 3. Create DDL scripts in ddl_scripts/
# Add numbered .sql or .py files for your tables and procedures

# 4. Generate DDL notebooks
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# 5. Deploy to your environment
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development

# 6. Generate and run platform tests
ingen_fab test platform generate
```


## Documentation

Additional documentation is available in the subdirectories:

- **[sample_project/README.md](sample_project/README.md)** - Complete example workspace with step-by-step workflow
- **[ingen_fab/python_libs/README.md](ingen_fab/python_libs/README.md)** - Reusable Python and PySpark libraries
- **[ingen_fab/ddl_scripts/README.md](ingen_fab/ddl_scripts/README.md)** - DDL notebook generation templates
- **[ingen_fab/python_libs/python/README_notebook_utils.md](ingen_fab/python_libs/python/README_notebook_utils.md)** - Notebook utilities abstraction
- **[ingen_fab/python_libs/python/sql_template_factory/README.md](ingen_fab/python_libs/python/sql_template_factory/README.md)** - SQL template system

## License

This project is provided for demonstration purposes and has no specific license.
