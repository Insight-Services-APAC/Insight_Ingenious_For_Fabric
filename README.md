# Ingenious Fabric Accelerator

Ingenious Fabric Accelerator is a comprehensive command line tool built with [Typer](https://typer.tiangolo.com/) that helps create and manage Microsoft Fabric workspace projects. It provides a complete development workflow for Fabric workspaces, including project initialization, DDL notebook generation, environment management, deployment automation, and data integration through packages for flat file ingestion, Synapse synchronization, and extract generation.

## Features

- **Project Initialization**: Create new Fabric workspace projects with proper structure and templates
- **DDL Notebook Generation**: Generate DDL notebooks from Jinja templates for both lakehouses and warehouses
- **Environment Management**: Deploy and manage artifacts across multiple environments (development, test, production)
- **Orchestrator Notebooks**: Create orchestrator notebooks to run generated notebooks in sequence
- **Notebook Utilities**: Scan, analyze, and transform existing notebook code and content
- **Testing Framework**: Test notebooks both locally and on the Fabric platform
- **Python Libraries**: Reusable Python and PySpark libraries with variable injection for common Fabric operations
- **Extension Packages**:
  - Flat file ingestion for lakehouses and warehouses
  - Synapse synchronization with incremental and snapshot support
  - Extract generation for automated data extraction workflows
  - Synthetic data generation for testing and development
- **DBT Integration**: Generate Fabric notebooks from dbt models and tests with automatic profile management and intelligent lakehouse selection
- **Metadata Extraction**: Extract schema and table metadata from lakehouses and warehouses via SQL endpoints

## Requirements

- Python 3.12+
- Dependencies listed in `pyproject.toml`

## Installation

### For Users

Install the Ingenious Fabric Accelerator using pip:

=== "macOS/Linux"

    ```bash
    # Install from PyPI (when available)
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    ```

=== "Windows"

    ```powershell
    # Install from PyPI (when available)
    pip install insight-ingenious-for-fabric

    # Or install from GitHub
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    ```

For complete installation instructions, see our [Installation Guide](docs/user_guide/installation.md).

### For Developers

To contribute or modify the source code:

=== "macOS/Linux"

    ```bash
    # Clone the repository
    git clone https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    cd ingen_fab

    # Set up development environment
    uv sync --all-extras  # or: pip install -e .[dev,docs,dbt]
    ```

=== "Windows"

    ```powershell
    # Clone the repository
    git clone https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
    cd ingen_fab

    # Set up development environment
    uv sync --all-extras  # or: pip install -e .[dev,docs,dbt]
    ```

See the [Developer Guide](docs/developer_guide/index.md) for complete development setup.

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
- **`package`** - Compile and run extension packages (e.g., flat file ingestion, synapse sync, extract generation)
- **`libs`** - Compile and manage Python libraries with variable injection
- **`dbt`** - Proxy commands to dbt_wrapper and generate notebooks from dbt outputs

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

# Get metadata for lakehouse/warehouse
ingen_fab deploy get-metadata --target lakehouse --format csv
ingen_fab deploy get-metadata --target warehouse --format json
ingen_fab deploy get-metadata --target both --format table

# Compile flat file ingestion package for lakehouse
ingen_fab package ingest compile --target-datastore lakehouse --include-samples

# Compile flat file ingestion package for warehouse
ingen_fab package ingest compile --target-datastore warehouse --include-samples

# Run flat file ingestion
ingen_fab package ingest run --config-id CONFIG_ID --execution-group 1

# Compile synapse sync package
ingen_fab package synapse compile --include-samples

# Run synapse sync
ingen_fab package synapse run --master-execution-id EXEC_ID

# Compile extract generation package
ingen_fab package extract compile --target-datastore warehouse --include-samples

# Run extract generation
ingen_fab package extract extract-run --extract-name EXTRACT_NAME --run-type FULL

# Compile Python libraries with variable injection
ingen_fab libs compile --target-file path/to/file.py

# Generate notebooks from dbt outputs
ingen_fab dbt create-notebooks --dbt-project-name my_dbt_project
ingen_fab dbt convert-metadata --dbt-project-dir ./dbt_project
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
├── packages/             # Extension packages (flat file ingestion, synapse sync)
├── python_libs/          # Shared Python and PySpark libraries
│   ├── common/          # Common utilities (config, data, workflow)
│   ├── interfaces/      # Abstract interfaces
│   ├── python/          # CPython/Fabric runtime libraries
│   └── pyspark/         # PySpark-specific implementations
├── python_libs_tests/   # Test suites for Python libraries
├── templates/           # Jinja2 templates for testing and generation
├── utils/               # Core utility modules (path utils, resource manager)
sample_project/          # Example workspace demonstrating project layout
project_templates/       # Templates for new project initialization
scripts/                 # Helper scripts (dev container setup, SQL Server, etc.)
tests/                   # Unit tests for core functionality
docs/                    # Documentation source files
```

## Environment Variables

Set up environment variables to avoid specifying them on each command:

```bash
export FABRIC_WORKSPACE_REPO_DIR="./sample_project"
export FABRIC_ENVIRONMENT="development"
```

See [Environment Variables](docs/user_guide/environment_variables.md) for the complete list including authentication variables.

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

Additional documentation is available in the subdirectories and the complete documentation site:

- **[docs/](docs/)** - Complete documentation source files (build with `mkdocs serve`)
- **[sample_project/README.md](sample_project/README.md)** - Complete example workspace with step-by-step workflow
- **[ingen_fab/python_libs/README.md](ingen_fab/python_libs/README.md)** - Reusable Python and PySpark libraries
- **[ingen_fab/ddl_scripts/README.md](ingen_fab/ddl_scripts/README.md)** - DDL notebook generation templates
- **[ingen_fab/python_libs/python/README_notebook_utils.md](ingen_fab/python_libs/python/README_notebook_utils.md)** - Notebook utilities abstraction
- **[ingen_fab/python_libs/python/sql_template_factory/README.md](ingen_fab/python_libs/python/sql_template_factory/README.md)** - SQL template system

## Documentation

Complete documentation is available in the `docs/` directory. To serve locally:

```bash
uv sync --group docs  # Install docs dependencies
mkdocs serve --dev-addr=0.0.0.0:8000
```

## License

This project is provided for demonstration purposes and has no specific license.
# Extract metadata for lakehouse/warehouse
ingen_fab deploy get-metadata --lakehouse-name MyLakehouse --format table --target lakehouse
