# API Reference

[Home](../index.md) > API Reference

!!! warning "Documentation Status"
    The documentation in this guide needs to be reviewed and updated.

## Overview

The Ingenious Fabric Accelerator is primarily a CLI tool for building, deploying, and managing Microsoft Fabric applications. It provides Python libraries that are used within generated notebooks and can be imported for custom development.

## API Categories

### CLI Commands
Command-line interface for interacting with the accelerator tools. See the [CLI Reference](../user_guide/cli_reference.md) for complete documentation.

### [Python APIs](python_apis.md)
Python libraries and modules for use in Fabric notebooks and local development.

## Quick Start

### Command Line Interface

```bash
# Clone and install (see installation guide for complete instructions)
git clone <repository-url>
cd ingen_fab
uv sync  # or pip install -e .[dev]

# Get help
ingen_fab --help

# Initialize new project
ingen_fab init new --project-name "My Project"

# Compile DDL scripts
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
```

### Python Libraries

The Python libraries are designed to be used within Fabric notebooks:

```python
# Import DDL utilities (within Fabric notebook)
from python_libs.python.ddl_utils import DDLUtils
from python_libs.python.lakehouse_utils import LakehouseUtils

# Create utilities
ddl_utils = DDLUtils()
lakehouse_utils = LakehouseUtils()

# Execute DDL
sql = "CREATE TABLE IF NOT EXISTS my_table (id INT, name STRING) USING DELTA"
ddl_utils.execute_ddl(sql, "Create my_table")
```

## Installation

Install the package from source:

```bash
# Clone the repository (replace with your actual repository URL)
git clone <repository-url>
cd ingen_fab

# Using uv (recommended)
uv sync

# Or using pip for development installation
pip install -e .[dev]
```

## CLI Authentication

### Using Environment Variables

--8<-- "_includes/environment_setup.md"

### Using Azure CLI

```bash
# Login to Azure
az login

# The CLI will use your authenticated session
ingen_fab deploy deploy
```

## Python Library Structure

The accelerator provides libraries organized by runtime environment:

### Common Libraries (`python_libs/common/`)
- `config_utils.py` - Configuration and variable management
- `data_utils.py` - Common data processing utilities
- `workflow_utils.py` - Workflow management functions

### CPython/Fabric Libraries (`python_libs/python/`)
- `ddl_utils.py` - DDL execution and schema management
- `lakehouse_utils.py` - Lakehouse operations
- `warehouse_utils.py` - Warehouse operations
- `notebook_utils_abstraction.py` - Notebook utilities for Fabric runtime
- `pipeline_utils.py` - Data pipeline utilities
- `sql_templates.py` - SQL template processing

### PySpark Libraries (`python_libs/pyspark/`)
- `ddl_utils.py` - DDL execution using Spark SQL
- `lakehouse_utils.py` - Lakehouse operations with Spark
- `notebook_utils_abstraction.py` - Notebook utilities for Spark runtime
- `parquet_load_utils.py` - Parquet file loading utilities

### Interfaces (`python_libs/interfaces/`)
- `ddl_utils_interface.py` - Abstract interface for DDL operations
- `data_store_interface.py` - Abstract interface for data store operations

## Configuration

### Project Configuration

The accelerator uses YAML manifest files and JSON variable libraries:

```bash
# Project structure
my_project/
├── platform_manifest_development.yml
├── platform_manifest_production.yml
└── fabric_workspace_items/
    └── config/
        └── var_lib.VariableLibrary/
            ├── variables.json
            └── valueSets/
                ├── development.json
                ├── test.json
                └── production.json
```

### Variable Library Example

```json
// fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
{
  "fabric_environment": "development",
  "config_workspace_id": "dev-workspace-guid",
  "config_lakehouse_id": "dev-lakehouse-guid",
  "data_retention_days": 30
}
```

## Error Handling

### CLI Errors

```bash
# Use --help for command-specific guidance
ingen_fab ddl compile --help

# Check environment variables if commands fail
echo $FABRIC_WORKSPACE_REPO_DIR
echo $FABRIC_ENVIRONMENT
```

### Python Library Errors

Within Fabric notebooks, the libraries handle errors appropriately:

```python
try:
    ddl_utils.execute_ddl(sql, "Create table")
    print("✅ Table created successfully")
except Exception as e:
    print(f"❌ Failed to create table: {e}")
    raise
```

## Testing

### Local Testing

```bash
# Set environment for local testing
export FABRIC_ENVIRONMENT=local

# Test Python libraries
ingen_fab test local python

# Test PySpark libraries  
ingen_fab test local pyspark

# Test common libraries
ingen_fab test local common
```

### Platform Testing

```bash
# Generate platform test notebooks
ingen_fab test platform generate

# Test notebooks are created in:
# fabric_workspace_items/platform_testing/
```

## Common Patterns

### DDL Script Development

```python
# In a DDL script (ddl_scripts/Lakehouses/MyLakehouse/001_Setup/001_create_tables.py)
from python_libs.python.ddl_utils import DDLUtils

ddl_utils = DDLUtils()

# Create table with error handling
sql = """
CREATE TABLE IF NOT EXISTS config.metadata (
    id BIGINT,
    name STRING,
    value STRING,
    created_date TIMESTAMP
) USING DELTA
"""

try:
    ddl_utils.execute_ddl(sql, "Create metadata table")
    ddl_utils.log_execution("001_create_tables.py", "Created metadata table")
    print("✅ Successfully created metadata table")
except Exception as e:
    print(f"❌ Failed to create metadata table: {e}")
    raise
```

### Notebook Content Generation

```python
# Generated notebooks include standardized imports and patterns
# This is handled automatically by the DDL compilation process
```

### Variable Injection

```bash
# Compile libraries with environment-specific variables
ingen_fab libs compile --target-file "python_libs/common/config_utils.py"
```

## Package Management

### Flat File Ingestion

```bash
# Compile flat file ingestion package for lakehouse (PySpark runtime)
ingen_fab package ingest compile --target-datastore lakehouse --include-samples

# Compile flat file ingestion package for warehouse (Python runtime with COPY INTO)
ingen_fab package ingest compile --target-datastore warehouse --include-samples

# Run ingestion
ingen_fab package ingest run --config-id "customer_data" --execution-group 1
```

### Library Management

```bash
# Upload Python libraries to Fabric
ingen_fab deploy upload-python-libs --environment development

# Compile libraries with variable injection
ingen_fab libs compile
```

## Notebook Utilities

### Finding and Scanning Notebooks

```bash
# Find all notebook content files
ingen_fab notebook find-notebook-content-files --base-dir ./fabric_workspace_items

# Scan notebook blocks for analysis
ingen_fab notebook scan-notebook-blocks --base-dir ./fabric_workspace_items

# Perform code replacements
ingen_fab notebook perform-code-replacements
```

## Version Information

Current version: `0.1.0`

```python
import ingen_fab
print(ingen_fab.__version__)  # "0.1.0"
```

## Development

### Contributing

The project follows these development practices:

1. Use `ruff` for linting and formatting
2. Run tests with `pytest`
3. Use `uv` for dependency management
4. Follow the established project structure

### Testing Framework

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=ingen_fab

# Run specific test module
pytest tests/test_specific_module.py
```

## Support

### Getting Help

```bash
# CLI help
ingen_fab --help
ingen_fab COMMAND --help

# Specific command help
ingen_fab ddl compile --help
ingen_fab test local python --help
```

### Documentation

- **User Guide**: Step-by-step instructions for common workflows
- **Developer Guide**: Architecture and development information
- **CLI Reference**: Complete command reference
- **Examples**: Sample projects and templates

### Project Structure

The accelerator follows a consistent project structure:

```
ingen_fab/
├── cli.py                    # Main CLI entry point
├── cli_utils/               # CLI command implementations
├── ddl_scripts/             # DDL notebook generation
├── notebook_utils/          # Notebook scanning and management
├── python_libs/             # Runtime libraries
│   ├── common/             # Environment-agnostic utilities
│   ├── interfaces/         # Abstract interfaces
│   ├── python/             # CPython/Fabric implementations
│   └── pyspark/            # PySpark implementations
└── templates/              # Jinja2 templates for generation
```

This structure ensures consistent development patterns and clear separation of concerns between CLI operations, notebook generation, and runtime libraries.