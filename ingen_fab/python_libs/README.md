# Python Libraries

The `python_libs` folder contains reusable Python and PySpark libraries that are injected into Fabric notebooks to provide helper utilities and common functionality. These libraries enable consistent, testable code across both local development and Fabric runtime environments.

## Architecture

The library is organized into several subpackages:

- **`common/`** – Shared utilities used by both Python and PySpark implementations
- **`interfaces/`** – Abstract interfaces that define contracts for implementations
- **`python/`** – CPython/Fabric runtime implementations
- **`pyspark/`** – PySpark-specific implementations for Spark notebooks

## Common Modules

### `common/config_utils`
Configuration management utilities that load workspace configuration from tables and return structured `FabricConfig` dataclasses.

### `common/data_utils`
Data processing utilities including data validation, transformation helpers, and common data operations.

### `common/workflow_utils`
Workflow orchestration utilities for managing complex data processing pipelines.

## Python Modules

### `ddl_utils`
Execute DDL scripts exactly once and log their execution to prevent duplicate runs. Supports both SQL and Python DDL scripts.

### `lakehouse_utils`
Minimal helpers for interacting with Fabric Lakehouse storage, including file operations and metadata management.

### `notebook_utils_abstraction`
Environment-agnostic abstraction layer for `notebookutils` that works seamlessly in both local development and Fabric environments. See [README_notebook_utils.md](python/README_notebook_utils.md) for detailed documentation.

### `pipeline_utils`
Utilities for building and managing data pipelines, including dependency resolution and execution orchestration.

### `sql_template_factory`
Jinja-based SQL statement templates for multiple database dialects (Fabric, SQL Server). Provides consistent SQL generation across different environments. See [sql_template_factory/README.md](python/sql_template_factory/README.md) for details.

### `warehouse_utils`
Connect to Fabric or SQL Server warehouses and run queries. Supports both local testing with SQL Server and production deployment with Fabric warehouses.

## PySpark Modules

The PySpark modules mirror the Python implementations but operate on Spark `DataFrame` objects and Delta tables. They provide similar APIs for:

- **Configuration management** – Loading config from Delta tables
- **DDL execution** – Running DDL scripts in Spark environments
- **Lakehouse operations** – Working with Delta Lake tables
- **Notebook abstractions** – PySpark-compatible notebook utilities

## Testing

All libraries include comprehensive test suites located in `python_libs_tests/`. Tests are organized by module and runtime:

```bash
python_libs_tests/
├── common/              # Tests for shared utilities
├── interfaces/          # Interface contract tests
├── python/              # CPython implementation tests
└── pyspark/            # PySpark implementation tests
```

### Running Tests

```bash
# Run all tests
pytest ./ingen_fab/python_libs_tests/ -v

# Run tests for specific modules
pytest ./ingen_fab/python_libs_tests/python/test_warehouse_utils_pytest.py -v
pytest ./ingen_fab/python_libs_tests/pyspark/test_lakehouse_utils_pytest.py -v

# Run common utility tests
pytest ./ingen_fab/python_libs_tests/common/ -v
```

## Usage in Notebooks

Libraries are automatically injected into generated notebooks and can be imported directly:

```python
# In a Fabric notebook
from common.config_utils import FabricConfig
from lakehouse_utils import LakehouseUtils
from warehouse_utils import WarehouseUtils
from notebook_utils_abstraction import get_notebook_utils

# Get environment-appropriate utilities
utils = get_notebook_utils()
config = FabricConfig.from_table("config_table")
```

## Development Workflow

1. **Local Development**: Use the abstraction layers to develop and test locally
2. **Unit Testing**: Run comprehensive test suites for all modules
3. **Integration Testing**: Test with both local SQL Server and Fabric environments
4. **Deployment**: Libraries are automatically injected into generated notebooks

## Documentation

- **[README_notebook_utils.md](python/README_notebook_utils.md)** - Detailed documentation for the notebook utilities abstraction
- **[sql_template_factory/README.md](python/sql_template_factory/README.md)** - SQL template system documentation
