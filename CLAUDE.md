# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Important: Environment Activation

**ALWAYS activate the Python virtual environment and set required environment variables before running any commands:**
```bash
source .venv/bin/activate
export FABRIC_ENVIRONMENT="local"
export FABRIC_WORKSPACE_REPO_DIR="sample_project"
```
This must be done at the start of every session to ensure proper dependency access and correct CLI operation.

## Project Overview

Ingenious Fabric Accelerator (`ingen_fab`) is a CLI tool for creating and managing Microsoft Fabric workspace projects. It automates DDL notebook generation, environment deployment, and testing workflows for Fabric lakehouses and warehouses.

## Core Architecture

- **CLI Entry Point**: `ingen_fab/cli.py` - Typer-based CLI with command groups (init, deploy, ddl, test, notebook)
- **Command Modules**: `ingen_fab/cli_utils/` - Implementation of CLI commands
- **Python Libraries**: `ingen_fab/python_libs/` - Shared utilities split between `python/` (CPython/Fabric) and `pyspark/` (PySpark-specific)
- **DDL Templates**: `ingen_fab/ddl_scripts/` - Jinja2 templates for generating DDL notebooks
- **Notebook Utils**: `ingen_fab/notebook_utils/` - Tools for scanning, injecting, and converting notebooks
- **Sample Project**: `sample_project/` - Example workspace structure and configuration

## Environment Setup

```bash
# Using uv (preferred)
uv sync

# Or using pip
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Development Commands

### Testing
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=ingen_fab

# Run specific test file
pytest tests/test_specific_file.py
```

### Code Quality
```bash
# Lint with ruff
ruff check .

# Format with ruff
ruff format .

# Pre-commit hooks
pre-commit run --all-files
```

### Documentation
```bash
# Serve docs locally
./serve-docs.sh
# or
mkdocs serve --dev-addr=0.0.0.0:8000
```

### CLI Testing
```bash
# Test CLI commands (use sample_project for testing)
export FABRIC_WORKSPACE_REPO_DIR="./sample_project"
export FABRIC_ENVIRONMENT="development"

# Generate DDL notebooks
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# Test Python libraries locally (requires FABRIC_ENVIRONMENT=local)
export FABRIC_ENVIRONMENT=local
ingen_fab test local python
ingen_fab test local pyspark
```

## Key Configuration Files

- `pyproject.toml` - Dependencies, build config, tool settings
- `pytest.ini` - Pytest configuration
- `mkdocs.yml` - Documentation site configuration
- `sample_project/platform_manifest_*.yml` - Environment configurations
- `sample_project/fabric_workspace_items/config/var_lib.VariableLibrary/` - Variable definitions per environment

## Important Patterns

### Template System
The project uses Jinja2 templates extensively:
- DDL templates in `ingen_fab/ddl_scripts/_templates/`
- Notebook templates in `ingen_fab/notebook_utils/templates/`
- Generated notebooks include parameter cells and standardized imports

### Environment Variables
Key environment variables for development:
- `FABRIC_WORKSPACE_REPO_DIR` - Path to workspace project
- `FABRIC_ENVIRONMENT` - Target environment (development, test, production)
- Azure auth variables for deployment (AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET)

### Library Structure
Python libraries follow interface-based design:
- Abstract interfaces in `python_libs/interfaces/`
- CPython implementations in `python_libs/python/`
- PySpark implementations in `python_libs/pyspark/`
- Common utilities in `python_libs/common/`

## Testing Strategy

- Unit tests in `tests/` and `python_libs_tests/` 
- Integration tests marked with `@pytest.mark.e2e`
- Platform tests can run against live Fabric environments
- Tests are designed to run offline by default

## Linting and Formatting
- Uses `ruff` for linting and formatting



