# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Communication Style

**CRITICAL**: When working with this codebase:
- **NEVER use emojis** in any communication, code, comments, or documentation
- **Always maintain a concise, professional tone** in all interactions
- Provide direct, clear technical communication without unnecessary elaboration
- Focus on facts and technical accuracy over conversational language

## Python Package Management

**CRITICAL**: This project uses `uv` exclusively for all Python operations:

- **ALWAYS use `uv` for package management** - Never use `pip`, `poetry`, or other tools
- **ALWAYS use `uv run` to execute Python commands** - This ensures correct environment and dependencies
- **NEVER manually activate virtual environments** - `uv run` handles this automatically

### Key uv Commands

```bash
# Sync dependencies from pyproject.toml
uv sync

# Add a new dependency
uv add <package>

# Add a development dependency
uv add <package> --dev

# Remove a dependency
uv remove <package>

# Run Python commands (always use this pattern)
uv run pytest
uv run python script.py
uv run ingen_fab --help

# Run tests
uv run pytest tests/
uv run pytest tests/test_specific.py

# Code quality
uv run ruff check .
uv run ruff format .
uv run pre-commit run --all-files
```

## Important: Environment Variables

**ALWAYS set required environment variables before running commands:**

=== "macOS/Linux"

    ```bash
    export FABRIC_ENVIRONMENT="local"
    export FABRIC_WORKSPACE_REPO_DIR="sample_project"
    ```

=== "Windows"

    ```powershell
    $env:FABRIC_ENVIRONMENT = "local"
    $env:FABRIC_WORKSPACE_REPO_DIR = "sample_project"
    ```

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

**CRITICAL**: Only use `uv` for environment setup:

```bash
# Sync all dependencies from pyproject.toml
uv sync

# Sync with all optional dependency groups
uv sync --all-extras
```

**NEVER** use pip or manual venv creation. `uv` handles all environment management automatically.

## Development Commands

**CRITICAL**: Always use `uv run` prefix for all Python commands:

### Testing
```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=ingen_fab

# Run specific test file
uv run pytest tests/test_specific_file.py
```

### Code Quality
```bash
# Lint with ruff
uv run ruff check .

# Format with ruff
uv run ruff format .

# Pre-commit hooks
uv run pre-commit run --all-files
```

### Documentation
```bash
# Serve docs locally
uv run mkdocs serve --dev-addr=0.0.0.0:8000
```

### CLI Testing

**CRITICAL**: Always use `uv run` for CLI commands:

=== "macOS/Linux"

    ```bash
    # Set environment variables
    export FABRIC_WORKSPACE_REPO_DIR="./sample_project"
    export FABRIC_ENVIRONMENT="development"

    # Generate DDL notebooks
    uv run ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
    uv run ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

    # Test Python libraries locally (requires FABRIC_ENVIRONMENT=local)
    export FABRIC_ENVIRONMENT=local
    uv run ingen_fab test local python
    uv run ingen_fab test local pyspark
    ```

=== "Windows"

    ```powershell
    # Set environment variables
    $env:FABRIC_WORKSPACE_REPO_DIR = "./sample_project"
    $env:FABRIC_ENVIRONMENT = "development"

    # Generate DDL notebooks
    uv run ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
    uv run ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

    # Test Python libraries locally (requires FABRIC_ENVIRONMENT=local)
    $env:FABRIC_ENVIRONMENT = "local"
    uv run ingen_fab test local python
    uv run ingen_fab test local pyspark
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

### Abstraction Libraries and their Usage
- ***IMPORTANT**: never use standard spark or file operations directly. Always use the provided utility functions and abstractions contained in `python_libs/` to ensure compatibility across environments.*
- NEVER use relative imports in the `python_libs/` directory. Always use absolute imports to ensure compatibility with both PySpark and CPython environments.


## Testing Strategy

- Unit tests in `tests/` and `python_libs_tests/`
- Integration tests marked with `@pytest.mark.e2e`
- Platform tests can run against live Fabric environments
- Tests are designed to run offline by default

## Linting and Formatting
- Uses `ruff` for linting and formatting
