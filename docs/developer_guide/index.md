# Developer Guide

[Home](../index.md) > Developer Guide

!!! warning "Documentation Status"
    The documentation in this guide needs to be reviewed and updated.

Welcome to the Developer Guide for the Ingenious Fabric Accelerator! This section is designed for developers who want to understand the architecture, extend functionality, or contribute to the project.

## Architecture Overview

The Ingenious Fabric Accelerator is built with a modular architecture that separates concerns and enables extensibility:

```mermaid
graph TD
    A[CLI Interface] --> B[Command Modules]
    B --> C[Core Libraries]
    C --> D[Python Libraries]
    C --> E[PySpark Libraries]
    C --> F[Template Engine]
    D --> G[Fabric Runtime]
    E --> G
    F --> H[Generated Notebooks]
    H --> G
```

## Core Components

### [Python Libraries](python_libraries.md)
Reusable Python and PySpark libraries that provide common functionality for Fabric workspaces.

### [DDL Scripts](ddl_scripts.md)
Template system for generating DDL notebooks from SQL and Python scripts.

### [Notebook Utils](notebook_utils.md)
Environment-agnostic utilities that work in both local development and Fabric runtime.

### [SQL Templates](sql_templates.md)
Jinja-based SQL template system supporting multiple database dialects.

### [Variable Replacement System](variable_replacement.md)
Environment-specific configuration management through placeholder replacement and code injection.

### [Packages](packages.md)
Extension package system for adding custom functionality and reusable components.

### [DBT Profile Manager](dbt_profile_manager.md)
Automatic dbt profile management and lakehouse integration.

### [Pip Install Testing](pip_install_testing.md)
Testing the package installation and dependencies in various environments.

## Development Setup

### Prerequisites

- Python 3.12+
- Git
- IDE with Python support (VS Code, PyCharm, etc.)
- Docker (optional, for containerized development)

### Clone and Setup

--8<-- "_includes/developer_setup.md"

### Development Dependencies

The project uses several development tools:

- **Testing**: pytest, pytest-cov, pytest-asyncio
- **Linting**: ruff, vulture
- **Type checking**: mypy (optional)
- **Documentation**: mkdocs, mkdocs-material
- **Pre-commit**: pre-commit hooks for code quality

### Container Development Setup

The dev container in `.devcontainer/spark_minimal/` gives a consistent environment: Python
3.12, a JVM for local Spark sessions, `uv`, git and the SQL Server ODBC driver, with the
project's virtual environment created automatically. Its
[readme](https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric/blob/main/.devcontainer/spark_minimal/readme.md)
is the reference for what is inside, the volumes, SQL Server networking and the base-image
history; this section is the short version.

1. **Open in Dev Container**: Use VS Code's "Dev Containers" extension, select "Reopen in
   Container" and pick `spark_minimal`. On first start `postCreateCommand` runs
   `uv sync --all-extras`; the venv lives at `/opt/uv/venv`, on a named volume inside the
   container, and is on `PATH`.

2. **Verify** from the container terminal (`quick` replaces the full Spark test file with one
   Delta write/read through the library's session factory):

    ```bash
    bash .devcontainer/spark_minimal/verify.sh quick
    ```

3. **Azure login** (inside the container; the token cache persists in a named volume):

    ```bash
    az login --use-device-code
    ```

4. **SQL Server (optional)**: run it as a separate container on the host and reach it from the
   dev container as `host.docker.internal`; the readme has the commands and the connection
   string to pass, since the library's default is hard-coded to `localhost,1433`.

## Project Structure

```
ingen_fab/
├── cli_utils/              # CLI command implementations
│   ├── __init__.py
│   ├── deploy_commands.py  # Deployment commands
│   ├── init_commands.py    # Project initialization
│   ├── notebook_commands.py # Notebook management
│   └── workspace_commands.py # Workspace operations
├── ddl_scripts/            # DDL template system
│   ├── _templates/         # Jinja templates
│   ├── notebook_generator.py # Template processor
│   └── README.md
├── notebook_utils/         # Notebook utilities
│   ├── fabric_cli_notebook.py
│   ├── simple_notebook.py
│   └── templates/
├── python_libs/            # Core Python libraries
│   ├── common/            # Shared utilities
│   ├── interfaces/        # Abstract interfaces
│   ├── python/           # CPython implementations
│   └── pyspark/          # PySpark implementations
├── python_libs_tests/     # Test suites
├── cli.py                 # Main CLI entry point
└── project_config.py      # Project configuration
```

## API Documentation

### [API Reference](../api/index.md)
Comprehensive API documentation for Python libraries and utilities.

### [Python APIs](../api/python_apis.md)
Detailed documentation of Python API interfaces and implementations.

## Development Workflow

### 1. Feature Development

```bash
# Create feature branch
git checkout -b feature/new-feature

# Make changes
# ... develop your feature ...

# Run tests
pytest ./tests/ -v
pytest ./ingen_fab/python_libs_tests/ -v

# Check code quality
ruff check .
ruff format .

# Commit changes
git add .
git commit -m "Add new feature: description"
```

### 2. Testing

The suite is split into tiers by pytest marker (registered in `pytest.ini`, which takes
precedence over `pyproject.toml`), so the part that needs no infrastructure runs anywhere and
the rest is opted into:

| Tier | Marker | Needs | Where it runs in CI |
| --- | --- | --- | --- |
| Offline | (none) | nothing beyond the `dev` dependency group | `offline` job, every push and PR |
| Spark | `spark` | a JVM (JDK 17): the dev container, or `actions/setup-java` | `spark` job, every push and PR |
| Database | `database` | a PostgreSQL (the `local` environment's warehouse dialect) | `database` job, `workflow_dispatch` only |
| Documentation | `docs` | nothing; heuristic checks of docs against code, two are `xfail` | inside the offline tier |
| End to end | `e2e` | live network access | not in CI |

```bash
export FABRIC_ENVIRONMENT=local
export FABRIC_WORKSPACE_REPO_DIR=ingen_fab/sample_project

# Offline tier: what CI runs on every change (about ten seconds)
pytest tests/ ingen_fab/python_libs_tests/common ingen_fab/python_libs_tests/python \
  -m "not spark and not database and not e2e"

# Spark tier, from inside the dev container (or anywhere with a JDK 17 on PATH)
pytest ingen_fab/python_libs_tests/pyspark tests/test_lakehouse_utils.py -m spark

# Database tier: start a PostgreSQL first, then point the library at it
docker run -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=local -p 5432:5432 -d postgres:16
export POSTGRES_HOST=localhost POSTGRES_PORT=5432 POSTGRES_USER=postgres \
       POSTGRES_PASSWORD=postgres POSTGRES_DATABASE=local
pytest ingen_fab/python_libs_tests/python -m database

# One library's tests through the CLI (resolves test_<lib>_pytest.py)
ingen_fab test local python warehouse_utils
ingen_fab test local pyspark lakehouse_utils

# Coverage
pytest --cov=ingen_fab --cov-report=html
```

`.github/workflows/tests.yml` runs the `lint` (ruff on changed files), `offline` and `spark`
jobs on every push and pull request and folds them into the `test` status check the branch
ruleset requires. Running the trees separately matters: `tests/conftest.py` installs a
`notebookutils` stand-in, and the CI Spark job runs both Spark paths in one session so any
leak between them shows up there.

The CLI help snippets under `docs/snippets/cli/` are generated files. Regenerate them from the
project's own interpreter (`scripts/refresh_cli_help.sh`, or `python
scripts/generate_cli_help_snippets.py`) and check that each file starts with a usage block; the
documentation tests fail on a snippet that holds a traceback.

### 3. Documentation

```bash
# Serve documentation locally
mkdocs serve

# Build documentation
mkdocs build

# Deploy documentation
mkdocs gh-deploy
```

## Adding New Features

### Adding a New CLI Command

1. **Create command module**:
   ```python
   # cli_utils/my_new_commands.py
   import typer
   from typing_extensions import Annotated

   def my_new_command(
       param: Annotated[str, typer.Option(help="Description")]
   ):
       """My new command description."""
       print(f"Executing with param: {param}")
   ```

2. **Register command in CLI**:
   ```python
   # cli.py
   from cli_utils import my_new_commands

   # Add command to app
   app.add_typer(
       my_new_commands.app,
       name="mynew",
       help="My new command group"
   )
   ```

3. **Add tests**:
   ```python
   # tests/test_my_new_commands.py
   from typer.testing import CliRunner
   from ingen_fab.cli import app

   def test_my_new_command():
       runner = CliRunner()
       result = runner.invoke(app, ["mynew", "command", "--param", "value"])
       assert result.exit_code == 0
   ```

### Adding a New Python Library

1. **Create the library**:
   ```python
   # python_libs/python/my_new_utils.py
   from typing import Any
   from .notebook_utils_abstraction import get_notebook_utils

   class MyNewUtils:
       def __init__(self):
           self.notebook_utils = get_notebook_utils()

       def my_function(self) -> Any:
           """New utility function."""
           return "result"
   ```

2. **Create tests**:
   ```python
   # python_libs_tests/python/test_my_new_utils_pytest.py
   import pytest
   from ingen_fab.python_libs.python.my_new_utils import MyNewUtils

   def test_my_function():
       utils = MyNewUtils()
       result = utils.my_function()
       assert result == "result"
   ```

3. **Add to template injection**:
   ```python
   # python_libs/gather_python_libs.py
   # Add your library to the collection process
   ```

### Adding a New DDL Template

1. **Create templates**:
   ```jinja2
   <!-- ddl_scripts/_templates/common/my_new_template.py.jinja -->
   # Generated DDL script for {{ entity_name }}
   from my_new_utils import MyNewUtils

   utils = MyNewUtils()
   result = utils.my_function()
   print(f"Result: {result}")
   ```

2. **Update notebook generator**:
   ```python
   # ddl_scripts/notebook_generator.py
   # Add template to the generation process
   ```

## Code Standards

### Python Style

- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Write docstrings for all public functions
- Use meaningful variable and function names

### Testing

- Write tests for all new functionality
- Aim for >80% code coverage
- Use descriptive test names
- Test both success and failure cases

### Documentation

- Update documentation for new features
- Include code examples
- Write clear, concise explanations
- Update CLI help text

## Debugging

### Local Development

```bash
# Run with verbose output
ingen_fab --help

# Debug specific commands - use VS Code launch configuration
See .vscode/launch.json for pre-configured debug setups

# Use logging
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Testing Issues

```bash
# Run specific test
pytest ./tests/test_cli.py::test_specific_function -v -s

# Debug test failures
pytest ./tests/test_cli.py::test_specific_function --pdb

# Check test coverage
pytest --cov=ingen_fab --cov-report=term-missing
```

## Contributing

### Pull Request Process

1. **Fork the repository**
2. **Create feature branch**
3. **Make changes with tests**
4. **Update documentation**
5. **Submit pull request**

### Code Review Checklist

- [ ] Code follows style guidelines
- [ ] Tests are included and passing
- [ ] Documentation is updated
- [ ] No breaking changes (or properly documented)
- [ ] Performance impact is considered

## Advanced Topics

### Custom Template Development

Learn how to create custom templates for specific use cases.

### Plugin Architecture

Understand how to extend the CLI with plugins.

### Performance Optimization

Best practices for optimizing generation and deployment performance.

### Integration Testing

Setting up comprehensive integration tests with Fabric.

## Resources

### Internal Documentation

- [Python Libraries](python_libraries.md) - Detailed library documentation
- [DDL Scripts](ddl_scripts.md) - Template system guide
- [Notebook Utils](notebook_utils.md) - Utility abstractions
- [SQL Templates](sql_templates.md) - SQL template reference
- [Variable Replacement System](variable_replacement.md) - Configuration management guide

### External Resources

- [Typer Documentation](https://typer.tiangolo.com/)
- [Jinja2 Documentation](https://jinja.palletsprojects.com/)
- [Microsoft Fabric Documentation](https://docs.microsoft.com/en-us/fabric/)
- [pytest Documentation](https://docs.pytest.org/)

## Getting Help

- **Documentation**: This site covers most development topics
- **CLI Help**: Use `ingen_fab --help` and `ingen_fab COMMAND --help` for command assistance
- **Code Examples**: Check the sample_project/ directory for working examples
- **Testing**: Run `pytest` to verify your development environment

Ready to contribute? Start with the [Python Libraries](python_libraries.md) guide to understand the core architecture!
