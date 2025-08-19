# Gemini Code Assistant Context

This document provides context for the Gemini Code Assistant to understand the `ingenious-fabric` project.

## Project Overview

`ingenious-fabric` is a command-line interface (CLI) tool for managing and automating development workflows for Microsoft Fabric. It is built with Python and the Typer library.

The project helps users to:

*   Initialize new Fabric projects.
*   Generate Data Definition Language (DDL) notebooks from templates.
*   Deploy and manage artifacts across different environments.
*   Create and run orchestrator notebooks.
*   Scan and analyze notebook code.
*   Test notebooks locally and on the Fabric platform.
*   Manage and reuse Python and PySpark libraries.
*   Use extension packages for common workloads like flat file ingestion and Synapse synchronization.

The project is structured as a Python package with a CLI entry point. It uses `pyproject.toml` for dependency management and `pytest` for testing.

## Building and Running

### Installation

The recommended way to install the project and its dependencies is using `uv`:

```bash
uv sync
```

Alternatively, you can use `pip`:

```bash
pip install -e .[dev]
```

### Running the CLI

The main entry point for the CLI is the `ingen_fab` command. You can see all available commands by running:

```bash
ingen_fab --help
```

### Running Tests

The project uses `pytest` for testing. To run the test suite, use the following command:

```bash
pytest
```

## Development Conventions

### Project Structure

The project follows a standard Python project structure:

*   `ingen_fab/`: The main source code for the CLI tool.
    *   `cli.py`: The main entry point for the CLI, using Typer.
    *   `cli_utils/`: Implementation of the CLI commands.
    *   `ddl_scripts/`: Jinja templates for DDL notebook generation.
    *   `notebook_utils/`: Helper functions for working with notebooks.
    *   `packages/`: Reusable extension packages.
    *   `python_libs/`: Shared Python and PySpark libraries.
*   `tests/`: Unit tests for the project.
*   `docs/`: Documentation for the project.
*   `pyproject.toml`: Defines project metadata and dependencies.
*   `README.md`: Provides a high-level overview of the project.

### Coding Style

The project uses `ruff` for linting and code formatting. The configuration for `ruff` can be found in the `pyproject.toml` file.

### Commits and Versioning

The project does not have explicit conventions for commit messages or versioning in the provided files. It is recommended to follow standard practices for both.
