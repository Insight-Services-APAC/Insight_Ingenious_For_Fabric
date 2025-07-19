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
- **Packages**: Reusable workload extensions for common data processing scenarios

## Quick Start

**Tip:** You can set environment variables to avoid specifying them in every CLI operation. This makes commands simpler and less repetitive.

### Primary Environment Variables

- `FABRIC_WORKSPACE_REPO_DIR`: Sets the default directory containing your Fabric workspace repository files. If not specified in a CLI command, the CLI will use this value automatically.
- `FABRIC_ENVIRONMENT`: Sets the default environment name (e.g., `local`, `development`, `production`). This controls which environment configuration and variable library are used for operations. If not specified in a CLI command, the CLI will use this value automatically.

**Usage Example:**

In PowerShell:
```powershell
$env:FABRIC_WORKSPACE_REPO_DIR = "sample_project"
$env:FABRIC_ENVIRONMENT = "local"
```
In bash:
```bash
export FABRIC_WORKSPACE_REPO_DIR="sample_project"
export FABRIC_ENVIRONMENT="local"
```
Once set, all CLI commands will use these values by default, so you do not need to pass `--fabric-workspace-repo-dir` or `--fabric-environment` every time.

### Initialize a New Project

```bash
# Create a new Fabric workspace project with complete starter template
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

### Use Packages

```bash
# Compile flat file ingestion package for lakehouse
ingen_fab package ingest compile --target-datastore lakehouse --include-samples

# Compile flat file ingestion package for warehouse
ingen_fab package ingest compile --target-datastore warehouse --include-samples

# Compile synapse sync package
ingen_fab package synapse compile --include-samples
```

## Getting Started

!!! tip "New to Ingenious Fabric Accelerator?"
    Start with our [Installation Guide](user_guide/installation.md) to get up and running quickly.

!!! example "Ready to dive in?"
    Check out our [Sample Project](examples/sample_project.md) for a complete walkthrough.

## Architecture

The tool is organized into several key components:

```
ingen_fab/
├── cli_utils/            # CLI command implementations
├── ddl_scripts/          # Jinja templates for DDL notebook generation
├── notebook_utils/       # Notebook scanning and injection helpers
├── python_libs/          # Shared Python and PySpark libraries
├── python_libs_tests/    # Test suites for Python libraries
sample_project/           # Example workspace demonstrating project layout
project_templates/        # Templates for new project initialization
```

## Command Groups

- **[`init`](user_guide/cli_reference.md#init)** - Initialize solutions and projects
- **[`ddl`](user_guide/cli_reference.md#ddl)** - Compile DDL notebooks from templates
- **[`deploy`](user_guide/cli_reference.md#deploy)** - Deploy to environments and manage workspace items
- **[`notebook`](user_guide/cli_reference.md#notebook)** - Manage and scan notebook content
- **[`test`](user_guide/cli_reference.md#test)** - Test notebooks and Python blocks (local and platform)
- **[`package`](user_guide/cli_reference.md#package)** - Compile and run extension packages
- **[`libs`](user_guide/cli_reference.md#libs)** - Compile and manage Python libraries

## Core Concepts

### Environment Management
Manage multiple environments (development, test, production) with environment-specific configurations and variable libraries.

### DDL Script Management
Organize DDL scripts in numbered sequence for controlled execution, supporting both SQL and Python scripts with idempotent execution.

### Notebook Generation
Automatically generate notebooks from templates with proper error handling, logging, and orchestration capabilities.

### Packages
Reusable workload extensions that provide specialized functionality for common data processing scenarios like flat file ingestion and synapse sync.

### Testing Framework
Comprehensive testing framework supporting both local development and Fabric platform testing.

## Next Steps

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Get Started**

    ---

    Install and configure Ingenious Fabric Accelerator

    [:octicons-arrow-right-24: Installation Guide](user_guide/installation.md)

-   :material-book-open-page-variant:{ .lg .middle } **User Guide**

    ---

    Learn how to use all features and commands

    [:octicons-arrow-right-24: User Guide](user_guide/index.md)

-   :material-code-braces:{ .lg .middle } **Developer Guide**

    ---

    Understand the architecture and extend functionality

    [:octicons-arrow-right-24: Developer Guide](developer_guide/index.md)

-   :material-lightbulb:{ .lg .middle } **Examples**

    ---

    See real-world examples and best practices

    [:octicons-arrow-right-24: Examples](examples/index.md)

-   :material-package:{ .lg .middle } **Packages**

    ---

    Explore reusable workload extensions

    [:octicons-arrow-right-24: Packages](packages/index.md)

</div>

## Support

- **Issues**: Report bugs and request features on [GitHub Issues](https://github.com/your-org/ingen_fab/issues)
- **Discussions**: Join community discussions on [GitHub Discussions](https://github.com/your-org/ingen_fab/discussions)
- **Documentation**: Browse the complete documentation on this site