# Ingenious Fabric Accelerator

Ingenious for Fabric is a comprehensive command line tool built with [Typer](https://typer.tiangolo.com/) that helps create and manage Microsoft Fabric assets. It provides a complete development workflow for Fabric workspaces, including project initialization, DDL notebook generation, environment management, testing and deployment automation.

## Features

<div class="grid cards" markdown>

-   :material-folder-plus:{ .lg .middle } **Project Initialization**

    ---

    Scaffold a workspace repo with a default variable library, sample DDL, and ready-to-run, workload-oriented notebooks.

    [:octicons-arrow-right-24: Learn more](user_guide/quick_start.md)

-   :material-table:{ .lg .middle } **DDL Notebook Generation**

    ---

    Compile SQL or Python DDL into ordered, idempotent Fabric notebooks for lakehouse and warehouse targets.

    [:octicons-arrow-right-24: Learn more](developer_guide/ddl_scripts.md)

-   :material-cloud-upload:{ .lg .middle } **Environment & Deployment**

    ---

    Manage variables per environment and deploy workspace items consistently across dev, test, and prod.

    [:octicons-arrow-right-24: Learn more](user_guide/deploy_guide.md)

-   :material-playlist-play:{ .lg .middle } **Orchestrator Notebooks**

    ---

    Auto-generate orchestrators that run DDL notebooks in sequence with logging and safety checks.

    [:octicons-arrow-right-24: Learn more](user_guide/quick_start.md#step-4-generate-ddl-notebooks)

-   :material-magnify-scan:{ .lg .middle } **Notebook Utilities**

    ---

    Scan, analyze, and transform notebook content with reusable helpers that work locally and in Fabric.

    [:octicons-arrow-right-24: Learn more](developer_guide/notebook_utils.md)

-   :material-test-tube:{ .lg .middle } **Testing Framework**

    ---

    Run local library tests and platform validations to keep notebooks and libs reliable.

    [:octicons-arrow-right-24: Learn more](user_guide/cli_reference.md#test)

-   :material-language-python:{ .lg .middle } **Python Libraries**

    ---

    Shared Python and PySpark libraries with environment-aware variable injection and Fabric-friendly APIs.

    [:octicons-arrow-right-24: Learn more](developer_guide/python_libraries.md)

-   :material-package-variant-closed:{ .lg .middle } **Packages**

    ---

    Plug-and-play workloads (ingestion, extracts, sync, synthetic data) you can compile and run.

    [:octicons-arrow-right-24: Learn more](packages/index.md)

</div>

## Getting Started

!!! tip "New to Ingenious Fabric Accelerator?"
    Start with our [Installation Guide](user_guide/installation.md) to get up and running quickly.

!!! example "Ready to dive in?"
    Check out our [Sample Project](examples/sample_project.md) for a complete walkthrough.



## Quick Navigation

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Getting Started**

    ---

    New to Ingenious? Start here for installation and your first project.

    [:octicons-arrow-right-24: Quick Start Guide](getting-started/quick-start.md)

-   :material-console:{ .lg .middle } **User Guides**

    ---

    Task-oriented guides for common workflows and operations.

    [:octicons-arrow-right-24: CLI Reference](guides/cli-reference.md) · [:octicons-arrow-right-24: Deployment](guides/deployment.md) · [:octicons-arrow-right-24: Packages](guides/packages.md)

-   :material-book:{ .lg .middle } **Reference**

    ---

    Technical reference for configuration and architecture.

    [:octicons-arrow-right-24: Environment Variables](reference/environment-variables.md) · [:octicons-arrow-right-24: Workspace Layout](reference/workspace-layout.md)

-   :material-code-braces:{ .lg .middle } **Developer Guide**

    ---

    Architecture, libraries, and extending the accelerator.

    [:octicons-arrow-right-24: Developer Documentation](developer_guide/index.md)

</div>

## Common Workflows

**5-Minute Start**: Create and deploy your first project

```bash
# 1. Create project
ingen_fab init new --project-name "My Project"

# 2. Set environment
export FABRIC_WORKSPACE_REPO_DIR="./My Project"
export FABRIC_ENVIRONMENT="development"

# 3. Generate notebooks
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse

# 4. Deploy
ingen_fab deploy deploy
```

**Package Usage**: Add data ingestion capabilities

```bash
# Compile flat file ingestion for lakehouse
ingen_fab package ingest compile --target-datastore lakehouse --include-samples

# Compile extract generation
ingen_fab package extract compile --include-samples --target-datastore warehouse
```


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

- **Documentation**: Browse the complete documentation on this site
- **Help**: Use `ingen_fab --help` for CLI assistance
- **Local Development**: Use the sample project for testing and learning
