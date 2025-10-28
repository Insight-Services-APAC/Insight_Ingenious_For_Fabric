# Documentation Sitemap

[Home](index.md)

Complete navigation map of all documentation in the Ingenious Fabric Accelerator project.

---

## 📚 Main Documentation Sections

### 🏠 [Home](index.md)
Main documentation landing page with feature overview and quick navigation.

### 👤 [User Guide](user_guide/index.md)
Complete user documentation for working with the Ingenious Fabric Accelerator.

- [Installation](user_guide/installation.md) - Setup and installation instructions
- [Quick Start](user_guide/quick_start.md) - Get started in minutes
- [CLI Reference](user_guide/cli_reference.md) - Complete command documentation
- [Workflows](user_guide/workflows.md) - Development and deployment workflows
- [DBT Integration](user_guide/dbt_integration.md) - dbt integration and profile management
- [Common Tasks](user_guide/common_tasks.md) - Quick reference for frequently used commands
- [Environment Variables](reference/environment-variables.md) - Configuration reference
- [Workspace Layout](user_guide/workspace_layout.md) - Project structure overview
- [Deploy Guide](user_guide/deploy_guide.md) - Deployment strategies and best practices

### 🚀 [Getting Started](getting-started/installation.md)
Step-by-step onboarding journey for new users.

- [Installation](getting-started/installation.md) - Detailed installation guide
- [Quick Start](getting-started/quick-start.md) - Your first project
- [First Project](getting-started/first-project.md) - Complete walkthrough

### 💻 [Developer Guide](developer_guide/index.md)
Technical documentation for developers and contributors.

- [Python Libraries](developer_guide/python_libraries.md) - Core Python and PySpark libraries
- [DDL Scripts](developer_guide/ddl_scripts.md) - Template system for DDL notebooks
- [Notebook Utils](developer_guide/notebook_utils.md) - Environment-agnostic utilities
- [SQL Templates](developer_guide/sql_templates.md) - Jinja-based SQL templates
- [Variable Replacement](developer_guide/variable_replacement.md) - Configuration management
- [Packages](developer_guide/packages.md) - Extension package system
- [DBT Profile Manager](developer_guide/dbt_profile_manager.md) - dbt integration internals
- [Pip Install Testing](developer_guide/pip_install_testing.md) - Package installation testing

### 📦 [Packages](packages/index.md)
Extension packages for specialized functionality.

#### Core Packages
- [Flat File Ingestion](packages/flat_file_ingestion.md) - Import CSV/Parquet files
- [Synthetic Data Generation](packages/synthetic_data_generation.md) - Generate test datasets
- [Extract Generation](packages/extract_generation.md) - Create file extracts
- [Synapse Sync](packages/synapse_sync.md) - Synchronize with Azure Synapse

#### Package Extensions & Integration
- [Incremental Synthetic Data Generation](packages/incremental_synthetic_data_generation.md) - Time-series data generation
- [Incremental Synthetic Data Import Integration](packages/incremental_synthetic_data_import_integration.md) - Import time-series data
- [Extract Generation - Synthetic Data Alignment](packages/extract_generation_synthetic_data_alignment.md) - Extract integration
- [Synthetic Data Generation Enhancements](packages/synthetic_data_generation_enhancements.md) - Latest features

### 📖 [Examples](examples/index.md)
Practical examples and real-world scenarios.

- [Sample Project](examples/sample_project.md) - Complete project walkthrough
- [Project Templates](examples/templates.md) - Template usage guide
- [Flat File Ingestion Example](examples/flat_file_ingestion.md) - Detailed ingestion example

### 📘 [API Reference](api/index.md)
Python API documentation.

- [Python APIs](api/python_apis.md) - Library interfaces and implementations

### 📋 [Reference](reference/environment-variables.md)
Technical reference documentation.

- [Environment Variables](reference/environment-variables.md) - Complete variable reference
- [Workspace Layout](reference/workspace-layout.md) - Directory structure reference

### 📖 [Guides](user_guide/index.md)
Additional guides and references (legacy - prefer user_guide/).
- [Workflows](guides/workflows.md) - Workflow patterns
- [Troubleshooting](guides/troubleshooting.md) - Common issues
- [Packages](guides/packages.md) - Package guide
- [Deployment](guides/deployment.md) - Deployment guide
- [Common Tasks](guides/common-tasks.md) - Task reference
- [CI/CD Deployment](guides/cicd-deployment.md) - DevOps integration

---

## 🛠️ Component Documentation

### Python Libraries
- [Python Libraries README](../ingen_fab/python_libs/README.md) - Overview of reusable libraries
- [Notebook Utils](../ingen_fab/python_libs/python/README_notebook_utils.md) - Abstraction layer documentation
- [SQL Template Factory](../ingen_fab/python_libs/python/sql_template_factory/README.md) - SQL template system

### DDL Scripts
- [DDL Scripts README](../ingen_fab/ddl_scripts/README.md) - DDL template system documentation

### Project Templates
- [Project Templates README](../ingen_fab/project_templates/README.md) - Template structure
- [dbt Project](../ingen_fab/project_templates/dbt_project/README.md) - dbt project template
- [Fabric Workspace Items](../ingen_fab/project_templates/fabric_workspace_items/Readme.md) - Workspace structure

### Package Sample Projects
- [Extract Generation Sample](../ingen_fab/packages/extract_generation/sample_project/README.md) - Extract generation examples
- [Flat File Ingestion Sample](../ingen_fab/packages/flat_file_ingestion/sample_project/README.md) - Ingestion examples
- [Synthetic Data Sample](../ingen_fab/packages/synthetic_data_generation/sample_project/README.md) - Data generation examples

### Migration & Technical Docs
- [Synthetic Data Migration Guide](../ingen_fab/packages/synthetic_data_generation/MIGRATION_GUIDE.md) - Upgrade guide

---

## 📄 Project Documentation

### Root Documentation
- [README](../README.md) - Project overview and quick start
- [AGENTS](../AGENTS.md) - Repository guidelines for AI agents
- [CLAUDE](../CLAUDE.md) - Claude-specific documentation
- [GEMINI](../GEMINI.md) - Gemini-specific documentation

### Audit Reports
- [Documentation Audit Report](DOCUMENTATION_AUDIT_REPORT.md) - Comprehensive documentation review
- [Audit Report](AUDIT_REPORT.md) - Original audit findings

### Development & Testing
- [Copilot Instructions](../.github/copilot-instructions.md) - GitHub Copilot guidelines
- [Dev Container Readme](../.devcontainer/spark_minimal/readme.md) - Container setup
- [Postgres Setup](../scripts/dev_container_scripts/spark_minimal/README_postgres_setup.md) - Database setup

---

## 🔧 Tool-Specific Documentation

### Prompts (AI Assistant Instructions)
- [Documentation Review](../prompts/documentation_review.md) - Doc review template
- [Package Ingest Refine (Lakehouse)](../prompts/package_ingest_refine_lakehouse.md) - Lakehouse ingestion
- [Package Ingest Refine (Warehouse)](../prompts/package_ingest_refine_warehouse.md) - Warehouse ingestion
- [Package Extracts Create](../prompts/package_extracts_create.md) - Extract creation

### CLI Help Snippets (Embedded Documentation)
Located in `docs/snippets/cli/`:
- [Root Help](snippets/cli/root_help.md)
- [Init Help](snippets/cli/init_help.md)
- [DDL Help](snippets/cli/ddl_help.md)
- [Deploy Help](snippets/cli/deploy_help.md)
- [Deploy Get Metadata Help](snippets/cli/deploy_get_metadata_help.md)
- [Notebook Help](snippets/cli/notebook_help.md)
- [Test Help](snippets/cli/test_help.md)
- [Package Help](snippets/cli/package_help.md)
- [Libs Help](snippets/cli/libs_help.md)
- [DBT Help](snippets/cli/dbt_help.md)
- [Extract Help](snippets/cli/extract_help.md)

### Include Files (Reusable Snippets)
Located in `docs/_includes/`:
- [Developer Setup](/_includes/developer_setup.md) - Development environment setup
- [Environment Setup](/_includes/environment_setup.md) - Environment configuration
- [Pip Install](/_includes/pip_install.md) - Installation instructions

---

## 🗂️ Documentation by Role

### **For New Users**
1. [Installation](getting-started/installation.md)
2. [Quick Start](getting-started/quick-start.md)
3. [First Project](getting-started/first-project.md)
4. [User Guide](user_guide/index.md)

### **For Regular Users**
1. [CLI Reference](user_guide/cli_reference.md)
2. [Common Tasks](user_guide/common_tasks.md)
3. [Workflows](user_guide/workflows.md)
4. [Packages](packages/index.md)
5. [Examples](examples/index.md)

### **For Developers**
1. [Developer Guide](developer_guide/index.md)
2. [Python Libraries](developer_guide/python_libraries.md)
3. [API Reference](api/index.md)
4. [Package Development](developer_guide/packages.md)

### **For Contributors**
1. [Developer Setup](developer_guide/index.md#development-setup)
2. [Project Structure](developer_guide/index.md#project-structure)
3. [Development Workflow](developer_guide/index.md#development-workflow)
4. [AGENTS.md](../AGENTS.md) - Repository guidelines

---

## 📊 Documentation Statistics

- **Total Markdown Files**: 208
- **Main Documentation Pages**: ~75
- **Component READMEs**: ~15
- **CLI Help Snippets**: 11
- **Include Files**: 3
- **Package Docs**: 9
- **Example Pages**: 3
- **API Pages**: 2

---

## 🔍 Finding Documentation

**Search Tips**:
- Use your browser's find function (Ctrl+F / Cmd+F) on this page
- Check the relevant section index first (User Guide, Developer Guide, etc.)
- CLI commands are documented in [CLI Reference](user_guide/cli_reference.md)
- Examples are in the [Examples](examples/index.md) section
- Package-specific docs are under [Packages](packages/index.md)

**Common Topics**:
- Installation → [Installation Guide](getting-started/installation.md)
- Commands → [CLI Reference](user_guide/cli_reference.md)
- Deployment → [Deploy Guide](user_guide/deploy_guide.md)
- Testing → [CLI Reference: test command](user_guide/cli_reference.md#test)
- Python APIs → [API Reference](api/index.md)
- dbt Integration → [DBT Integration](user_guide/dbt_integration.md)

---

**Last Updated**: Documentation audit completed with comprehensive link validation.
**Maintained By**: Insight Services APAC
**Repository**: [Insight_Ingenious_For_Fabric](https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric)
