# Ingenious Fabric Accelerator

A command-line tool for managing Microsoft Fabric workspaces with support for:
- DDL notebook generation from SQL scripts
- Variable injection into Jinja templates
- Environment-specific deployments
- Workspace synchronization

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd ingen_fab

# Install using uv (recommended)
uv sync
```

## Workflow

### 1. Create Your Workspace
First, create a Microsoft Fabric workspace in the Azure portal or Fabric interface.

### 2. Initialize a Solution
```bash
# Create a new project directory
mkdir my_fabric_project
cd my_fabric_project

# Initialize the solution structure
ingen_fab init-solution --project-name "My Fabric Project"
```

This creates the following structure:
```
my_fabric_project/
├── ddl_scripts/           # DDL scripts for tables, stored procedures, and configuration data
├── fabric_workspace_items/ # Fabric artifacts (notebooks, pipelines, etc.)
├── var_lib/               # Environment-specific variables
└── .gitignore
```

### 3. Configure Variables
Edit the variable library files in `var_lib/` to define your environment-specific values:

```bash
# Edit environment variables
vim var_lib/development.yml
vim var_lib/production.yml
```

Example variable file:
```yaml
# var_lib/development.yml
fabric_environment: development
config_workspace_id: "your-workspace-guid"
config_lakehouse_id: "your-lakehouse-guid"
# Add more variables as needed
```

### 4. Add Lakehouses and Warehouses
Create your lakehouses and warehouses in Fabric, then add their IDs to your variable files:

```yaml
# var_lib/development.yml
edw_lakehouse_id: "lakehouse-guid"
edw_warehouse_id: "warehouse-guid"
edw_workspace_id: "workspace-guid"
```

### 5. Deploy Initial Solution
Deploy your solution to the Fabric environment:

```bash
# Deploy to development
ingen_fab deploy-to-environment \
    --fabric-workspace-repo-dir . \
    --fabric-environment development
```

### 6. Define DDL Scripts
Create DDL scripts in the `ddl_scripts/` folder following this structure:

```
ddl_scripts/
├── Lakehouses/
│   └── MyLakehouse/
│       ├── 001_create_tables.sql
│       └── 002_insert_metadata.py
└── Warehouses/
    └── MyWarehouse/
        ├── 001_create_schemas.sql
        └── 002_create_tables.sql
```

Scripts are executed in numerical order and only run once per environment.

### 7. Generate DDL Notebooks
Compile your DDL scripts into Fabric notebooks:

```bash
# Generate notebooks for warehouses
ingen_fab compile-ddl-notebooks \
    --output-mode fabric \
    --generation-mode warehouse

# Generate notebooks for lakehouses
ingen_fab compile-ddl-notebooks \
    --output-mode fabric \
    --generation-mode lakehouse
```

This creates notebooks in `fabric_workspace_items/ddl_scripts/` that:
- Execute scripts in order
- Track execution state
- Support idempotent operations

### 8. Create Transformation Notebooks
Add your transformation and ingestion notebooks to `fabric_workspace_items/`:

```
fabric_workspace_items/
├── ddl_scripts/        # Generated DDL notebooks
├── transformations/    # Your transformation notebooks
└── ingestion/          # Your ingestion notebooks
```

### 9. Deploy Complete Solution
Deploy all artifacts including generated DDL notebooks:

```bash
ingen_fab deploy-to-environment \
    --fabric-workspace-repo-dir . \
    --fabric-environment development
```

### 10. Iterate and Redeploy
As you develop:
1. Add new DDL scripts as needed
2. Regenerate DDL notebooks
3. Create new transformation notebooks
4. Update variable files
5. Redeploy to see changes

```bash
# Quick iteration cycle
ingen_fab compile-ddl-notebooks --output-mode fabric --generation-mode warehouse
ingen_fab deploy-to-environment --fabric-workspace-repo-dir . --fabric-environment development
```

## Commands Reference

### Initialize Solution
```bash
ingen_fab init-solution --project-name "Project Name"
```

### Compile DDL Notebooks
```bash
# For warehouses
ingen_fab compile-ddl-notebooks \
    --output-mode [local|fabric] \
    --generation-mode warehouse

# For lakehouses
ingen_fab compile-ddl-notebooks \
    --output-mode [local|fabric] \
    --generation-mode lakehouse
```

### Deploy to Environment
```bash
ingen_fab deploy-to-environment \
    --fabric-workspace-repo-dir <path> \
    --fabric-environment <environment>
```

### Inject Variables
```bash
ingen_fab inject-notebook-vars \
    --template-path <notebook-path> \
    --environment <environment>
```

### Other Commands
```bash
# Find notebook files
ingen_fab find-notebook-content-files --base-dir <path>

# Scan notebook blocks
ingen_fab scan-notebook-blocks --base-dir <path>

# Help
ingen_fab --help
ingen_fab <command> --help
```

## Project Structure

| Folder | Purpose |
| ------ | ------- |
| `ddl_scripts/` | SQL and Python scripts for DDL operations |
| `fabric_workspace_items/` | Fabric artifacts (notebooks, pipelines, etc.) |
| `var_lib/` | Environment-specific variable files |
| `diagrams/` | Architecture diagrams |

## Environment Variables

Set these environment variables for authentication:
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`

## Testing

Run the test suite:
```bash
uv run pytest
```

## Documentation

- [Python Libraries](ingen_fab/python_libs/README.md) - Reusable Python utilities
- [DDL Scripts](ingen_fab/ddl_scripts/README.md) - DDL notebook generation
- [Sample Project](sample_project/README.md) - Example workspace structure

