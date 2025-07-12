# Quick Start

Get up and running with the Ingenious Fabric Accelerator in just a few minutes! This guide will walk you through creating your first project and deploying it to Microsoft Fabric.

## Prerequisites

Before starting, ensure you have:

- [x] Installed the Ingenious Fabric Accelerator ([Installation Guide](installation.md))
- [x] Access to a Microsoft Fabric workspace
- [x] Basic understanding of SQL and Python

## Step 1: Initialize Your First Project

Create a new project directory and initialize it:

```bash
# Create and navigate to your project directory
mkdir my-fabric-project
cd my-fabric-project

# Initialize the project
ingen_fab init init-solution --project-name "My First Fabric Project"
```

This creates the following structure:
```
my-fabric-project/
├── ddl_scripts/              # Your DDL scripts go here
├── fabric_workspace_items/   # Generated Fabric artifacts
├── diagrams/                 # Architecture diagrams
└── platform_manifest_*.yml  # Environment configurations
```

## Step 2: Configure Your Environment

Edit the variable library with your Fabric workspace details:

```bash
# Edit the development environment variables
vim fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
```

Update the configuration with your workspace IDs:

```json
{
  "fabric_environment": "development",
  "config_workspace_id": "your-workspace-guid",
  "config_lakehouse_id": "your-lakehouse-guid",
  "edw_workspace_id": "your-workspace-guid",
  "edw_lakehouse_id": "your-lakehouse-guid",
  "edw_warehouse_id": "your-warehouse-guid"
}
```

!!! tip "Finding Your Workspace IDs"
    You can find workspace and lakehouse IDs in the Microsoft Fabric portal URL when you navigate to your workspace or lakehouse.

## Step 3: Create Your First DDL Script

Create a simple DDL script to set up a configuration table:

```bash
# Create the DDL script directory
mkdir -p ddl_scripts/Lakehouses/Config/001_Initial_Setup

# Create your first DDL script
cat > ddl_scripts/Lakehouses/Config/001_Initial_Setup/001_create_config_table.py << 'EOF'
# Create configuration table for the project
from lakehouse_utils import LakehouseUtils
from ddl_utils import DDLUtils

# Initialize utilities
lakehouse_utils = LakehouseUtils()
ddl_utils = DDLUtils()

# Create the configuration table
sql_create_table = """
CREATE TABLE IF NOT EXISTS config.project_metadata (
    id BIGINT,
    project_name STRING,
    environment STRING,
    created_date TIMESTAMP,
    last_updated TIMESTAMP
) USING DELTA
LOCATION 'Tables/config/project_metadata'
"""

# Execute the DDL
ddl_utils.execute_ddl(sql_create_table, "Create project metadata table")

# Insert initial configuration
sql_insert_config = """
INSERT INTO config.project_metadata VALUES (
    1,
    'My First Fabric Project',
    'development',
    current_timestamp(),
    current_timestamp()
)
"""

ddl_utils.execute_ddl(sql_insert_config, "Insert initial project metadata")

print("✅ Project configuration table created successfully!")
EOF
```

## Step 4: Generate DDL Notebooks

Transform your DDL scripts into executable notebooks:

```bash
# Generate notebooks for lakehouses
ingen_fab ddl compile \
    --output-mode fabric \
    --generation-mode lakehouse

# Generate notebooks for warehouses (if you have any)
ingen_fab ddl compile \
    --output-mode fabric \
    --generation-mode warehouse
```

This creates orchestrator notebooks in `fabric_workspace_items/ddl_scripts/` that will:
- Execute your DDL scripts in the correct order
- Track execution state to prevent duplicate runs
- Provide comprehensive logging and error handling

## Step 5: Deploy to Fabric

Deploy your project to your Fabric workspace:

```bash
# Deploy to development environment
ingen_fab deploy deploy \
    --fabric-workspace-repo-dir . \
    --fabric-environment development
```

!!! note "Authentication Required"
    Make sure you've set up your Azure credentials before deploying. You can use Azure CLI (`az login`) or environment variables.

## Step 6: Run Your DDL Scripts

1. **Navigate to your Fabric workspace** in the Microsoft Fabric portal
2. **Find the generated notebooks** in the `ddl_scripts` folder
3. **Run the orchestrator notebook**: `00_all_lakehouses_orchestrator`

The orchestrator will:
- Execute all DDL scripts in the correct sequence
- Log execution status and prevent duplicate runs
- Handle errors gracefully with detailed logging

## Step 7: Verify Your Deployment

Test that everything is working correctly:

```bash
# Test your deployment
ingen_fab test platform notebooks \
    --fabric-workspace-repo-dir . \
    --fabric-environment development
```

Or run the platform testing notebooks directly in Fabric:
- `platform_testing/python_platform_test.Notebook`
- `platform_testing/pyspark_platform_test.Notebook`

## Common First-Time Workflows

### Adding More DDL Scripts

```bash
# Create a new DDL script for additional tables
cat > ddl_scripts/Lakehouses/Config/001_Initial_Setup/002_create_data_tables.py << 'EOF'
# Create additional data tables
from lakehouse_utils import LakehouseUtils
from ddl_utils import DDLUtils

lakehouse_utils = LakehouseUtils()
ddl_utils = DDLUtils()

# Create a sample data table
sql_create_table = """
CREATE TABLE IF NOT EXISTS data.sample_data (
    id BIGINT,
    name STRING,
    value DOUBLE,
    created_date TIMESTAMP
) USING DELTA
LOCATION 'Tables/data/sample_data'
"""

ddl_utils.execute_ddl(sql_create_table, "Create sample data table")
print("✅ Sample data table created successfully!")
EOF

# Regenerate notebooks
ingen_fab ddl compile --output-mode fabric --generation-mode lakehouse

# Redeploy
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development
```

### Testing Changes Locally

```bash
# Test your Python libraries locally
ingen_fab test local libraries --base-dir .

# Test specific notebooks
ingen_fab test local notebooks --base-dir ./fabric_workspace_items
```

## Next Steps

Now that you have a working project, you can:

1. **[Learn more commands](cli_reference.md)** - Explore all available CLI commands
2. **[Study the sample project](../examples/sample_project.md)** - See a complete real-world example
3. **[Understand workflows](workflows.md)** - Learn best practices for development and deployment
4. **[Explore Python libraries](../developer_guide/python_libraries.md)** - Understand the available utilities

## Troubleshooting

### Common Issues

**Deployment fails with authentication error:**
```bash
# Set up Azure authentication
az login
# Or set environment variables
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
```

**DDL scripts fail to execute:**
- Check that your workspace and lakehouse IDs are correct
- Ensure your DDL scripts have valid syntax
- Review the execution logs in the Fabric notebook output

**Notebooks not generated:**
- Verify that your DDL scripts are in the correct directory structure
- Check that file names start with numbers (001_, 002_, etc.)
- Ensure scripts have proper file extensions (.py or .sql)

### Getting Help

- Use `ingen_fab --help` for command-specific help
- Check the [Examples](../examples/index.md) for more complex scenarios
- Review the [Workflows](workflows.md) for best practices
- Report issues on [GitHub](https://github.com/your-org/ingen_fab/issues)

!!! success "Congratulations!"
    You've successfully created and deployed your first Fabric project! You're now ready to build more complex data solutions with the Ingenious Fabric Accelerator.