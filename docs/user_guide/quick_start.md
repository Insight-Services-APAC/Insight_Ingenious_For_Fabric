# Quick Start

Get up and running with the Ingenious Fabric Accelerator in just a few minutes! This guide will walk you through creating your first project and deploying it to Microsoft Fabric.

## Prerequisites

Before starting, ensure you have:

- [x] Installed the Ingenious Fabric Accelerator ([Installation Guide](installation.md))
- [x] Access to a Microsoft Fabric workspace
- [x] Basic understanding of SQL and Python

## Step 1: Initialize Your First Project

Create a new project with the complete starter template:

```bash
# Initialize the project with complete template
ingen_fab init new --project-name "My First Fabric Project"

# Navigate to the created project
cd "My First Fabric Project"
```

This creates the following structure with complete starter files:
```
My First Fabric Project/
├── ddl_scripts/                    # Sample DDL scripts included
│   ├── Lakehouses/Config/         # Python DDL scripts for Delta tables
│   └── Warehouses/               # SQL DDL scripts for warehouses
│       ├── Config_WH/            # Config warehouse scripts
│       └── Sample_WH/            # Sample warehouse scripts
├── fabric_workspace_items/        # Complete Fabric workspace structure
│   ├── config/var_lib.VariableLibrary/  # Pre-configured variable library
│   ├── lakehouses/                # Sample lakehouse definitions
│   └── warehouses/                # Sample warehouse definitions
├── platform_manifest_*.yml       # Environment deployment tracking
└── README.md                      # Complete setup instructions
```

## Step 2: Configure Your Environment

Set up environment variables and configure your workspace details:

```bash
# Set up environment variables
export FABRIC_WORKSPACE_REPO_DIR="./My First Fabric Project"
export FABRIC_ENVIRONMENT="development"

# Edit the development environment variables
vim fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
```

Replace the placeholder values with your actual workspace IDs:

```json
{
  "name": "development",
  "variableOverrides": [
    {
      "name": "fabric_environment",
      "value": "development"
    },
    {
      "name": "fabric_deployment_workspace_id",
      "value": "your-workspace-guid"  // ← Replace this
    },
    {
      "name": "config_workspace_id",
      "value": "your-config-workspace-guid"  // ← Replace this
    },
    {
      "name": "config_lakehouse_id",
      "value": "your-config-lakehouse-guid"  // ← Replace this
    },
    {
      "name": "sample_lakehouse_id",
      "value": "your-sample-lakehouse-guid"  // ← Replace this
    }
  ]
}
```

!!! tip "Finding Your Workspace IDs"
    You can find workspace and lakehouse IDs in the Microsoft Fabric portal URL when you navigate to your workspace or lakehouse.

## Step 3: Explore the Sample DDL Scripts

The project template includes sample DDL scripts to get you started. Take a look at what's included:

```bash
# View the sample Lakehouse DDL script
cat ddl_scripts/Lakehouses/Config/001_Initial_Creation/001_sample_customer_table_create.py

# View the sample Warehouse DDL script  
cat ddl_scripts/Warehouses/Sample_WH/001_Initial_Creation/001_sample_customer_table_create.sql
```

The sample scripts create a customer table and insert sample data. You can:
- **Use them as-is** to test the workflow
- **Modify them** for your data model
- **Create additional scripts** following the same pattern

!!! tip "DDL Script Conventions"
    - Scripts are executed in alphabetical order by filename
    - Use numbered prefixes (001_, 002_, etc.) to control execution order
    - Lakehouse scripts use Python with Spark SQL
    - Warehouse scripts use T-SQL syntax

## Step 4: Generate DDL Notebooks

Transform your DDL scripts into executable notebooks:

```bash
# Generate notebooks for lakehouses
ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Lakehouse

# Generate notebooks for warehouses (if you have any)
ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Warehouse
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
# Generate platform tests
ingen_fab test platform generate \
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
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# Redeploy
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development
```

### Testing Changes Locally

```bash
# Test your Python libraries locally (requires FABRIC_ENVIRONMENT=local)
export FABRIC_ENVIRONMENT=local
ingen_fab test local python
ingen_fab test local pyspark

# Scan notebook content
ingen_fab notebook scan-notebook-blocks --base-dir ./fabric_workspace_items
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
- Use the sample_project/ directory for hands-on learning

!!! success "Congratulations!"
    You've successfully created and deployed your first Fabric project! You're now ready to build more complex data solutions with the Ingenious Fabric Accelerator.