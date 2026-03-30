# Quick Start

[Home](../index.md) > [User Guide](index.md) > Quick Start

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
ingen_fab init new --project-name "dp"

```

!!! tip "Using Sample Project Template"
    For a more comprehensive starting point with platform manifests and example configurations, add the `--with-samples` flag:
    ```bash
    ingen_fab init new --project-name "dp" --with-samples
    ```
    This uses the `sample_project` template which includes pre-configured platform manifests for multiple environments.

This creates the following structure with complete starter files:
```
dp/
├── dbt_project/                    # dbt project folder      
│   ├── macros/                    # dbt macros
│   ├── metaextracts/              # dbt metadata extracts
│   ├── models/                    # dbt models
│   ├── dbt_project.yml             # dbt project configuration
│   └── README.md                   # initial README file
├── ddl_scripts/                    # Sample DDL scripts included
│   ├── Lakehouses/                # Python DDL scripts for Delta tables
│   └── Warehouses/               # SQL DDL scripts for warehouses
├── fabric_workspace_items/        # Complete Fabric workspace structure
│   ├── config/                    # Pre-configured variable library
│   ├── lakehouses/                # Sample lakehouse definitions
│   └── warehouses/                # Sample warehouse definitions
├── platform_manifest_*.yml       # Environment deployment tracking
└── README.md                      # Complete setup instructions
```

## Step 2: Configure Your Environment

Set up environment variables and configure your workspace details:

=== "macOS / Linux"
    ```bash
    # Set environment 
    export FABRIC_ENVIRONMENT="development"
    
    # Set workspace directory 
    export FABRIC_WORKSPACE_REPO_DIR="dp"
    ```

=== "Windows PowerShell"
    ```powershell
    # Set environment (development, UAT, production)
    $env:FABRIC_ENVIRONMENT = "development"
    
    # Set workspace directory 
    $env:FABRIC_WORKSPACE_REPO_DIR = "dp"
    ```

Now edit the development environment variables:

Replace the placeholder values with your actual workspace IDs:
Note: You can modify lakehouse and warehouse names as needed. To do that, variable names must match the names under variables.json file. ** Do not modify config lakehouse/warehouse names **


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
      "value": "your-config-lakehouse-guid"  // ← Replace this once created
    }
  ]
}
```

!!! tip "Finding Your Workspace IDs"
    You can find workspace and lakehouse IDs in the Microsoft Fabric portal URL when you navigate to your workspace or lakehouse.

## Step 3: Configure Storage and Generate Artifacts

Define your lakehouses, warehouses, and SQL databases in the storage configuration file, then automatically generate the required Fabric artifacts:

### 3.1 Review and Edit storage_config.yaml

Open `dp/fabric_config/storage_config.yaml` and configure your lakehouses, warehouses, and SQL databases:

```yaml
storage:
  - lakehouse: local
    lh_bronze: lh_bronze
    lh_silver: lh_silver
    lh_gold: lh_gold

  - warehouses: local
    wh_gold: wh_gold
    wh_reporting: DO_NOT_CREATE  # Use DO_NOT_CREATE to skip resources
  
  - sqldatabases: local
    sqldb_analytics: sqldb_analytics
    sqldb_staging: none  # Use none or DO_NOT_CREATE to skip resources
```

!!! tip "Storage Configuration Tips"
    - **Add or remove lakehouses, warehouses, or SQL databases** as needed for your project
    - **Use `DO_NOT_CREATE` or `none`** for any resource you don't want to create
    - The `local` value indicates these resources belong to the local workspace configuration

### 3.2 Generate Storage Artifacts

Run the storage configuration command to automatically create all required files:

```bash
# Generate lakehouse, warehouse, and SQL database artifacts
ingen_fab init storage-config
```

This command will:
- ✅ Create lakehouse folders (e.g., `lh_bronze.Lakehouse`, `lh_silver.Lakehouse`, `lh_gold.Lakehouse`)
- ✅ Create warehouse folders (e.g., `wh_gold.Warehouse`)
- ✅ Create SQL database folders (e.g., `sqldb_analytics.SQLDatabase`) if configured
- ✅ Generate Fabric artifact files (`.platform`, metadata files, with random GUID for logicalID that will have no impact when deployed to fabric)
- ✅ Update `variables.json` with variable definitions
- ✅ Update all environment valueSet files (development.json, test.json, production.json....)

**Expected Output:**
```
Found 3 lakehouse(s): lh_bronze, lh_silver, lh_gold
Found 1 warehouse(s): wh_gold

📦 Processing lakehouses...
  ✓ Created lh_bronze.Lakehouse (ID: a1b2c3d4...)
  ✓ Created lh_silver.Lakehouse (ID: e5f6g7h8...)
  ✓ Created lh_gold.Lakehouse (ID: i9j0k1l2...)

🏢 Processing warehouses...
  ✓ Created wh_gold.Warehouse (ID: m3n4o5p6...)

📝 Updating variable library definitions...
  ✓ Added 12 variable definition(s) to variables.json

📝 Updating variable library valueSets...
  ✓ Updated development.json (added 12 variables)
  ✓ Updated test.json (added 12 variables)
  ✓ Updated production.json (added 12 variables)

============================================================
✓ Successfully created 4 artifact folder(s)
  • 3 lakehouse(s)
  • 1 warehouse(s)
✓ Updated variables.json with new variable definitions
✓ Updated 3 valueSet file(s) with new variables
```

!!! note "What Gets Created"
    For each lakehouse, the command creates:
    - Artifact folder: `fabric_workspace_items/lakehouses/{name}.Lakehouse/`
    - Variables: `{name}_workspace_id`, `{name}_lakehouse_name`, `{name}_lakehouse_id`
    
    For each warehouse, the command creates:
    - Artifact folder: `fabric_workspace_items/warehouses/{name}.Warehouse/`
    - Variables: `{name}_workspace_id`, `{name}_warehouse_name`, `{name}_warehouse_id`

### 3.3 Deployment Note

When you run the deployment command later (Step 6), these lakehouses and warehouses will be automatically deployed to your Fabric workspace afterwitch you can run the "ingen_fab init workspace --workspace-name 'dp" command to update the variable valueSet.json files.

## Step 4: Explore the Sample DDL Scripts

The sample project template includes sample DDL scripts to get you started. Take a look at what's included:

```
dp/
    ├── ddl_scripts/                # Sample DDL scripts included
    ├── Lakehouses/                # Python DDL scripts for Delta tables
    │   ├── lh_bronze/             # sample python ddl scripts for the bronze lakehouse
    │   ├── lh_silver/             # sample python ddl scripts for the silver lakehouse  
    │   ├── lh_gold/               # sample python ddl scripts for the gold lakehouse
    └── Warehouses/                # SQL DDL scripts for warehouses
        └── wh_gold/               # sample sql ddl scripts for the gold warehouse
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

## Step 5: Generate DDL Notebooks

Transform your DDL scripts into executable notebooks:

💡Remember to set your environment variables first.

```bash
# Generate notebooks for lakehouses
ingen_fab ddl compile --generation-mode Lakehouse

# Generate notebooks for warehouses (if you have any)
ingen_fab ddl compile --generation-mode Warehouse
```

This creates orchestrator notebooks in `fabric_workspace_items/ddl_scripts/` that will:
- Execute your DDL scripts in the correct order
- Track execution state to prevent duplicate runs
- Provide comprehensive logging and error handling

## Step 6: Deploy to Fabric

Deploy your project to your Fabric workspace:

```bash
# Deploy to whichever environment is set using your environment variables
ingen_fab deploy deploy
```

Update Lakehouse and Warehouse guids:
```bash
# Overwrite the deffault config values for Warehouses and Lakehouses
ingen_fab init workspace --workspace-name "<your-workspace-name>"
```

Deploy python libraries:

```bash
# Deploy python libraries - these libraries are required for ddl script execution
ingen_fab deploy upload-python-libs
```

!!! note "Authentication Required"
    Make sure you've set up your Azure credentials before deploying. You can use Azure CLI (`az login`) or environment variables.

!!! success "Lakehouse & Warehouse Deployment"
    When you run `ingen_fab deploy deploy`, the lakehouses and warehouses you configured in Step 3 will be automatically created in your Fabric workspace!

## Step 7: Run Your DDL Scripts

1. **Navigate to your Fabric workspace** in the Microsoft Fabric portal
2. **Find the generated notebooks** in the `ddl_scripts` folder
3. **Run the orchestrator notebook**: `00_all_lakehouses_orchestrator`

The orchestrator will:
- Execute all DDL scripts in the correct sequence
- Log execution status and prevent duplicate runs
- Handle errors gracefully with detailed logging

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
- Check that you have deployed your python libraries
- Review the execution logs in the Fabric notebook output

**DDL Notebooks not generated:**
- Verify that your DDL scripts are in the correct directory structure
- Check that file names start with numbers (001_, 002_, etc.)
- Ensure scripts have proper file extensions (.py or .sql)

### Getting Help

- Use `ingen_fab --help` for command-specific help
- Check the [Examples](../examples/index.md) for more complex scenarios
- Review the [Workflows](workflows.md) for best practices

!!! success "Congratulations!"
    You've successfully created and deployed your first Fabric project! You're now ready to build more complex data solutions with the Ingenious Fabric Accelerator.