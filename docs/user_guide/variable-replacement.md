# Variable Replacement

[Home](../index.md) > [User Guide](index.md) > Variable Replacement

Learn how to use environment-specific variables in your Fabric artifacts to create portable code that works across development, test, and production environments.

## Overview

Variable replacement allows you to write code once and deploy it to multiple environments with different configuration values. Instead of hardcoding workspace IDs, lakehouse names, or connection strings, you use placeholders that get replaced during deployment.

## How It Works

The Ingenious Fabric Accelerator replaces variables in your Fabric artifacts during deployment:

1. **Define variables** in environment-specific JSON files (development.json, test.json, production.json)
2. **Use placeholders** in your notebooks, data pipelines, and other artifacts using `{{varlib:variable_name}}` syntax
3. **Deploy** using `ingen_fab deploy` - variables are automatically replaced with environment-specific values

## Variable Placeholder Syntax

Use `{{varlib:variable_name}}` syntax in your Fabric artifacts for environment-specific values:

```python
# In your notebook
workspace_id = "{{varlib:fabric_deployment_workspace_id}}"
lakehouse_name = "{{varlib:config_lakehouse_name}}"
environment = "{{varlib:fabric_environment}}"

# After deployment to development, becomes:
workspace_id = "544530ea-a8c9-4464-8878-f666d2a8f418"
lakehouse_name = "config"
environment = "development"
```

## Supported Artifact Types

Variable replacement works in the following Fabric artifact types:

### Notebooks (.Notebook)

Python and PySpark notebooks support variable placeholders:

```python
# fabric_workspace_items/my_notebook.Notebook/notebook-content.py

# Using placeholders for lakehouse names
config_lakehouse = "{{varlib:config_lakehouse_name}}"
bronze_lakehouse = "{{varlib:bronze_lakehouse_name}}"
workspace = "{{varlib:config_workspace_name}}"

# Using placeholders for workspace IDs
workspace_id = "{{varlib:fabric_deployment_workspace_id}}"

# Using placeholders for environment
environment = "{{varlib:fabric_environment}}"
```

### Data Pipelines (.DataPipeline)

Pipeline definitions can include variable placeholders in JSON configuration:

```json
{
  "properties": {
    "activities": [
      {
        "name": "CopyData",
        "type": "Copy",
        "typeProperties": {
          "source": {
            "type": "LakehouseTableSource",
            "lakehouseName": "{{varlib:bronze_lakehouse_name}}"
          },
          "sink": {
            "type": "LakehouseTableSink",
            "lakehouseName": "{{varlib:silver_lakehouse_name}}"
          }
        }
      }
    ]
  }
}
```

### Spark Job Definitions (.SparkJobDefinition)

Spark job configurations can use variables in arguments:

```json
{
  "jobType": "SparkJob",
  "mainClass": "com.example.MainClass",
  "arguments": [
    "--workspace={{varlib:config_workspace_name}}",
    "--environment={{varlib:fabric_environment}}",
    "--lakehouse={{varlib:bronze_lakehouse_name}}"
  ]
}
```

### SQL Files in DDL Scripts

SQL scripts compiled to notebooks support placeholders:

```sql
-- ddl_scripts/Warehouses/wh_gold/001_Initial_Creation/001_create_view.sql

CREATE OR ALTER VIEW DW.vDim_Cities
AS 
SELECT * 
FROM [{{varlib:gold_lakehouse_name}}].[dbo].[dim_cities]
WHERE environment = '{{varlib:fabric_environment}}'
```

### Python Files in DDL Scripts

Python DDL scripts support placeholders in strings:

```python
# ddl_scripts/Lakehouses/lh_config/001_Initial_Creation/001_create_table.py

# Using placeholders in OneLake paths
table_location = "abfss://{{varlib:config_workspace_id}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_workspace_name}}.Lakehouse/Tables/"

# Using placeholders in SQL
sql = f"""
CREATE TABLE IF NOT EXISTS config.metadata (
    id BIGINT,
    workspace_name STRING DEFAULT '{{varlib:config_workspace_name}}',
    environment STRING DEFAULT '{{varlib:fabric_environment}}'
) USING DELTA
LOCATION '{table_location}metadata'
"""
```

## Defining Variables

### Variable Configuration Files

Variables are defined in your project's variable library configuration:

```
fabric_workspace_items/
  config/
    var_lib.VariableLibrary/
      variables.json          # Variable definitions
      development.json        # Development environment values
      test.json              # Test environment values
      production.json        # Production environment values
```

### variables.json Structure

Define your variables with metadata:

```json
{
  "variables": [
    {
      "name": "fabric_environment",
      "type": "string",
      "description": "Current deployment environment"
    },
    {
      "name": "config_lakehouse_name",
      "type": "string",
      "description": "Name of the configuration lakehouse"
    },
    {
      "name": "bronze_lakehouse_name",
      "type": "string",
      "description": "Name of the bronze layer lakehouse"
    },
    {
      "name": "fabric_deployment_workspace_id",
      "type": "string",
      "description": "Target workspace GUID for deployment"
    }
  ]
}
```

### Environment-Specific Value Files

Each environment file contains the actual values:

**development.json:**
```json
{
  "valueSets": [
    {
      "name": "default",
      "values": [
        {
          "name": "fabric_environment",
          "value": "development"
        },
        {
          "name": "config_lakehouse_name",
          "value": "config"
        },
        {
          "name": "bronze_lakehouse_name",
          "value": "lh_bronze"
        },
        {
          "name": "fabric_deployment_workspace_id",
          "value": "544530ea-a8c9-4464-8878-f666d2a8f418"
        }
      ]
    }
  ]
}
```

**production.json:**
```json
{
  "valueSets": [
    {
      "name": "default",
      "values": [
        {
          "name": "fabric_environment",
          "value": "production"
        },
        {
          "name": "config_lakehouse_name",
          "value": "config"
        },
        {
          "name": "bronze_lakehouse_name",
          "value": "lh_bronze"
        },
        {
          "name": "fabric_deployment_workspace_id",
          "value": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        }
      ]
    }
  ]
}
```

## Common Use Cases

### 1. Environment-Aware Lakehouse References

Write notebooks that work in any environment:

```python
# Reference lakehouses using variables
config_lakehouse = "{{varlib:config_lakehouse_name}}"
bronze_lakehouse = "{{varlib:bronze_lakehouse_name}}"
silver_lakehouse = "{{varlib:silver_lakehouse_name}}"

# Read from bronze, write to silver
df = spark.read.table(f"{bronze_lakehouse}.raw_customers")
df_transformed = transform_data(df)
df_transformed.write.mode("overwrite").saveAsTable(f"{silver_lakehouse}.customers")
```

### 2. Dynamic OneLake Paths

Build OneLake paths that work across workspaces:

```python
# OneLake path with variables
workspace_id = "{{varlib:fabric_deployment_workspace_id}}"
workspace_name = "{{varlib:config_workspace_name}}"
lakehouse_name = "{{varlib:bronze_lakehouse_name}}"

onelake_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{workspace_name}.Lakehouse/Files/"
```

### 3. Environment-Specific Logic

Add environment-specific behavior:

```python
environment = "{{varlib:fabric_environment}}"

if environment == "production":
    # Production-specific settings
    checkpoint_location = "/checkpoints/prod"
    enable_logging = True
else:
    # Development/test settings
    checkpoint_location = "/checkpoints/dev"
    enable_logging = False
```

### 4. Dynamic Table Naming

Create environment-specific table names:

```python
environment = "{{varlib:fabric_environment}}"
table_prefix = f"{environment}_"

# Tables automatically get environment prefix
df.write.saveAsTable(f"bronze.{table_prefix}customers")
df.write.saveAsTable(f"bronze.{table_prefix}orders")
```

### 5. Connection Strings and API Endpoints

Store environment-specific connection details:

```python
# Define these variables in your variable library
api_endpoint = "{{varlib:data_api_endpoint}}"
storage_account = "{{varlib:external_storage_account}}"
key_vault_url = "{{varlib:key_vault_url}}"

# Use in your code
response = requests.get(f"{api_endpoint}/data")
```

## Deployment Workflow

### Step 1: Define Variables

Create variables in `variables.json`:

```json
{
  "variables": [
    {
      "name": "my_lakehouse",
      "type": "string",
      "description": "Primary lakehouse name"
    }
  ]
}
```

### Step 2: Set Environment Values

Set values for each environment:

**development.json:**
```json
{
  "valueSets": [
    {
      "name": "default",
      "values": [
        {
          "name": "my_lakehouse",
          "value": "dev_lakehouse"
        }
      ]
    }
  ]
}
```

**production.json:**
```json
{
  "valueSets": [
    {
      "name": "default",
      "values": [
        {
          "name": "my_lakehouse",
          "value": "prod_lakehouse"
        }
      ]
    }
  ]
}
```

### Step 3: Use Placeholders in Code

Add placeholders to your notebooks:

```python
lakehouse = "{{varlib:my_lakehouse}}"
df = spark.read.table(f"{lakehouse}.customers")
```

### Step 4: Deploy to Target Environment

Deploy using the CLI:

```bash
# Deploy to development
ingen_fab deploy --environment development

# Deploy to production
ingen_fab deploy --environment production
```

During deployment:
- The accelerator reads the target environment's JSON file
- All `{{varlib:variable_name}}` placeholders are replaced with actual values
- The modified artifacts are deployed to the target workspace

## Best Practices

### Naming Conventions

- Use descriptive variable names: `bronze_lakehouse_name` instead of `lakehouse1`
- Include the resource type: `config_workspace_id` vs just `workspace`
- Use consistent naming patterns across environments

### Variable Organization

- Group related variables together in `variables.json`
- Use clear descriptions for each variable
- Document any special formatting requirements

### Security

- **Never commit secrets** to variable files - use Azure Key Vault references instead
- Keep production values separate from development/test
- Use Azure DevOps variable groups for sensitive values in CI/CD pipelines

### Testing

- Test variable replacement in development first
- Verify all placeholders are replaced (no `{{varlib:}}` in deployed code)
- Use consistent variable names across all artifacts

### Documentation

- Document all variables in your project README
- Include examples of valid values
- Note which variables are required vs optional

## Troubleshooting

### Placeholder Not Replaced

**Problem:** After deployment, you see `{{varlib:my_variable}}` in your code

**Solutions:**
- Verify the variable name exists in `variables.json`
- Check that the environment JSON file (development.json, production.json) has a value defined
- Ensure variable names match exactly (case-sensitive)

### Wrong Value After Deployment

**Problem:** The deployed code has an incorrect value

**Solutions:**
- Check you deployed to the correct environment (`--environment` flag)
- Verify the value in the target environment's JSON file
- Check for typos in variable names

### Variable Not Found Error

**Problem:** Deployment fails with "variable not found"

**Solutions:**
- Ensure the variable is defined in `variables.json`
- Verify the variable has a value in the target environment's JSON file
- Check for extra spaces or special characters in variable names

## Related Topics

- [Workflows](workflows.md) - Learn about the full development workflow
- [DDL Script Organization](ddl-organization.md) - Best practices for organizing DDL scripts
- [Developer Guide: Variable Replacement](../developer_guide/variable_replacement.md) - Technical details about the variable replacement system

## Summary

Variable replacement with `{{varlib:variable_name}}` placeholders enables:

- **Environment portability** - Same code works across dev/test/prod
- **Easy configuration** - Change values without modifying code
- **Safe deployments** - Environment-specific values automatically applied
- **Better collaboration** - Clear separation between code and configuration

Start using variables today to make your Fabric solutions more maintainable and portable!
