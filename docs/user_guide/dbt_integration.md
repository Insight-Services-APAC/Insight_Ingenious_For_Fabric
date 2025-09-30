# DBT Integration Guide

[Home](../index.md) > [User Guide](index.md) > DBT Integration

The Ingenious Fabric Accelerator provides seamless integration with dbt (data build tool) for Microsoft Fabric environments, allowing you to develop, test, and deploy dbt models directly to Fabric lakehouses and warehouses.

## Overview

The dbt integration enables you to:
- Generate Fabric notebooks from dbt models and tests
- Automatically manage dbt profiles for Fabric connections
- Select target lakehouses interactively
- Run dbt commands within your Fabric workspace context
- Convert dbt metadata to Fabric-compatible formats

## Automatic Profile Management

One of the key features is automatic dbt profile management. When you run any dbt command through `ingen_fab`, the system automatically handles your connection configuration.

### How It Works

1. **Environment Detection**: The system reads your current `FABRIC_ENVIRONMENT` setting
2. **Lakehouse Discovery**: Scans the environment configuration for all available lakehouses
3. **Interactive Selection**: If multiple lakehouses are found, you'll be prompted to choose
4. **Preference Persistence**: Your selection is saved and reused for future commands
5. **Profile Generation**: Creates or updates `~/.dbt/profiles.yml` with the correct settings

### Lakehouse Selection

When you first run a dbt command in a new environment, you'll see:

```
Available Lakehouse Configurations:

┏━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┓
┃ # ┃ Prefix    ┃ Lakehouse Name       ┃ Workspace Name    ┃ Lakehouse… ┃
┡━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━┩
│ 1 │ bronze    │ Bronze Layer         │ Analytics         │ abc123...  │
│ 2 │ silver    │ Silver Layer         │ Analytics         │ def456...  │
│ 3 │ gold      │ Gold Layer           │ Analytics         │ ghi789...  │
│ 4 │ sample_lh │ Sample Lakehouse     │ Development       │ jkl012...  │
└───┴───────────┴──────────────────────┴───────────────────┴────────────┘

Select a lakehouse configuration by number [1]:
```

Your selection is saved and will be used automatically for subsequent commands in the same environment.

### Environment-Specific Profiles

Different environments can use different lakehouses:

=== "Development"

    ```bash
    export FABRIC_ENVIRONMENT="development"
    ingen_fab dbt run  # Uses development lakehouse selection
    ```

=== "Production"

    ```bash
    export FABRIC_ENVIRONMENT="production"
    ingen_fab dbt run  # Uses production lakehouse selection
    ```

## Setting Up DBT Projects

### 1. Project Structure

Organize your dbt project within your Fabric workspace repository:

```
my_fabric_project/
├── fabric_workspace_items/
│   └── config/
│       └── var_lib.VariableLibrary/
│           └── valueSets/
│               ├── development.json
│               ├── test.json
│               └── production.json
├── my_dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   └── macros/
└── platform_manifest_*.yml
```

### 2. Configuring Lakehouses

In your environment configuration files (`valueSets/*.json`), define your lakehouses:

```json
{
  "variableOverrides": [
    {
      "name": "bronze_workspace_id",
      "value": "aaaa-bbbb-cccc-dddd"
    },
    {
      "name": "bronze_lakehouse_id",
      "value": "1111-2222-3333-4444"
    },
    {
      "name": "bronze_lakehouse_name",
      "value": "Bronze Layer"
    },
    {
      "name": "silver_workspace_id",
      "value": "eeee-ffff-gggg-hhhh"
    },
    {
      "name": "silver_lakehouse_id",
      "value": "5555-6666-7777-8888"
    },
    {
      "name": "silver_lakehouse_name",
      "value": "Silver Layer"
    }
  ]
}
```

The system will automatically discover these configurations and present them as options.

### 3. Running DBT Commands

All standard dbt commands are available through the `ingen_fab dbt` proxy:

```bash
# Run all models
ingen_fab dbt run

# Run specific models
ingen_fab dbt run --models staging.customers

# Test models
ingen_fab dbt test

# Generate documentation
ingen_fab dbt docs generate

# Run seeds
ingen_fab dbt seed
```

## Creating Fabric Notebooks from DBT

The `create-notebooks` command converts your dbt models into Fabric-compatible notebooks:

```bash
# Basic usage
ingen_fab dbt create-notebooks --dbt-project-name my_dbt_project

# With custom cadences for scheduling
ingen_fab dbt create-notebooks \
  --dbt-project-name my_dbt_project \
  --incremental-models-cadence HOURLY \
  --seed-refresh-cadence WEEKLY \
  --master-notebook-cadence DAILY
```

This generates notebooks in `fabric_workspace_items/my_dbt_project/` with:
- Individual notebooks for each model
- Test notebooks for data quality checks
- Orchestrator notebooks for scheduling
- Proper dependencies and execution order

## Advanced Configuration

### Smart Behavior for Automated Workflows

The `dbt exec` command provides intelligent behavior for both interactive and automated use:

**With Valid Saved Preference:**
```bash
# Shows notification and continues automatically
ingen_fab dbt exec run

# Output:
# Using saved lakehouse preference: Bronze Layer (Environment: development)
# Running dbt command...
```

**Without Saved Preference:**
```bash
# Always prompts for selection to ensure correct configuration
ingen_fab dbt exec run

# Output:
# No valid lakehouse preference found for environment 'development'. Please select a lakehouse:
# [Interactive table appears]
```

This ensures that `dbt exec` never fails silently due to missing configuration, while still being efficient when preferences are already established.

### Manual Profile Configuration

If needed, you can manually edit `~/.dbt/profiles.yml`:

```yaml
fabric-spark-testnb:
  outputs:
    my_project_target:
      type: fabricsparknb
      authentication: CLI
      endpoint: https://api.fabric.microsoft.com/v1
      lakehouse: Bronze Layer
      lakehouseid: 1111-2222-3333-4444
      workspaceid: aaaa-bbbb-cccc-dddd
      workspacename: Analytics
      _lakehouse_prefix: bronze  # Saved selection
  target: my_project_target
```

The `_lakehouse_prefix` field stores your selection preference.

### Multiple DBT Projects

Each dbt project can use different lakehouses. The selection is based on:
1. Current `FABRIC_ENVIRONMENT`
2. Available lakehouses in that environment
3. Your saved preference (if any)

## Troubleshooting

### No Lakehouses Found

If no lakehouses are discovered:
1. Check your environment configuration file exists
2. Verify lakehouse IDs don't contain "REPLACE_WITH" placeholders
3. Ensure both `*_lakehouse_id` and `*_workspace_id` are defined

### Profile Not Updating

If the profile doesn't update:
1. Check write permissions for `~/.dbt/profiles.yml`
2. Verify the environment configuration is valid JSON
3. Try deleting the profile to force recreation

### Selection Not Saved

If your selection isn't remembered:
1. Ensure the profile was written successfully
2. Check that `_lakehouse_prefix` is in the profile
3. Verify you're using the same `FABRIC_ENVIRONMENT`

## Best Practices

1. **Consistent Naming**: Use clear prefixes for your lakehouses (bronze, silver, gold)
2. **Environment Separation**: Keep development and production lakehouses separate
3. **Documentation**: Document which lakehouse should be used for which dbt project
4. **Version Control**: Don't commit `~/.dbt/profiles.yml` - it's user-specific
5. **CI/CD**: Use service principals and automated selection for pipelines

## Next Steps

- [CLI Reference](../guides/cli-reference.md#dbt) - Complete dbt command reference
- [Deploy Guide](deploy_guide.md) - Deploying dbt models to Fabric
- [Workflows](../guides/workflows.md) - Integrating dbt into your development workflow
