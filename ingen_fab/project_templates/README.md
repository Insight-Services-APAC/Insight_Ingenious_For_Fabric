[← Back to Main Documentation](../../docs/index.md) | [← Project README](../../README.md)

# {project_name}

Microsoft Fabric workspace project created with InGen Fab.

## Structure

- `ddl_scripts/` - Source DDL scripts for creating tables and loading configuration data
- `fabric_workspace_items/` - Fabric workspace artifacts (notebooks, lakehouses, warehouses, etc.)
- `platform_manifest_*.yml` - Environment-specific deployment tracking

## Quick Start

1. **Configure your environment variables:**
   Update the variable library values in `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json`:
   - Replace `REPLACE_WITH_YOUR_WORKSPACE_GUID` with your Fabric workspace ID
   - Replace `REPLACE_WITH_CONFIG_LAKEHOUSE_GUID` with your configuration lakehouse ID
   - Replace `REPLACE_WITH_SAMPLE_LAKEHOUSE_GUID` with your main lakehouse ID

2. **Generate DDL orchestrator notebooks:**
   ```bash
   export FABRIC_WORKSPACE_REPO_DIR="./{project_name}"
   export FABRIC_ENVIRONMENT="development"
   
   # Generate lakehouse DDL notebooks
   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse
   
   # Generate warehouse DDL notebooks (optional)
   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
   ```

3. **Deploy to Fabric:**
   ```bash
   ingen_fab deploy deploy --environment development
   ```

## What's Included

This starter project includes:
- **Variable Library**: Environment-specific configuration management
- **Sample DDL Scripts**: Example customer table for both lakehouse and warehouse
- **Sample Lakehouse**: Pre-configured for Delta tables
- **Sample Warehouse**: Pre-configured for T-SQL analytics
- **Platform Manifests**: Deployment tracking for each environment

## Next Steps

1. Create your own DDL scripts in `ddl_scripts/Lakehouses/` or `ddl_scripts/Warehouses/`
2. Add your notebooks to `fabric_workspace_items/`
3. Run the compile and deploy commands to sync to Fabric
4. Execute the generated orchestrator notebooks in your Fabric workspace

## Environment Management

- **local**: For testing code without deploying to Fabric
- **development**: Your development Fabric workspace
- **production**: Your production Fabric workspace

Switch environments by setting `FABRIC_ENVIRONMENT` and updating the corresponding `valueSets/*.json` file.
