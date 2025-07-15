# Sample Project - Ingenious Fabric Accelerator

This sample project demonstrates the complete workflow for managing a Microsoft Fabric workspace using the Ingenious Fabric Accelerator. It includes:

- Complete project structure with DDL scripts, notebooks, and configuration
- Environment-specific variable management
- Generated DDL notebooks for both warehouses and lakehouses
- Example transformation and ingestion notebooks
- Platform testing notebooks

## Project Structure

```
sample_project/
├── ddl_scripts/              # DDL scripts for tables and configuration
│   ├── Lakehouses/          # Lakehouse DDL scripts
│   │   └── Config/          # Configuration tables
│   └── Warehouses/          # Warehouse DDL scripts
│       └── Config/          # Configuration tables
├── fabric_workspace_items/   # Generated Fabric artifacts
│   ├── config/              # Variable library
│   ├── ddl_scripts/         # Generated DDL notebooks
│   ├── extract/             # Data extraction notebooks
│   ├── load/                # Data loading notebooks
│   ├── lakehouses/          # Lakehouse definitions
│   ├── platform_testing/    # Platform testing notebooks
│   └── warehouses/          # Warehouse definitions
├── diagrams/                # Architecture diagrams
└── platform_manifest_*.yml  # Environment-specific configurations
```

## Getting Started

### 1. Create Your Workspace
First, create a Microsoft Fabric workspace in the Azure portal or Fabric interface.

### 2. Configure Your Environment
Update the variable library files in `fabric_workspace_items/config/var_lib.VariableLibrary/` to match your Fabric workspace:

```bash
# Edit development environment variables
vim fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
```

Example configuration:
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

### 3. Review DDL Scripts
Examine the DDL scripts in `ddl_scripts/` to understand the data model:

**Lakehouse DDL Scripts** (`ddl_scripts/Lakehouses/Config/`):
- `001_config_parquet_loads_create.py` - Creates parquet load configuration table
- `002_config.synapse_extract_objects.py` - Creates Synapse extract objects
- `003_log_parquet_loads_create.py` - Creates parquet load logging table
- `004_log_synapse_loads_create.py` - Creates Synapse load logging table
- `005_config_synapse_loads_insert.py` - Inserts Synapse load configuration
- `006_config_parquet_loads_insert.py` - Inserts parquet load configuration

**Warehouse DDL Scripts** (`ddl_scripts/Warehouses/Config/`):
- Similar structure with SQL scripts for warehouse-specific tables

### 4. Generate DDL Notebooks
From the root directory, generate DDL notebooks for your environment:

```bash
# Generate DDL notebooks for warehouses
ingen_fab ddl compile \
    --fabric-workspace-repo-dir ./sample_project \
    --fabric-environment development \
    --output-mode fabric_workspace_repo \
    --generation-mode Warehouse

# Generate DDL notebooks for lakehouses
ingen_fab ddl compile \
    --fabric-workspace-repo-dir ./sample_project \
    --fabric-environment development \
    --output-mode fabric_workspace_repo \
    --generation-mode Lakehouse
```

This generates orchestrator notebooks in `fabric_workspace_items/ddl_scripts/` that will:
- Execute DDL scripts in the correct order
- Track execution state to prevent duplicate runs
- Provide logging and error handling

### 5. Deploy to Fabric
Deploy the complete solution to your Fabric workspace:

```bash
# Deploy all artifacts to development environment
ingen_fab deploy deploy \
    --fabric-workspace-repo-dir ./sample_project \
    --fabric-environment development
```

This will:
- Deploy the variable library with environment-specific values
- Deploy all generated DDL notebooks
- Deploy transformation and ingestion notebooks
- Deploy lakehouse and warehouse configurations

### 6. Run DDL Scripts
Execute the DDL notebooks in Fabric to create your data model:

1. Navigate to your Fabric workspace
2. Run the orchestrator notebooks in this order:
   - `00_all_warehouses_orchestrator` (if using warehouses)
   - `00_all_lakehouses_orchestrator` (if using lakehouses)

The orchestrator notebooks will:
- Execute all DDL scripts in the correct sequence
- Log execution status to prevent duplicate runs
- Handle errors gracefully

### 7. Test Your Deployment
Run the platform testing notebooks to verify everything is working:

```bash
# Generate platform test notebooks
ingen_fab test platform generate \
    --fabric-workspace-repo-dir ./sample_project \
    --fabric-environment development
```

Or run the testing notebooks directly in Fabric:
- `platform_testing/python_platform_test.Notebook`
- `platform_testing/pyspark_platform_test.Notebook`

## Key Features Demonstrated

### Environment-Specific Configuration
- Variable library with multiple environments (development, test, production)
- Environment-specific deployment configurations
- Secure secret management through variable sets

### DDL Script Management
- Numbered DDL scripts for controlled execution order
- Support for both SQL and Python DDL scripts
- Idempotent execution with logging
- Separate orchestration for warehouses and lakehouses

### Notebook Generation
- Automatic notebook generation from DDL scripts
- Orchestrator notebooks for batch execution
- Platform testing notebooks for validation
- Proper error handling and logging

### Data Architecture
- Configuration-driven data loading
- Synapse integration for legacy data sources
- Parquet file processing capabilities
- Comprehensive logging and monitoring

## Customization

To adapt this sample for your own project:

1. **Update Variable Library**: Modify the JSON files in `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/` with your workspace IDs
2. **Modify DDL Scripts**: Update the scripts in `ddl_scripts/` to match your data model
3. **Add Custom Notebooks**: Create your own transformation and ingestion notebooks
4. **Update Configurations**: Modify the platform manifest files for your environments

## Testing

The sample includes comprehensive testing capabilities:

```bash
# Test locally (requires FABRIC_ENVIRONMENT=local)
export FABRIC_ENVIRONMENT=local
ingen_fab test local python
ingen_fab test local pyspark

# Generate platform tests
export FABRIC_ENVIRONMENT=development
ingen_fab test platform generate
```

## Related Documentation

- **[Main README](../README.md)** - Complete CLI reference and installation guide
- **[Python Libraries](../ingen_fab/python_libs/README.md)** - Reusable Python utilities documentation
- **[DDL Scripts](../ingen_fab/ddl_scripts/README.md)** - DDL notebook generation system
- **[Notebook Utils](../ingen_fab/python_libs/python/README_notebook_utils.md)** - Environment abstraction layer
- **[SQL Templates](../ingen_fab/python_libs/python/sql_template_factory/README.md)** - SQL template system

## Support

For issues and questions:
- Check the main project documentation
- Review the generated notebook logs for execution details
- Test locally using the testing framework before deploying to Fabric

