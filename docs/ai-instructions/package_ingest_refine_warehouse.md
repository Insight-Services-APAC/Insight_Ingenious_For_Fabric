# Flat File Ingestion Package Testing & Refinement Guide - Warehouse Version

## Overview
The flat file ingestion package provides automated ingestion of various file formats (CSV, JSON, Parquet, Avro, XML) into Fabric warehouse tables based on metadata configuration. This guide covers comprehensive testing and refinement procedures for the warehouse implementation.


## Ask
- **Review** the package implementation to ensure it meets requirements
- **Test** all features with sample data
- **Refine** documentation and code based on findings


## Package Architecture
- **Core Compiler**: `ingen_fab/packages/flat_file_ingestion/flat_file_ingestion.py` - Extends BaseNotebookCompiler
- **Templates**: `templates/flat_file_ingestion_notebook.py.jinja` - Main processing notebook template
- **DDL Scripts**: Warehouse-specific DDL with config and log table definitions
- **Sample Data**: CSV and Parquet files for testing in warehouse context 

## Key Components
1. **FlatFileIngestionCompiler**: Manages template compilation and DDL script generation
2. **FlatFileProcessor**: Runtime processor using warehouse_utils abstractions
3. **Configuration Tables**: `config_flat_file_ingestion` stores ingestion metadata
4. **Logging Tables**: `log_flat_file_ingestion` tracks execution history

## END-TO-END TEST PROCEDURE

### 1. Environment Setup
```bash
# Activate virtual environment and set environment variables
source .venv/bin/activate
export FABRIC_ENVIRONMENT="local"
export FABRIC_WORKSPACE_REPO_DIR="sample_project"
```

### 2. Compile Package Assets
```bash
# Compile package notebooks and DDL scripts with sample data
python -m ingen_fab.cli package ingest compile --include-samples

# Compile project DDL scripts for warehouse environments
python -m ingen_fab.cli ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
```

### 3. Execute DDL Scripts
```bash
# Create configuration tables in warehouse
python sample_project/fabric_workspace_items/ddl_scripts/Warehouses/Config/001_Initial_Creation_Ingestion/001_config_flat_file_ingestion_create.Notebook/notebook-content.py
python sample_project/fabric_workspace_items/ddl_scripts/Warehouses/Config/001_Initial_Creation_Ingestion/002_log_flat_file_ingestion_create.Notebook/notebook-content.py

# Load sample configuration data
python sample_project/fabric_workspace_items/ddl_scripts/Warehouses/Config/002_Sample_Data_Ingestion/003_sample_data_insert.Notebook/notebook-content.py
```

### 4. Run Ingestion Processor
```bash
# Execute the main ingestion notebook
python sample_project/fabric_workspace_items/flat_file_ingestion/flat_file_ingestion_processor.Notebook/notebook-content.py
```

### 5. Validation Tests
- Verify CSV file ingestion (sales_data.csv)
- Verify Parquet file ingestion (products.parquet, customers.parquet)
- Check log table for execution records
- Validate data in target warehouse tables (raw.sales_data, raw.products, raw.customers)


## TROUBLESHOOTING GUIDELINES

### Template Issues
- Files are generated from templates in `/workspaces/ingen_fab/ingen_fab/packages/flat_file_ingestion/`
- Never modify generated files directly - update source templates
- Template variables: `target_warehouse` (warehouse_utils instance), `du` (ddl_utils instance)

### Runtime Dependencies
- All SQL/file operations must use abstraction layers:
  - `warehouse_utils` for table operations and SQL execution
  - `ddl_utils` for DDL operations
  - Never use raw SQL or filesystem methods directly

### Common Issues & Solutions
1. **Schema Mismatch**: Check `custom_schema_json` configuration
2. **File Not Found**: Verify `source_file_path` is accessible from warehouse context
3. **Permission Errors**: Ensure proper workspace/warehouse IDs in config
4. **Data Type Issues**: Review `date_format` and `timestamp_format` settings

## TESTING CHECKLIST
- [ ] Environment variables set correctly
- [ ] Package compilation successful
- [ ] DDL scripts executed without errors
- [ ] Configuration table populated with test data
- [ ] Sample files accessible from warehouse context
- [ ] Ingestion processor runs successfully
- [ ] Log table shows completed executions
- [ ] Target tables contain expected data
- [ ] Error handling tested (invalid file, wrong format, etc.)
- [ ] Performance acceptable for large files

## AUDIT REQUIREMENTS
Document all modifications with:
1. **File Path**: Complete path to modified file
2. **Change Type**: Added/Modified/Deleted
3. **Specific Changes**: Line numbers and descriptions
4. **Rationale**: Why the change was necessary
5. **Impact**: Downstream effects of the change

## PERFORMANCE OPTIMIZATION
- Use appropriate `write_mode` (overwrite vs append vs merge)
- Configure `partition_columns` for large datasets
- Set `sort_columns` for query optimization
- Consider `execution_group` for parallel processing

## ULTRATHINK CONSIDERATIONS
When analyzing and refining:
1. Review abstraction layer usage for consistency
2. Validate error handling strategies
3. Check logging completeness
4. Ensure configuration flexibility
5. Verify warehouse-specific functionality and performance
6. Assess security implications of file access patterns

## ENVIRONMENT TROUBLESHOOTING
- Ensure that hive metastore is running: sudo mysqld_safe --user=mysql --datadir=/var/lib/mysql --socket=/var/run/mysqld/mysqld.sock --pid-file=/var/run/mysqld/mysqld.pid &

- Ensure that SQL Server is running: /opt/mssql/bin/sqlservr &