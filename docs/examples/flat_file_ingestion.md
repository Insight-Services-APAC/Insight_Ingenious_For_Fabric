# Flat File Ingestion Package

[Home](../index.md) > [Examples](index.md) > Flat File Ingestion

> Note: For authoritative CLI options and flags, see Packages → [Flat File Ingestion](../packages/flat_file_ingestion.md). This example focuses on the scenario walkthrough and keeps options brief to avoid duplication.

The flat file ingestion package provides automated ingestion of various file formats (CSV, JSON, Parquet, Avro, XML) into Microsoft Fabric tables with configuration-driven metadata management.

## Features

- **Multi-Format Support**: CSV, JSON, Parquet, Avro, and XML file processing
- **Dual Target Support**: Both lakehouse (Delta tables) and warehouse (SQL tables) targets
- **COPY INTO Operations**: Efficient warehouse bulk loading with staging patterns
- **Configuration-Driven**: Metadata-based processing with flexible execution grouping
- **Error Handling**: Multiple strategies (fail, skip, log) with comprehensive logging
- **Schema Management**: Automatic inference or custom schema definitions
- **Data Validation**: Configurable validation rules and data quality checks

## Architecture Overview

### Target Datastore Types

| Target | Runtime | Processing Approach | Best For |
|--------|---------|-------------------|----------|
| **Lakehouse** | PySpark | Direct Delta table operations | Schema evolution, transformations, analytics |
| **Warehouse** | Python | COPY INTO → staging → target | High-performance bulk loading, SQL operations |

### Warehouse Processing Flow (COPY INTO)

```
Source File → COPY INTO staging_table → MERGE/INSERT into target_table → Log execution → Cleanup staging
```

### Configuration Schema

The universal configuration table supports both target types:

```sql
CREATE TABLE config_flat_file_ingestion (
    config_id NVARCHAR(50) NOT NULL,
    config_name NVARCHAR(200) NOT NULL,
    source_file_path NVARCHAR(500) NOT NULL,
    source_file_format NVARCHAR(20) NOT NULL,
    target_workspace_id NVARCHAR(50) NOT NULL,
    target_datastore_id NVARCHAR(50) NOT NULL,
    target_datastore_type NVARCHAR(20) NOT NULL, -- 'lakehouse' or 'warehouse'
    target_schema_name NVARCHAR(50) NOT NULL,
    target_table_name NVARCHAR(100) NOT NULL,
    staging_table_name NVARCHAR(100) NULL, -- For warehouse COPY INTO
    write_mode NVARCHAR(20) NOT NULL, -- overwrite, append, merge
    -- ... additional configuration fields
);
```

## Usage Examples

### Compilation Options

```bash
# Compile for lakehouse (default)
ingen_fab package ingest compile --target-datastore lakehouse --include-samples

# Compile for warehouse with COPY INTO
ingen_fab package ingest compile --target-datastore warehouse --include-samples

# Compile both versions
ingen_fab package ingest compile --target-datastore both --include-samples
```

### Sample Configuration

**Lakehouse Target:**
```json
{
    "config_id": "csv_lakehouse_001",
    "config_name": "Sales Data to Lakehouse",
    "source_file_path": "Files/data/sales.csv",
    "source_file_format": "csv",
    "target_datastore_type": "lakehouse",
    "target_datastore_id": "lakehouse-guid",
    "target_schema_name": "bronze",
    "target_table_name": "sales_raw",
    "write_mode": "overwrite",
    "execution_group": 1
}
```

**Warehouse Target:**
```json
{
    "config_id": "csv_warehouse_001",
    "config_name": "Sales Data to Warehouse",
    "source_file_path": "Files/data/sales.csv",
    "source_file_format": "csv",
    "target_datastore_type": "warehouse",
    "target_datastore_id": "warehouse-guid",
    "target_schema_name": "bronze",
    "target_table_name": "sales_raw",
    "staging_table_name": "staging_sales_raw",
    "write_mode": "overwrite",
    "execution_group": 2
}
```

## Generated Artifacts

### Lakehouse Version
- **Notebook**: `flat_file_ingestion_processor_lakehouse.Notebook`
- **Runtime**: PySpark (synapse_pyspark)
- **Libraries**: Uses `lakehouse_utils` for Delta operations
- **Processing**: Direct DataFrame operations with Delta optimizations

### Warehouse Version
- **Notebook**: `flat_file_ingestion_processor_warehouse.Notebook`
- **Runtime**: Python (synapse_python)
- **Libraries**: Uses `warehouse_utils` and `ddl_utils` for SQL operations
- **Processing**: COPY INTO staging → MERGE/INSERT to target

### DDL Scripts
- **Configuration Tables**: Universal schema supporting both targets
- **Log Tables**: Execution tracking with performance metrics
- **Sample Data**: Test configurations for both lakehouse and warehouse

## Key Capabilities

### File Format Support

| Format | Lakehouse | Warehouse | Notes |
|--------|-----------|-----------|-------|
| CSV | ✅ | ✅ | Header detection, custom delimiters |
| JSON | ✅ | ✅ | Nested structure support |
| Parquet | ✅ | ✅ | Native columnar format |
| Avro | ✅ | ⚠️ | Schema evolution support |
| XML | ⚠️ | ⚠️ | Basic structure parsing |

### Write Modes

- **Overwrite**: Truncate and reload (both targets)
- **Append**: Add new records (both targets)
- **Merge**: Upsert based on merge keys (warehouse with custom SQL)

### Error Handling Strategies

- **Fail**: Stop processing on first error
- **Skip**: Continue processing, log errors
- **Log**: Process with error logging, continue execution

## Performance Considerations

### Lakehouse Optimization
- Partition by date columns for time-series data
- Use Delta optimizations (Z-ORDER, VACUUM)
- Enable schema evolution for changing data structures

### Warehouse Optimization
- Use COPY INTO for bulk loading efficiency
- Implement staging patterns for data validation
- Leverage columnstore indexes for analytical queries
- Configure appropriate distribution strategies

## Monitoring and Logging

All executions are tracked in the `log_flat_file_ingestion` table with:

- Execution timing and performance metrics
- Data quality statistics (records processed/failed)
- Error details and troubleshooting information
- File metadata (size, modification time)
- Lineage tracking with execution IDs

## Best Practices

1. **Target Selection**:
   - Use lakehouse for analytical workloads and schema evolution
   - Use warehouse for high-volume transactional loading

2. **Configuration Management**:
   - Group related files with execution_group for parallel processing
   - Use descriptive config_name values for operational clarity

3. **Error Handling**:
   - Start with "fail" strategy for new configurations
   - Move to "log" strategy for production stability

4. **Performance Tuning**:
   - Configure appropriate partition columns
   - Use staging tables for complex warehouse transformations
   - Monitor execution logs for optimization opportunities

## Troubleshooting

### Common Issues

1. **Schema Mismatch**: Check `custom_schema_json` configuration
2. **File Not Found**: Verify `source_file_path` relative to lakehouse Files/
3. **Permission Errors**: Ensure workspace/datastore IDs are correct
4. **Performance Issues**: Review partition strategy and file sizes

### Debug Mode

Enable detailed logging by setting error_handling_strategy to "log" for comprehensive troubleshooting information.
