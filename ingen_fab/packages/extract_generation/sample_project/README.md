# Extract Generation Sample Project

This sample project demonstrates the Extract Generation package capabilities for creating automated file extracts from Fabric warehouse and lakehouse tables.

## Sample Configurations

The sample data includes various extract scenarios:

1. **SAMPLE_CUSTOMERS_DAILY** - Simple table extract to CSV
   - Direct table extract from `dbo.customers`
   - Daily execution with timestamp in filename
   - Standard CSV format with headers

2. **SAMPLE_SALES_SUMMARY_MONTHLY** - View extract with compression
   - Extract from `reporting.v_sales_summary` view
   - ZIP compression with NORMAL level
   - Monthly execution schedule

3. **SAMPLE_FINANCIAL_REPORT** - Stored procedure extract with trigger file
   - Execute `finance.sp_generate_financial_report` procedure
   - Generate Parquet format output
   - Create `.done` trigger file after completion

4. **SAMPLE_TRANSACTIONS_EXPORT** - Large table with file splitting
   - Extract from `dbo.transactions` table
   - Split into 500K row chunks
   - GZIP compression with MAXIMUM level
   - TSV format output

5. **SAMPLE_ORDERS_INCREMENTAL** - Incremental extract with validation
   - Incremental load from `dbo.orders`
   - Pre-extraction validation via stored procedure
   - CSV format with quote handling

## Testing the Package

1. **Compile DDL Scripts**:
   ```bash
   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse --package-name extract_generation
   ```

2. **Deploy Configuration**:
   - Run the DDL scripts to create schemas and tables
   - Run `sample_data_insert.sql` to load sample configurations

3. **Generate Extract Notebook**:
   ```python
   from ingen_fab.packages.extract_generation import ExtractGenerationCompiler
   
   compiler = ExtractGenerationCompiler()
   notebook_path = compiler.compile_notebook()
   ```

4. **Execute Extracts**:
   - Run the generated notebook with parameters:
     - `extract_name`: One of the SAMPLE_* configurations
     - `execution_group`: Optional group filter
     - `environment`: Target environment
     - `run_type`: FULL or INCREMENTAL

## File Output Structure

Extracts are organized in the following structure:
```
extracts/
├── customers/
│   └── customers_20240725_142530.csv
├── sales/monthly/
│   └── sales_summary_archive.zip
├── finance/reports/
│   ├── financial_report_20240725.parquet
│   └── financial_report_20240725.done
└── transactions/daily/
    ├── transactions_20240725_142530_part0001.tsv.gz
    ├── transactions_20240725_142530_part0002.tsv.gz
    └── transactions_20240725_142530_part0003.tsv.gz
```

## Configuration Details

### Main Configuration (config_extract_generation)
- Defines the source object (table/view/stored procedure)
- Sets execution parameters (full/incremental, execution group)
- Links to Fabric workspace and warehouse/lakehouse

### File Details (config_extract_details)
- Specifies output format and file properties
- Configures compression settings
- Defines file naming patterns and paths
- Sets trigger file generation

### Logging (log_extract_generation)
- Tracks all extract executions
- Records row counts and file sizes
- Captures errors and performance metrics
- Provides audit trail for compliance

## Best Practices

1. **Performance**: 
   - Use appropriate `file_properties_max_rows_per_file` for large extracts
   - Consider Parquet format for analytical workloads
   - Enable compression for network transfer optimization

2. **Reliability**:
   - Always enable logging for troubleshooting
   - Use validation procedures for data quality checks
   - Implement trigger files for downstream dependencies

3. **Security**:
   - Leverage Fabric workspace permissions
   - Use secure paths for sensitive data
   - Consider encryption for compliance requirements