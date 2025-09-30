# Extract Report Generation Package Creation - Warehouse & Lakehouse Versions

## Overview
The extract report generation package provides automated generation of flat file extracts from Fabric warehouse and lakehouse tables/views based on metadata configuration. This package is designed as a migration target for legacy systems and closely matches existing metadata structures for compatibility. The package supports various output formats (CSV, TSV, Parquet) with optional compression and encryption capabilities.

## Ask
- **Create** a new package for generating extract reports from warehouse/lakehouse
- **Design** to match legacy system metadata structure as closely as possible
- **Implement** support for views, stored procedures, and direct table extracts
- **Include** file generation features: compression, encryption, trigger files

## Package Architecture
- **Core Compiler**: `ingen_fab/packages/extract_generation/extract_generation.py` - Extends BaseNotebookCompiler
- **Templates**: `templates/extract_generation_notebook.py.jinja` - Main processing notebook template
- **DDL Scripts**: Warehouse-specific DDL with config and log table definitions matching legacy structure
- **Sample Data**: Example configurations for testing various extract scenarios

## Key Components
1. **ExtractGenerationCompiler**: Manages template compilation and DDL script generation
2. **ExtractProcessor**: Runtime processor using warehouse_utils abstractions for SQL execution
3. **Configuration Tables**: 
   - `config_extract_generation` - Main extract configuration (maps to ConfigurationExtract)
   - `config_extract_details` - File generation details (maps to ConfigurationExtractDetails)
4. **Logging Tables**: `log_extract_generation` tracks execution history

## LEGACY METADATA MAPPING

### ConfigurationExtract → config_extract_generation
```sql
-- Core extract configuration matching legacy structure
CREATE TABLE [config].[config_extract_generation] (
    creation_time DATETIME2(7),
    is_active BIT NOT NULL,
    trigger_name VARCHAR(100),
    extract_name VARCHAR(100) NOT NULL PRIMARY KEY,
    extract_pipeline_name VARCHAR(100),
    extract_sp_name VARCHAR(100),
    extract_sp_schema VARCHAR(100),
    extract_table_name VARCHAR(100),
    extract_table_schema VARCHAR(100),
    extract_view_name VARCHAR(100),
    extract_view_schema VARCHAR(100),
    validation_table_sp_name VARCHAR(100),
    validation_table_sp_schema VARCHAR(100),
    is_full_load BIT NOT NULL,
    -- Additional Fabric-specific columns
    workspace_id VARCHAR(100),
    lakehouse_id VARCHAR(100),
    warehouse_id VARCHAR(100),
    execution_group VARCHAR(50)
);
```

### ConfigurationExtractDetails → config_extract_details
```sql
-- File generation details matching legacy structure
CREATE TABLE [config].[config_extract_details] (
    creation_time DATETIME2(7),
    is_active BIT NOT NULL,
    extract_name VARCHAR(100) NOT NULL,
    file_generation_group VARCHAR(100),
    kv_azure_data_lake_url VARCHAR(1000),
    kv_azure_data_lake_secret_name VARCHAR(100),
    extract_container VARCHAR(100),
    extract_directory VARCHAR(100),
    extract_file_name VARCHAR(100),
    extract_file_name_timestamp_format VARCHAR(100),
    extract_file_name_period_end_day INT,
    extract_file_name_extension VARCHAR(100),
    extract_file_name_ordering INT,
    file_properties_column_delimiter VARCHAR(5),
    file_properties_row_delimiter VARCHAR(5),
    file_properties_encoding NVARCHAR(5),
    file_properties_quote_character NVARCHAR(1),
    file_properties_escape_character NVARCHAR(1),
    file_properties_header BIT NOT NULL,
    file_properties_null_value NVARCHAR(5),
    file_properties_max_rows_per_file INT,
    is_trigger_file BIT NOT NULL,
    trigger_file_extension VARCHAR(100),
    is_compressed BIT NOT NULL,
    compressed_type VARCHAR(50),
    compressed_level VARCHAR(50),
    compressed_is_compress_multiple_files BIT NOT NULL,
    compressed_is_delete_old_files BIT NOT NULL,
    compressed_file_name VARCHAR(100),
    compressed_timestamp_format VARCHAR(100),
    compressed_period_end_day INT,
    compressed_extension VARCHAR(100),
    compressed_name_ordering INT,
    compressed_is_trigger_file BIT NOT NULL,
    compressed_trigger_file_extension VARCHAR(100),
    is_encrypt_api BIT NOT NULL,
    encrypt_api_is_encrypt BIT NOT NULL,
    encrypt_api_is_create_trigger_file BIT NOT NULL,
    encrypt_api_trigger_file_extension VARCHAR(10),
    encrypt_api_kv_token_authorisation VARCHAR(100),
    encrypt_api_kv_azure_data_lake_connection_string VARCHAR(1000),
    encrypt_api_kv_azure_data_lake_account_name VARCHAR(100),
    is_validation_table BIT NOT NULL,
    is_validation_table_external_data_source VARCHAR(100),
    is_validation_table_external_file_format VARCHAR(100),
    compressed_extract_container VARCHAR(100),
    compressed_extract_directory VARCHAR(100),
    -- Additional Fabric-specific columns
    output_format VARCHAR(50), -- 'csv', 'tsv', 'parquet'
    fabric_lakehouse_path VARCHAR(500)
);
```

## FEATURE REQUIREMENTS

### Extract Sources
1. **Direct Table Extracts**: Read from specified table
2. **View-Based Extracts**: Execute view and export results
3. **Stored Procedure Extracts**: Execute SP and export results
4. **Custom SQL Extracts**: Execute arbitrary SQL and export

### File Generation Features
1. **Multiple Output Formats**: CSV, TSV, Parquet
2. **Compression Support**: ZIP, GZIP with configurable levels
3. **File Splitting**: Max rows per file configuration
4. **Dynamic Naming**: Timestamp formats, period end days
5. **Trigger Files**: Empty marker files for downstream processing
6. **Encryption**: Optional API-based encryption

### Runtime Processing
1. **Parallel Execution**: Group-based processing
2. **Incremental/Full Loads**: Based on is_full_load flag
3. **Validation**: Pre/post extract validation procedures
4. **Error Handling**: Comprehensive logging and retry logic

## END-TO-END TEST SCENARIOS

### 1. Simple Table Extract
```python
# Configuration for direct table extract to CSV
{
    "extract_name": "customers_daily",
    "extract_table_name": "customers",
    "extract_table_schema": "dbo",
    "is_full_load": True,
    "file_properties_header": True,
    "extract_file_name_extension": "csv"
}
```

### 2. View-Based Extract with Compression
```python
# Configuration for view extract with ZIP compression
{
    "extract_name": "sales_summary_monthly",
    "extract_view_name": "v_sales_summary",
    "extract_view_schema": "reporting",
    "is_compressed": True,
    "compressed_type": "ZIP",
    "compressed_level": "NORMAL"
}
```

### 3. Stored Procedure Extract with Trigger File
```python
# Configuration for SP extract with trigger file
{
    "extract_name": "financial_report",
    "extract_sp_name": "sp_generate_financial_report",
    "extract_sp_schema": "finance",
    "is_trigger_file": True,
    "trigger_file_extension": ".done"
}
```

## IMPLEMENTATION GUIDELINES

### Template Structure
```python
# Main processing logic in extract_generation_notebook.py.jinja
class ExtractProcessor:
    def __init__(self, warehouse_utils, ddl_utils):
        self.warehouse = warehouse_utils
        self.ddl = ddl_utils
    
    def process_extract(self, config, details):
        # Determine source type (table/view/sp)
        # Execute query/procedure
        # Apply file properties
        # Handle compression if needed
        # Generate trigger files
        # Log execution
```

### Abstraction Layer Usage
- Use `warehouse_utils` for all SQL operations
- Use `ddl_utils` for DDL operations
- Use Fabric APIs for file operations (no direct filesystem access)
- Handle both warehouse and lakehouse contexts

### Error Handling Strategy
1. Validate configuration before execution
2. Check source object existence
3. Handle SQL execution errors gracefully
4. Implement retry logic for transient failures
5. Log all errors with context

## TESTING CHECKLIST
- [ ] Package structure follows ingen_fab conventions
- [ ] DDL scripts create tables matching legacy structure
- [ ] Sample configurations cover all extract types
- [ ] Table extracts work correctly
- [ ] View extracts execute successfully
- [ ] Stored procedure extracts handle parameters
- [ ] File splitting works with max_rows_per_file
- [ ] Compression generates valid archives
- [ ] Trigger files are created when configured
- [ ] Timestamp formatting works correctly
- [ ] Parallel execution via execution_group
- [ ] Error scenarios handled appropriately
- [ ] Logging captures all relevant information

## MIGRATION CONSIDERATIONS
1. **Column Name Mapping**: Snake_case in Fabric vs PascalCase in legacy
2. **Data Type Compatibility**: Ensure proper type conversions
3. **Path Handling**: Fabric paths vs Azure Data Lake paths
4. **Authentication**: Fabric workspace context vs Key Vault secrets
5. **Performance**: Optimize for Fabric compute patterns

## PERFORMANCE OPTIMIZATION
- Use appropriate batch sizes for large extracts
- Implement parallel processing for multiple extracts
- Consider partitioning for very large tables
- Use efficient file formats (Parquet for analytics)
- Minimize data movement between compute and storage

## SECURITY REQUIREMENTS
- Respect Fabric workspace permissions
- Handle sensitive data appropriately
- Implement column-level masking if needed
- Audit all extract operations
- Secure file storage locations