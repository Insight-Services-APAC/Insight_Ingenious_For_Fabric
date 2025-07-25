-- Sample data for Extract Generation package
-- This script inserts sample configurations for testing various extract scenarios

-- Clear existing sample data
DELETE FROM [config].[config_extract_details] WHERE extract_name LIKE 'SAMPLE_%';
DELETE FROM [config].[config_extract_generation] WHERE extract_name LIKE 'SAMPLE_%';

-- Sample 1: Simple table extract to CSV
INSERT INTO [config].[config_extract_generation] (
    extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group
) VALUES (
    'SAMPLE_CUSTOMERS_DAILY', 1, 'customers', 'dbo', 
    1, 'DAILY_EXTRACTS'
);

INSERT INTO [config].[config_extract_details] (
    extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_row_delimiter,
    file_properties_encoding, file_properties_header, output_format
) VALUES (
    'SAMPLE_CUSTOMERS_DAILY', 1, 'CUSTOMER_DATA',
    'extracts', 'customers', 'customers',
    'yyyyMMdd_HHmmss', 'csv',
    ',', '\n',
    'UTF-8', 1, 'csv'
);

-- Sample 2: View extract with ZIP compression
INSERT INTO [config].[config_extract_generation] (
    extract_name, is_active, extract_view_name, extract_view_schema, 
    is_full_load, execution_group
) VALUES (
    'SAMPLE_SALES_SUMMARY_MONTHLY', 1, 'v_sales_summary', 'reporting', 
    1, 'MONTHLY_REPORTS'
);

INSERT INTO [config].[config_extract_details] (
    extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    is_compressed, compressed_type, compressed_level,
    compressed_file_name, compressed_extension, output_format
) VALUES (
    'SAMPLE_SALES_SUMMARY_MONTHLY', 1, 'SALES_REPORTS',
    'extracts', 'sales/monthly', 'sales_summary',
    'yyyyMM', 'csv',
    ',', 1,
    1, 'ZIP', 'NORMAL',
    'sales_summary_archive', '.zip', 'csv'
);

-- Sample 3: Stored procedure extract with trigger file
INSERT INTO [config].[config_extract_generation] (
    extract_name, is_active, extract_sp_name, extract_sp_schema, 
    is_full_load, execution_group
) VALUES (
    'SAMPLE_FINANCIAL_REPORT', 1, 'sp_generate_financial_report', 'finance', 
    0, 'FINANCIAL_REPORTS'
);

INSERT INTO [config].[config_extract_details] (
    extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    is_trigger_file, trigger_file_extension,
    output_format, file_properties_max_rows_per_file
) VALUES (
    'SAMPLE_FINANCIAL_REPORT', 1, 'FINANCE',
    'extracts', 'finance/reports', 'financial_report',
    'yyyyMMdd', 'parquet',
    1, '.done',
    'parquet', 1000000
);

-- Sample 4: Large table extract with file splitting and GZIP compression
INSERT INTO [config].[config_extract_generation] (
    extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group
) VALUES (
    'SAMPLE_TRANSACTIONS_EXPORT', 1, 'transactions', 'dbo', 
    1, 'LARGE_EXPORTS'
);

INSERT INTO [config].[config_extract_details] (
    extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    file_properties_max_rows_per_file,
    is_compressed, compressed_type, compressed_level,
    compressed_is_compress_multiple_files, output_format
) VALUES (
    'SAMPLE_TRANSACTIONS_EXPORT', 1, 'TRANSACTIONS',
    'extracts', 'transactions/daily', 'transactions',
    'yyyyMMdd_HHmmss', 'tsv',
    '\t', 1,
    500000,
    1, 'GZIP', 'MAXIMUM',
    1, 'tsv'
);

-- Sample 5: Incremental extract with validation
INSERT INTO [config].[config_extract_generation] (
    extract_name, is_active, extract_table_name, extract_table_schema,
    validation_table_sp_name, validation_table_sp_schema,
    is_full_load, execution_group
) VALUES (
    'SAMPLE_ORDERS_INCREMENTAL', 1, 'orders', 'dbo',
    'sp_validate_orders_extract', 'dbo',
    0, 'INCREMENTAL_EXTRACTS'
);

INSERT INTO [config].[config_extract_details] (
    extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_quote_character,
    file_properties_escape_character, file_properties_header,
    file_properties_null_value, output_format,
    is_validation_table
) VALUES (
    'SAMPLE_ORDERS_INCREMENTAL', 1, 'ORDERS',
    'extracts', 'orders/incremental', 'orders_delta',
    'yyyyMMdd_HHmmss', 'csv',
    ',', '"',
    '\\', 1,
    'NULL', 'csv',
    1
);

-- Display inserted sample data
SELECT 
    g.extract_name,
    g.is_active,
    COALESCE(g.extract_table_name, g.extract_view_name, g.extract_sp_name) as source_object,
    COALESCE(g.extract_table_schema, g.extract_view_schema, g.extract_sp_schema) as source_schema,
    CASE 
        WHEN g.extract_table_name IS NOT NULL THEN 'TABLE'
        WHEN g.extract_view_name IS NOT NULL THEN 'VIEW'
        WHEN g.extract_sp_name IS NOT NULL THEN 'STORED_PROCEDURE'
    END as source_type,
    g.is_full_load,
    g.execution_group,
    d.output_format,
    d.is_compressed,
    d.is_trigger_file
FROM [config].[config_extract_generation] g
LEFT JOIN [config].[config_extract_details] d ON g.extract_name = d.extract_name
WHERE g.extract_name LIKE 'SAMPLE_%'
ORDER BY g.extract_name;