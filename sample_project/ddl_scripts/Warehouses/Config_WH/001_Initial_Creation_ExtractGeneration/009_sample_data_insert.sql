-- Sample data for Extract Generation package
-- This script inserts sample configurations for testing various extract scenarios
-- NOTE: For synthetic data compatibility, use 009_sample_data_insert_aligned.sql instead

-- Clear existing sample data
DELETE FROM [config].[config_extract_details] WHERE extract_name LIKE 'SAMPLE_%';
DELETE FROM [config].[config_extract_generation] WHERE extract_name LIKE 'SAMPLE_%';

-- Sample 1: Simple table extract to CSV
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_CUSTOMERS_DAILY', 1, 'customers', 'dbo', 
    1, 'DAILY_EXTRACTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_row_delimiter,
    file_properties_encoding, file_properties_header, output_format,
    is_trigger_file, is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_CUSTOMERS_DAILY', 1, 'CUSTOMER_DATA',
    'extracts', 'customers', 'customers',
    'yyyyMMdd_HHmmss', 'csv',
    ',', '\n',
    'UTF-8', 1, 'csv',
    0, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 2: View extract with ZIP compression
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_view_name, extract_view_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_SALES_SUMMARY_MONTHLY', 1, 'v_sales_summary', 'reporting', 
    1, 'MONTHLY_REPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    is_compressed, compressed_type, compressed_level,
    compressed_file_name, compressed_extension, output_format,
    is_trigger_file, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_SALES_SUMMARY_MONTHLY', 1, 'SALES_REPORTS',
    'extracts', 'sales/monthly', 'sales_summary',
    'yyyyMM', 'csv',
    ',', 1,
    1, 'ZIP', 'NORMAL',
    'sales_summary_archive', '.zip', 'csv',
    0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 3: Stored procedure extract with trigger file  
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_sp_name, extract_sp_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_FINANCIAL_REPORT', 1, 'sp_generate_financial_report', 'finance', 
    0, 'FINANCIAL_REPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    is_trigger_file, trigger_file_extension,
    output_format, file_properties_max_rows_per_file,
    is_compressed, is_encrypt_api, is_validation_table,
    file_properties_header,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_FINANCIAL_REPORT', 1, 'FINANCE',
    'extracts', 'finance/reports', 'financial_report',
    'yyyyMMdd', 'parquet',
    1, '.done',
    'parquet', 1000000,
    0, 0, 0,
    1,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 4: Large table extract with file splitting and GZIP compression
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_TRANSACTIONS_EXPORT', 1, 'transactions', 'dbo', 
    1, 'LARGE_EXPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    file_properties_max_rows_per_file,
    is_compressed, compressed_type, compressed_level,
    compressed_is_compress_multiple_files, output_format,
    is_trigger_file, is_encrypt_api, is_validation_table,
    compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_TRANSACTIONS_EXPORT', 1, 'TRANSACTIONS',
    'extracts', 'transactions/daily', 'transactions',
    'yyyyMMdd_HHmmss', 'tsv',
    '\t', 1,
    500000,
    1, 'GZIP', 'MAXIMUM',
    1, 'tsv',
    0, 0, 0,
    1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 5: Incremental extract with validation
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema,
    validation_table_sp_name, validation_table_sp_schema,
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDERS_INCREMENTAL', 1, 'orders', 'dbo',
    'sp_validate_orders_extract', 'dbo',
    0, 'INCREMENTAL_EXTRACTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_quote_character,
    file_properties_escape_character, file_properties_header,
    file_properties_null_value, output_format,
    is_validation_table, is_trigger_file, is_compressed, is_encrypt_api,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDERS_INCREMENTAL', 1, 'ORDERS',
    'extracts', 'orders/incremental', 'orders_delta',
    'yyyyMMdd_HHmmss', 'csv',
    ',', '"',
    '|', 1,
    'NULL', 'csv',
    1, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

