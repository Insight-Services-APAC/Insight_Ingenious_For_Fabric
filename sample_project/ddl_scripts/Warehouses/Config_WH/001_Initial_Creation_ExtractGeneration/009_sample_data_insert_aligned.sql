-- Sample data for Extract Generation package - Aligned with Synthetic Data Tables
-- This script inserts sample configurations that work with the actual synthetic data schema

-- Clear existing sample data
DELETE FROM [config].[config_extract_details] WHERE extract_name LIKE 'SAMPLE_%';
DELETE FROM [config].[config_extract_generation] WHERE extract_name LIKE 'SAMPLE_%';

-- Sample 1: Customers table extract to CSV (using actual synthetic data table)
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
    'extracts', 'customers', 'customers_full',
    'yyyyMMdd_HHmmss', 'csv',
    ',', '\n',
    'UTF-8', 1, 'csv',
    0, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 2: Products extract to Parquet
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_PRODUCTS_CATALOG', 1, 'products', 'dbo', 
    1, 'CATALOG_EXPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    output_format, file_properties_header,
    is_trigger_file, is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_PRODUCTS_CATALOG', 1, 'CATALOG',
    'extracts', 'products', 'product_catalog',
    'yyyyMMdd', 'parquet',
    'parquet', 1,
    1, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 3: Orders extract with GZIP compression
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDERS_EXPORT', 1, 'orders', 'dbo', 
    1, 'ORDER_EXPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    is_compressed, compressed_type, compressed_level,
    output_format,
    is_trigger_file, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDERS_EXPORT', 1, 'ORDERS',
    'extracts', 'orders/daily', 'orders',
    'yyyyMMdd_HHmmss', 'csv',
    ',', 1,
    1, 'GZIP', 'NORMAL',
    'csv',
    0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 4: Sales Summary View extract with ZIP compression
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

-- Sample 5: Order Items with file splitting
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema,
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDER_ITEMS_EXPORT', 1, 'order_items', 'dbo',
    1, 'LARGE_EXPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    file_properties_max_rows_per_file, output_format,
    is_trigger_file, is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDER_ITEMS_EXPORT', 1, 'ORDER_ITEMS',
    'extracts', 'order_items/daily', 'order_items',
    'yyyyMMdd_HHmmss', 'tsv',
    '\t', 1,
    10000, 'tsv',
    0, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 6: Financial Report using stored procedure
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
    output_format, file_properties_header,
    is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_FINANCIAL_REPORT', 1, 'FINANCE',
    'extracts', 'finance/reports', 'financial_report',
    'yyyyMMdd', 'parquet',
    1, '.done',
    'parquet', 1,
    0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 7: Customer Segments Report  
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_sp_name, extract_sp_schema,
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_CUSTOMER_SEGMENTS', 1, 'sp_generate_customer_segment_report', 'reporting',
    1, 'CUSTOMER_REPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    output_format,
    is_trigger_file, is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_CUSTOMER_SEGMENTS', 1, 'CUSTOMER_ANALYTICS',
    'extracts', 'customers/analytics', 'customer_segments',
    'yyyyMMdd', 'csv',
    ',', 1,
    'csv',
    0, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 8: Product Sales Summary View
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_view_name, extract_view_schema,
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_PRODUCT_SALES', 1, 'v_product_sales_summary', 'reporting',
    1, 'PRODUCT_REPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_quote_character,
    file_properties_header, output_format,
    is_trigger_file, is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_PRODUCT_SALES', 1, 'PRODUCT_ANALYTICS',
    'extracts', 'products/analytics', 'product_sales_summary',
    'yyyyMMdd', 'csv',
    '|', '"',
    1, 'csv',
    0, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 9: Incremental Orders Extract with Validation
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
    '\\', 1,
    'NULL', 'csv',
    1, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 10: Customers Mapped View (using compatibility view)
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_view_name, extract_view_schema,
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_CUSTOMERS_MAPPED', 1, 'v_customers_mapped', 'dbo',
    1, 'COMPATIBILITY_EXPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    output_format,
    is_trigger_file, is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_CUSTOMERS_MAPPED', 1, 'CUSTOMER_DATA',
    'extracts', 'customers/legacy', 'customers_legacy_format',
    'yyyyMMdd', 'csv',
    ',', 1,
    'csv',
    0, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);