-- Sample data for Extract Generation package - Aligned with Synthetic Retail OLTP Data
-- This script inserts sample configurations based on synthetic data files:
-- Tables: customers (snapshot), products (snapshot), orders (incremental), order_items (incremental)

-- Clear existing sample data
DELETE FROM [config].[config_extract_details] WHERE extract_name LIKE 'SAMPLE_%';
DELETE FROM [config].[config_extract_generation] WHERE extract_name LIKE 'SAMPLE_%';

-- Sample 1: Customers snapshot extract to Parquet
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_CUSTOMERS_SNAPSHOT', 1, 'customers', 'dbo', 
    1, 'SNAPSHOT_EXTRACTS', SYSTEM_USER, GETDATE()
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
    GETDATE(), 'SAMPLE_CUSTOMERS_SNAPSHOT', 1, 'CUSTOMER_SNAPSHOT',
    'extracts', 'snapshot/customers', 'customers',
    'yyyyMMdd', 'parquet',
    ',', '\n',
    'UTF-8', 1, 'parquet',
    0, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 2: Products snapshot extract to Parquet with trigger file
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_PRODUCTS_SNAPSHOT', 1, 'products', 'dbo', 
    1, 'SNAPSHOT_EXTRACTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    output_format, file_properties_header,
    is_trigger_file, trigger_file_extension, is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_PRODUCTS_SNAPSHOT', 1, 'PRODUCT_SNAPSHOT',
    'extracts', 'snapshot/products', 'products',
    'yyyyMMdd', 'parquet',
    'parquet', 1,
    1, '.done', 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 3: Orders incremental extract with Snappy compression
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDERS_INCREMENTAL', 1, 'orders', 'dbo', 
    0, 'INCREMENTAL_EXTRACTS', SYSTEM_USER, GETDATE()
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
    GETDATE(), 'SAMPLE_ORDERS_INCREMENTAL', 1, 'ORDER_INCREMENTAL',
    'extracts', 'incremental/orders', 'orders',
    'yyyyMMdd', 'parquet',
    ',', 1,
    1, 'SNAPPY', 'NORMAL',
    'parquet',
    0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 4: Order items incremental extract with Snappy compression
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema,
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDER_ITEMS_INCREMENTAL', 1, 'order_items', 'dbo',
    0, 'INCREMENTAL_EXTRACTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    file_properties_max_rows_per_file, output_format,
    is_trigger_file, is_compressed, compressed_type, compressed_level, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDER_ITEMS_INCREMENTAL', 1, 'ORDER_ITEMS_INCREMENTAL',
    'extracts', 'incremental/order_items', 'order_items',
    'yyyyMMdd', 'parquet',
    ',', 1,
    NULL, 'parquet',
    0, 1, 'SNAPPY', 'NORMAL', 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);