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
    source_workspace_id, source_datastore_id, source_datastore_type, source_schema_name,
    target_workspace_id, target_datastore_id, target_datastore_type, target_file_root_path,
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
    NULL, NULL, 'lakehouse', 'default',  -- source: use defaults (sample lakehouse)
    NULL, NULL, 'lakehouse', 'Files',    -- target: use defaults (config lakehouse)
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
    source_workspace_id, source_datastore_id, source_datastore_type, source_schema_name,
    target_workspace_id, target_datastore_id, target_datastore_type, target_file_root_path,
    is_trigger_file, trigger_file_extension, is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_PRODUCTS_SNAPSHOT', 1, 'PRODUCT_SNAPSHOT',
    'extracts', 'snapshot/products', 'products',
    'yyyyMMdd', 'parquet',
    'parquet', 1,
    NULL, NULL, 'lakehouse', 'default',  -- source: use defaults (sample lakehouse)
    NULL, NULL, 'lakehouse', 'Files',    -- target: use defaults (config lakehouse)
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
    source_workspace_id, source_datastore_id, source_datastore_type, source_schema_name,
    target_workspace_id, target_datastore_id, target_datastore_type, target_file_root_path,
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
    NULL, NULL, 'lakehouse', 'default',  -- source: use defaults (sample lakehouse)
    NULL, NULL, 'lakehouse', 'Files',    -- target: use defaults (config lakehouse)
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
    source_workspace_id, source_datastore_id, source_datastore_type, source_schema_name,
    target_workspace_id, target_datastore_id, target_datastore_type, target_file_root_path,
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
    NULL, NULL, 'lakehouse', 'default',  -- source: use defaults (sample lakehouse)
    NULL, NULL, 'lakehouse', 'Files',    -- target: use defaults (config lakehouse)
    0, 1, 'SNAPPY', 'NORMAL', 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 5: TSV Format Table Extract
INSERT INTO {{ config_schema | default('config') }}.extract_details (
    extract_initiate_timestamp, extract_name, is_active, extract_table_name,
    extract_file_name_prefix, extract_file_name_folder, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    output_format,
    source_workspace_id, source_datastore_id, source_datastore_type, source_schema_name,
    target_workspace_id, target_datastore_id, target_datastore_type, target_file_root_path,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_INVENTORY_TSV', 1, 'INVENTORY',
    'extracts', 'tsv_files/inventory', 'inventory_snapshot',
    'yyyyMMdd_HHmmss', 'tsv',
    '\t', 1,
    'tsv',
    NULL, NULL, 'lakehouse', 'default',  -- source: use defaults (sample lakehouse)
    NULL, NULL, 'lakehouse', 'Files',    -- target: use defaults (config lakehouse)
    SYSTEM_USER, GETDATE()
);

-- Sample 6: DAT Format Table Extract with Pipe Delimiter
INSERT INTO {{ config_schema | default('config') }}.extract_details (
    extract_initiate_timestamp, extract_name, is_active, extract_table_name,
    extract_file_name_prefix, extract_file_name_folder, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    output_format,
    source_workspace_id, source_datastore_id, source_datastore_type, source_schema_name,
    target_workspace_id, target_datastore_id, target_datastore_type, target_file_root_path,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_PRODUCTS_DAT', 1, 'PRODUCTS',
    'extracts', 'dat_files/products', 'products_export',
    'yyyyMMdd', 'dat',
    '|', 1,
    'dat',
    NULL, NULL, 'lakehouse', 'default',  -- source: use defaults (sample lakehouse)
    NULL, NULL, 'lakehouse', 'Files',    -- target: use defaults (config lakehouse)
    SYSTEM_USER, GETDATE()
);