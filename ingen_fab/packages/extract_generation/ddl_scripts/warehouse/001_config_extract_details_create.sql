-- Create config_extract_details table
-- Maps to legacy ConfigurationExtractDetails table structure

CREATE TABLE [config].[config_extract_details] (
    -- Core columns matching legacy structure
    creation_time DATETIME2(6) NOT NULL,
    is_active BIT NOT NULL,
    extract_name VARCHAR(100) NOT NULL,
    file_generation_group VARCHAR(100),
    
    -- Azure Data Lake settings
    kv_azure_data_lake_url VARCHAR(1000),
    kv_azure_data_lake_secret_name VARCHAR(100),
    extract_container VARCHAR(100),
    extract_directory VARCHAR(100),
    
    -- File naming configuration
    extract_file_name VARCHAR(100),
    extract_file_name_timestamp_format VARCHAR(100),
    extract_file_name_period_end_day INT,
    extract_file_name_extension VARCHAR(100),
    extract_file_name_ordering INT,
    
    -- File properties
    file_properties_column_delimiter VARCHAR(5),
    file_properties_row_delimiter VARCHAR(5),
    file_properties_encoding VARCHAR(50),
    file_properties_quote_character VARCHAR(1),
    file_properties_escape_character VARCHAR(1),
    file_properties_header BIT NOT NULL,
    file_properties_null_value VARCHAR(5),
    file_properties_max_rows_per_file INT,
    
    -- Trigger file settings
    is_trigger_file BIT NOT NULL,
    trigger_file_extension VARCHAR(100),
    
    -- Compression settings
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
    compressed_extract_container VARCHAR(100),
    compressed_extract_directory VARCHAR(100),
    
    -- Encryption API settings
    is_encrypt_api BIT NOT NULL,
    encrypt_api_is_encrypt BIT NOT NULL,
    encrypt_api_is_create_trigger_file BIT NOT NULL,
    encrypt_api_trigger_file_extension VARCHAR(10),
    encrypt_api_kv_token_authorisation VARCHAR(100),
    encrypt_api_kv_azure_data_lake_connection_string VARCHAR(1000),
    encrypt_api_kv_azure_data_lake_account_name VARCHAR(100),
    
    -- Validation settings
    is_validation_table BIT NOT NULL,
    is_validation_table_external_data_source VARCHAR(100),
    is_validation_table_external_file_format VARCHAR(100),
    
    -- Additional Fabric-specific columns
    output_format VARCHAR(50), -- 'csv', 'tsv', 'parquet'
    fabric_lakehouse_path VARCHAR(500),
    
    -- Metadata columns
    created_by VARCHAR(100),
    created_timestamp DATETIME2(6),
    modified_by VARCHAR(100),
    modified_timestamp DATETIME2(6),
    
);