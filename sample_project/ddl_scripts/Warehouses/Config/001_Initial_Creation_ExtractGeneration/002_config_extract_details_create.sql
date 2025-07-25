-- Create config_extract_details table
-- Maps to legacy ConfigurationExtractDetails table structure

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'config' AND TABLE_NAME = 'config_extract_details')
BEGIN
    CREATE TABLE [config].[config_extract_details] (
        -- Core columns matching legacy structure
        creation_time DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        is_active BIT NOT NULL DEFAULT 1,
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
        file_properties_encoding NVARCHAR(50),
        file_properties_quote_character NVARCHAR(1),
        file_properties_escape_character NVARCHAR(1),
        file_properties_header BIT NOT NULL DEFAULT 1,
        file_properties_null_value NVARCHAR(5),
        file_properties_max_rows_per_file INT,
        
        -- Trigger file settings
        is_trigger_file BIT NOT NULL DEFAULT 0,
        trigger_file_extension VARCHAR(100),
        
        -- Compression settings
        is_compressed BIT NOT NULL DEFAULT 0,
        compressed_type VARCHAR(50),
        compressed_level VARCHAR(50),
        compressed_is_compress_multiple_files BIT NOT NULL DEFAULT 0,
        compressed_is_delete_old_files BIT NOT NULL DEFAULT 1,
        compressed_file_name VARCHAR(100),
        compressed_timestamp_format VARCHAR(100),
        compressed_period_end_day INT,
        compressed_extension VARCHAR(100),
        compressed_name_ordering INT,
        compressed_is_trigger_file BIT NOT NULL DEFAULT 0,
        compressed_trigger_file_extension VARCHAR(100),
        compressed_extract_container VARCHAR(100),
        compressed_extract_directory VARCHAR(100),
        
        -- Encryption API settings
        is_encrypt_api BIT NOT NULL DEFAULT 0,
        encrypt_api_is_encrypt BIT NOT NULL DEFAULT 0,
        encrypt_api_is_create_trigger_file BIT NOT NULL DEFAULT 0,
        encrypt_api_trigger_file_extension VARCHAR(10),
        encrypt_api_kv_token_authorisation VARCHAR(100),
        encrypt_api_kv_azure_data_lake_connection_string VARCHAR(1000),
        encrypt_api_kv_azure_data_lake_account_name VARCHAR(100),
        
        -- Validation settings
        is_validation_table BIT NOT NULL DEFAULT 0,
        is_validation_table_external_data_source VARCHAR(100),
        is_validation_table_external_file_format VARCHAR(100),
        
        -- Additional Fabric-specific columns
        output_format VARCHAR(50) DEFAULT 'csv', -- 'csv', 'tsv', 'parquet'
        fabric_lakehouse_path VARCHAR(500),
        
        -- Metadata columns
        created_by VARCHAR(100) DEFAULT SYSTEM_USER,
        created_timestamp DATETIME2(7) DEFAULT GETUTCDATE(),
        modified_by VARCHAR(100) DEFAULT SYSTEM_USER,
        modified_timestamp DATETIME2(7) DEFAULT GETUTCDATE(),
        
        CONSTRAINT PK_config_extract_details PRIMARY KEY (extract_name),
        CONSTRAINT FK_config_extract_details_generation 
            FOREIGN KEY (extract_name) REFERENCES [config].[config_extract_generation](extract_name)
    );
    
    -- Create indexes for performance
    CREATE INDEX IX_config_extract_details_active ON [config].[config_extract_details] (is_active);
    CREATE INDEX IX_config_extract_details_group ON [config].[config_extract_details] (file_generation_group);
    
    -- Add check constraints
    ALTER TABLE [config].[config_extract_details] 
    ADD CONSTRAINT CHK_output_format CHECK (output_format IN ('csv', 'tsv', 'parquet'));
    
    ALTER TABLE [config].[config_extract_details] 
    ADD CONSTRAINT CHK_compressed_type CHECK (compressed_type IS NULL OR compressed_type IN ('ZIP', 'GZIP', 'BZIP2'));
    
    ALTER TABLE [config].[config_extract_details] 
    ADD CONSTRAINT CHK_compressed_level CHECK (compressed_level IS NULL OR compressed_level IN ('MINIMUM', 'NORMAL', 'MAXIMUM'));
END;