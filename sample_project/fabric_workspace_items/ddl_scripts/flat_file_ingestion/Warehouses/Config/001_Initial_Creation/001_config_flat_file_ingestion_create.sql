-- Configuration table for flat file ingestion metadata - Warehouse version
CREATE TABLE config_flat_file_ingestion (
    config_id NVARCHAR(50) NOT NULL,
    config_name NVARCHAR(255) NOT NULL,
    source_file_path NVARCHAR(500) NOT NULL,
    source_file_format NVARCHAR(50) NOT NULL, -- csv, json, parquet, avro, xml
    target_lakehouse_workspace_id NVARCHAR(50) NOT NULL,
    target_lakehouse_id NVARCHAR(50) NOT NULL,
    target_schema_name NVARCHAR(128) NOT NULL,
    target_table_name NVARCHAR(128) NOT NULL,
    file_delimiter NVARCHAR(10) NULL, -- for CSV files
    has_header BIT NULL, -- for CSV files
    encoding NVARCHAR(50) NULL, -- utf-8, latin-1, etc.
    date_format NVARCHAR(50) NULL, -- for date columns
    timestamp_format NVARCHAR(50) NULL, -- for timestamp columns
    schema_inference BIT NOT NULL, -- whether to infer schema
    custom_schema_json NVARCHAR(MAX) NULL, -- custom schema definition
    partition_columns NVARCHAR(500) NULL, -- comma-separated list
    sort_columns NVARCHAR(500) NULL, -- comma-separated list
    write_mode NVARCHAR(50) NOT NULL, -- overwrite, append, merge
    merge_keys NVARCHAR(500) NULL, -- for merge operations
    data_validation_rules NVARCHAR(MAX) NULL, -- JSON validation rules
    error_handling_strategy NVARCHAR(50) NOT NULL, -- fail, skip, log
    execution_group INT NOT NULL,
    active_yn NVARCHAR(1) NOT NULL,
    created_date DATETIME2 NOT NULL,
    modified_date DATETIME2 NULL,
    created_by NVARCHAR(100) NOT NULL,
    modified_by NVARCHAR(100) NULL,
    CONSTRAINT PK_config_flat_file_ingestion PRIMARY KEY (config_id),
    CONSTRAINT CHK_active_yn CHECK (active_yn IN ('Y', 'N')),
    CONSTRAINT CHK_source_file_format CHECK (source_file_format IN ('csv', 'json', 'parquet', 'avro', 'xml')),
    CONSTRAINT CHK_write_mode CHECK (write_mode IN ('overwrite', 'append', 'merge')),
    CONSTRAINT CHK_error_handling_strategy CHECK (error_handling_strategy IN ('fail', 'skip', 'log'))
);