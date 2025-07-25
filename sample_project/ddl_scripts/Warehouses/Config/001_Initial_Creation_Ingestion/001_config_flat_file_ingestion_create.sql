-- Configuration table for flat file ingestion metadata - Universal schema (Warehouse version)

DROP TABLE IF EXISTS config.config_flat_file_ingestion;

CREATE TABLE config.config_flat_file_ingestion (
    config_id NVARCHAR(50) NOT NULL,
    config_name NVARCHAR(200) NOT NULL,
    source_file_path NVARCHAR(500) NOT NULL,
    source_file_format NVARCHAR(20) NOT NULL, -- csv, json, parquet, avro, xml
    target_workspace_id NVARCHAR(50) NOT NULL, -- Universal field for workspace
    target_datastore_id NVARCHAR(50) NOT NULL, -- Universal field for lakehouse/warehouse
    target_datastore_type NVARCHAR(20) NOT NULL, -- 'lakehouse' or 'warehouse'
    target_schema_name NVARCHAR(50) NOT NULL,
    target_table_name NVARCHAR(100) NOT NULL,
    staging_table_name NVARCHAR(100) NULL, -- For warehouse COPY INTO staging
    file_delimiter NVARCHAR(5) NULL, -- for CSV files
    has_header BIT NULL, -- for CSV files
    encoding NVARCHAR(20) NULL, -- utf-8, latin-1, etc.
    date_format NVARCHAR(50) NULL, -- for date columns
    timestamp_format NVARCHAR(50) NULL, -- for timestamp columns
    schema_inference BIT NOT NULL, -- whether to infer schema
    custom_schema_json NVARCHAR(MAX) NULL, -- custom schema definition
    partition_columns NVARCHAR(500) NULL, -- comma-separated list
    sort_columns NVARCHAR(500) NULL, -- comma-separated list
    write_mode NVARCHAR(20) NOT NULL, -- overwrite, append, merge
    merge_keys NVARCHAR(500) NULL, -- for merge operations
    data_validation_rules NVARCHAR(MAX) NULL, -- JSON validation rules
    error_handling_strategy NVARCHAR(20) NOT NULL, -- fail, skip, log
    execution_group INT NOT NULL,
    active_yn NVARCHAR(1) NOT NULL,
    created_date NVARCHAR(50) NOT NULL,
    modified_date NVARCHAR(50) NULL,
    created_by NVARCHAR(100) NOT NULL,
    modified_by NVARCHAR(100) NULL,
    
    CONSTRAINT PK_config_flat_file_ingestion PRIMARY KEY (config_id)
);