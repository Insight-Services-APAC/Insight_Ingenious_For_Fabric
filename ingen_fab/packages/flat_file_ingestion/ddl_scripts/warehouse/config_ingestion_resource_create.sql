-- Config table for ingestion resource configuration - Warehouse version
-- Stores ResourceConfig objects with polymorphic source parameters
-- MapType fields are stored as JSON strings in NVARCHAR(MAX)
-- Primary Key: config_id
CREATE TABLE config.config_ingestion_resource (
    -- Identity
    config_id NVARCHAR(100) NOT NULL,
    resource_name NVARCHAR(100) NOT NULL,
    source_name NVARCHAR(100) NOT NULL,
    -- Source configuration
    source_type NVARCHAR(50) NOT NULL, -- 'filesystem', 'api', 'database'
    connection_params NVARCHAR(MAX) NULL, -- JSON map: workspace_name, lakehouse_name, etc.
    -- Authentication
    auth_type NVARCHAR(50) NULL,
    auth_params NVARCHAR(MAX) NULL, -- JSON map: token_key_vault, etc.
    -- Source description
    source_description NVARCHAR(500) NULL,
    -- Extraction settings (for future extraction framework)
    extraction_output_path NVARCHAR(500) NULL,
    extraction_params NVARCHAR(MAX) NULL, -- JSON map
    -- File loading settings
    source_file_path NVARCHAR(500) NULL,
    source_file_format NVARCHAR(50) NULL,
    -- POLYMORPHIC PARAMETERS (different per source_type!)
    loading_params NVARCHAR(MAX) NULL, -- JSON map: import_pattern, discovery_pattern, etc.
    -- Target settings
    target_workspace_name NVARCHAR(100) NULL,
    target_datastore_name NVARCHAR(100) NULL,
    target_datastore_type NVARCHAR(50) NULL, -- 'lakehouse', 'warehouse'
    target_schema_name NVARCHAR(100) NULL,
    target_table_name NVARCHAR(128) NULL,
    -- Write settings
    write_mode NVARCHAR(50) NULL, -- 'overwrite', 'append', 'merge'
    merge_keys NVARCHAR(500) NULL, -- JSON array
    partition_columns NVARCHAR(500) NULL, -- JSON array
    enable_schema_evolution BIT NULL,
    -- Data validation
    custom_schema_json NVARCHAR(MAX) NULL,
    data_validation_rules NVARCHAR(MAX) NULL,
    -- Execution control
    execution_group INT NULL,
    active_yn NVARCHAR(1) NULL,
    -- Metadata
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    created_by NVARCHAR(100) NULL,
    updated_by NVARCHAR(100) NULL,
    CONSTRAINT PK_config_ingestion_resource PRIMARY KEY (config_id)
);
