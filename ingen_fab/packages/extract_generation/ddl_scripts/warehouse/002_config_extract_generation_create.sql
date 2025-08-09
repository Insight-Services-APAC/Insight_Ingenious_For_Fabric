-- Create config_extract_generation table
-- Maps to legacy ConfigurationExtract table structure

CREATE TABLE [config].[config_extract_generation] (
    -- Core columns matching legacy structure
    creation_time DATETIME2(6) NOT NULL,
    is_active BIT NOT NULL,
    trigger_name VARCHAR(100),
    extract_name VARCHAR(100) NOT NULL,
    extract_pipeline_name VARCHAR(100),
    extract_sp_name VARCHAR(100),
    extract_sp_schema VARCHAR(100),
    extract_table_name VARCHAR(100),
    extract_table_schema VARCHAR(100),
    extract_view_name VARCHAR(100),
    extract_view_schema VARCHAR(100),
    validation_table_sp_name VARCHAR(100),
    validation_table_sp_schema VARCHAR(100),
    is_full_load BIT NOT NULL,
    
    -- Additional Fabric-specific columns
    workspace_id VARCHAR(100),
    lakehouse_id VARCHAR(100),
    warehouse_id VARCHAR(100),
    execution_group VARCHAR(50),
    
    -- Metadata columns
    created_by VARCHAR(100),
    created_timestamp DATETIME2(6),
    modified_by VARCHAR(100),
    modified_timestamp DATETIME2(6),
    
);
