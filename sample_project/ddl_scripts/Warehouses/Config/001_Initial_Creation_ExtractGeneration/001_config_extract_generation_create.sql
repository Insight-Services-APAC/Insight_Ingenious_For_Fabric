-- Create config_extract_generation table
-- Maps to legacy ConfigurationExtract table structure

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'config' AND TABLE_NAME = 'config_extract_generation')
BEGIN
    CREATE TABLE [config].[config_extract_generation] (
        -- Core columns matching legacy structure
        creation_time DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        is_active BIT NOT NULL DEFAULT 1,
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
        is_full_load BIT NOT NULL DEFAULT 1,
        
        -- Additional Fabric-specific columns
        workspace_id VARCHAR(100),
        lakehouse_id VARCHAR(100),
        warehouse_id VARCHAR(100),
        execution_group VARCHAR(50),
        
        -- Metadata columns
        created_by VARCHAR(100) DEFAULT SYSTEM_USER,
        created_timestamp DATETIME2(7) DEFAULT GETUTCDATE(),
        modified_by VARCHAR(100) DEFAULT SYSTEM_USER,
        modified_timestamp DATETIME2(7) DEFAULT GETUTCDATE(),
        
        CONSTRAINT PK_config_extract_generation PRIMARY KEY (extract_name)
    );
    
    -- Create indexes for performance
    CREATE INDEX IX_config_extract_generation_active ON [config].[config_extract_generation] (is_active);
    CREATE INDEX IX_config_extract_generation_group ON [config].[config_extract_generation] (execution_group);
    
    -- Add check constraints
    ALTER TABLE [config].[config_extract_generation] 
    ADD CONSTRAINT CHK_extract_source CHECK (
        (extract_table_name IS NOT NULL AND extract_table_schema IS NOT NULL) OR
        (extract_view_name IS NOT NULL AND extract_view_schema IS NOT NULL) OR
        (extract_sp_name IS NOT NULL AND extract_sp_schema IS NOT NULL)
    );
END;