-- Create log_extract_generation table
-- Tracks execution history of extract generation runs

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'log' AND TABLE_NAME = 'log_extract_generation')
BEGIN
    CREATE TABLE [log].[log_extract_generation] (
        -- Primary key
        log_id BIGINT IDENTITY(1,1) NOT NULL,
        
        -- Extract identification
        extract_name VARCHAR(100) NOT NULL,
        execution_group VARCHAR(50),
        
        -- Run information
        run_id VARCHAR(100) NOT NULL,
        run_timestamp DATETIME2(7) NOT NULL DEFAULT GETUTCDATE(),
        run_status VARCHAR(50) NOT NULL,
        run_type VARCHAR(50), -- 'FULL', 'INCREMENTAL'
        
        -- Source information
        source_type VARCHAR(50), -- 'TABLE', 'VIEW', 'STORED_PROCEDURE'
        source_object VARCHAR(200),
        source_schema VARCHAR(100),
        
        -- File generation details
        output_format VARCHAR(50),
        output_file_path VARCHAR(1000),
        output_file_name VARCHAR(500),
        output_file_size_bytes BIGINT,
        
        -- Record counts
        rows_extracted BIGINT,
        rows_written BIGINT,
        files_generated INT,
        
        -- Compression details
        is_compressed BIT,
        compressed_file_path VARCHAR(1000),
        compressed_file_name VARCHAR(500),
        compressed_file_size_bytes BIGINT,
        compression_type VARCHAR(50),
        
        -- Trigger file details
        trigger_file_created BIT,
        trigger_file_path VARCHAR(1000),
        
        -- Timing information
        start_time DATETIME2(7),
        end_time DATETIME2(7),
        duration_seconds INT,
        
        -- Error handling
        error_message NVARCHAR(MAX),
        error_details NVARCHAR(MAX),
        
        -- Fabric specific
        workspace_id VARCHAR(100),
        lakehouse_id VARCHAR(100),
        warehouse_id VARCHAR(100),
        notebook_run_id VARCHAR(100),
        
        -- Metadata
        created_by VARCHAR(100) DEFAULT SYSTEM_USER,
        created_timestamp DATETIME2(7) DEFAULT GETUTCDATE(),
        
        CONSTRAINT PK_log_extract_generation PRIMARY KEY (log_id)
    );
    
    -- Create indexes for performance
    CREATE INDEX IX_log_extract_generation_extract ON [log].[log_extract_generation] (extract_name, run_timestamp DESC);
    CREATE INDEX IX_log_extract_generation_run ON [log].[log_extract_generation] (run_id);
    CREATE INDEX IX_log_extract_generation_status ON [log].[log_extract_generation] (run_status, run_timestamp DESC);
    CREATE INDEX IX_log_extract_generation_timestamp ON [log].[log_extract_generation] (run_timestamp DESC);
END;

-- Create view for latest run status
IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID(N'[log].[v_extract_generation_latest_runs]'))
    DROP VIEW [log].[v_extract_generation_latest_runs];
GO

CREATE VIEW [log].[v_extract_generation_latest_runs] AS
WITH LatestRuns AS (
    SELECT 
        extract_name,
        MAX(run_timestamp) as latest_run_timestamp
    FROM [log].[log_extract_generation]
    GROUP BY extract_name
)
SELECT 
    l.*
FROM [log].[log_extract_generation] l
INNER JOIN LatestRuns lr 
    ON l.extract_name = lr.extract_name 
    AND l.run_timestamp = lr.latest_run_timestamp;
GO