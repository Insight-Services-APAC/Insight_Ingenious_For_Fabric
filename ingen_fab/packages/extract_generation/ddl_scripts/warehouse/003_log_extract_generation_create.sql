-- Create log_extract_generation table
-- Tracks execution history of extract generation runs
-- NOTE: IDENTITY columns are not supported in Microsoft Fabric SQL Warehouse
-- Applications must generate unique log_id values manually (e.g., using NEWID(), timestamps, or sequence logic)
CREATE TABLE [log].[log_extract_generation] (
    -- Primary key (Note: IDENTITY not supported in Fabric Warehouse - app must provide unique values)
    log_id BIGINT NOT NULL,
    
    -- Extract identification
    extract_name VARCHAR(100) NOT NULL,
    execution_group VARCHAR(50),
    
    -- Run information
    run_id VARCHAR(100) NOT NULL,
    run_timestamp DATETIME2(6) NOT NULL,
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
    start_time DATETIME2(6),
    end_time DATETIME2(6),
    duration_seconds INT,
    
    -- Performance metrics
    throughput_rows_per_second INT,
    memory_usage_mb_before INT,
    memory_usage_mb_after INT,
    
    -- Error handling
    error_message VARCHAR(8000),
    error_details VARCHAR(8000),
    
    -- Fabric specific
    workspace_id VARCHAR(100),
    lakehouse_id VARCHAR(100),
    warehouse_id VARCHAR(100),
    notebook_run_id VARCHAR(100),
    
    -- Metadata
    created_by VARCHAR(100),
    created_timestamp DATETIME2(6),
    
);
