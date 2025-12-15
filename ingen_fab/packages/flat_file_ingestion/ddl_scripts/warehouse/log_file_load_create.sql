-- Log table for file load tracking - Warehouse version
-- Uses Airflow-style single-row-per-execution pattern with status updates via MERGE
-- Primary Key: load_id
CREATE TABLE log.log_file_load (
    -- Primary key and identifiers
    load_id NVARCHAR(50) NOT NULL,
    execution_id NVARCHAR(50) NOT NULL,
    config_id NVARCHAR(50) NOT NULL,
    status NVARCHAR(50) NOT NULL, -- running, completed, failed, duplicate, skipped
    -- Source file information
    source_file_path NVARCHAR(500) NOT NULL,
    source_file_size_bytes BIGINT NULL,
    source_file_modified_time DATETIME2 NULL,
    target_table_name NVARCHAR(128) NOT NULL,
    -- Processing metrics
    records_processed BIGINT NULL,
    records_inserted BIGINT NULL,
    records_updated BIGINT NULL,
    records_deleted BIGINT NULL,
    -- Performance metrics
    source_row_count BIGINT NULL,
    target_row_count_before BIGINT NULL,
    target_row_count_after BIGINT NULL,
    row_count_reconciliation_status NVARCHAR(50) NULL, -- matched, mismatched, not_verified
    corrupt_records_count BIGINT NULL,
    data_read_duration_ms BIGINT NULL,
    total_duration_ms BIGINT NULL,
    -- Error tracking
    error_message NVARCHAR(MAX) NULL,
    error_details NVARCHAR(MAX) NULL,
    execution_duration_seconds INT NULL,
    -- Partition tracking for incremental loads
    source_file_partition_cols NVARCHAR(MAX) NULL, -- JSON array of partition column names
    source_file_partition_values NVARCHAR(MAX) NULL, -- JSON array of partition values
    date_partition NVARCHAR(50) NULL, -- Extracted date partition (YYYY-MM-DD format)
    filename_attributes_json NVARCHAR(MAX) NULL, -- Extracted filename attributes as JSON
    control_file_path NVARCHAR(500) NULL, -- Path to associated control file if required
    -- Timestamp tracking (Airflow-style)
    started_at DATETIME2 NOT NULL, -- Immutable - set on first insert
    updated_at DATETIME2 NOT NULL, -- Always updated on merge
    completed_at DATETIME2 NULL, -- Set when status becomes completed/failed
    attempt_count INT NOT NULL, -- Incremented on retries
    CONSTRAINT PK_log_file_load PRIMARY KEY (load_id)
);
