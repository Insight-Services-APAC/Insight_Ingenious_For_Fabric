-- Log table for config execution tracking - Warehouse version
-- Uses Airflow-style single-row-per-execution pattern with status updates via MERGE
-- Primary Key: (execution_id, config_id)
CREATE TABLE log.log_config_execution (
    -- Primary key (composite)
    execution_id NVARCHAR(50) NOT NULL,
    config_id NVARCHAR(50) NOT NULL,
    status NVARCHAR(50) NOT NULL, -- running, completed, failed, no_data
    -- File counts
    files_discovered BIGINT NULL,
    files_processed BIGINT NULL,
    files_failed BIGINT NULL,
    files_skipped BIGINT NULL,
    -- Aggregated metrics across all files in this execution
    records_processed BIGINT NULL,
    records_inserted BIGINT NULL,
    records_updated BIGINT NULL,
    records_deleted BIGINT NULL,
    total_duration_ms BIGINT NULL,
    -- Error tracking
    error_message NVARCHAR(MAX) NULL,
    error_details NVARCHAR(MAX) NULL,
    -- Timestamp tracking (Airflow-style)
    started_at DATETIME2 NOT NULL, -- Immutable - set on first insert
    updated_at DATETIME2 NOT NULL, -- Always updated on merge
    completed_at DATETIME2 NULL, -- Set when status becomes completed/failed/no_data
    CONSTRAINT PK_log_config_execution PRIMARY KEY (execution_id, config_id)
);
