-- Log table for flat file ingestion execution tracking - Warehouse version
CREATE TABLE log.log_flat_file_ingestion (
    log_id NVARCHAR(50) NOT NULL,
    config_id NVARCHAR(50) NOT NULL,
    execution_id NVARCHAR(50) NOT NULL,
    job_start_time DATETIME2 NOT NULL,
    job_end_time DATETIME2 NULL,
    status NVARCHAR(50) NOT NULL, -- running, completed, failed, cancelled
    source_file_path NVARCHAR(500) NOT NULL,
    source_file_size_bytes BIGINT NULL,
    source_file_modified_time DATETIME2 NULL,
    target_table_name NVARCHAR(128) NOT NULL,
    records_processed BIGINT NULL,
    records_inserted BIGINT NULL,
    records_updated BIGINT NULL,
    records_deleted BIGINT NULL,
    records_failed BIGINT NULL,
    error_message NVARCHAR(MAX) NULL,
    error_details NVARCHAR(MAX) NULL,
    execution_duration_seconds INT NULL,
    spark_application_id NVARCHAR(255) NULL,
    created_date DATETIME2 NOT NULL,
    created_by NVARCHAR(100) NOT NULL,
    CONSTRAINT PK_log_flat_file_ingestion PRIMARY KEY (log_id)
);