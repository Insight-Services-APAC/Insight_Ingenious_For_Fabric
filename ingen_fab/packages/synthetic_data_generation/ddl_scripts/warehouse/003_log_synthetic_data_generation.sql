-- Log table for synthetic data generation execution tracking
-- This table tracks detailed execution metrics and performance

CREATE TABLE log_synthetic_data_generation (
    log_id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
    job_id NVARCHAR(50) NOT NULL,
    execution_step NVARCHAR(50) NOT NULL, -- e.g., 'initialization', 'generation', 'validation'
    table_name NVARCHAR(100),
    rows_generated BIGINT,
    chunk_number INT,
    execution_time_ms BIGINT,
    memory_usage_mb DECIMAL(10,2),
    status NVARCHAR(20) CHECK (status IN ('started', 'completed', 'failed')),
    message NVARCHAR(500),
    error_details NVARCHAR(MAX),
    execution_timestamp DATETIME2 DEFAULT GETDATE(),
    
    FOREIGN KEY (job_id) REFERENCES config_synthetic_data_generation_jobs(job_id)
);