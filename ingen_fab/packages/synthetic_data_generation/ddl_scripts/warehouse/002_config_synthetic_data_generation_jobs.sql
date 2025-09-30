-- Configuration table for synthetic data generation jobs
-- This table tracks generation requests and their parameters

CREATE TABLE config_synthetic_data_generation_jobs (
    job_id NVARCHAR(50) PRIMARY KEY,
    dataset_id NVARCHAR(50) NOT NULL,
    job_name NVARCHAR(100) NOT NULL,
    target_rows BIGINT NOT NULL,
    generation_mode NVARCHAR(20) NOT NULL CHECK (generation_mode IN ('python', 'pyspark')),
    target_environment NVARCHAR(20) NOT NULL CHECK (target_environment IN ('lakehouse', 'warehouse')),
    target_location NVARCHAR(200), -- lakehouse name or warehouse schema
    chunk_size BIGINT DEFAULT 1000000, -- rows per chunk for large datasets
    parallel_workers INT DEFAULT 1,
    seed_value INT, -- for reproducible generation
    custom_config_json NVARCHAR(MAX), -- job-specific overrides
    status NVARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    created_date DATETIME2 DEFAULT GETDATE(),
    started_date DATETIME2,
    completed_date DATETIME2,
    error_message NVARCHAR(MAX),

    FOREIGN KEY (dataset_id) REFERENCES config_synthetic_data_datasets(dataset_id)
);
