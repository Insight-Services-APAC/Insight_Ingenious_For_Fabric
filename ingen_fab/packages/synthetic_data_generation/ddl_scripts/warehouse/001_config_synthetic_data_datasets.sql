-- Configuration table for synthetic data generation datasets
-- This table defines the available dataset templates and their configurations

CREATE TABLE config_synthetic_data_datasets (
    dataset_id NVARCHAR(50) PRIMARY KEY,
    dataset_name NVARCHAR(100) NOT NULL,
    dataset_type NVARCHAR(20) NOT NULL CHECK (dataset_type IN ('transactional', 'analytical')),
    schema_pattern NVARCHAR(20) NOT NULL CHECK (schema_pattern IN ('oltp', 'star_schema', 'snowflake')),
    domain NVARCHAR(50) NOT NULL, -- e.g., 'retail', 'finance', 'healthcare', 'ecommerce'
    max_recommended_rows BIGINT NOT NULL,
    description NVARCHAR(500),
    config_json NVARCHAR(MAX), -- JSON configuration for generators
    is_active BIT DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE(),
    modified_date DATETIME2 DEFAULT GETDATE()
);
