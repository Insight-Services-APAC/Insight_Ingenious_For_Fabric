
CREATE TABLE log.log_synapse_extracts (
    execution_id VARCHAR(300) NOT NULL,
    cfg_synapse_connection_name VARCHAR(300) NOT NULL,
    source_schema_name VARCHAR(300) NOT NULL,
    source_table_name VARCHAR(300) NOT NULL,
    extract_file_name VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000) NOT NULL,
    status VARCHAR(300) NOT NULL,
    error_messages VARCHAR(4000),
    start_date BIGINT NOT NULL,
    finish_date BIGINT NOT NULL,
    update_date BIGINT NOT NULL
);
