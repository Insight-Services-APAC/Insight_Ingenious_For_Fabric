CREATE TABLE [log].[synapse_extracts]
(
  execution_id VARCHAR(300) NOT NULL,
  synapse_connection_name VARCHAR(300) NOT NULL,
  source_schema_name VARCHAR(300) NOT NULL,
  source_table_name VARCHAR(300) NOT NULL,
  extract_file_name varchar(300) NOT NULL,
  partition_clause varchar(1000) NULL,
  status VARCHAR(300) NOT NULL,
  error_messages varchar(4000) NULL,
  update_date DATETIME2(1) NOT NULL
)
