CREATE TABLE [log].[synapse_extracts] (

	[execution_id] varchar(300) NOT NULL, 
	[synapse_connection_name] varchar(300) NOT NULL, 
	[source_schema_name] varchar(300) NOT NULL, 
	[source_table_name] varchar(300) NOT NULL, 
	[extract_file_name] varchar(300) NOT NULL, 
	[partition_clause] varchar(1000) NULL, 
	[status] varchar(300) NOT NULL, 
	[error_messages] varchar(4000) NULL, 
	[update_date] datetime2(1) NOT NULL
);