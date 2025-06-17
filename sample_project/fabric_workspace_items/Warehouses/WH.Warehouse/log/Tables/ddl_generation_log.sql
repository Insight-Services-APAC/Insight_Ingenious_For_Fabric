CREATE TABLE [log].[ddl_generation_log] (

	[execution_id] varchar(36) NOT NULL, 
	[notebook_name] varchar(100) NOT NULL, 
	[source_file] varchar(50) NOT NULL, 
	[start_time] datetime2(6) NOT NULL, 
	[end_time] datetime2(6) NULL, 
	[status] varchar(20) NOT NULL, 
	[error_message] varchar(max) NULL
);