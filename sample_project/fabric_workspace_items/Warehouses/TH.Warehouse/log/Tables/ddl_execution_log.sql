CREATE TABLE [log].[ddl_execution_log] (

	[notebook_execution_id] varchar(36) NOT NULL, 
	[notebook_start_time] datetime2(0) NOT NULL, 
	[notebook_end_time] datetime2(0) NULL, 
	[notebook_name] varchar(255) NULL, 
	[ddl_script_execution_id] varchar(36) NOT NULL, 
	[ddl_script_order] int NULL, 
	[ddl_file_name] varchar(255) NULL, 
	[ddl_script_start_time] datetime2(0) NULL, 
	[ddl_script_end_time] datetime2(0) NULL, 
	[status] varchar(50) NULL, 
	[pipeline_status] varchar(50) NULL, 
	[error_message] varchar(max) NULL
);