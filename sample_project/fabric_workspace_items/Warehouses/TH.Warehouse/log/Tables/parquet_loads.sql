CREATE TABLE [log].[parquet_loads] (

	[execution_id] varchar(300) NOT NULL, 
	[target_lakehouse_workspace_id] varchar(300) NOT NULL, 
	[target_lakehouse_name] varchar(300) NOT NULL, 
	[source_lakehouse_workspace_id] varchar(300) NOT NULL, 
	[source_lakehouse_name] varchar(300) NOT NULL, 
	[source_file_path] varchar(300) NOT NULL, 
	[source_file_name] varchar(300) NOT NULL, 
	[source_where_clause] varchar(300) NOT NULL, 
	[status] varchar(300) NOT NULL, 
	[error_messages] varchar(4000) NULL, 
	[start_date] datetime2(1) NOT NULL, 
	[finish_date] datetime2(1) NOT NULL, 
	[update_date] datetime2(1) NOT NULL
);