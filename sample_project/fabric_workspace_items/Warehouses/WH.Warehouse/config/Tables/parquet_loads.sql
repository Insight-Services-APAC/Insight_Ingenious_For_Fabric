CREATE TABLE [config].[parquet_loads] (

	[target_lakehouse_workspace_id] varchar(300) NOT NULL, 
	[target_lakehouse_name] varchar(300) NOT NULL, 
	[target_partition_columns] varchar(300) NOT NULL, 
	[target_sort_columns] varchar(300) NOT NULL, 
	[target_replace_where] varchar(300) NOT NULL, 
	[source_lakehouse_workspace_id] varchar(300) NOT NULL, 
	[source_lakehouse_name] varchar(300) NOT NULL, 
	[source_file_path] varchar(300) NOT NULL, 
	[source_file_name] varchar(300) NOT NULL, 
	[synapse_connection_name] varchar(300) NOT NULL, 
	[synapse_source_schema_name] varchar(300) NOT NULL, 
	[synapse_source_table_name] varchar(300) NOT NULL, 
	[synapse_partition_clause] varchar(300) NOT NULL, 
	[execution_group] int NOT NULL, 
	[active_yn] varchar(1) NOT NULL
);


GO
ALTER TABLE [config].[parquet_loads] ADD CONSTRAINT PK_parquet_loads primary key NONCLUSTERED ([target_lakehouse_workspace_id], [target_lakehouse_name]);