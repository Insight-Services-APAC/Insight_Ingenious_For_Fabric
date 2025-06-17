CREATE TABLE [config].[synapse_extracts] (

	[synapse_connection_name] varchar(300) NOT NULL, 
	[source_schema_name] varchar(300) NOT NULL, 
	[source_table_name] varchar(300) NOT NULL, 
	[partition_clause] varchar(300) NOT NULL, 
	[execution_group] int NOT NULL, 
	[active_yn] varchar(1) NOT NULL
);


GO
ALTER TABLE [config].[synapse_extracts] ADD CONSTRAINT PK_synapse_extracts primary key NONCLUSTERED ([synapse_connection_name], [source_schema_name], [source_table_name]);