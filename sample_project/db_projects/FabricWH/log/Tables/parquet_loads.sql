CREATE TABLE [log].[parquet_loads] (
    execution_id VARCHAR(300) NOT NULL,
    [target_lakehouse_workspace_id] varchar(300) NOT NULL,
    [target_lakehouse_name] varchar(300) NOT NULL,
    [partition_clause] varchar(300) NULL,
    status VARCHAR(300) NOT NULL,
    error_messages varchar(4000) NULL,
    start_date DATETIME2(1) NOT NULL,
    finish_date DATETIME2(1) NOT NULL,
    update_date DATETIME2(1) NOT NULL
);

ALTER TABLE
    [log].[parquet_loads]
ADD
    CONSTRAINT PK_parquet_loads_log PRIMARY KEY NONCLUSTERED (
        [target_lakehouse_workspace_id],
        [target_lakehouse_name],
        [partition_clause]
    ) NOT ENFORCED