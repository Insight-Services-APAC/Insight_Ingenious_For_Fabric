INSERT INTO
    [config].[synapse_extracts] (
        [synapse_connection_name],
        [source_schema_name],
        [source_table_name],
        [partition_clause],
        [execution_group],
        [active_yn]
    )
VALUES
    (
        'synapse_connection',
        'dbo',
        'dim_customer',
        '',
        1,
        'Y'
    ),
    (
        'synapse_connection',
        'dbo',
        'fact_transactions',
        'where year = @year and month = @month',
        1,
        'Y'
    )