
INSERT INTO config.config_synapse_extracts (
    cfg_legacy_synapse_connection_name,
    source_schema_name,
    source_table_name,
    partition_clause,
    execution_group,
    active_yn
)
VALUES
('legacy_synapse_connection_name', 'dbo', 'dim_customer', '', 1, 'Y'),
('legacy_synapse_connection_name', 'dbo', 'fact_transactions', 'where year = @year and month = @month', 1, 'Y');
