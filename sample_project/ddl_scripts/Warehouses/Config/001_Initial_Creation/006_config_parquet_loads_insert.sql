
INSERT INTO config.config_parquet_loads (
    cfg_target_lakehouse_workspace_id,
    cfg_target_lakehouse_id,
    target_partition_columns,
    target_sort_columns,
    target_replace_where,
    cfg_source_lakehouse_workspace_id,
    cfg_source_lakehouse_id,
    cfg_source_file_path,
    source_file_path,
    source_file_name,
    cfg_legacy_synapse_connection_name,
    synapse_source_schema_name,
    synapse_source_table_name,
    synapse_partition_clause,
    execution_group,
    active_yn
)
VALUES
('edw_workspace_id', 'edw_lakehouse_id', '', '', '', 'edw_lakehouse_workspace_id', 'edw_lakehouse_id', 'synapse_export_shortcut_path_in_onelake', 'dbo_dim_customer', '', 'legacy_synapse_connection_name', 'dbo', 'dim_customer', '', 1, 'Y'),
('edw_workspace_id', 'edw_lakehouse_id', 'year, month', 'year, month', 'WHERE year = @year AND month = @month', 'edw_workspace_id', 'edw_lakehouse_id', 'synapse_export_shortcut_path_in_onelake', 'dbo_dim_customer', '', 'legacy_synapse_connection_name', 'dbo', 'fact_transactions', 'WHERE year = @year AND month = @month', 1, 'Y');
