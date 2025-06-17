-- Auto Generated (Do not modify) D1C20D8919476B1A29B2FDC2E323E68507FF3C1F5DAC32EB17E90CFAFDC8F475
create view [vw_parquet_loads] as 
with latest_synapse_extracts as
(
    select  synapse_connection_name,
            source_schema_name, 
            source_table_name, 
            max(update_date) update_date
    from [log].[synapse_extracts]
    group by synapse_connection_name,
            source_schema_name, 
            source_table_name 
),
latest_synapse_extracts_with_status as
(
    Select a.* 
    from log.synapse_extracts a 
    inner join latest_synapse_extracts c    
    on a.synapse_connection_name = c.synapse_connection_name 
        and a.source_schema_name = c.source_schema_name 
        and a.source_table_name = c.source_table_name
        and a.update_date = c.update_date

)
select a.*, c.status as synapse_extract_status, c.execution_id as synapse_exctract_execution_id from 
[config].[parquet_loads] a
left join latest_synapse_extracts_with_status c
    on a.synapse_connection_name = c.synapse_connection_name 
        and a.synapse_source_schema_name = c.source_schema_name 
        and a.synapse_source_table_name = c.source_table_name
where a.active_yn = 'Y' and c.status = 'Completed'

--left join log.parquet_loads b 
--    on a.target_lakehouse_workspace_id = b.target_lakehouse_workspace_id
--        and a.target_lakehouse_name = b.target_lakehouse_name
--        and a.target_lakehouse_workspace_id = b.source_lakehouse_workspace_id
--        and a.target_lakehouse_name = b.source_lakehouse_name