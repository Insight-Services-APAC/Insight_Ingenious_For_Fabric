# Sample data for config_synapse_extract_objects - Lakehouse version

from datetime import datetime

# Sample data
sample_data = [
    {
        "synapse_connection_name": "SynapseConnection",
        "source_schema_name": "dbo",
        "source_table_name": "DimCustomer",
        "extract_mode": "snapshot",
        "single_date_filter": None,
        "date_range_filter": None,
        "execution_group": 1,
        "active_yn": "Y",
        "pipeline_id": "00000000-0000-0000-0000-000000000000",
        "synapse_datasource_name": "SynapseDatasource",
        "synapse_datasource_location": "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files",
        "created_timestamp": datetime.now(),
        "updated_timestamp": datetime.now()
    },
    {
        "synapse_connection_name": "SynapseConnection",
        "source_schema_name": "dbo",
        "source_table_name": "FactSales",
        "extract_mode": "incremental",
        "single_date_filter": "WHERE DATE_SK = @date",
        "date_range_filter": "WHERE DATE_SK BETWEEN @start_date AND @end_date",
        "execution_group": 2,
        "active_yn": "Y",
        "pipeline_id": "00000000-0000-0000-0000-000000000000",
        "synapse_datasource_name": "SynapseDatasource",
        "synapse_datasource_location": "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files",
        "created_timestamp": datetime.now(),
        "updated_timestamp": datetime.now()
    }
]

target_lakehouse.write_to_table(
    table_name="config_synapse_extract_objects",
    df=sample_data,
    mode="append"
)