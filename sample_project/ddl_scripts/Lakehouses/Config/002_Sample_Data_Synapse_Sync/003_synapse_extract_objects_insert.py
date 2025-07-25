# Sample data for synapse_extract_objects - Lakehouse version

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
        "synapse_datasource_location": "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files"
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
        "synapse_datasource_location": "https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse/Files"
    }
]

schema = StructType([
    StructField("synapse_connection_name", StringType(), False),
    StructField("source_schema_name", StringType(), False),
    StructField("source_table_name", StringType(), False),
    StructField("extract_mode", StringType(), False),
    StructField("single_date_filter", StringType(), True),
    StructField("date_range_filter", StringType(), True),
    StructField("execution_group", IntegerType(), False),
    StructField("active_yn", StringType(), False),
    StructField("pipeline_id", StringType(), False),
    StructField("synapse_datasource_name", StringType(), False),
    StructField("synapse_datasource_location", StringType(), False)
])

# Convert list to DataFrame using target_lakehouse
df = target_lakehouse.spark.createDataFrame(sample_data, schema)

target_lakehouse.write_to_table(
    df=df,
    table_name="synapse_extract_objects",
    mode="append"
)