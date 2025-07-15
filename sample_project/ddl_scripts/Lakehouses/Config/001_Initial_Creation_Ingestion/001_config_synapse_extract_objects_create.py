# Configuration table for synapse extract objects - Lakehouse version

# Define schema for the table
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

target_lakehouse.create_table(
    table_name="config_synapse_extract_objects",
    schema=schema,
    mode="overwrite",
    options={
        "parquet.vorder.default": "true",
        "overwriteSchema": "true"
    }
)