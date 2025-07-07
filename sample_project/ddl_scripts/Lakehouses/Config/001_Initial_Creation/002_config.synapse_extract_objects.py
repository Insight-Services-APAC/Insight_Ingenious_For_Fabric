schema = StructType([
    StructField("synapse_connection_name",     StringType(), nullable=False),
    StructField("source_schema_name",          StringType(), nullable=False),
    StructField("source_table_name",           StringType(), nullable=False),
    StructField("partition_clause",            StringType(), nullable=False),
    StructField("extract_mode",                StringType(), nullable=False),
    StructField("single_date_filter",          StringType(), nullable=True),
    StructField("date_range_filter",           StringType(), nullable=True),
    StructField("execution_group",             IntegerType(), nullable=False),
    StructField("active_yn",                   StringType(), nullable=False),
    StructField("pipeline_id",                 StringType(), nullable=False),
    StructField("synapse_datasource_name",     StringType(), nullable=False),
    StructField("synapse_datasource_location", StringType(), nullable=False),
])

target_lakehouse.create_table(
        table_name="config_synapse_extract_objects",
        schema=schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"  # Ensure Parquet files are written in vorder order
        },
)