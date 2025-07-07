schema = StructType(
    [
        StructField("execution_id", StringType(), nullable=False),
        StructField("cfg_synapse_connection_name", StringType(), nullable=False),
        StructField("source_schema_name", StringType(), nullable=False),
        StructField("source_table_name", StringType(), nullable=False),
        StructField("extract_file_name", StringType(), nullable=False),
        StructField("partition_clause", StringType(), nullable=False),
        StructField("status", StringType(), nullable=False),
        StructField("error_messages", StringType(), nullable=True),
        StructField("start_date", LongType(), nullable=False),
        StructField("finish_date", LongType(), nullable=False),
        StructField("update_date", LongType(), nullable=False),
    ]
)

target_lakehouse.create_table(
        table_name="log_synapse_extracts",
        schema=schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"  # Ensure Parquet files are written in vorder order
        },
)