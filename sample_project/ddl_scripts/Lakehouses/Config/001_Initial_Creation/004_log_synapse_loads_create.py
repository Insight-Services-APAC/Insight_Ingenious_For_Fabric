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
        StructField("update_date", LongType(), nullable=False)
    ]
)

empty_df = spark.createDataFrame([], schema)
(
    empty_df.write.format("delta")
    .option("parquet.vorder.default", "true")
    .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
    .save(
        f"{lu.lakehouse_tables_uri()}log_synapse_extracts"  # noqa: E501
    )
)
