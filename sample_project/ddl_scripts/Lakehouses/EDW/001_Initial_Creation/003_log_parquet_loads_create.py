schema = StructType(
    [
        StructField("execution_id", StringType(), nullable=False),
        StructField("target_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("target_lakehouse_name", StringType(), nullable=False),
        StructField("partition_clause", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("error_messages", StringType(), nullable=True),
        StructField("start_date", TimestampType(), nullable=False),
        StructField("finish_date", TimestampType(), nullable=False),
        StructField("update_date", TimestampType(), nullable=False),
    ]
)

empty_df = spark.createDataFrame([], schema)
(
    empty_df.write.format("delta")
    .option("parquet.vorder.default", "true")
    .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
    .save(
        f"{lu.lakehouse_tables_uri()}log_parquet_loads"  # noqa: E501
    )
)
