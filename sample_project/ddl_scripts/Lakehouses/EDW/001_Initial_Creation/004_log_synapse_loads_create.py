schema = StructType(
    [
        StructField("synapse_connection_name", StringType(), nullable=False),
        StructField("source_schema_name", StringType(), nullable=False),
        StructField("source_table_name", StringType(), nullable=False),
        StructField("partition_clause", StringType(), nullable=False),
        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False),
    ]
)

empty_df = spark.createDataFrame([], schema)
(
    empty_df.write.format("delta")
    .option("parquet.vorder.default", "true")
    .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
    .save(
        f"{lu.lakehouse_tables_uri()}log_synapse_loads"  # noqa: E501
    )
)
