schema = StructType([
        StructField("cfg_target_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("cfg_target_lakehouse_id", StringType(), nullable=False),
        StructField("target_partition_columns", StringType(), nullable=False),
        StructField("target_sort_columns", StringType(), nullable=False),
        StructField("target_replace_where", StringType(), nullable=False),

        StructField("cfg_source_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("cfg_source_lakehouse_id", StringType(), nullable=False),
        StructField("cfg_source_file_path", StringType(), nullable=False),
        StructField("source_file_path", StringType(), nullable=False),
        StructField("source_file_name", StringType(), nullable=False),

        StructField("cfg_legacy_synapse_connection_name", StringType(), nullable=False),
        StructField("synapse_source_schema_name", StringType(), nullable=False),
        StructField("synapse_source_table_name", StringType(), nullable=False),
        StructField("synapse_partition_clause", StringType(), nullable=False),

        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False)
    ])

empty_df = spark.createDataFrame([], schema)
(
    empty_df.write
    .format("delta")
    .option("parquet.vorder.default","true")
    .mode("overwrite")  # will error if table exists; change to "overwrite" to replace.
    .save(f"{lu.lakehouse_tables_uri()}config_parquet_loads")
)
