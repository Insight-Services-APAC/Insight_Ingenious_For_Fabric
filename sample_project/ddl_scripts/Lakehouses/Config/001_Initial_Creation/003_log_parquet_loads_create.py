schema = StructType(
    [
        StructField("execution_id", StringType(), nullable=False),
        StructField("cfg_target_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("cfg_target_lakehouse_id", StringType(), nullable=False),
        StructField("partition_clause", StringType(), nullable=True),
        StructField("status", StringType(), nullable=False),
        StructField("error_messages", StringType(), nullable=True),
        StructField("start_date", LongType(), nullable=False),
        StructField("finish_date", LongType(), nullable=False),
        StructField("update_date", LongType(), nullable=False),
    ]
)


target_lakehouse.create_table(
        table_name="log_parquet_loads",
        schema=schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"  # Ensure Parquet files are written in vorder order
        },
)


