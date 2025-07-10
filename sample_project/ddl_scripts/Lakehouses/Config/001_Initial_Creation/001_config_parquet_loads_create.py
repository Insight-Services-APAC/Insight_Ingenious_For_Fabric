schema = StructType(
    [
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
        StructField("active_yn", StringType(), nullable=False),
    ]
)

target_lakehouse.create_table(
        table_name="config_parquet_loads",
        schema=schema,
        mode="overwrite",
        options={
            "parquet.vorder.default": "true"  # Ensure Parquet files are written in vorder order
        },
)
