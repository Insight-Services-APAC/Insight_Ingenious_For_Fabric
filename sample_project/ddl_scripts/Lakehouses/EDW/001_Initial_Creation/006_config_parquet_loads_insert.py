schema = StructType([
        StructField("cfg_target_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("cfg_target_lakehouse_name", StringType(), nullable=False),
        StructField("target_partition_columns", StringType(), nullable=False),
        StructField("target_sort_columns", StringType(), nullable=False),
        StructField("target_replace_where", StringType(), nullable=False),

        StructField("cfg_source_lakehouse_workspace_id", StringType(), nullable=False),
        StructField("cfg_source_lakehouse_name", StringType(), nullable=False),
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


# ──────────────────────────────────────────────────────────────────────────────
# Build the data rows
# ──────────────────────────────────────────────────────────────────────────────
data = [
    (
        "edw_lakehouse_workspace_id",
        "edw_lakehouse_name",
        "",  # target_partition_columns
        "",  # target_sort_columns
        "",  # target_replace_where

        "edw_lakehouse_workspace_id",
        "edw_lakehouse_name",
        "synapse_export_shortcut_path_in_onelake",
        "dbo_dim_customer",
        "",

        "cfg_legacy_synapse_connection_name",
        "dbo",
        "dim_customer",
        "",

        1,
        "Y"
    ),
    (
        "edw_lakehouse_workspace_id",
        "edw_lakehouse_name",
        "year, month",
        "year, month",
        "WHERE year = @year AND month = @month",

        "edw_lakehouse_workspace_id",
        "edw_lakehouse_name",
        "synapse_export_shortcut_path_in_onelake",
        "dbo_dim_customer",
        "",

        "synapse_connection",
        "dbo",
        "fact_transactions",
        "WHERE year = @year AND month = @month",

        1,
        "Y"
    ),
]

insert_df = spark.createDataFrame(data, schema)

# 3. Append to the existing Delta table
table_path = f"{lu.lakehouse_tables_uri()}config_parquet_loads" 

insert_df.write \
    .format("delta") \
    .mode("append") \
    .save(table_path)

