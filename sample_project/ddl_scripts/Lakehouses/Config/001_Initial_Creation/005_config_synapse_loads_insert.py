data = [
    ("legacy_synapse_connection_name", "dbo", "dim_customer", "", 1, "Y"),
    (
        "legacy_synapse_connection_name",
        "dbo",
        "fact_transactions",
        "where year = @year and month = @month",
        1,
        "Y",
    ),
]

# 2. Create a DataFrame
schema = StructType(
    [
        StructField("cfg_legacy_synapse_connection_name", StringType(), nullable=False),
        StructField("source_schema_name", StringType(), nullable=False),
        StructField("source_table_name", StringType(), nullable=False),
        StructField("partition_clause", StringType(), nullable=False),
        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False),
    ]
)
insert_df = target_lakehouse.get_connection.createDataFrame(data, schema)

target_lakehouse.write_to_table(
    table_name="config_synapse_extracts",
    df=insert_df,
    mode="append"
)
