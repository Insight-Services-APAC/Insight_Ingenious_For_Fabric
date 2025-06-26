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
insert_df = spark.createDataFrame(data, schema)

# 3. Append to the existing Delta table
table_path = f"{lu.lakehouse_tables_uri()}config_synapse_extracts"

insert_df.write.format("delta").mode("append").save(table_path)
