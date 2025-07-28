schema = StructType(
    [
        StructField("store_name", StringType(), nullable=False),
        StructField("department", StringType(), nullable=False)
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df,
    table_name="store",
    schema_name="sample")

