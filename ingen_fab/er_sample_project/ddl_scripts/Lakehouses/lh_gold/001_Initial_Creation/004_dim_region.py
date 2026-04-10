from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# DDL script for creating the region dimension table in lh_gold.

schema = StructType(
    [
        StructField("region_key", IntegerType(), False),
        StructField("region_id", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("zone", StringType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="dim_region", schema_name=""
)
