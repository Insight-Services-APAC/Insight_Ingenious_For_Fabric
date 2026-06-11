from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

# DDL script for creating the raw regions table in lh_bronze.

schema = StructType(
    [
        StructField("region_id", StringType(), True),
        StructField("region_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("zone", StringType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="regions", schema_name=""
)
