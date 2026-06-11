from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# DDL script for creating the raw suppliers table in lh_bronze.

schema = StructType(
    [
        StructField("supplier_id", StringType(), True),
        StructField("supplier_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("performance_rating", DecimalType(3, 1), True),
        StructField("lead_time_days", IntegerType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="suppliers", schema_name=""
)
