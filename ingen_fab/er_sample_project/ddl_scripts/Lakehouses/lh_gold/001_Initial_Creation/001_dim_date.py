from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# DDL script for creating the date dimension table in lh_gold.

schema = StructType(
    [
        StructField("date_key", IntegerType(), False),
        StructField("full_date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("week", IntegerType(), True),
        StructField("day_of_week", StringType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="dim_date", schema_name=""
)
