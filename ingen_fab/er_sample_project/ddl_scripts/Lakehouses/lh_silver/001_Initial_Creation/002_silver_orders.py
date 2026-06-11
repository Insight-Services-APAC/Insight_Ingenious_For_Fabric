from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

# DDL script for creating the cleaned orders table in lh_silver.

schema = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("required_date", DateType(), True),
        StructField("region_id", StringType(), True),
        StructField("priority", StringType(), True),
        StructField("total_value", DecimalType(14, 2), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="orders", schema_name=""
)
