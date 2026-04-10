from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)

# DDL script for creating the cleaned products table in lh_silver.

schema = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("unit_weight_kg", DecimalType(8, 2), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="products", schema_name=""
)
