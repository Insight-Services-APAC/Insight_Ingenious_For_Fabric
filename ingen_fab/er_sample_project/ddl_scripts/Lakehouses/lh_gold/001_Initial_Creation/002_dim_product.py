from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# DDL script for creating the product dimension table in lh_gold.

schema = StructType(
    [
        StructField("product_key", IntegerType(), False),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("unit_weight_kg", DecimalType(8, 2), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="dim_product", schema_name=""
)
