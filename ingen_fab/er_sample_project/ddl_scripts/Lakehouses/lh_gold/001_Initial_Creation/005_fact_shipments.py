from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# DDL script for creating the fact_shipments table in lh_gold.
# This is the central fact table of the star schema.

schema = StructType(
    [
        StructField("shipment_key", IntegerType(), False),
        StructField("shipment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_key", IntegerType(), True),
        StructField("supplier_key", IntegerType(), True),
        StructField("origin_region_key", IntegerType(), True),
        StructField("destination_region_key", IntegerType(), True),
        StructField("date_key", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_cost", DecimalType(12, 2), True),
        StructField("freight_cost", DecimalType(12, 2), True),
        StructField("total_cost", DecimalType(14, 2), True),
        StructField("scheduled_days", IntegerType(), True),
        StructField("actual_days", IntegerType(), True),
        StructField("is_on_time", BooleanType(), True),
        StructField("status", StringType(), True),
        StructField("priority", StringType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="fact_shipments", schema_name=""
)
