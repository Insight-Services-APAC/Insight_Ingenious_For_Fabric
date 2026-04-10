from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# DDL script for creating the raw shipments table in lh_bronze.
# Data is populated separately via ingen_fab synthetic data generation.

schema = StructType(
    [
        StructField("shipment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("supplier_id", StringType(), True),
        StructField("origin_region_id", StringType(), True),
        StructField("destination_region_id", StringType(), True),
        StructField("shipment_date", DateType(), True),
        StructField("estimated_delivery_date", DateType(), True),
        StructField("actual_delivery_date", DateType(), True),
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
    df=empty_df, table_name="shipments", schema_name=""
)
