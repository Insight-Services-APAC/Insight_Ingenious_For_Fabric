from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType, BooleanType

# Sample DDL script for creating a customer table in lakehouse
# This demonstrates the basic pattern for creating Delta tables


# Define the schema for the sample customers table
schema = StructType(
    [
        StructField("customer_id", LongType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True)
    ]
)

# Create an empty DataFrame with the schema
empty_df = target_lakehouse.spark.createDataFrame([], schema)

# Write the empty DataFrame to create the table
target_lakehouse.write_to_table(
    df=empty_df,
    table_name="customers",
    schema_name="sample")