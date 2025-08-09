from datetime import datetime

from pyspark.pandas.generic import LongType
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Sample DDL script for creating a customer table in lakehouse
# This demonstrates the basic pattern for creating Delta tables
# Define schema for customer table
schema = StructType(
    [
        StructField("customer_id", LongType(), nullable=False),
        StructField("first_name", StringType(), nullable=False),
        StructField("last_name", StringType(), nullable=False),
        StructField("email", StringType(), nullable=False),
        StructField("created_at", TimestampType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
    ]
)

# Create DataFrame with sample data
data = [
    (1, "John", "Doe", "john.doe@example.com", datetime(2024, 1, 1, 10, 0, 0), True),
    (
        2,
        "Jane",
        "Smith",
        "jane.smith@example.com",
        datetime(2024, 1, 2, 11, 0, 0),
        True,
    ),
    (
        3,
        "Bob",
        "Johnson",
        "bob.johnson@example.com",
        datetime(2024, 1, 3, 12, 0, 0),
        False,
    ),
]

df = target_lakehouse.spark.createDataFrame(data, schema)

target_lakehouse.write_to_table(df=df, table_name="customers", schema_name="sample")
