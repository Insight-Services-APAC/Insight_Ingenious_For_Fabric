# Log table for synthetic data generation execution tracking (Lakehouse version)
# This creates a Delta table for detailed execution metrics

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DecimalType, TimestampType

# Define schema for log_synthetic_data_generation table
log_schema = StructType([
    StructField("log_id", StringType(), False),
    StructField("job_id", StringType(), False),
    StructField("execution_step", StringType(), False),  # 'initialization', 'generation', 'validation'
    StructField("table_name", StringType(), True),
    StructField("rows_generated", LongType(), True),
    StructField("chunk_number", IntegerType(), True),
    StructField("execution_time_ms", LongType(), True),
    StructField("memory_usage_mb", DecimalType(10, 2), True),
    StructField("status", StringType(), False),  # 'started', 'completed', 'failed'
    StructField("message", StringType(), True),
    StructField("error_details", StringType(), True),
    StructField("execution_timestamp", TimestampType(), False)
])

target_lakehouse.create_table(
    table_name="log_synthetic_data_generation",
    schema=log_schema,
    mode="overwrite",
    options={
        "parquet.vorder.default": "true"
    }
)

print("✅ Created log_synthetic_data_generation Delta table")