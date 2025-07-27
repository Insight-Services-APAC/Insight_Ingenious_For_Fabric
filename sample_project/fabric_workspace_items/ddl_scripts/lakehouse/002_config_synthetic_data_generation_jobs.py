# Configuration table for synthetic data generation jobs (Lakehouse version)
# This creates a Delta table to track generation requests and their parameters

from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType

# Define schema for config_synthetic_data_generation_jobs table
config_jobs_schema = StructType([
    StructField("job_id", StringType(), False),
    StructField("dataset_id", StringType(), False),
    StructField("job_name", StringType(), False),
    StructField("target_rows", LongType(), False),
    StructField("generation_mode", StringType(), False),  # 'python' or 'pyspark'
    StructField("target_environment", StringType(), False),  # 'lakehouse' or 'warehouse'
    StructField("target_location", StringType(), True),  # lakehouse name or schema
    StructField("chunk_size", LongType(), True),
    StructField("parallel_workers", IntegerType(), True),
    StructField("seed_value", IntegerType(), True),
    StructField("custom_config_json", StringType(), True),
    StructField("status", StringType(), False),  # 'pending', 'running', 'completed', 'failed'
    StructField("created_date", TimestampType(), False),
    StructField("started_date", TimestampType(), True),
    StructField("completed_date", TimestampType(), True),
    StructField("error_message", StringType(), True)
])

# Create empty DataFrame with schema
config_jobs_df = spark.createDataFrame([], config_jobs_schema)

# Write as Delta table
config_jobs_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("config_synthetic_data_generation_jobs")

print("✅ Created config_synthetic_data_generation_jobs Delta table")