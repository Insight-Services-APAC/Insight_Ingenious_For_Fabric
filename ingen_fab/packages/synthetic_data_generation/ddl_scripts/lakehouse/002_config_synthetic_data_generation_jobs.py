# Configuration table for synthetic data generation jobs (Lakehouse version)
# This creates a Delta table to track generation requests and their parameters

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Define schema for config_synthetic_data_generation_jobs table
config_jobs_schema = StructType(
    [
        StructField("job_id", StringType(), False),
        StructField("dataset_id", StringType(), False),
        StructField("job_name", StringType(), False),
        StructField("target_rows", LongType(), False),
        StructField("generation_mode", StringType(), False),  # 'python' or 'pyspark'
        StructField(
            "target_environment", StringType(), False
        ),  # 'lakehouse' or 'warehouse'
        StructField("target_location", StringType(), True),  # lakehouse name or schema
        StructField("chunk_size", LongType(), True),
        StructField("parallel_workers", IntegerType(), True),
        StructField("seed_value", IntegerType(), True),
        StructField("custom_config_json", StringType(), True),
        StructField(
            "status", StringType(), False
        ),  # 'pending', 'running', 'completed', 'failed'
        StructField("created_date", TimestampType(), False),
        StructField("started_date", TimestampType(), True),
        StructField("completed_date", TimestampType(), True),
        StructField("error_message", StringType(), True),
    ]
)

target_lakehouse.create_table(  # noqa: F821
    table_name="config_synthetic_data_generation_jobs",
    schema=config_jobs_schema,
    mode="overwrite",
    options={"parquet.vorder.default": "true"},
)

print("✅ Created config_synthetic_data_generation_jobs Delta table")
