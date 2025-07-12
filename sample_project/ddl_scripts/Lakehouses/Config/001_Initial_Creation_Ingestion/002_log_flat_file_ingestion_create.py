# Log table for flat file ingestion execution tracking - Lakehouse version
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

schema = StructType([
    StructField("log_id", StringType(), nullable=False),
    StructField("config_id", StringType(), nullable=False),
    StructField("execution_id", StringType(), nullable=False),
    StructField("job_start_time", TimestampType(), nullable=False),
    StructField("job_end_time", TimestampType(), nullable=True),
    StructField("status", StringType(), nullable=False),  # running, completed, failed, cancelled
    StructField("source_file_path", StringType(), nullable=False),
    StructField("source_file_size_bytes", LongType(), nullable=True),
    StructField("source_file_modified_time", TimestampType(), nullable=True),
    StructField("target_table_name", StringType(), nullable=False),
    StructField("records_processed", LongType(), nullable=True),
    StructField("records_inserted", LongType(), nullable=True),
    StructField("records_updated", LongType(), nullable=True),
    StructField("records_deleted", LongType(), nullable=True),
    StructField("records_failed", LongType(), nullable=True),
    StructField("error_message", StringType(), nullable=True),
    StructField("error_details", StringType(), nullable=True),
    StructField("execution_duration_seconds", IntegerType(), nullable=True),
    StructField("spark_application_id", StringType(), nullable=True),
    StructField("created_date", TimestampType(), nullable=False),
    StructField("created_by", StringType(), nullable=False)
])

target_lakehouse.create_table(
    table_name="log_flat_file_ingestion",
    schema=schema,
    mode="overwrite",
    options={
        "parquet.vorder.default": "true"
    }
)