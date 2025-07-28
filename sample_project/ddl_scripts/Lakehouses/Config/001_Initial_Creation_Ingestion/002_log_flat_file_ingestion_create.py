# Log table for flat file ingestion execution tracking - Lakehouse version
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

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
    # Performance metrics
    StructField("source_row_count", LongType(), nullable=True),
    StructField("staging_row_count", LongType(), nullable=True),
    StructField("target_row_count_before", LongType(), nullable=True),
    StructField("target_row_count_after", LongType(), nullable=True),
    StructField("row_count_reconciliation_status", StringType(), nullable=True),  # matched, mismatched, not_verified
    StructField("row_count_difference", LongType(), nullable=True),
    StructField("data_read_duration_ms", LongType(), nullable=True),
    StructField("staging_write_duration_ms", LongType(), nullable=True),
    StructField("merge_duration_ms", LongType(), nullable=True),
    StructField("total_duration_ms", LongType(), nullable=True),
    StructField("avg_rows_per_second", FloatType(), nullable=True),
    StructField("data_size_mb", FloatType(), nullable=True),
    StructField("throughput_mb_per_second", FloatType(), nullable=True),
    # Error tracking
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