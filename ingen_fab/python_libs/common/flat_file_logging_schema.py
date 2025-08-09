# Centralized schema definition for flat file ingestion logging
# This ensures consistency between DDL creation and logging operations

from pyspark.sql.types import (
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_flat_file_ingestion_log_schema() -> StructType:
    """
    Returns the standardized schema for the log_flat_file_ingestion table.
    This schema is used both for table creation (DDL) and for logging operations.
    """
    return StructType(
        [
            StructField("log_id", StringType(), nullable=False),
            StructField("config_id", StringType(), nullable=False),
            StructField("execution_id", StringType(), nullable=False),
            StructField("job_start_time", TimestampType(), nullable=False),
            StructField("job_end_time", TimestampType(), nullable=True),
            StructField(
                "status", StringType(), nullable=False
            ),  # running, completed, failed, cancelled
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
            StructField(
                "row_count_reconciliation_status", StringType(), nullable=True
            ),  # matched, mismatched, not_verified
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
            # Partition tracking for incremental loads
            StructField(
                "source_file_partition_cols", StringType(), nullable=True
            ),  # JSON array of partition column names
            StructField(
                "source_file_partition_values", StringType(), nullable=True
            ),  # JSON array of partition values
            StructField(
                "date_partition", StringType(), nullable=True
            ),  # Extracted date partition (YYYY-MM-DD format)
            StructField("created_date", TimestampType(), nullable=False),
            StructField("created_by", StringType(), nullable=False),
        ]
    )


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_flat_file_ingestion_log_schema()
    return [field.name for field in schema.fields]
