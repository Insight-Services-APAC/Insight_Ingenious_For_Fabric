# Centralized schema definition for file load logging
# This ensures consistency between DDL creation and logging operations

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_file_load_log_schema() -> StructType:
    """
    Returns the standardized schema for the log_file_load table.
    This schema tracks individual file loads (one record per file, updated via merge).
    Uses Airflow-style single-row-per-execution pattern with status updates.

    Primary Key: load_id
    """
    return StructType(
        [
            # Primary key and identifiers
            StructField("load_id", StringType(), nullable=False),  # PK
            StructField("execution_id", StringType(), nullable=False),
            StructField("config_id", StringType(), nullable=False),
            StructField(
                "status", StringType(), nullable=False
            ),  # running, completed, failed, duplicate, skipped
            # Source file information
            StructField("source_file_path", StringType(), nullable=False),
            StructField("source_file_size_bytes", LongType(), nullable=True),
            StructField("source_file_modified_time", TimestampType(), nullable=True),
            StructField("target_table_name", StringType(), nullable=False),
            # Processing metrics
            StructField("records_processed", LongType(), nullable=True),
            StructField("records_inserted", LongType(), nullable=True),
            StructField("records_updated", LongType(), nullable=True),
            StructField("records_deleted", LongType(), nullable=True),
            # Performance metrics
            StructField("source_row_count", LongType(), nullable=True),
            StructField("target_row_count_before", LongType(), nullable=True),
            StructField("target_row_count_after", LongType(), nullable=True),
            StructField(
                "row_count_reconciliation_status", StringType(), nullable=True
            ),  # matched, mismatched, not_verified
            StructField("corrupt_records_count", LongType(), nullable=True),
            StructField("data_read_duration_ms", LongType(), nullable=True),
            StructField("total_duration_ms", LongType(), nullable=True),
            # Error tracking
            StructField("error_message", StringType(), nullable=True),
            StructField("error_details", StringType(), nullable=True),
            StructField("execution_duration_seconds", IntegerType(), nullable=True),
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
            StructField(
                "filename_attributes_json", StringType(), nullable=True
            ),  # Extracted filename attributes as JSON
            StructField(
                "control_file_path", StringType(), nullable=True
            ),  # Path to associated control file if required
            # Timestamp tracking (Airflow-style)
            StructField(
                "started_at", TimestampType(), nullable=False
            ),  # Immutable - set on first insert
            StructField(
                "updated_at", TimestampType(), nullable=False
            ),  # Always updated on merge
            StructField(
                "completed_at", TimestampType(), nullable=True
            ),  # Set when status becomes completed/failed
            StructField(
                "attempt_count", IntegerType(), nullable=False
            ),  # Incremented on retries
        ]
    )


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_file_load_log_schema()
    return [field.name for field in schema.fields]
