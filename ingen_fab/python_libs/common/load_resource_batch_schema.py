# Centralized schema definition for loading batch logging
# Batch-level loading tracking - what files were loaded to bronze tables

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_load_resource_batch_schema() -> StructType:
    """
    Returns the standardized schema for the log_resource_load_batch table.
    This schema tracks individual file loads (one record per batch).
    Uses UPDATE pattern for state tracking (Airflow-style).

    Primary Key: load_batch_id
    Foreign Key: load_run_id → log_resource_load
    Foreign Key: extract_batch_id → log_resource_extract_batch
    Partitioned By: source_name, resource_name (for isolation and performance)
    """
    return StructType(
        [
            # Primary key and identifiers
            StructField("load_batch_id", StringType(), nullable=False),  # PK
            StructField("load_run_id", StringType(), nullable=False),  # FK to log_resource_load
            StructField("master_execution_id", StringType(), nullable=False),  # Orchestrator execution (denormalized)
            StructField("extract_batch_id", StringType(), nullable=True),  # FK to log_resource_extract_batch
            StructField("source_name", StringType(), nullable=False),  # Source system (denormalized for partitioning)
            StructField("resource_name", StringType(), nullable=False),  # Resource/table name (denormalized for partitioning)
            StructField(
                "load_state", StringType(), nullable=False
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
            StructField("execution_duration_seconds", IntegerType(), nullable=True),
            # Metadata
            StructField(
                "filename_attributes_json", StringType(), nullable=True
            ),  # Extracted filename attributes as JSON
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


def get_loading_batch_schema() -> StructType:
    """
    Backwards compatibility alias for get_load_resource_batch_schema().

    DEPRECATED: Use get_load_resource_batch_schema() instead.
    """
    return get_load_resource_batch_schema()


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_load_resource_batch_schema()
    return [field.name for field in schema.fields]
