# Centralized schema definition for config execution logging
# This ensures consistency between DDL creation and logging operations

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_config_execution_log_schema() -> StructType:
    """
    Returns the standardized schema for the log_config_execution table.
    This schema tracks each configuration execution run (one record per config per execution, updated via merge).
    Uses Airflow-style single-row-per-execution pattern with status updates.

    Primary Key: (execution_id, config_id)
    """
    return StructType(
        [
            # Primary key (composite)
            StructField("execution_id", StringType(), nullable=False),
            StructField("config_id", StringType(), nullable=False),
            StructField(
                "status", StringType(), nullable=False
            ),  # running, completed, failed, no_data
            # File counts
            StructField("files_discovered", LongType(), nullable=True),
            StructField("files_processed", LongType(), nullable=True),
            StructField("files_failed", LongType(), nullable=True),
            StructField("files_skipped", LongType(), nullable=True),
            # Aggregated metrics across all files in this execution
            StructField("records_processed", LongType(), nullable=True),
            StructField("records_inserted", LongType(), nullable=True),
            StructField("records_updated", LongType(), nullable=True),
            StructField("records_deleted", LongType(), nullable=True),
            StructField("total_duration_ms", LongType(), nullable=True),
            # Error tracking
            StructField("error_message", StringType(), nullable=True),
            StructField("error_details", StringType(), nullable=True),
            # Timestamp tracking (Airflow-style)
            StructField(
                "started_at", TimestampType(), nullable=False
            ),  # Immutable - set on first insert
            StructField(
                "updated_at", TimestampType(), nullable=False
            ),  # Always updated on merge
            StructField(
                "completed_at", TimestampType(), nullable=True
            ),  # Set when status becomes completed/failed/no_data
        ]
    )


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_config_execution_log_schema()
    return [field.name for field in schema.fields]
