# Centralized schema definition for extraction run logging
# Resource-level extraction tracking - summary of extraction run per resource

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_extraction_run_schema() -> StructType:
    """
    Returns the standardized schema for the log_extract_run table.

    This table tracks resource-level extraction runs (one row per resource per execution).
    Uses UPDATE pattern for state tracking (Airflow-style).

    Primary Key: (execution_id, source_name, resource_name)
    Partitioned By: source_name, resource_name (for isolation and performance)
    """
    return StructType(
        [
            # Primary key (composite)
            StructField("execution_id", StringType(), nullable=False),  # Orchestrator run ID
            StructField("source_name", StringType(), nullable=False),  # Source system (e.g., "edl", "sap")
            StructField("resource_name", StringType(), nullable=False),  # Resource/table name
            StructField(
                "status", StringType(), nullable=False
            ),  # running, completed, failed, no_data
            # File counts
            StructField("files_discovered", IntegerType(), nullable=True),  # Files found in source
            StructField("files_promoted", IntegerType(), nullable=True),  # Successfully extracted
            StructField("files_failed", IntegerType(), nullable=True),  # Failed extraction
            StructField("files_duplicated", IntegerType(), nullable=True),  # Skipped duplicates
            # Data metrics
            StructField("total_bytes", LongType(), nullable=True),  # Total data size
            StructField("duration_ms", LongType(), nullable=True),  # Total extraction time
            # Error tracking
            StructField("error_message", StringType(), nullable=True),
            # Timestamp tracking (append-only pattern)
            StructField(
                "started_at", TimestampType(), nullable=False
            ),  # When this log entry was created
            StructField(
                "updated_at", TimestampType(), nullable=False
            ),  # Same as started_at (immutable)
            StructField(
                "completed_at", TimestampType(), nullable=True
            ),  # Set when status is final
        ]
    )


def get_extraction_config_log_schema() -> StructType:
    """
    Backwards compatibility alias for get_extraction_run_schema().

    DEPRECATED: Use get_extraction_run_schema() instead.
    """
    return get_extraction_run_schema()


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_extraction_run_schema()
    return [field.name for field in schema.fields]
