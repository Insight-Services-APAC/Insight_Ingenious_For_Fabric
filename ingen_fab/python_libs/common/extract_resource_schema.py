# Centralized schema definition for extraction resource logging
# Resource-level extraction tracking - summary of extraction run per resource

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_extract_resource_schema() -> StructType:
    """
    Returns the standardized schema for the log_resource_extract table.

    This table tracks resource-level extraction runs (one row per resource per execution).
    Uses UPDATE pattern for state tracking (Airflow-style).

    Primary Key: extract_run_id
    Partitioned By: source_name, resource_name (for isolation and performance)
    """
    return StructType(
        [
            # Primary key and identifiers
            StructField("extract_run_id", StringType(), nullable=False),  # PK - Resource run
            StructField("master_execution_id", StringType(), nullable=False),  # Orchestrator execution
            StructField("source_name", StringType(), nullable=False),  # Source system (e.g., "edl", "sap")
            StructField("resource_name", StringType(), nullable=False),  # Resource/table name
            StructField(
                "extract_state", StringType(), nullable=False
            ),  # running, completed, failed, no_data
            # Error tracking
            StructField("error_message", StringType(), nullable=True),
            # Timestamp tracking (append-only pattern)
            StructField(
                "started_at", TimestampType(), nullable=False
            ),  # When this log entry was created
            StructField(
                "updated_at", TimestampType(), nullable=False
            ),  # Updated on status change
            StructField(
                "completed_at", TimestampType(), nullable=True
            ),  # Set when status is final
        ]
    )


def get_extraction_run_schema() -> StructType:
    """
    Backwards compatibility alias for get_extract_resource_schema().

    DEPRECATED: Use get_extract_resource_schema() instead.
    """
    return get_extract_resource_schema()


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_extract_resource_schema()
    return [field.name for field in schema.fields]
