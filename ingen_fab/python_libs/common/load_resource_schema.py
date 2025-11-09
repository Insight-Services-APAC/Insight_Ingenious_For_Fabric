# Centralized schema definition for loading resource logging
# Resource-level loading tracking - summary of loading run per resource

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_load_resource_schema() -> StructType:
    """
    Returns the standardized schema for the log_resource_load table.
    This schema tracks each resource execution run (one record per resource per execution).
    Uses UPDATE pattern for state tracking (Airflow-style).

    Primary Key: load_run_id
    Partitioned By: source_name, resource_name (for isolation and performance)
    """
    return StructType(
        [
            # Primary key (composite)
            StructField("load_run_id", StringType(), nullable=False),  # PK - Resource run
            StructField("master_execution_id", StringType(), nullable=False),  # Orchestrator execution
            StructField("source_name", StringType(), nullable=False),  # Source system (e.g., "edl", "sap")
            StructField("resource_name", StringType(), nullable=False),  # Resource/table name
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


def get_loading_run_schema() -> StructType:
    """
    Backwards compatibility alias for get_load_resource_schema().

    DEPRECATED: Use get_load_resource_schema() instead.
    """
    return get_load_resource_schema()


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_load_resource_schema()
    return [field.name for field in schema.fields]
