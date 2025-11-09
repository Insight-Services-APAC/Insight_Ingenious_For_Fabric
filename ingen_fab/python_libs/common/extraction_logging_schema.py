# Centralized schema definition for extraction batch logging
# Batch-level extraction tracking - what files were extracted and where they landed

from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_extraction_batch_schema() -> StructType:
    """
    Returns the standardized schema for the log_extract_batch table.

    This table tracks individual extraction batches (file or folder level).
    Acts as the work queue for the loading orchestrator.

    Key Pattern (dlt-inspired):
    - Extraction writes: "I promoted file/folder X to destination_path Y"
    - Loading queries: "Show me completed extractions not yet loaded"

    Primary Key: extraction_id
    Partitioned By: source_name, resource_name (for isolation and performance)
    """
    return StructType(
        [
            # Primary key and identifiers
            StructField("extraction_id", StringType(), nullable=False),  # PK - unique batch ID
            StructField("execution_id", StringType(), nullable=False),  # Orchestrator run ID
            StructField("source_name", StringType(), nullable=False),  # Source system (e.g., "edl", "sap")
            StructField("resource_name", StringType(), nullable=False),  # Resource/table name
            StructField(
                "status", StringType(), nullable=False
            ),  # running, completed, failed, duplicate
            StructField(
                "load_state", StringType(), nullable=False
            ),  # pending, loading, completed, failed, skipped - tracks downstream loading status
            # Path information (CRITICAL for loading)
            StructField(
                "source_path", StringType(), nullable=True
            ),  # Original location (inbound, API, DB)
            StructField(
                "destination_path", StringType(), nullable=False
            ),  # Where promoted (raw/landing) - LOADER READS THIS
            # Batch metrics
            StructField("file_count", IntegerType(), nullable=True),  # Files in batch
            StructField("file_size_bytes", LongType(), nullable=True),  # Total size
            StructField("promoted_count", IntegerType(), nullable=True),  # Successfully promoted
            StructField("failed_count", IntegerType(), nullable=True),  # Failed to promote
            StructField("duplicate_count", IntegerType(), nullable=True),  # Skipped duplicates
            # Timing
            StructField(
                "started_at", TimestampType(), nullable=False
            ),  # When extraction started
            StructField(
                "completed_at", TimestampType(), nullable=True
            ),  # When extraction finished
            StructField("duration_ms", LongType(), nullable=True),  # Extraction duration
            # Error tracking
            StructField("error_message", StringType(), nullable=True),
            StructField("error_details", StringType(), nullable=True),
        ]
    )


def get_extraction_log_schema() -> StructType:
    """
    Backwards compatibility alias for get_extraction_batch_schema().

    DEPRECATED: Use get_extraction_batch_schema() instead.
    """
    return get_extraction_batch_schema()


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_extraction_batch_schema()
    return [field.name for field in schema.fields]
