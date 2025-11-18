# Centralized schema definition for extraction batch logging
# Batch-level extraction tracking - what files were extracted and where they landed

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_extract_resource_batch_schema() -> StructType:
    """
    Returns the standardized schema for the log_resource_extract_batch table.

    This table tracks individual extraction batches (file or folder level).
    Acts as the work queue for the loading orchestrator.

    Key Pattern (dlt-inspired):
    - Extraction writes: "I promoted file/folder X to destination_path Y"
    - Loading queries: "Show me completed extractions not yet loaded"

    Primary Key: extract_batch_id
    Foreign Key: extract_run_id → log_resource_extract
    Partitioned By: source_name, resource_name (for isolation and performance)
    """
    return StructType(
        [
            # Primary key and identifiers
            StructField("extract_batch_id", StringType(), nullable=False),  # PK - unique batch ID
            StructField("extract_run_id", StringType(), nullable=False),  # FK to log_resource_extract
            StructField("master_execution_id", StringType(), nullable=False),  # Orchestrator execution (denormalized)
            StructField("source_name", StringType(), nullable=False),  # Source system (denormalized for partitioning)
            StructField("resource_name", StringType(), nullable=False),  # Resource/table name (denormalized for partitioning)
            StructField(
                "extract_state", StringType(), nullable=False
            ),  # running, completed, failed, duplicate
            StructField(
                "load_state", StringType(), nullable=False
            ),  # pending, loading, completed, failed, skipped - tracks downstream loading status
            # Path information (CRITICAL for loading)
            StructField(
                "source_path", StringType(), nullable=True
            ),  # Original location (inbound)
            StructField(
                "extract_file_paths", ArrayType(StringType()), nullable=False
            ),  # Files/folders promoted to raw - LOADER READS THIS
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
        ]
    )


def get_extraction_batch_schema() -> StructType:
    """
    Backwards compatibility alias for get_extract_resource_batch_schema().

    DEPRECATED: Use get_extract_resource_batch_schema() instead.
    """
    return get_extract_resource_batch_schema()


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_extract_resource_batch_schema()
    return [field.name for field in schema.fields]
