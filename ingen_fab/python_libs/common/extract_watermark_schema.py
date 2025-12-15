# Centralized schema definition for extraction watermark tracking
# Tracks last extracted watermark value for incremental database extraction

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_extract_watermark_schema() -> StructType:
    """
    Returns the standardized schema for the log_extract_watermark table.

    This table tracks watermark state for incremental database extraction.
    One row per (source_name, resource_name) combination.

    Key Pattern:
    - Uses MERGE/UPSERT pattern (insert if new, update if exists)
    - Watermark value stored as string for type flexibility
    - Previous watermark enables rollback on failure

    Primary Key: (source_name, resource_name)
    Partitioned By: source_name, resource_name (for query performance)
    """
    return StructType(
        [
            # Primary key
            StructField("source_name", StringType(), nullable=False),  # PK - source system
            StructField("resource_name", StringType(), nullable=False),  # PK - resource/table name
            # Watermark configuration
            StructField(
                "watermark_column", StringType(), nullable=False
            ),  # Column used for watermark (e.g., 'updated_at')
            StructField(
                "watermark_type", StringType(), nullable=False
            ),  # 'timestamp', 'date', 'integer', 'string'
            # Watermark state
            StructField(
                "watermark_value", StringType(), nullable=False
            ),  # Last extracted value (stored as string, cast on read)
            StructField(
                "previous_watermark_value", StringType(), nullable=True
            ),  # Previous value for rollback
            # Audit trail
            StructField(
                "updated_at", TimestampType(), nullable=False
            ),  # When watermark was updated
            StructField(
                "extract_batch_id", StringType(), nullable=True
            ),  # FK to log_resource_extract_batch
        ]
    )


def get_column_names() -> list[str]:
    """
    Returns the list of column names in the correct order for the log table.
    Useful for creating tuples of data in the correct order.
    """
    schema = get_extract_watermark_schema()
    return [field.name for field in schema.fields]
