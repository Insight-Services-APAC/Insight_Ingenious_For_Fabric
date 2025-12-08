"""PySpark schema definitions for export configuration and log tables."""

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_config_export_resource_schema() -> StructType:
    """
    Returns the standardized schema for the config_export_resource table.

    This table stores export configurations.

    Primary Key: (export_group_name, export_name)
    """
    return StructType(
        [
            # Primary identifiers (composite key)
            StructField("export_group_name", StringType(), False),
            StructField("export_name", StringType(), False),
            StructField("execution_group", IntegerType(), False),
            # Source configuration
            StructField("source_type", StringType(), False),  # "lakehouse" or "warehouse"
            StructField("source_workspace", StringType(), False),
            StructField("source_datastore", StringType(), False),
            StructField("source_schema", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("source_query", StringType(), True),
            # Target configuration
            StructField("target_workspace", StringType(), False),
            StructField("target_lakehouse", StringType(), False),
            StructField("target_path", StringType(), False),
            StructField("target_filename_pattern", StringType(), True),
            # File format configuration
            StructField("file_format", StringType(), False),  # csv, tsv, dat, parquet, json
            StructField("compression", StringType(), True),  # gzip, zipdeflate, snappy, none
            StructField("compressed_filename_pattern", StringType(), True),
            StructField("format_options", MapType(StringType(), StringType()), True),
            # File splitting
            StructField("max_rows_per_file", IntegerType(), True),
            StructField("compress_bundle_files", BooleanType(), True),
            # Extract type configuration (for period date calculation)
            StructField("extract_type", StringType(), True),  # "full", "period", "incremental"
            StructField("period_length_days", IntegerType(), True),
            StructField("period_end_day", StringType(), True),  # e.g., "Saturday"
            StructField("incremental_column", StringType(), True),  # Column for incremental tracking
            # Trigger file configuration
            StructField("trigger_file_enabled", BooleanType(), True),
            StructField("trigger_file_pattern", StringType(), True),
            # Metadata
            StructField("description", StringType(), True),
            StructField("is_active", BooleanType(), False),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
            StructField("updated_by", StringType(), True),
        ]
    )


def get_log_resource_export_schema() -> StructType:
    """
    Returns the standardized schema for the log_resource_export table.

    This table tracks export execution state.

    Primary Key: export_run_id
    """
    return StructType(
        [
            # Primary identifiers
            StructField("export_run_id", StringType(), False),
            StructField("master_execution_id", StringType(), False),
            StructField("export_group_name", StringType(), False),
            StructField("export_name", StringType(), False),
            # Execution state
            StructField(
                "export_state", StringType(), False
            ),  # pending, running, success, warning, error
            # Source information
            StructField("source_type", StringType(), False),
            StructField("source_workspace", StringType(), True),
            StructField("source_datastore", StringType(), True),
            StructField("source_table", StringType(), True),
            # Target information
            StructField("target_path", StringType(), False),
            StructField("file_format", StringType(), True),
            StructField("compression", StringType(), True),
            # Timing
            StructField("started_at", TimestampType(), False),
            StructField("completed_at", TimestampType(), True),
            StructField("duration_ms", LongType(), True),
            # Metrics
            StructField("rows_exported", LongType(), True),
            StructField("files_created", IntegerType(), True),
            StructField("total_bytes", LongType(), True),
            # Output files
            StructField("file_paths", ArrayType(StringType()), True),
            StructField("trigger_file_path", StringType(), True),
            # Error handling
            StructField("error_message", StringType(), True),
            # Audit
            StructField("created_at", TimestampType(), True),
            StructField("created_by", StringType(), True),
        ]
    )
