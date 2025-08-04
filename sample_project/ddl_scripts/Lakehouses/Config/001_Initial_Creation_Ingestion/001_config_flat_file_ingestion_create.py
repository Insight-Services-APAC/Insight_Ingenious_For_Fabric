# Configuration table for flat file ingestion metadata - Universal schema (Lakehouse version)
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType([
    StructField("config_id", StringType(), nullable=False),
    StructField("config_name", StringType(), nullable=False),
    StructField("source_file_path", StringType(), nullable=False),
    StructField("source_file_format", StringType(), nullable=False),  # csv, json, parquet, avro, xml
    # Source location fields (optional - defaults to target or raw workspace)
    StructField("source_workspace_id", StringType(), nullable=True),  # Source workspace (defaults to target if null)
    StructField("source_datastore_id", StringType(), nullable=True),  # Source lakehouse/warehouse (defaults to raw if null)
    StructField("source_datastore_type", StringType(), nullable=True),  # 'lakehouse' or 'warehouse' (defaults to lakehouse)
    StructField("source_file_root_path", StringType(), nullable=True),  # Root path override (e.g., "Files", "Tables")
    # Target location fields
    StructField("target_workspace_id", StringType(), nullable=False),  # Universal field for workspace
    StructField("target_datastore_id", StringType(), nullable=False),  # Universal field for lakehouse/warehouse
    StructField("target_datastore_type", StringType(), nullable=False),  # 'lakehouse' or 'warehouse'
    StructField("target_schema_name", StringType(), nullable=False),
    StructField("target_table_name", StringType(), nullable=False),
    StructField("staging_table_name", StringType(), nullable=True),  # For warehouse COPY INTO staging
    StructField("file_delimiter", StringType(), nullable=True),  # for CSV files
    StructField("has_header", BooleanType(), nullable=True),  # for CSV files
    StructField("encoding", StringType(), nullable=True),  # utf-8, latin-1, etc.
    StructField("date_format", StringType(), nullable=True),  # for date columns
    StructField("timestamp_format", StringType(), nullable=True),  # for timestamp columns
    StructField("schema_inference", BooleanType(), nullable=False),  # whether to infer schema
    StructField("custom_schema_json", StringType(), nullable=True),  # custom schema definition
    StructField("partition_columns", StringType(), nullable=True),  # comma-separated list
    StructField("sort_columns", StringType(), nullable=True),  # comma-separated list
    StructField("write_mode", StringType(), nullable=False),  # overwrite, append, merge
    StructField("merge_keys", StringType(), nullable=True),  # for merge operations
    StructField("data_validation_rules", StringType(), nullable=True),  # JSON validation rules
    StructField("error_handling_strategy", StringType(), nullable=False),  # fail, skip, log
    StructField("execution_group", IntegerType(), nullable=False),
    StructField("active_yn", StringType(), nullable=False),
    StructField("created_date", StringType(), nullable=False),
    StructField("modified_date", StringType(), nullable=True),
    StructField("created_by", StringType(), nullable=False),
    StructField("modified_by", StringType(), nullable=True),
    # Advanced CSV configuration fields
    StructField("quote_character", StringType(), nullable=True),  # Default: '"'
    StructField("escape_character", StringType(), nullable=True),  # Default: '"' (Excel style)
    StructField("multiline_values", BooleanType(), nullable=True),  # Default: True
    StructField("ignore_leading_whitespace", BooleanType(), nullable=True),  # Default: False
    StructField("ignore_trailing_whitespace", BooleanType(), nullable=True),  # Default: False
    StructField("null_value", StringType(), nullable=True),  # Default: ""
    StructField("empty_value", StringType(), nullable=True),  # Default: ""
    StructField("comment_character", StringType(), nullable=True),  # Default: None
    StructField("max_columns", IntegerType(), nullable=True),  # Default: 100
    StructField("max_chars_per_column", IntegerType(), nullable=True),  # Default: 50000
    # New fields for incremental synthetic data import support
    StructField("import_pattern", StringType(), nullable=True),  # 'single_file', 'date_partitioned', 'wildcard_pattern'
    StructField("date_partition_format", StringType(), nullable=True),  # Date partition format (e.g., 'YYYY/MM/DD')
    StructField("table_relationship_group", StringType(), nullable=True),  # Group for related table imports
    StructField("batch_import_enabled", BooleanType(), nullable=True),  # Enable batch processing
    StructField("file_discovery_pattern", StringType(), nullable=True),  # Pattern for automatic file discovery
    StructField("import_sequence_order", IntegerType(), nullable=True),  # Order for related table imports
    StructField("date_range_start", StringType(), nullable=True),  # Start date for batch import
    StructField("date_range_end", StringType(), nullable=True),  # End date for batch import
    StructField("skip_existing_dates", BooleanType(), nullable=True),  # Skip already imported dates
    StructField("source_is_folder", BooleanType(), nullable=True)  # True for folder with part files, False for single file
])

target_lakehouse.create_table(
    table_name="config_flat_file_ingestion",
    schema=schema,
    mode="overwrite",
    options={
        "parquet.vorder.default": "true"
    }
)