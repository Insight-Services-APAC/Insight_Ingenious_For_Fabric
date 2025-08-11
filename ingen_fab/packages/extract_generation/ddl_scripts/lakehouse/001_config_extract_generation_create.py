# Configuration tables for extract generation metadata - Lakehouse version
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Extract configuration main table schema
extract_config_schema = StructType(
    [
        StructField("extract_name", StringType(), nullable=False),
        StructField("is_active", BooleanType(), nullable=False),
        StructField("trigger_name", StringType(), nullable=True),
        StructField("extract_pipeline_name", StringType(), nullable=True),
        # Source configuration
        StructField("extract_table_name", StringType(), nullable=True),
        StructField("extract_table_schema", StringType(), nullable=True),
        StructField("extract_view_name", StringType(), nullable=True),
        StructField("extract_view_schema", StringType(), nullable=True),
        # Load configuration
        StructField("is_full_load", BooleanType(), nullable=False),
        StructField("execution_group", StringType(), nullable=True),
        # Audit fields
        StructField("created_date", StringType(), nullable=False),
        StructField("created_by", StringType(), nullable=False),
        StructField("modified_date", StringType(), nullable=True),
        StructField("modified_by", StringType(), nullable=True),
    ]
)

# Extract file details configuration schema
extract_details_schema = StructType(
    [
        StructField("extract_name", StringType(), nullable=False),
        StructField("file_generation_group", StringType(), nullable=True),
        StructField("extract_container", StringType(), nullable=False),
        StructField("extract_directory", StringType(), nullable=True),
        # File naming
        StructField("extract_file_name", StringType(), nullable=False),
        StructField("extract_file_name_timestamp_format", StringType(), nullable=True),
        StructField("extract_file_name_period_end_day", IntegerType(), nullable=True),
        StructField("extract_file_name_extension", StringType(), nullable=False),
        StructField("extract_file_name_ordering", IntegerType(), nullable=True),
        # File properties
        StructField("file_properties_column_delimiter", StringType(), nullable=False),
        StructField("file_properties_row_delimiter", StringType(), nullable=False),
        StructField("file_properties_encoding", StringType(), nullable=False),
        StructField("file_properties_quote_character", StringType(), nullable=False),
        StructField("file_properties_escape_character", StringType(), nullable=False),
        StructField("file_properties_header", BooleanType(), nullable=False),
        StructField("file_properties_null_value", StringType(), nullable=False),
        StructField("file_properties_max_rows_per_file", IntegerType(), nullable=True),
        # Output format
        StructField("output_format", StringType(), nullable=False),
        # Trigger file
        StructField("is_trigger_file", BooleanType(), nullable=False),
        StructField("trigger_file_extension", StringType(), nullable=True),
        # Compression
        StructField("is_compressed", BooleanType(), nullable=False),
        StructField("compressed_type", StringType(), nullable=True),
        StructField("compressed_level", StringType(), nullable=True),
        StructField("compressed_file_name", StringType(), nullable=True),
        StructField("compressed_extension", StringType(), nullable=True),
        # Source location fields (for reading data)
        StructField("source_workspace_id", StringType(), nullable=True),
        StructField("source_datastore_id", StringType(), nullable=True),
        StructField(
            "source_datastore_type", StringType(), nullable=True
        ),  # 'lakehouse' or 'warehouse'
        StructField("source_schema_name", StringType(), nullable=True),
        # Target location fields (for writing extract files)
        StructField("target_workspace_id", StringType(), nullable=True),
        StructField("target_datastore_id", StringType(), nullable=True),
        StructField(
            "target_datastore_type", StringType(), nullable=True
        ),  # 'lakehouse' or 'warehouse'
        StructField(
            "target_file_root_path", StringType(), nullable=True
        ),  # e.g., 'Files', 'Tables'
        # Legacy field for backward compatibility
        StructField("fabric_lakehouse_path", StringType(), nullable=True),
        # Performance options
        StructField("force_single_file", BooleanType(), nullable=True),
        # Audit fields
        StructField("created_date", StringType(), nullable=False),
        StructField("created_by", StringType(), nullable=False),
        StructField("modified_date", StringType(), nullable=True),
        StructField("modified_by", StringType(), nullable=True),
    ]
)

# Create extract configuration table
target_lakehouse.create_table(  # noqa: F821
    table_name="config_extract_generation",
    schema=extract_config_schema,
    mode="overwrite",
    options={"parquet.vorder.default": "true"},
)

# Create extract details configuration table
target_lakehouse.create_table(  # noqa: F821
    table_name="config_extract_generation_details",
    schema=extract_details_schema,
    mode="overwrite",
    options={"parquet.vorder.default": "true"},
)

print("✓ Created extract generation configuration tables:")
print("  - config_extract_generation")
print("  - config_extract_generation_details")
