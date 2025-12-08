# Configuration table for export resources - Lakehouse version
# Stores export configuration for exporting tables to files

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

# Define schema for config_export_resource table
schema = StructType([
    # Primary identifiers (composite key: export_group_name + export_name)
    StructField("export_group_name", StringType(), False),
    StructField("export_name", StringType(), False),
    StructField("is_active", BooleanType(), False),
    StructField("execution_group", IntegerType(), False),

    # Source configuration
    StructField("source_type", StringType(), False),  # "lakehouse" or "warehouse"
    StructField("source_workspace", StringType(), False),
    StructField("source_datastore", StringType(), False),
    StructField("source_schema", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("source_query", StringType(), True),

    # Target configuration (Lakehouse Files only)
    StructField("target_workspace", StringType(), False),
    StructField("target_lakehouse", StringType(), False),
    StructField("target_path", StringType(), False),
    StructField("target_filename_pattern", StringType(), True),

    # File format configuration
    StructField("file_format", StringType(), False),  # csv, tsv, dat, parquet, json
    StructField("compression", StringType(), True),  # gzip, zip, snappy, none
    StructField("format_options", MapType(StringType(), StringType()), True),

    # File splitting (optional)
    StructField("max_rows_per_file", IntegerType(), True),
    StructField("partition_by", StringType(), True),  # Comma-separated column names

    # Trigger file configuration (optional)
    StructField("trigger_file_enabled", BooleanType(), True),
    StructField("trigger_file_pattern", StringType(), True),

    # Metadata
    StructField("description", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
    StructField("created_by", StringType(), True),
    StructField("updated_by", StringType(), True),
])

target_lakehouse.create_table(
    table_name="config_export_resource",
    schema=schema,
    mode="overwrite",
    partition_by=["export_group_name", "export_name"],
    options={
        "parquet.vorder.default": "true",
        "overwriteSchema": "true"
    }
)