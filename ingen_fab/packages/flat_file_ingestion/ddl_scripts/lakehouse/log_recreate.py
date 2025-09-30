# Recreate log table with updated schema for flat file ingestion execution tracking
# This script drops and recreates the table to add the partition tracking columns

# Drop existing table if it exists
try:
    target_lakehouse.spark.sql("DROP TABLE IF EXISTS log_flat_file_ingestion")  # noqa: F821
    print("✓ Dropped existing log_flat_file_ingestion table")
except Exception as e:
    print(f"Warning: Could not drop existing table: {e}")

# Import centralized schema to ensure consistency
from ingen_fab.python_libs.common.flat_file_logging_schema import (
    get_flat_file_ingestion_log_schema,
)

schema = get_flat_file_ingestion_log_schema()

target_lakehouse.create_table(  # noqa: F821
    table_name="log_flat_file_ingestion",
    schema=schema,
    mode="overwrite",
    options={"parquet.vorder.default": "true"},
)

print("✓ Created log_flat_file_ingestion table with updated schema including partition tracking columns")
print("✓ Table now includes: source_file_partition_cols, source_file_partition_values, date_partition")
