# Log table for extract generation run tracking - Lakehouse version
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    TimestampType,
    StructField,
    StructType,
)

# Extract generation log schema
extract_log_schema = StructType([
    StructField("log_id", StringType(), nullable=False),
    StructField("extract_name", StringType(), nullable=False),
    StructField("execution_group", StringType(), nullable=True),
    StructField("run_id", StringType(), nullable=False),
    StructField("run_timestamp", TimestampType(), nullable=False),
    StructField("run_status", StringType(), nullable=False),  # IN_PROGRESS, SUCCESS, FAILED
    StructField("run_type", StringType(), nullable=False),    # FULL, INCREMENTAL
    StructField("start_time", TimestampType(), nullable=False),
    StructField("end_time", TimestampType(), nullable=True),
    StructField("duration_seconds", IntegerType(), nullable=True),
    StructField("error_message", StringType(), nullable=True),
    
    # Source information
    StructField("source_type", StringType(), nullable=True),  # TABLE, VIEW
    StructField("source_object", StringType(), nullable=True),
    StructField("source_schema", StringType(), nullable=True),
    
    # Extract metrics
    StructField("rows_extracted", IntegerType(), nullable=True),
    StructField("rows_written", IntegerType(), nullable=True),
    StructField("files_generated", IntegerType(), nullable=True),
    
    # Output information
    StructField("output_format", StringType(), nullable=True),
    StructField("output_file_path", StringType(), nullable=True),
    StructField("output_file_name", StringType(), nullable=True),
    StructField("output_file_size_bytes", IntegerType(), nullable=True),
    
    # Compression information
    StructField("is_compressed", BooleanType(), nullable=True),
    StructField("compression_type", StringType(), nullable=True),
    
    # Trigger file information
    StructField("trigger_file_created", BooleanType(), nullable=True),
    StructField("trigger_file_path", StringType(), nullable=True),
    
    # Environment information
    StructField("workspace_id", StringType(), nullable=True),
    StructField("lakehouse_id", StringType(), nullable=True),
    StructField("created_by", StringType(), nullable=False),
])

# Create extract generation log table
target_lakehouse.create_table(
    table_name="log_extract_generation",
    schema=extract_log_schema,
    mode="overwrite",
    options={
        "parquet.vorder.default": "true"
    },
    partition_by=["run_status", "extract_name"]
)

print("✓ Created extract generation log table: log_extract_generation")
print("✓ Partitioned by run_status and extract_name for efficient querying")