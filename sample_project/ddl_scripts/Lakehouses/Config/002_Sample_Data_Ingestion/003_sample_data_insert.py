# Sample configuration data for flat file ingestion testing - Lakehouse version
from pyspark.sql import Row

# Import the schema definition
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
    StructField("target_lakehouse_workspace_id", StringType(), nullable=False),
    StructField("target_lakehouse_id", StringType(), nullable=False),
    StructField("target_schema_name", StringType(), nullable=False),
    StructField("target_table_name", StringType(), nullable=False),
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
    StructField("modified_by", StringType(), nullable=True)
])

# Sample configuration records for testing
sample_configs = [
    Row(
        config_id="csv_test_001",
        config_name="CSV Sales Data Test",
        source_file_path="Files/sample_data/sales_data.csv",
        source_file_format="csv",
        target_lakehouse_workspace_id="{{varlib:config_workspace_id}}",
        target_lakehouse_id="{{varlib:config_lakehouse_id}}",
        target_schema_name="raw",
        target_table_name="sales_data",
        file_delimiter=",",
        has_header=True,
        encoding="utf-8",
        date_format="yyyy-MM-dd",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        schema_inference=True,
        custom_schema_json=None,
        partition_columns="",
        sort_columns="date",
        write_mode="overwrite",
        merge_keys="",
        data_validation_rules=None,
        error_handling_strategy="fail",
        execution_group=1,
        active_yn="Y",
        created_date="2024-01-15",
        modified_date=None,
        created_by="system",
        modified_by=None
    ),
    Row(
        config_id="json_test_002",
        config_name="JSON Products Data Test",
        source_file_path="Files/sample_data/products.json",
        source_file_format="json",
        target_lakehouse_workspace_id="{{varlib:config_workspace_id}}",
        target_lakehouse_id="{{varlib:config_lakehouse_id}}",
        target_schema_name="raw",
        target_table_name="products",
        file_delimiter=None,
        has_header=None,
        encoding="utf-8",
        date_format="yyyy-MM-dd",
        timestamp_format="yyyy-MM-dd'T'HH:mm:ss'Z'",
        schema_inference=True,
        custom_schema_json=None,
        partition_columns="category",
        sort_columns="product_id",
        write_mode="overwrite",
        merge_keys="",
        data_validation_rules=None,
        error_handling_strategy="log",
        execution_group=1,
        active_yn="Y",
        created_date="2024-01-15",
        modified_date=None,
        created_by="system",
        modified_by=None
    ),
    Row(
        config_id="parquet_test_003",
        config_name="Parquet Customers Data Test",
        source_file_path="Files/sample_data/customers.parquet",
        source_file_format="parquet",
        target_lakehouse_workspace_id="{{varlib:config_workspace_id}}",
        target_lakehouse_id="{{varlib:config_lakehouse_id}}",
        target_schema_name="raw",
        target_table_name="customers",
        file_delimiter=None,
        has_header=None,
        encoding=None,
        date_format="yyyy-MM-dd",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        schema_inference=True,
        custom_schema_json=None,
        partition_columns="region",
        sort_columns="customer_id",
        write_mode="overwrite",
        merge_keys="",
        data_validation_rules=None,
        error_handling_strategy="fail",
        execution_group=2,
        active_yn="Y",        
        created_date="2024-01-15",
        modified_date=None,
        created_by="system",
        modified_by=None
    )
]

# Create DataFrame and insert records
df = target_lakehouse.get_connection.createDataFrame(sample_configs, schema)
target_lakehouse.write_to_table(
    df=df,
    table_name="config_flat_file_ingestion",
    mode="append"
)

print("✓ Inserted " + str(len(sample_configs)) + " sample configuration records")