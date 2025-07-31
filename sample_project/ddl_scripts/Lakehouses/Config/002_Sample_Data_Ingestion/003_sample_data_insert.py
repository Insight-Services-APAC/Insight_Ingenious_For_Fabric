# Sample configuration data for flat file ingestion testing - Universal schema (Lakehouse version)
from pyspark.sql import Row

# Import the universal schema definition
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

# Sample configuration records for testing - Using synthetic data generator files
sample_configs = [
    Row(
        config_id="synthetic_customers_folder_001",
        config_name="Synthetic Data - Customers Folder (Retail OLTP Small)",
        source_file_path="synthetic_data/single/retail_oltp_small/customers.csv",
        source_file_format="csv",
        target_workspace_id="{{varlib:config_workspace_id}}",
        target_datastore_id="{{varlib:config_lakehouse_id}}",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="synthetic_customers",
        staging_table_name=None,
        file_delimiter=",",
        has_header=True,
        encoding="utf-8",
        date_format="yyyy-MM-dd",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        schema_inference=True,
        custom_schema_json=None,
        partition_columns="",
        sort_columns="customer_id",
        write_mode="overwrite",
        merge_keys="",
        data_validation_rules=None,
        error_handling_strategy="log",
        execution_group=1,
        active_yn="Y",
        created_date="2024-01-15",
        modified_date=None,
        created_by="system",
        modified_by=None,
        quote_character='"',
        escape_character='"',
        multiline_values=True,
        ignore_leading_whitespace=False,
        ignore_trailing_whitespace=False,
        null_value="",
        empty_value="",
        comment_character=None,
        max_columns=100,
        max_chars_per_column=50000,
        # New fields for incremental synthetic data import support
        import_pattern="single_file",
        date_partition_format=None,
        table_relationship_group="retail_oltp_single",
        batch_import_enabled=False,
        file_discovery_pattern=None,
        import_sequence_order=1,
        date_range_start=None,
        date_range_end=None,
        skip_existing_dates=None,
        source_is_folder=True  # Read all part files from the folder
    ),
    Row(
        config_id="synthetic_customers_file_001", 
        config_name="Synthetic Data - Customers Single File (Retail OLTP Small)",
        source_file_path="Files/synthetic_data/single/retail_oltp_small/customers.csv/part-00000-51f889c5-7ce6-41ed-b41e-f772a1e181a9-c000.csv",
        source_file_format="csv",
        target_workspace_id="{{varlib:config_workspace_id}}",
        target_datastore_id="{{varlib:config_lakehouse_id}}",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="synthetic_customers_single",
        staging_table_name=None,
        file_delimiter=",",
        has_header=True,
        encoding="utf-8",
        date_format="yyyy-MM-dd",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        schema_inference=True,
        custom_schema_json=None,
        partition_columns="",
        sort_columns="customer_id",
        write_mode="overwrite",
        merge_keys="",
        data_validation_rules=None,
        error_handling_strategy="log",
        execution_group=2,
        active_yn="N",  # Disabled by default to test folder read first
        created_date="2024-01-15",
        modified_date=None,
        created_by="system",
        modified_by=None,
        quote_character='"',
        escape_character='"',
        multiline_values=True,
        ignore_leading_whitespace=False,
        ignore_trailing_whitespace=False,
        null_value="",
        empty_value="",
        comment_character=None,
        max_columns=100,
        max_chars_per_column=50000,
        # New fields for incremental synthetic data import support
        import_pattern="single_file",
        date_partition_format=None,
        table_relationship_group="retail_oltp_single",
        batch_import_enabled=False,
        file_discovery_pattern=None,
        import_sequence_order=1,
        date_range_start=None,
        date_range_end=None,
        skip_existing_dates=None,
        source_is_folder=False  # Read single specific file
    ),
    # Incremental data configurations - Orders (transactional/incremental)
    Row(
        config_id="synthetic_orders_incremental_001",
        config_name="Synthetic Data - Orders Incremental (Retail OLTP Small)",
        source_file_path="synthetic_data/incremental/retail_oltp_small/files/incremental/",
        source_file_format="parquet",
        target_workspace_id="{{varlib:config_workspace_id}}",
        target_datastore_id="{{varlib:config_lakehouse_id}}",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="synthetic_orders_incremental",
        staging_table_name=None,
        file_delimiter=None,
        has_header=None,
        encoding="utf-8",
        date_format="yyyy-MM-dd",
        timestamp_format="yyyy-MM-dd HH:mm:ss.SSSSSS",
        schema_inference=True,
        custom_schema_json=None,
        partition_columns="order_date",
        sort_columns="order_id",
        write_mode="append",
        merge_keys="order_id",
        data_validation_rules=None,
        error_handling_strategy="log",
        execution_group=3,
        active_yn="Y",
        created_date="2024-01-15",
        modified_date=None,
        created_by="system",
        modified_by=None,
        quote_character=None,
        escape_character=None,
        multiline_values=None,
        ignore_leading_whitespace=None,
        ignore_trailing_whitespace=None,
        null_value=None,
        empty_value=None,
        comment_character=None,
        max_columns=None,
        max_chars_per_column=None,
        # Incremental data import configuration
        import_pattern="date_partitioned",
        date_partition_format="YYYYMMDD",
        table_relationship_group="retail_oltp_incremental",
        batch_import_enabled=True,
        file_discovery_pattern="orders_*.parquet",
        import_sequence_order=1,
        date_range_start="2024-01-01",
        date_range_end="2024-01-03",
        skip_existing_dates=True,
        source_is_folder=True  # Read all part files from each dated folder
    )
]

# Create DataFrame and insert records
df = target_lakehouse.get_connection.createDataFrame(sample_configs, schema)
target_lakehouse.write_to_table(
    df=df,
    table_name="config_flat_file_ingestion",
    mode="append"
)

print("✓ Inserted " + str(len(sample_configs)) + " sample configuration records using synthetic data generator files")
print("✓ Includes configurations demonstrating:")
print("  - Folder reading: Read all part files from a Spark-generated folder")
print("  - Single file reading: Read a specific part file")
print("  - Incremental data processing: Date-partitioned orders and order_items with append mode")
print("  - Snapshot data processing: Date-partitioned customers and products with overwrite mode")
print("  - Both single (CSV) and incremental/snapshot (Parquet) data formats")
print("  - All configurations use the retail OLTP synthetic dataset")