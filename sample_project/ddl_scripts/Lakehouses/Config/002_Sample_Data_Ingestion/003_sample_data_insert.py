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

# Sample configuration records for testing - Using synthetic data generator files
sample_configs = [
    Row(
        config_id="synthetic_customers_folder_001",
        config_name="Synthetic Data - Customers Folder (Retail OLTP Small)",
        source_file_path="synthetic_data/csv/series/retail_oltp_small/test_data_feb_small/flat/snapshot_customers/snapshot_customers_20240201.csv",
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
        execution_group=1,  # Folder-based snapshot processing (Group 1)
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
        source_file_path="Files/synthetic_data/csv/series/retail_oltp_small/test_data_feb_small/flat/snapshot_customers/snapshot_customers_20240201.csv/part-00000-542fdb30-a52e-447b-ab32-df49d909d873-c000.csv",
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
        execution_group=2,  # Single file processing (Group 2)
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
        source_is_folder=False  # Read single specific file
    ),
    # Incremental data configurations - Orders (transactional/incremental)
    Row(
        config_id="synthetic_orders_incremental_001",
        config_name="Synthetic Data - Orders Incremental (Retail OLTP Small)",
        source_file_path="synthetic_data/csv/series/retail_oltp_small/test_data_feb_small/flat/orders/",
        source_file_format="csv",
        target_workspace_id="{{varlib:config_workspace_id}}",
        target_datastore_id="{{varlib:config_lakehouse_id}}",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="synthetic_orders_incremental",
        staging_table_name=None,
        file_delimiter=",",
        has_header=True,
        encoding="utf-8",
        date_format="yyyy-MM-dd",
        timestamp_format="yyyy-MM-dd HH:mm:ss",
        schema_inference=True,
        custom_schema_json=None,
        partition_columns="order_date",
        sort_columns="order_id",
        write_mode="append",
        merge_keys="order_id",
        data_validation_rules=None,
        error_handling_strategy="log",
        execution_group=3,  # Incremental processing (Group 3)
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
        # Incremental data import configuration
        import_pattern="wildcard_pattern",
        date_partition_format="YYYY/MM/DD",
        table_relationship_group="retail_oltp_incremental",
        batch_import_enabled=True,
        file_discovery_pattern="orders_*.csv",
        import_sequence_order=1,
        date_range_start="2024-01-01",
        date_range_end="2024-01-30",
        skip_existing_dates=True,
        source_is_folder=True  # Read all part files from each dated folder
    ),
    # Additional configurations for retail_oltp_large dataset
    Row(
        config_id="synthetic_customers_large_001",
        config_name="Synthetic Data - Customers Large (Retail OLTP Large)",
        source_file_path="synthetic_data/csv/single/retail_oltp_large/customers.csv",
        source_file_format="csv",
        target_workspace_id="{{varlib:config_workspace_id}}",
        target_datastore_id="{{varlib:config_lakehouse_id}}",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="synthetic_customers_large",
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
        execution_group=4,  # Large dataset processing (Group 4)
        active_yn="N",  # Disabled by default - enable when data is available
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
        table_relationship_group="retail_oltp_large",
        batch_import_enabled=False,
        file_discovery_pattern=None,
        import_sequence_order=1,
        date_range_start=None,
        date_range_end=None,
        skip_existing_dates=None,
        source_is_folder=True  # Read all part files from the folder
    ),
    # Parquet format example - using available parquet files
    Row(
        config_id="synthetic_customers_parquet_001",
        config_name="Synthetic Data - Customers Parquet (Retail OLTP Small)",
        source_file_path="synthetic_data/parquet/single/retail_oltp_small/customers.parquet",
        source_file_format="parquet",
        target_workspace_id="{{varlib:config_workspace_id}}",
        target_datastore_id="{{varlib:config_lakehouse_id}}",
        target_datastore_type="lakehouse",
        target_schema_name="raw",
        target_table_name="synthetic_customers_parquet",
        staging_table_name=None,
        file_delimiter=None,
        has_header=None,
        encoding=None,
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
        execution_group=5,  # Parquet processing (Group 5)
        active_yn="N",  # Disabled by default - enable when data is available
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
        # New fields for incremental synthetic data import support
        import_pattern="single_file",
        date_partition_format=None,
        table_relationship_group="retail_oltp_parquet",
        batch_import_enabled=False,
        file_discovery_pattern=None,
        import_sequence_order=1,
        date_range_start=None,
        date_range_end=None,
        skip_existing_dates=None,
        source_is_folder=True  # Read all part files from the folder
    ),
    # Cross-workspace example - reading from external workspace
    Row(
        config_id="cross_workspace_example_001",
        config_name="Cross-Workspace Data Import Example",
        source_file_path="external_data/customers/export.csv",
        source_file_format="csv",
        # Source location (different workspace)
        source_workspace_id="{{varlib:raw_lh_workspace_id}}",
        source_datastore_id="{{varlib:raw_lh_lakehouse_id}}",
        source_datastore_type="lakehouse",
        source_file_root_path="Files",
        # Target location
        target_workspace_id="{{varlib:config_workspace_id}}",
        target_datastore_id="{{varlib:config_lakehouse_id}}",
        target_datastore_type="lakehouse",
        target_schema_name="external",
        target_table_name="external_customers",
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
        execution_group=6,  # Cross-workspace processing (Group 6)
        active_yn="N",  # Disabled by default
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
        # New fields for cross-workspace import
        import_pattern="single_file",
        date_partition_format=None,
        table_relationship_group="external_data",
        batch_import_enabled=False,
        file_discovery_pattern=None,
        import_sequence_order=1,
        date_range_start=None,
        date_range_end=None,
        skip_existing_dates=None,
        source_is_folder=False  # Single file import
    ),
    # Example using Tables root path instead of Files
    Row(
        config_id="tables_path_example_001",
        config_name="Table Path Example - Delta Table Import",
        source_file_path="delta_exports/customer_export",
        source_file_format="delta",
        # Source location with Tables root path
        source_workspace_id="{{varlib:config_workspace_id}}",
        source_datastore_id="{{varlib:config_lakehouse_id}}",
        source_datastore_type="lakehouse",
        source_file_root_path="Tables",  # Using Tables instead of Files
        # Target location
        target_workspace_id="{{varlib:config_workspace_id}}",
        target_datastore_id="{{varlib:config_lakehouse_id}}",
        target_datastore_type="lakehouse",
        target_schema_name="imported",
        target_table_name="imported_customers",
        staging_table_name=None,
        file_delimiter=None,
        has_header=None,
        encoding=None,
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
        execution_group=7,  # Delta table processing (Group 7)
        active_yn="N",  # Disabled by default
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
        # New fields
        import_pattern="single_file",
        date_partition_format=None,
        table_relationship_group="delta_imports",
        batch_import_enabled=False,
        file_discovery_pattern=None,
        import_sequence_order=1,
        date_range_start=None,
        date_range_end=None,
        skip_existing_dates=None,
        source_is_folder=False
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
print("✓ Active configurations:")
print("  - Customers Folder: Read all part files from a Spark-generated CSV folder")
print("  - Customers Single File: Read a specific CSV part file")
print("  - Orders Incremental: Date-partitioned CSV orders with wildcard pattern")
print("✓ Inactive configurations (enable when data is available):")
print("  - Large dataset support: Configuration for retail_oltp_large dataset")
print("  - Parquet format: Customers data in Parquet format")
print("  - Cross-workspace and Delta table examples")
print("✓ Files use synthetic data from test_data_feb_small directory")