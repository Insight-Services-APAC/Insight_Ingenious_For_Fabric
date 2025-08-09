# Sample configuration data for extract generation testing - Lakehouse version

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Define schema for sample data (matching config_create.py)
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
        # Fabric paths
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

# Sample extract configuration records based on synthetic retail OLTP data
sample_extract_configs = [
    Row(
        extract_name="SAMPLE_CUSTOMERS_SNAPSHOT",
        is_active=True,
        trigger_name=None,
        extract_pipeline_name=None,
        extract_table_name="customers",
        extract_table_schema="default",
        extract_view_name=None,
        extract_view_schema=None,
        is_full_load=True,
        execution_group="SNAPSHOT_EXTRACTS",
        created_date="2024-01-15",
        created_by="system",
        modified_date=None,
        modified_by=None,
    ),
    Row(
        extract_name="SAMPLE_PRODUCTS_SNAPSHOT",
        is_active=True,
        trigger_name=None,
        extract_pipeline_name=None,
        extract_table_name="products",
        extract_table_schema="default",
        extract_view_name=None,
        extract_view_schema=None,
        is_full_load=True,
        execution_group="SNAPSHOT_EXTRACTS",
        created_date="2024-01-15",
        created_by="system",
        modified_date=None,
        modified_by=None,
    ),
    Row(
        extract_name="SAMPLE_ORDERS_INCREMENTAL",
        is_active=True,
        trigger_name=None,
        extract_pipeline_name=None,
        extract_table_name="orders",
        extract_table_schema="default",
        extract_view_name=None,
        extract_view_schema=None,
        is_full_load=False,
        execution_group="INCREMENTAL_EXTRACTS",
        created_date="2024-01-15",
        created_by="system",
        modified_date=None,
        modified_by=None,
    ),
    Row(
        extract_name="SAMPLE_ORDER_ITEMS_INCREMENTAL",
        is_active=True,
        trigger_name=None,
        extract_pipeline_name=None,
        extract_table_name="order_items",
        extract_table_schema="default",
        extract_view_name=None,
        extract_view_schema=None,
        is_full_load=False,
        execution_group="INCREMENTAL_EXTRACTS",
        created_date="2024-01-15",
        created_by="system",
        modified_date=None,
        modified_by=None,
    ),
]

# Sample extract details configuration records based on synthetic retail OLTP data
sample_extract_details = [
    Row(
        extract_name="SAMPLE_CUSTOMERS_SNAPSHOT",
        file_generation_group="CUSTOMER_SNAPSHOT",
        extract_container="Files/extracts",
        extract_directory="snapshot/customers",
        extract_file_name="customers",
        extract_file_name_timestamp_format="yyyyMMdd",
        extract_file_name_period_end_day=None,
        extract_file_name_extension="parquet",
        extract_file_name_ordering=1,
        file_properties_column_delimiter=",",
        file_properties_row_delimiter="\\n",
        file_properties_encoding="UTF-8",
        file_properties_quote_character='"',
        file_properties_escape_character="\\",
        file_properties_header=True,
        file_properties_null_value="",
        file_properties_max_rows_per_file=None,
        output_format="parquet",
        is_trigger_file=False,
        trigger_file_extension=None,
        is_compressed=False,
        compressed_type=None,
        compressed_level=None,
        compressed_file_name=None,
        compressed_extension=None,
        fabric_lakehouse_path="tmp/spark/Files/synthetic_data/incremental/retail_oltp_small/files/snapshot/customers",
        force_single_file=True,
        created_date="2024-01-15",
        created_by="system",
        modified_date=None,
        modified_by=None,
    ),
    Row(
        extract_name="SAMPLE_PRODUCTS_SNAPSHOT",
        file_generation_group="PRODUCT_SNAPSHOT",
        extract_container="Files/extracts",
        extract_directory="snapshot/products",
        extract_file_name="products",
        extract_file_name_timestamp_format="yyyyMMdd",
        extract_file_name_period_end_day=None,
        extract_file_name_extension="parquet",
        extract_file_name_ordering=1,
        file_properties_column_delimiter=",",
        file_properties_row_delimiter="\\n",
        file_properties_encoding="UTF-8",
        file_properties_quote_character='"',
        file_properties_escape_character="\\",
        file_properties_header=True,
        file_properties_null_value="",
        file_properties_max_rows_per_file=None,
        output_format="parquet",
        is_trigger_file=True,
        trigger_file_extension=".done",
        is_compressed=False,
        compressed_type=None,
        compressed_level=None,
        compressed_file_name=None,
        compressed_extension=None,
        fabric_lakehouse_path="tmp/spark/Files/synthetic_data/incremental/retail_oltp_small/files/snapshot/products",
        force_single_file=True,
        created_date="2024-01-15",
        created_by="system",
        modified_date=None,
        modified_by=None,
    ),
    Row(
        extract_name="SAMPLE_ORDERS_INCREMENTAL",
        file_generation_group="ORDER_INCREMENTAL",
        extract_container="Files/extracts",
        extract_directory="incremental/orders",
        extract_file_name="orders",
        extract_file_name_timestamp_format="yyyyMMdd",
        extract_file_name_period_end_day=None,
        extract_file_name_extension="parquet",
        extract_file_name_ordering=1,
        file_properties_column_delimiter=",",
        file_properties_row_delimiter="\\n",
        file_properties_encoding="UTF-8",
        file_properties_quote_character='"',
        file_properties_escape_character="\\",
        file_properties_header=True,
        file_properties_null_value="",
        file_properties_max_rows_per_file=None,
        output_format="parquet",
        is_trigger_file=False,
        trigger_file_extension=None,
        is_compressed=True,
        compressed_type="SNAPPY",
        compressed_level="NORMAL",
        compressed_file_name=None,
        compressed_extension=".parquet",
        fabric_lakehouse_path="tmp/spark/Files/synthetic_data/incremental/retail_oltp_small/files/orders",
        force_single_file=True,
        created_date="2024-01-15",
        created_by="system",
        modified_date=None,
        modified_by=None,
    ),
    Row(
        extract_name="SAMPLE_ORDER_ITEMS_INCREMENTAL",
        file_generation_group="ORDER_ITEMS_INCREMENTAL",
        extract_container="Files/extracts",
        extract_directory="incremental/order_items",
        extract_file_name="order_items",
        extract_file_name_timestamp_format="yyyyMMdd",
        extract_file_name_period_end_day=None,
        extract_file_name_extension="parquet",
        extract_file_name_ordering=1,
        file_properties_column_delimiter=",",
        file_properties_row_delimiter="\\n",
        file_properties_encoding="UTF-8",
        file_properties_quote_character='"',
        file_properties_escape_character="\\",
        file_properties_header=True,
        file_properties_null_value="",
        file_properties_max_rows_per_file=None,
        output_format="parquet",
        is_trigger_file=False,
        trigger_file_extension=None,
        is_compressed=True,
        compressed_type="SNAPPY",
        compressed_level="NORMAL",
        compressed_file_name=None,
        compressed_extension=".parquet",
        fabric_lakehouse_path="tmp/spark/Files/synthetic_data/incremental/retail_oltp_small/files/order_items",
        force_single_file=True,
        created_date="2024-01-15",
        created_by="system",
        modified_date=None,
        modified_by=None,
    ),
]

# Create DataFrames and insert records
config_df = target_lakehouse.get_connection.createDataFrame(  # noqa: F821
    sample_extract_configs, extract_config_schema
)
target_lakehouse.write_to_table(  # noqa: F821
    df=config_df, table_name="config_extract_generation", mode="append"
)

details_df = target_lakehouse.get_connection.createDataFrame(  # noqa: F821
    sample_extract_details, extract_details_schema
)
target_lakehouse.write_to_table(  # noqa: F821
    df=details_df, table_name="config_extract_generation_details", mode="append"
)

print(
    "✓ Inserted " + str(len(sample_extract_configs)) + " extract configuration records"
)
print(
    "✓ Inserted "
    + str(len(sample_extract_details))
    + " extract details configuration records"
)
print("✓ Extract configurations based on synthetic retail OLTP data include:")
print("  - SAMPLE_CUSTOMERS_SNAPSHOT: Full load, Parquet format, weekly snapshots")
print(
    "  - SAMPLE_PRODUCTS_SNAPSHOT: Full load, Parquet format, weekly snapshots, with trigger file"
)
print(
    "  - SAMPLE_ORDERS_INCREMENTAL: Incremental load, Parquet format, daily updates, Snappy compressed"
)
print(
    "  - SAMPLE_ORDER_ITEMS_INCREMENTAL: Incremental load, Parquet format, daily updates, Snappy compressed"
)
