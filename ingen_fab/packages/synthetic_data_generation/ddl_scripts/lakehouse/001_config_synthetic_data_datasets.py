# Configuration table for synthetic data generation datasets (Lakehouse version)
# This creates a Delta table to define available dataset templates

from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType

# Define schema for config_synthetic_data_datasets table
config_datasets_schema = StructType([
    StructField("dataset_id", StringType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("dataset_type", StringType(), False),  # 'transactional' or 'analytical'
    StructField("schema_pattern", StringType(), False),  # 'oltp', 'star_schema', 'snowflake'
    StructField("domain", StringType(), False),  # e.g., 'retail', 'finance', 'healthcare'
    StructField("max_recommended_rows", LongType(), False),
    StructField("description", StringType(), True),
    StructField("config_json", StringType(), True),  # JSON configuration
    StructField("is_active", BooleanType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("modified_date", TimestampType(), False)
])

target_lakehouse.create_table(
    table_name="config_synthetic_data_datasets",
    schema=config_datasets_schema,
    mode="overwrite",
    options={
        "parquet.vorder.default": "true"
    }
)

print("✅ Created config_synthetic_data_datasets Delta table")