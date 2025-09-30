# Sample dataset configurations (Lakehouse version)
# Insert predefined dataset templates into Delta table

from datetime import datetime

from pyspark.sql import Row
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Sample dataset configurations data using Row objects
sample_configs = [
    # Retail Transactional (OLTP)
    Row(
        dataset_id="retail_oltp_small",
        dataset_name="Retail OLTP - Small",
        dataset_type="transactional",
        schema_pattern="oltp",
        domain="retail",
        max_recommended_rows=1000000,
        description="Small retail transactional system with customers, orders, products",
        config_json='{"tables": ["customers", "products", "orders", "order_items"], "relationships": "normalized"}',
        is_active=True,
        created_date=datetime(2024, 1, 15, 0, 0),
        modified_date=datetime(2024, 1, 15, 0, 0),
    ),
    Row(
        dataset_id="retail_oltp_large",
        dataset_name="Retail OLTP - Large",
        dataset_type="transactional",
        schema_pattern="oltp",
        domain="retail",
        max_recommended_rows=1000000000,
        description="Large-scale retail transactional system",
        config_json='{"tables": ["customers", "products", "orders", "order_items"], "relationships": "normalized", "partitioning": "date"}',
        is_active=True,
        created_date=datetime(2024, 1, 15, 0, 0),
        modified_date=datetime(2024, 1, 15, 0, 0),
    ),
    # Retail Analytics (Star Schema)
    Row(
        dataset_id="retail_star_small",
        dataset_name="Retail Star Schema - Small",
        dataset_type="analytical",
        schema_pattern="star_schema",
        domain="retail",
        max_recommended_rows=10000000,
        description="Small retail data warehouse with fact_sales and dimensions",
        config_json='{"fact_tables": ["fact_sales"], "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store"]}',
        is_active=True,
        created_date=datetime(2024, 1, 15, 0, 0),
        modified_date=datetime(2024, 1, 15, 0, 0),
    ),
    Row(
        dataset_id="retail_star_large",
        dataset_name="Retail Star Schema - Large",
        dataset_type="analytical",
        schema_pattern="star_schema",
        domain="retail",
        max_recommended_rows=5000000000,
        description="Large retail data warehouse with multiple fact tables",
        config_json='{"fact_tables": ["fact_sales", "fact_inventory"], "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store", "dim_supplier"]}',
        is_active=True,
        created_date=datetime(2024, 1, 15, 0, 0),
        modified_date=datetime(2024, 1, 15, 0, 0),
    ),
    # Financial Transactional
    Row(
        dataset_id="finance_oltp_small",
        dataset_name="Financial OLTP - Small",
        dataset_type="transactional",
        schema_pattern="oltp",
        domain="finance",
        max_recommended_rows=500000,
        description="Small financial system with accounts, transactions, customers",
        config_json='{"tables": ["customers", "accounts", "transactions", "account_types"], "compliance": "pci_dss"}',
        is_active=True,
        created_date=datetime(2024, 1, 15, 0, 0),
        modified_date=datetime(2024, 1, 15, 0, 0),
    ),
    Row(
        dataset_id="finance_oltp_large",
        dataset_name="Financial OLTP - Large",
        dataset_type="transactional",
        schema_pattern="oltp",
        domain="finance",
        max_recommended_rows=2000000000,
        description="Large financial system with high transaction volume",
        config_json='{"tables": ["customers", "accounts", "transactions", "account_types"], "compliance": "pci_dss", "high_frequency": true}',
        is_active=True,
        created_date=datetime(2024, 1, 15, 0, 0),
        modified_date=datetime(2024, 1, 15, 0, 0),
    ),
    # E-commerce Analytics
    Row(
        dataset_id="ecommerce_star_small",
        dataset_name="E-commerce Star Schema - Small",
        dataset_type="analytical",
        schema_pattern="star_schema",
        domain="ecommerce",
        max_recommended_rows=5000000,
        description="E-commerce analytics with web events and sales",
        config_json='{"fact_tables": ["fact_web_events", "fact_orders"], "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date"]}',
        is_active=True,
        created_date=datetime(2024, 1, 15, 0, 0),
        modified_date=datetime(2024, 1, 15, 0, 0),
    ),
    Row(
        dataset_id="ecommerce_star_large",
        dataset_name="E-commerce Star Schema - Large",
        dataset_type="analytical",
        schema_pattern="star_schema",
        domain="ecommerce",
        max_recommended_rows=10000000000,
        description="Large e-commerce analytics with clickstream data",
        config_json='{"fact_tables": ["fact_web_events", "fact_orders", "fact_page_views"], "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date", "dim_geography"]}',
        is_active=True,
        created_date=datetime(2024, 1, 15, 0, 0),
        modified_date=datetime(2024, 1, 15, 0, 0),
    ),
]

# Define schema
schema = StructType(
    [
        StructField("dataset_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("dataset_type", StringType(), False),
        StructField("schema_pattern", StringType(), False),
        StructField("domain", StringType(), False),
        StructField("max_recommended_rows", LongType(), False),
        StructField("description", StringType(), True),
        StructField("config_json", StringType(), True),
        StructField("is_active", BooleanType(), False),
        StructField("created_date", TimestampType(), False),
        StructField("modified_date", TimestampType(), True),
    ]
)

# Create DataFrame from sample data using injected utils
sample_df = target_lakehouse.get_connection.createDataFrame(sample_configs, schema)  # noqa: F821

# Insert into the config table using injected utils
target_lakehouse.write_to_table(  # noqa: F821
    df=sample_df, table_name="config_synthetic_data_datasets", mode="append"
)

print(f"✅ Inserted {len(sample_configs)} sample dataset configurations")

# Show what was inserted using injected utils
configs_table = target_lakehouse.read_table("config_synthetic_data_datasets")  # noqa: F821
print("📋 Sample configurations:")
configs_table.select("dataset_id", "dataset_name", "dataset_type", "domain").show(truncate=False)
