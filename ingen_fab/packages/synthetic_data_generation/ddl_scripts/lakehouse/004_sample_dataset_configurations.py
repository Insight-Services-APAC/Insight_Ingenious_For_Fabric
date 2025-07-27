# Sample dataset configurations (Lakehouse version)
# Insert predefined dataset templates into Delta table

from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, TimestampType

# Sample dataset configurations data
sample_configs = [
    # Retail Transactional (OLTP)
    ("retail_oltp_small", "Retail OLTP - Small", "transactional", "oltp", "retail", 
     1000000, "Small retail transactional system with customers, orders, products",
     '{"tables": ["customers", "products", "orders", "order_items"], "relationships": "normalized"}', True),
    
    ("retail_oltp_large", "Retail OLTP - Large", "transactional", "oltp", "retail", 
     1000000000, "Large-scale retail transactional system",
     '{"tables": ["customers", "products", "orders", "order_items"], "relationships": "normalized", "partitioning": "date"}', True),
    
    # Retail Analytics (Star Schema)
    ("retail_star_small", "Retail Star Schema - Small", "analytical", "star_schema", "retail", 
     10000000, "Small retail data warehouse with fact_sales and dimensions",
     '{"fact_tables": ["fact_sales"], "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store"]}', True),
    
    ("retail_star_large", "Retail Star Schema - Large", "analytical", "star_schema", "retail", 
     5000000000, "Large retail data warehouse with multiple fact tables",
     '{"fact_tables": ["fact_sales", "fact_inventory"], "dimensions": ["dim_customer", "dim_product", "dim_date", "dim_store", "dim_supplier"]}', True),
    
    # Financial Transactional
    ("finance_oltp_small", "Financial OLTP - Small", "transactional", "oltp", "finance", 
     500000, "Small financial system with accounts, transactions, customers",
     '{"tables": ["customers", "accounts", "transactions", "account_types"], "compliance": "pci_dss"}', True),
    
    ("finance_oltp_large", "Financial OLTP - Large", "transactional", "oltp", "finance", 
     2000000000, "Large financial system with high transaction volume",
     '{"tables": ["customers", "accounts", "transactions", "account_types"], "compliance": "pci_dss", "high_frequency": true}', True),
    
    # E-commerce Analytics
    ("ecommerce_star_small", "E-commerce Star Schema - Small", "analytical", "star_schema", "ecommerce", 
     5000000, "E-commerce analytics with web events and sales",
     '{"fact_tables": ["fact_web_events", "fact_orders"], "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date"]}', True),
    
    ("ecommerce_star_large", "E-commerce Star Schema - Large", "analytical", "star_schema", "ecommerce", 
     10000000000, "Large e-commerce analytics with clickstream data",
     '{"fact_tables": ["fact_web_events", "fact_orders", "fact_page_views"], "dimensions": ["dim_customer", "dim_product", "dim_session", "dim_date", "dim_geography"]}', True)
]

# Define schema
schema = StructType([
    StructField("dataset_id", StringType(), False),
    StructField("dataset_name", StringType(), False),
    StructField("dataset_type", StringType(), False),
    StructField("schema_pattern", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("max_recommended_rows", LongType(), False),
    StructField("description", StringType(), True),
    StructField("config_json", StringType(), True),
    StructField("is_active", BooleanType(), False)
])

# Create DataFrame from sample data
sample_df = spark.createDataFrame(sample_configs, schema)

# Add timestamps
sample_df = sample_df.withColumn("created_date", current_timestamp()) \
                    .withColumn("modified_date", current_timestamp())

# Insert into the config table
sample_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("config_synthetic_data_datasets")

print(f"✅ Inserted {sample_df.count()} sample dataset configurations")

# Show what was inserted
print("📋 Sample configurations:")
spark.table("config_synthetic_data_datasets").select("dataset_id", "dataset_name", "dataset_type", "domain").show(truncate=False)