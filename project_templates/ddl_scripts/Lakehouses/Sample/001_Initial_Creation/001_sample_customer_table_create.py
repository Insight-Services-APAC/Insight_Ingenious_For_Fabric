# Sample DDL script for creating a customer table in lakehouse
# This demonstrates the basic pattern for creating Delta tables

# Create the sample customer table
spark.sql("""
CREATE TABLE IF NOT EXISTS sample.customers (
    customer_id BIGINT,
    first_name STRING,
    last_name STRING,
    email STRING,
    created_date TIMESTAMP,
    is_active BOOLEAN
)
USING DELTA
LOCATION 'Tables/customers'
""")

print("✓ Created sample customers table")

# Add some sample data
spark.sql("""
INSERT INTO sample.customers VALUES
(1, 'John', 'Doe', 'john.doe@example.com', '2024-01-01 10:00:00', true),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '2024-01-02 11:00:00', true),
(3, 'Bob', 'Johnson', 'bob.johnson@example.com', '2024-01-03 12:00:00', false)
""")

print("✓ Inserted sample data into customers table")