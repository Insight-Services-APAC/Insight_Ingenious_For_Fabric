"""
Example usage script for lakehouse_utils class.

This script demonstrates how to use the lakehouse_utils class
in a typical Microsoft Fabric environment.
"""

from __future__ import annotations

import traceback
from datetime import datetime

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


def example_usage():
    """Demonstrate basic usage of lakehouse_utils."""
    print("🚀 lakehouse_utils Usage Example")
    print("=" * 50)
    
    # Step 1: Initialize the utility with your workspace and lakehouse IDs
    print("\n📋 Step 1: Initialize lakehouse_utils")
    workspace_id = "your-workspace-id-here"  # Replace with actual workspace ID
    lakehouse_id = "your-lakehouse-id-here"  # Replace with actual lakehouse ID
    
    utils = lakehouse_utils(workspace_id, lakehouse_id)
    print(f"✅ {utils.spark_version} Spark session initialized.")
    print(utils.spark)
    print(f"✅ Initialized for workspace: {workspace_id}")
    print(f"✅ Target lakehouse: {lakehouse_id}")
    
    # Step 2: Generate lakehouse URI
    print("\n🔗 Step 2: Generate lakehouse tables URI")
    uri = utils.lakehouse_tables_uri()
    print(f"✅ Tables URI: {uri}")
    
    # Step 3: Create sample data
    print("\n📊 Step 3: Create sample DataFrame")
    
    # Define schema
    sales_schema = StructType([
        StructField("sale_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("sale_date", StringType(), True)
    ])
    
    # Sample data
    sales_data = [
        (1, "Laptop", "John Doe", 1200, "2024-06-01 10:00:00"),
        (2, "Mouse", "Jane Smith", 25, "2024-06-02 14:30:00"),
        (3, "Keyboard", "Bob Johnson", 75, "2024-06-03 09:15:00"),
        (4, "Monitor", "Alice Brown", 300, "2024-06-04 16:45:00"),
        (5, "Headphones", "Carol Wilson", 150, "2024-06-05 11:20:00"),
    ]
    
    # Create DataFrame
    sales_df = utils.spark.createDataFrame(sales_data, sales_schema)
    print(f"✅ Created sales DataFrame with {sales_df.count()} rows")
    
    # Display the data
    print("\nSample data:")
    sales_df.show(truncate=False)
    
    # Step 4: Check if table exists
    print("\n🔍 Step 4: Check table existence")
    table_name = "sales_data"
    table_path = f"{uri}{table_name}"
    
    exists_before = utils.check_if_table_exists(table_path)
    print(f"✅ Table '{table_name}' exists before write: {exists_before}")
    
    # Step 5: Write to lakehouse (example - uncomment in real environment)
    print("\n💾 Step 5: Write to lakehouse table")
    print("   This will create/overwrite the table in your lakehouse.")
    
    # Uncomment the following line in a real Fabric environment:
    utils.write_to_table(sales_df, table_name)

    if utils.check_if_table_exists(table_name):
        print("✅ Table exists after write.")
    else:
        print("❌ Table does not exist after write.")

    # Step 6: Write with custom options (example)
    print("\n⚙️  Step 6: Write with custom options (example)")
    custom_options = {
        "mergeSchema": "true",
        "overwriteSchema": "true"
    }
    print("Custom write options:")
    for key, value in custom_options.items():
        print(f"   {key}: {value}")
    
    # Uncomment in real environment:
    utils.write_to_table(sales_df, table_name, options=custom_options)

    print("✅ Custom options applied.")

    # Step 7: Append mode example
    print("\n➕ Step 7: Append mode example")
    additional_data = [
        (6, "Tablet", "David Lee", 450, datetime(2024, 6, 6, 13, 10))
    ]
    additional_df = utils.spark.createDataFrame(additional_data, sales_schema)
    print(f"✅ Created additional DataFrame with {additional_df.count()} rows")
    
    # Uncomment in real environment:
    utils.write_to_table(additional_df, table_name, mode="append")

    if utils.check_if_table_exists(table_name):
        print("✅ Append succeeded")
    else:
        print("❌ Table does not exist after append.")

    # Step 8: Read data back (example)
    print("\n📖 Step 8: Read data back (example)")
    read_df = utils.spark.read.format("delta").load(table_path)
    read_df.show()
    print(utils.list_tables())
    
    print("\n" + "=" * 50)
    print("✅ Example completed!")



def advanced_example():
    """Show advanced usage patterns."""
    print("\n🔬 Advanced Usage Patterns")
    print("=" * 50)
    
    print("\n1. 📝 Multiple table management:")
    
    # Initialize once, use for multiple tables
    utils = lakehouse_utils("workspace-id", "lakehouse-id")
    
    # Write different tables
    # Create sample DataFrames for customers, orders, and products

    # Customers DataFrame
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("signup_date", TimestampType(), True)
    ])
    customers_data = [
        (1, "John Doe", "john.doe@example.com", datetime(2023, 1, 15, 10, 0)),
        (2, "Jane Smith", "jane.smith@example.com", datetime(2023, 2, 20, 14, 30)),
        (3, "Alice Brown", "alice.brown@example.com", datetime(2023, 3, 25, 9, 15))
    ]
    customers_df = utils.spark.createDataFrame(customers_data, customers_schema)

    # Orders DataFrame
    orders_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("total_amount", IntegerType(), True)
    ])
    orders_data = [
        (101, 1, datetime(2023, 4, 10, 11, 0), 1200),
        (102, 2, datetime(2023, 4, 15, 16, 45), 300),
        (103, 3, datetime(2023, 4, 20, 13, 20), 450)
    ]
    orders_df = utils.spark.createDataFrame(orders_data, orders_schema)

    # Products DataFrame
    products_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True)
    ])
    products_data = [
        (1, "Laptop", "Electronics", 1200),
        (2, "Mouse", "Accessories", 25),
        (3, "Keyboard", "Accessories", 75)
    ]
    products_df = utils.spark.createDataFrame(products_data, products_schema)

    # Write the DataFrames to lakehouse tables
    utils.write_to_table(customers_df, "customers")
    utils.write_to_table(orders_df, "orders")
    utils.write_to_table(products_df, "products")
    
    
    print("\n2. 🔄 Different write modes:")
    
    # Overwrite (default)
    sp_df = utils.execute_query("SELECT COUNT(*) FROM customers")
    row_count_pre = sp_df.collect()[0][0]    
    utils.write_to_table(customers_df, "customers", mode="overwrite")
    sp_df = utils.execute_query("SELECT COUNT(*) FROM customers")
    row_count_post = sp_df.collect()[0][0]
    if row_count_post != row_count_pre:
        print(f"❌ Overwrite test failed. Expected: {row_count_pre}, Got: {row_count_post}")
    else:
        print("✅ Overwrite test passed.")

    # Append new data
    sp_df = utils.execute_query("SELECT COUNT(*) FROM customers")
    row_count_pre = sp_df.collect()[0][0]
    utils.write_to_table(customers_df, "customers", mode="append")
    sp_df = utils.execute_query("SELECT COUNT(*) FROM customers")
    row_count_post = sp_df.collect()[0][0]
    if row_count_post != row_count_pre + 3:
        print(f"❌ Append test failed. Expected: {row_count_pre + 3}, Got: {row_count_post}")
    else:
        print("✅ Append test passed.")
    # Error if exists
    error_occured = False
    try:
        utils.write_to_table(customers_df, "customers", mode="error")
    except Exception:
        error_occured = True
    
    if not error_occured:
        print("❌ Error test failed.")
    else:
        print("✅ Error test passed.")


    print("\n3. ⚙️  Custom write options:")
    

    options = {
        "mergeSchema": "true",           # Merge schemas when writing
        "overwriteSchema": "true",       # Allow schema evolution
        "maxRecordsPerFile": "10000",    # Control file sizes
        "replaceWhere": "date >= '2024-01-01'"  # Conditional overwrite
    }
    utils.write_to_table(customers_df, "customers", options=options)
    
    
    print("\n4. 🔍 Table existence checking:")
    
    table_path = utils.lakehouse_tables_uri() + "customers"
    if not utils.check_if_table_exists(table_path):
        # Create table
        utils.write_to_table(customers_df, "customers")
    else:
        # Append to existing table
        utils.write_to_table(customers_df, "customers", mode="append")
    
    
    
    print("\n5. ⚠️  Cleanup operations:")
    
    # WARNING: This drops ALL tables in the lakehouse!
    # Only use in test environments
    utils.drop_all_tables()
    


def main():
    """Main function to run examples."""
    try:
        example_usage()
        # advanced_example()
        
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        traceback.print_exc()
        print("\nNote: Some operations require a real Fabric environment with Delta Lake configured.")


if __name__ == "__main__":
    main()
