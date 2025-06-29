"""
Example usage script for lakehouse_utils class.

This script demonstrates how to use the lakehouse_utils class
in a typical Microsoft Fabric environment.
"""

from __future__ import annotations

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
        StructField("sale_date", TimestampType(), True)
    ])
    
    # Sample data
    sales_data = [
        (1, "Laptop", "John Doe", 1200, datetime(2024, 6, 1, 10, 0)),
        (2, "Mouse", "Jane Smith", 25, datetime(2024, 6, 2, 14, 30)),
        (3, "Keyboard", "Bob Johnson", 75, datetime(2024, 6, 3, 9, 15)),
        (4, "Monitor", "Alice Brown", 300, datetime(2024, 6, 4, 16, 45)),
        (5, "Headphones", "Carol Wilson", 150, datetime(2024, 6, 5, 11, 20))
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
    utils.write_to_lakehouse_table(sales_df, table_name)
    
    # Step 6: Write with custom options (example)
    print("\n⚙️  Step 6: Write with custom options (example)")
    custom_options = {
        "mergeSchema": "true",
        "overwriteSchema": "true"
    }
    print("Custom write options:")
    for key, value in custom_options.items():
        print(f"   {key}: {value}")
    
    print("⚠️  In a real environment:")
    print(f"   utils.write_to_lakehouse_table(sales_df, '{table_name}', options=custom_options)")
    
    # Uncomment in real environment:
    utils.write_to_lakehouse_table(sales_df, table_name, options=custom_options)
    
    # Step 7: Append mode example
    print("\n➕ Step 7: Append mode example")
    additional_data = [
        (6, "Tablet", "David Lee", 450, datetime(2024, 6, 6, 13, 10))
    ]
    additional_df = utils.spark.createDataFrame(additional_data, sales_schema)
    print(f"✅ Created additional DataFrame with {additional_df.count()} rows")
    
    print("⚠️  To append data:")
    print(f"   utils.write_to_lakehouse_table(additional_df, '{table_name}', mode='append')")
    
    # Uncomment in real environment:
    # utils.write_to_lakehouse_table(additional_df, table_name, mode="append")
    
    # Step 8: Read data back (example)
    print("\n📖 Step 8: Read data back (example)")
    print("⚠️  To read the data back:")
    print(f"   df = spark.read.format('delta').load('{table_path}')")
    print("   df.show()")
    
    # In real environment:
    # read_df = utils.spark.read.format("delta").load(table_path)
    # read_df.show()
    
    print("\n" + "=" * 50)
    print("✅ Example completed!")
    print("\nTo use in a real Fabric environment:")
    print("1. Replace workspace_id and lakehouse_id with actual values")
    print("2. Uncomment the write operations")
    print("3. Run in a Fabric notebook with Delta Lake configured")


def advanced_example():
    """Show advanced usage patterns."""
    print("\n🔬 Advanced Usage Patterns")
    print("=" * 50)
    
    print("\n1. 📝 Multiple table management:")
    print("""
    # Initialize once, use for multiple tables
    utils = lakehouse_utils("workspace-id", "lakehouse-id")
    
    # Write different tables
    utils.write_to_lakehouse_table(customers_df, "customers")
    utils.write_to_lakehouse_table(orders_df, "orders") 
    utils.write_to_lakehouse_table(products_df, "products")
    """)
    
    print("\n2. 🔄 Different write modes:")
    print("""
    # Overwrite (default)
    utils.write_to_lakehouse_table(df, "table_name", mode="overwrite")
    
    # Append new data
    utils.write_to_lakehouse_table(df, "table_name", mode="append")
    
    # Error if exists
    utils.write_to_lakehouse_table(df, "table_name", mode="error")
    """)
    
    print("\n3. ⚙️  Custom write options:")
    print("""
    options = {
        "mergeSchema": "true",           # Merge schemas when writing
        "overwriteSchema": "true",       # Allow schema evolution
        "maxRecordsPerFile": "10000",    # Control file sizes
        "replaceWhere": "date >= '2024-01-01'"  # Conditional overwrite
    }
    utils.write_to_lakehouse_table(df, "table_name", options=options)
    """)
    
    print("\n4. 🔍 Table existence checking:")
    print("""
    table_path = utils.lakehouse_tables_uri() + "my_table"
    if not utils.check_if_table_exists(table_path):
        # Create table
        utils.write_to_lakehouse_table(df, "my_table")
    else:
        # Append to existing table
        utils.write_to_lakehouse_table(df, "my_table", mode="append")
    """)
    
    print("\n5. ⚠️  Cleanup operations:")
    print("""
    # WARNING: This drops ALL tables in the lakehouse!
    # Only use in test environments
    utils.drop_all_tables()
    """)


def main():
    """Main function to run examples."""
    try:
        example_usage()
        advanced_example()
        
    except Exception as e:
        print(f"\n❌ Example failed: {e}")
        print("\nNote: Some operations require a real Fabric environment with Delta Lake configured.")


if __name__ == "__main__":
    main()
