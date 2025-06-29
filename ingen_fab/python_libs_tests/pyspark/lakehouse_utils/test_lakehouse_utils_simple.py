"""
Simple test script for lakehouse_utils in Spark/Fabric environment.

This script can be run directly in a Spark notebook or environment
where you have access to a real lakehouse.

Usage in Fabric Notebook:
1. Replace WORKSPACE_ID and LAKEHOUSE_ID with your actual values
2. Run this cell to test the lakehouse_utils functionality
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

# =============================================================================
# CONFIGURATION - UPDATE THESE VALUES FOR YOUR ENVIRONMENT
# =============================================================================

# Replace with your actual Fabric workspace and lakehouse IDs
WORKSPACE_ID = "your-workspace-id-here"  # Update this!
LAKEHOUSE_ID = "your-lakehouse-id-here"  # Update this!

# =============================================================================
# TEST FUNCTIONS
# =============================================================================


def create_sample_data():
    """Create sample DataFrames for testing."""
    print("📊 Creating sample test data...")
    
    # Customer data
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])
    
    customer_data = [
        (1, "John Doe", "john@example.com", datetime(2024, 1, 1, 10, 0)),
        (2, "Jane Smith", "jane@example.com", datetime(2024, 1, 2, 11, 0)),
        (3, "Bob Johnson", "bob@example.com", datetime(2024, 1, 3, 12, 0))
    ]
    
    customer_df = spark.createDataFrame(customer_data, customer_schema)  # type: ignore # noqa: F821
    
    # Product data
    product_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True)  # Using int for simplicity
    ])
    
    product_data = [
        (101, "Laptop", "Electronics", 999),
        (102, "Coffee Mug", "Kitchen", 15),
        (103, "Notebook", "Office", 5)
    ]
    
    product_df = spark.createDataFrame(product_data, product_schema)  # type: ignore # noqa: F821
    
    print(f"✅ Created customer data: {customer_df.count()} rows")
    print(f"✅ Created product data: {product_df.count()} rows")
    
    return customer_df, product_df


def test_basic_functionality():
    """Test basic lakehouse_utils functionality."""
    print("\n🧪 Testing lakehouse_utils basic functionality...")
    
    # Initialize lakehouse utils
    utils = lakehouse_utils(WORKSPACE_ID, LAKEHOUSE_ID)
    print(f"✅ Initialized lakehouse_utils for workspace: {WORKSPACE_ID}")
    
    # Test URI generation
    uri = utils.lakehouse_tables_uri()
    print(f"✅ Generated tables URI: {uri}")
    
    # Create sample data
    customer_df, product_df = create_sample_data()
    
    # Test table existence check
    test_table_path = f"{uri}test_customers"
    exists_before = utils.check_if_table_exists(test_table_path)
    print(f"✅ Table exists before creation: {exists_before}")
    
    return utils, customer_df, product_df


def test_write_operations(utils, customer_df, product_df):
    """Test writing data to lakehouse tables."""
    print("\n📝 Testing write operations...")
    
    try:
        # Write customer data
        print("Writing customer data...")
        utils.write_to_lakehouse_table(customer_df, "test_customers")
        print("✅ Customer data written successfully")
        
        # Write product data with custom options
        print("Writing product data with custom options...")
        custom_options = {
            "mergeSchema": "true",
            "overwriteSchema": "true"
        }
        utils.write_to_lakehouse_table(
            product_df, 
            "test_products",
            options=custom_options
        )
        print("✅ Product data written successfully")
        
        # Test append mode
        print("Testing append mode...")
        additional_customer_data = [(4, "Alice Brown", "alice@example.com", datetime(2024, 1, 4, 13, 0))]
        additional_customer_schema = customer_df.schema
        additional_df = spark.createDataFrame(additional_customer_data, additional_customer_schema)  # type: ignore # noqa: F821
        
        utils.write_to_lakehouse_table(
            additional_df,
            "test_customers",
            mode="append"
        )
        print("✅ Append operation completed successfully")
        
    except Exception as e:
        print(f"❌ Write operation failed: {e}")
        raise


def test_read_back_data(utils):
    """Test reading back the data we wrote."""
    print("\n📖 Testing data read-back...")
    
    try:
        # Read customer data
        customer_table_path = f"{utils.lakehouse_tables_uri()}test_customers"
        if utils.check_if_table_exists(customer_table_path):
            customer_read_df = spark.read.format("delta").load(customer_table_path)  # type: ignore # noqa: F821
            customer_count = customer_read_df.count()
            print(f"✅ Read customer data: {customer_count} rows")
            
            print("Customer data sample:")
            customer_read_df.show(5, truncate=False)
        else:
            print("❌ Customer table does not exist")
        
        # Read product data
        product_table_path = f"{utils.lakehouse_tables_uri()}test_products"
        if utils.check_if_table_exists(product_table_path):
            product_read_df = spark.read.format("delta").load(product_table_path)  # type: ignore # noqa: F821
            product_count = product_read_df.count()
            print(f"✅ Read product data: {product_count} rows")
            
            print("Product data sample:")
            product_read_df.show(5, truncate=False)
        else:
            print("❌ Product table does not exist")
            
    except Exception as e:
        print(f"❌ Read operation failed: {e}")
        raise


def cleanup_test_tables(utils):
    """Clean up test tables (optional)."""
    print("\n🧹 Cleaning up test tables...")
    
    try:
        # Note: This is a destructive operation - use with caution!
        # Uncomment the line below if you want to clean up ALL tables
        # utils.drop_all_tables()
        
        print("⚠️  Cleanup skipped - uncomment utils.drop_all_tables() to enable")
        print("   You can manually delete test_customers and test_products tables if needed")
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")


def main():
    """Main test execution."""
    print("🚀 Starting lakehouse_utils tests in Spark environment")
    print("=" * 60)
    
    # Check configuration
    if WORKSPACE_ID == "your-workspace-id-here" or LAKEHOUSE_ID == "your-lakehouse-id-here":
        print("❌ ERROR: Please update WORKSPACE_ID and LAKEHOUSE_ID at the top of this script!")
        return
    
    try:
        # Test basic functionality
        utils, customer_df, product_df = test_basic_functionality()
        
        # Test write operations
        test_write_operations(utils, customer_df, product_df)
        
        # Test reading data back
        test_read_back_data(utils)
        
        # Optional cleanup
        cleanup_test_tables(utils)
        
        print("\n" + "=" * 60)
        print("🎉 All tests completed successfully!")
        print("✅ lakehouse_utils class is working properly")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == "__main__":
    main()
else:
    # If running in a notebook, you can call main() directly
    print("Script loaded. Call main() to run tests.")
    print("Remember to update WORKSPACE_ID and LAKEHOUSE_ID first!")
