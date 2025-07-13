#!/usr/bin/env python3
"""
Check the results of the flat file ingestion
"""

from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("CheckResults") \
    .getOrCreate()

print("🔍 Checking Flat File Ingestion Results")
print("=" * 50)

try:
    # Check the CSV data that was successfully processed
    print("✅ CSV Sales Data Results:")
    csv_df = spark.sql("SELECT * FROM raw_sales_data")
    print(f"Records in raw_sales_data: {csv_df.count()}")
    
    print("\nSample records:")
    csv_df.show(5)
    
    print("\nTable schema:")
    csv_df.printSchema()
    
    print("\nIngestion metadata:")
    csv_df.select("_ingestion_timestamp", "_ingestion_execution_id", "_source_file_path").distinct().show(truncate=False)
    
    print("\n📊 Data Analysis:")
    print("Sales by region:")
    csv_df.groupBy("region").count().show()
    
    print("Sales by product:")
    csv_df.groupBy("product_id").agg({"quantity": "sum", "total_amount": "sum"}).show()
    
except Exception as e:
    print(f"Error checking results: {e}")

finally:
    spark.stop()
    
print("\n🎉 Results check complete!")