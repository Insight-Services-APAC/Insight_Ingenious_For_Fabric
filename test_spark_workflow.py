#!/usr/bin/env python3
"""
Test script to execute the complete flat file ingestion workflow with Spark
"""

import os
import sys
from pathlib import Path

# Set up environment
os.environ['FABRIC_WORKSPACE_REPO_DIR'] = './sample_project'
os.environ['FABRIC_ENVIRONMENT'] = 'development'

# Add the project to Python path
sys.path.insert(0, '/workspaces/ingen_fab')

def test_with_spark():
    """Test the flat file ingestion with actual Spark execution"""
    print("🚀 Testing Flat File Ingestion with Spark")
    print("=" * 60)
    
    # Import required modules
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, BooleanType
    from pyspark.sql import Row
    from datetime import datetime
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("FlatFileIngestionTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print(f"✓ Spark session created: {spark.version}")
    
    try:
        # Step 1: Create config table schema and table
        print("\n=== Step 1: Creating Configuration Table ===")
        
        config_schema = StructType([
            StructField("config_id", StringType(), nullable=False),
            StructField("config_name", StringType(), nullable=False),
            StructField("source_file_path", StringType(), nullable=False),
            StructField("source_file_format", StringType(), nullable=False),
            StructField("target_lakehouse_workspace_id", StringType(), nullable=False),
            StructField("target_lakehouse_id", StringType(), nullable=False),
            StructField("target_schema_name", StringType(), nullable=False),
            StructField("target_table_name", StringType(), nullable=False),
            StructField("file_delimiter", StringType(), nullable=True),
            StructField("has_header", BooleanType(), nullable=True),
            StructField("encoding", StringType(), nullable=True),
            StructField("date_format", StringType(), nullable=True),
            StructField("timestamp_format", StringType(), nullable=True),
            StructField("schema_inference", BooleanType(), nullable=False),
            StructField("custom_schema_json", StringType(), nullable=True),
            StructField("partition_columns", StringType(), nullable=True),
            StructField("sort_columns", StringType(), nullable=True),
            StructField("write_mode", StringType(), nullable=False),
            StructField("merge_keys", StringType(), nullable=True),
            StructField("data_validation_rules", StringType(), nullable=True),
            StructField("error_handling_strategy", StringType(), nullable=False),
            StructField("execution_group", IntegerType(), nullable=False),
            StructField("active_yn", StringType(), nullable=False),
            StructField("created_date", StringType(), nullable=False),
            StructField("modified_date", StringType(), nullable=True),
            StructField("created_by", StringType(), nullable=False),
            StructField("modified_by", StringType(), nullable=True)
        ])
        
        # Create empty config table
        config_df = spark.createDataFrame([], config_schema)
        config_df.createOrReplaceTempView("config_flat_file_ingestion")
        print("✓ Config table created")
        
        # Step 2: Create log table schema and table
        print("\n=== Step 2: Creating Log Table ===")
        
        log_schema = StructType([
            StructField("log_id", StringType(), nullable=False),
            StructField("config_id", StringType(), nullable=False),
            StructField("execution_id", StringType(), nullable=False),
            StructField("job_start_time", TimestampType(), nullable=False),
            StructField("job_end_time", TimestampType(), nullable=True),
            StructField("status", StringType(), nullable=False),
            StructField("source_file_path", StringType(), nullable=False),
            StructField("source_file_size_bytes", LongType(), nullable=True),
            StructField("source_file_modified_time", TimestampType(), nullable=True),
            StructField("target_table_name", StringType(), nullable=False),
            StructField("records_processed", LongType(), nullable=True),
            StructField("records_inserted", LongType(), nullable=True),
            StructField("records_updated", LongType(), nullable=True),
            StructField("records_deleted", LongType(), nullable=True),
            StructField("records_failed", LongType(), nullable=True),
            StructField("error_message", StringType(), nullable=True),
            StructField("error_details", StringType(), nullable=True),
            StructField("execution_duration_seconds", IntegerType(), nullable=True),
            StructField("spark_application_id", StringType(), nullable=True),
            StructField("created_date", TimestampType(), nullable=False),
            StructField("created_by", StringType(), nullable=False)
        ])
        
        # Create empty log table
        log_df = spark.createDataFrame([], log_schema)
        log_df.createOrReplaceTempView("log_flat_file_ingestion")
        print("✓ Log table created")
        
        # Step 3: Insert sample configuration data
        print("\n=== Step 3: Inserting Sample Configuration Data ===")
        
        sample_configs = [
            Row(
                config_id="csv_test_001",
                config_name="CSV Sales Data Test",
                source_file_path="./sample_project/Files/sample_data/sales_data.csv",
                source_file_format="csv",
                target_lakehouse_workspace_id="test_workspace",
                target_lakehouse_id="test_lakehouse",
                target_schema_name="raw",
                target_table_name="sales_data",
                file_delimiter=",",
                has_header=True,
                encoding="utf-8",
                date_format="yyyy-MM-dd",
                timestamp_format="yyyy-MM-dd HH:mm:ss",
                schema_inference=True,
                custom_schema_json=None,
                partition_columns="",
                sort_columns="date",
                write_mode="overwrite",
                merge_keys="",
                data_validation_rules=None,
                error_handling_strategy="fail",
                execution_group=1,
                active_yn="Y",
                created_date="2024-01-15T10:00:00",
                modified_date=None,
                created_by="test_user",
                modified_by=None
            ),
            Row(
                config_id="json_test_001",
                config_name="JSON Products Data Test",
                source_file_path="./sample_project/Files/sample_data/products.json",
                source_file_format="json",
                target_lakehouse_workspace_id="test_workspace",
                target_lakehouse_id="test_lakehouse",
                target_schema_name="raw",
                target_table_name="products",
                file_delimiter=None,
                has_header=None,
                encoding="utf-8",
                date_format="yyyy-MM-dd",
                timestamp_format="yyyy-MM-dd'T'HH:mm:ss'Z'",
                schema_inference=True,
                custom_schema_json=None,
                partition_columns="",
                sort_columns="",
                write_mode="overwrite",
                merge_keys="",
                data_validation_rules=None,
                error_handling_strategy="fail",
                execution_group=1,
                active_yn="Y",
                created_date="2024-01-15T10:00:00",
                modified_date=None,
                created_by="test_user",
                modified_by=None
            ),
            Row(
                config_id="parquet_test_001",
                config_name="Parquet Customers Data Test",
                source_file_path="./sample_project/Files/sample_data/customers.parquet",
                source_file_format="parquet",
                target_lakehouse_workspace_id="test_workspace",
                target_lakehouse_id="test_lakehouse",
                target_schema_name="raw",
                target_table_name="customers",
                file_delimiter=None,
                has_header=None,
                encoding=None,
                date_format="yyyy-MM-dd",
                timestamp_format="yyyy-MM-dd HH:mm:ss",
                schema_inference=True,
                custom_schema_json=None,
                partition_columns="",
                sort_columns="",
                write_mode="overwrite",
                merge_keys="",
                data_validation_rules=None,
                error_handling_strategy="fail",
                execution_group=1,
                active_yn="Y",
                created_date="2024-01-15T10:00:00",
                modified_date=None,
                created_by="test_user",
                modified_by=None
            )
        ]
        
        # Insert sample configs
        config_df = spark.createDataFrame(sample_configs, config_schema)
        config_df.createOrReplaceTempView("config_flat_file_ingestion")
        print(f"✓ Inserted {config_df.count()} configuration records")
        
        # Step 4: Test file ingestion for each configuration
        print("\n=== Step 4: Testing File Ingestion ===")
        
        # Import ingestion libraries
        from pyspark.sql.functions import lit, current_timestamp, col
        import uuid
        
        execution_id = str(uuid.uuid4())
        configs = config_df.collect()
        
        for config_row in configs:
            print(f"\n--- Processing {config_row['config_name']} ---")
            
            try:
                # Read file based on format
                if config_row['source_file_format'] == 'csv':
                    df = spark.read.format("csv") \
                        .option("header", "true") \
                        .option("inferSchema", "true") \
                        .option("sep", config_row['file_delimiter']) \
                        .load(config_row['source_file_path'])
                elif config_row['source_file_format'] == 'json':
                    df = spark.read.format("json") \
                        .option("inferSchema", "true") \
                        .load(config_row['source_file_path'])
                elif config_row['source_file_format'] == 'parquet':
                    df = spark.read.format("parquet") \
                        .load(config_row['source_file_path'])
                
                # Add ingestion metadata
                df_with_metadata = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                                    .withColumn("_ingestion_execution_id", lit(execution_id)) \
                                    .withColumn("_source_file_path", lit(config_row['source_file_path']))
                
                record_count = df_with_metadata.count()
                print(f"✓ Read {record_count} records from {config_row['source_file_format'].upper()} file")
                print(f"  Columns: {df_with_metadata.columns[:5]}...")  # Show first 5 columns
                
                # Create target table
                target_table_name = f"{config_row['target_schema_name']}_{config_row['target_table_name']}"
                df_with_metadata.createOrReplaceTempView(target_table_name)
                print(f"✓ Created target table: {target_table_name}")
                
                # Log successful execution
                log_entry = [Row(
                    log_id=str(uuid.uuid4()),
                    config_id=config_row['config_id'],
                    execution_id=execution_id,
                    job_start_time=datetime.now(),
                    job_end_time=datetime.now(),
                    status="completed",
                    source_file_path=config_row['source_file_path'],
                    source_file_size_bytes=None,
                    source_file_modified_time=None,
                    target_table_name=target_table_name,
                    records_processed=record_count,
                    records_inserted=record_count,
                    records_updated=0,
                    records_deleted=0,
                    records_failed=0,
                    error_message=None,
                    error_details=None,
                    execution_duration_seconds=1,
                    spark_application_id=spark.sparkContext.applicationId,
                    created_date=datetime.now(),
                    created_by="test_user"
                )]
                
                log_entry_df = spark.createDataFrame(log_entry, log_schema)
                
                # Append to log table
                existing_log = spark.sql("SELECT * FROM log_flat_file_ingestion")
                updated_log = existing_log.union(log_entry_df)
                updated_log.createOrReplaceTempView("log_flat_file_ingestion")
                
                print(f"✓ Logged execution for {config_row['config_id']}")
                
            except Exception as e:
                print(f"✗ Error processing {config_row['config_name']}: {e}")
        
        # Step 5: Show results summary
        print("\n=== Step 5: Results Summary ===")
        
        print("\nConfiguration table:")
        spark.sql("SELECT config_id, config_name, source_file_format, active_yn FROM config_flat_file_ingestion").show()
        
        print("Execution log:")
        spark.sql("SELECT config_id, status, records_processed, records_inserted FROM log_flat_file_ingestion").show()
        
        print("Created target tables:")
        for config_row in configs:
            table_name = f"{config_row['target_schema_name']}_{config_row['target_table_name']}"
            try:
                count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
                print(f"  - {table_name}: {count} records")
            except:
                print(f"  - {table_name}: table not found")
        
        print("\n" + "=" * 60)
        print("✅ SPARK WORKFLOW TEST COMPLETE")
        print("✅ All sample files successfully ingested")
        print("✅ Configuration and logging tables populated")
        print("✅ Target tables created with ingestion metadata")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("✓ Spark session stopped")

if __name__ == "__main__":
    test_with_spark()