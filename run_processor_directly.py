#!/usr/bin/env python3
"""
Run the flat file processor directly with sample data
"""

import os
import sys
from pathlib import Path

# Set up environment for the notebook
os.environ['FABRIC_WORKSPACE_REPO_DIR'] = './sample_project'
os.environ['FABRIC_ENVIRONMENT'] = 'development'

# Set notebook parameters
config_id = ""  # Process all configs
execution_group = 1
environment = "development"

# Add the project to Python path
sys.path.insert(0, '/workspaces/ingen_fab')

print("🚀 Running Flat File Processor Directly")
print("=" * 50)
print(f"Config ID: {config_id if config_id else 'ALL ACTIVE'}")
print(f"Execution Group: {execution_group}")
print(f"Environment: {environment}")
print("=" * 50)

# Import the required modules that the notebook needs
import sys
import traceback

# Check if running in Fabric environment
if "notebookutils" in sys.modules:
    import sys
    
    notebookutils.fs.mount("abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_lakehouse_name}}.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
    mount_path = notebookutils.fs.getMountPath("/config_files")  # type: ignore # noqa: F821
    
    run_mode = "fabric"
    sys.path.insert(0, mount_path)

    # PySpark environment - spark session should be available
    
else:
    print("NotebookUtils not available, assumed running in local mode.")
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )
    notebookutils = NotebookUtilsFactory.create_instance()
    
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("LocalTesting").getOrCreate()
    
    mount_path = None
    run_mode = "local"

def load_python_modules_from_path(base_path: str, relative_files: list[str], max_chars: int = 1_000_000_000):
    """
    Executes Python files from a Fabric-mounted file path using notebookutils.fs.head.
    
    Args:
        base_path (str): The root directory where modules are located.
        relative_files (list[str]): List of relative paths to Python files (from base_path).
        max_chars (int): Max characters to read from each file (default: 1,000,000).
    """
    success_files = []
    failed_files = []

    for relative_path in relative_files:
        full_path = f"file:{base_path}/{relative_path}"
        try:
            print(f"🔄 Loading: {full_path}")
            code = notebookutils.fs.head(full_path, max_chars)
            exec(code, globals())  # Use globals() to share context across modules
            success_files.append(relative_path)
        except Exception as e:
            failed_files.append(relative_path)
            print(f"❌ Error loading {relative_path}")

    print("\\n✅ Successfully loaded:")
    for f in success_files:
        print(f" - {f}")

    if failed_files:
        print("\\n⚠️ Failed to load:")
        for f in failed_files:
            print(f" - {f}")

def clear_module_cache(prefix: str):
    """Clear module cache for specified prefix"""
    for mod in list(sys.modules):
        if mod.startswith(prefix):
            print("deleting..." + mod)
            del sys.modules[mod]

# Always clear the module cache - We may remove this once the libs are stable
clear_module_cache("ingen_fab.python_libs")
clear_module_cache("ingen_fab")

# Load configuration and initialize utilities
if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import get_configs_as_object, ConfigsObject
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.get_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py"
    ]
    load_python_modules_from_path(mount_path, files_to_load)

# Initialize configuration
configs: ConfigsObject = get_configs_as_object()

# Additional imports for flat file ingestion
import uuid
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, BooleanType
from pyspark.sql.functions import lit, current_timestamp, col, when, coalesce

# Initialize spark session and execution tracking
if run_mode == "local" and spark is None:
    spark = SparkSession.builder.appName("FlatFileIngestion").getOrCreate()
execution_id = str(uuid.uuid4())

print(f"Execution ID: {execution_id}")
print(f"Config ID: {config_id}")
print(f"Execution Group: {execution_group}")
print(f"Environment: {environment}")

# Since tables don't exist yet, let's create some sample configurations directly
print("\n=== Creating Sample Configuration Data ===")

# Create sample config data directly
sample_configs = [
    {
        "config_id": "csv_test_001",
        "config_name": "CSV Sales Data Test",
        "source_file_path": "./sample_project/Files/sample_data/sales_data.csv",
        "source_file_format": "csv",
        "target_lakehouse_workspace_id": "test_workspace",
        "target_lakehouse_id": "test_lakehouse",
        "target_schema_name": "raw",
        "target_table_name": "sales_data",
        "file_delimiter": ",",
        "has_header": True,
        "encoding": "utf-8",
        "date_format": "yyyy-MM-dd",
        "timestamp_format": "yyyy-MM-dd HH:mm:ss",
        "schema_inference": True,
        "custom_schema_json": None,
        "partition_columns": "",
        "sort_columns": "date",
        "write_mode": "overwrite",
        "merge_keys": "",
        "data_validation_rules": None,
        "error_handling_strategy": "fail",
        "execution_group": 1,
        "active_yn": "Y"
    },
    {
        "config_id": "json_test_002",
        "config_name": "JSON Products Data Test",
        "source_file_path": "./sample_project/Files/sample_data/products.json",
        "source_file_format": "json",
        "target_lakehouse_workspace_id": "test_workspace",
        "target_lakehouse_id": "test_lakehouse",
        "target_schema_name": "raw",
        "target_table_name": "products",
        "file_delimiter": None,
        "has_header": None,
        "encoding": "utf-8",
        "date_format": "yyyy-MM-dd",
        "timestamp_format": "yyyy-MM-dd'T'HH:mm:ss'Z'",
        "schema_inference": True,
        "custom_schema_json": None,
        "partition_columns": "category",
        "sort_columns": "product_id",
        "write_mode": "overwrite",
        "merge_keys": "",
        "data_validation_rules": None,
        "error_handling_strategy": "fail",
        "execution_group": 1,
        "active_yn": "Y"
    }
]

# Convert to DataFrame for processing
import pandas as pd
config_df = pd.DataFrame(sample_configs)

# Filter configurations
if config_id:
    config_df = config_df[config_df["config_id"] == config_id]
else:
    config_df = config_df[
        (config_df["execution_group"] == execution_group) & 
        (config_df["active_yn"] == "Y")
    ]

if config_df.empty:
    raise ValueError(f"No active configurations found for config_id: {config_id}, execution_group: {execution_group}")

print(f"Found {len(config_df)} configurations to process")

# Now run the flat file processing logic
print("\n=== Processing Files ===")

class FlatFileIngestionConfig:
    """Configuration class for flat file ingestion"""
    
    def __init__(self, config_row):
        self.config_id = config_row["config_id"]
        self.config_name = config_row["config_name"]
        self.source_file_path = config_row["source_file_path"]
        self.source_file_format = config_row["source_file_format"]
        self.target_lakehouse_workspace_id = config_row["target_lakehouse_workspace_id"]
        self.target_lakehouse_id = config_row["target_lakehouse_id"]
        self.target_schema_name = config_row["target_schema_name"]
        self.target_table_name = config_row["target_table_name"]
        self.file_delimiter = config_row.get("file_delimiter", ",")
        self.has_header = config_row.get("has_header", True)
        self.encoding = config_row.get("encoding", "utf-8")
        self.date_format = config_row.get("date_format", "yyyy-MM-dd")
        self.timestamp_format = config_row.get("timestamp_format", "yyyy-MM-dd HH:mm:ss")
        self.schema_inference = config_row.get("schema_inference", True)
        self.custom_schema_json = config_row.get("custom_schema_json")
        self.partition_columns = config_row.get("partition_columns", "").split(",") if config_row.get("partition_columns") else []
        self.sort_columns = config_row.get("sort_columns", "").split(",") if config_row.get("sort_columns") else []
        self.write_mode = config_row.get("write_mode", "overwrite")
        self.merge_keys = config_row.get("merge_keys", "").split(",") if config_row.get("merge_keys") else []
        self.data_validation_rules = config_row.get("data_validation_rules")
        self.error_handling_strategy = config_row.get("error_handling_strategy", "fail")
        self.execution_group = config_row["execution_group"]
        self.active_yn = config_row["active_yn"]

class FlatFileProcessor:
    """Main processor for flat file ingestion"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        
    def read_file(self, config: FlatFileIngestionConfig) -> DataFrame:
        """Read file based on format and configuration"""
        
        if config.source_file_format.lower() == "csv":
            return self._read_csv(config)
        elif config.source_file_format.lower() == "json":
            return self._read_json(config)
        elif config.source_file_format.lower() == "parquet":
            return self._read_parquet(config)
        else:
            raise ValueError(f"Unsupported file format: {config.source_file_format}")
    
    def _read_csv(self, config: FlatFileIngestionConfig) -> DataFrame:
        """Read CSV file"""
        reader = self.spark.read.format("csv")
        
        if config.has_header:
            reader = reader.option("header", "true")
        
        if config.file_delimiter:
            reader = reader.option("sep", config.file_delimiter)
            
        if config.encoding:
            reader = reader.option("encoding", config.encoding)
            
        if config.date_format:
            reader = reader.option("dateFormat", config.date_format)
            
        if config.timestamp_format:
            reader = reader.option("timestampFormat", config.timestamp_format)
            
        if config.schema_inference:
            reader = reader.option("inferSchema", "true")
            
        if config.custom_schema_json:
            schema = StructType.fromJson(json.loads(config.custom_schema_json))
            reader = reader.schema(schema)
            
        return reader.load(config.source_file_path)
    
    def _read_json(self, config: FlatFileIngestionConfig) -> DataFrame:
        """Read JSON file"""
        reader = self.spark.read.format("json")
        
        if config.date_format:
            reader = reader.option("dateFormat", config.date_format)
            
        if config.timestamp_format:
            reader = reader.option("timestampFormat", config.timestamp_format)
            
        if config.custom_schema_json:
            schema = StructType.fromJson(json.loads(config.custom_schema_json))
            reader = reader.schema(schema)
            
        return reader.load(config.source_file_path)
    
    def _read_parquet(self, config: FlatFileIngestionConfig) -> DataFrame:
        """Read Parquet file"""
        return self.spark.read.format("parquet").load(config.source_file_path)

# Initialize processor
processor = FlatFileProcessor(spark)

# Process each configuration
results = []
for _, config_row in config_df.iterrows():
    config = FlatFileIngestionConfig(config_row)
    start_time = datetime.now()
    
    try:
        print(f"\\n=== Processing {config.config_name} ===")
        print(f"Source: {config.source_file_path}")
        print(f"Target: {config.target_schema_name}.{config.target_table_name}")
        print(f"Format: {config.source_file_format}")
        print(f"Write Mode: {config.write_mode}")
        
        # Read file
        df = processor.read_file(config)
        print(f"Read {df.count()} records from source file")
        
        # Add ingestion metadata
        df_with_metadata = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                            .withColumn("_ingestion_execution_id", lit(execution_id)) \
                            .withColumn("_source_file_path", lit(config.source_file_path))
        
        record_count = df_with_metadata.count()
        
        # Create target table view for this session
        full_table_name = f"{config.target_schema_name}_{config.target_table_name}"
        df_with_metadata.createOrReplaceTempView(full_table_name)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"Processing completed in {duration:.2f} seconds")
        print(f"Records processed: {record_count}")
        print(f"Target table created: {full_table_name}")
        
        # Show sample of the data
        print("Sample data:")
        df_with_metadata.show(5, truncate=False)
        
        results.append({
            "config_id": config.config_id,
            "config_name": config.config_name,
            "status": "success",
            "duration": duration,
            "records_processed": record_count,
            "target_table": full_table_name
        })
        
    except Exception as e:
        end_time = datetime.now()
        error_message = str(e)
        
        print(f"Error processing {config.config_name}: {error_message}")
        
        results.append({
            "config_id": config.config_id,
            "config_name": config.config_name,
            "status": "failed",
            "error": error_message
        })
        
        if config.error_handling_strategy == "fail":
            raise e

# Print summary
print("\\n=== EXECUTION SUMMARY ===")
print(f"Execution ID: {execution_id}")
print(f"Total configurations processed: {len(results)}")

successful = [r for r in results if r["status"] == "success"]
failed = [r for r in results if r["status"] == "failed"]

print(f"Successful: {len(successful)}")
print(f"Failed: {len(failed)}")

if successful:
    print("\\nSuccessful configurations:")
    for result in successful:
        print(f"  - {result['config_name']}: {result['records_processed']} records in {result['duration']:.2f}s")
        print(f"    Table: {result['target_table']}")

if failed:
    print("\\nFailed configurations:")
    for result in failed:
        print(f"  - {result['config_name']}: {result['error']}")

print(f"\\nExecution completed at: {datetime.now()}")

# Show created tables
print("\\n=== CREATED TABLES ===")
for result in successful:
    table_name = result['target_table']
    try:
        count = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()[0]['count']
        print(f"✓ {table_name}: {count} records")
        
        # Show schema
        spark.sql(f"DESCRIBE {table_name}").show()
    except Exception as e:
        print(f"✗ Error accessing {table_name}: {e}")

spark.stop()
print("✓ Spark session stopped")
print("\\n🎉 FLAT FILE INGESTION COMPLETE!")