# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark",
# META     "display_name": "PySpark (Synapse)"
# META   },
# META   "language_info": {
# META     "name": "python",
# META     "language_group": "synapse_pyspark"
# META   }
# META }

# CELL ********************

# MARKDOWN ********************

# ## 📄 Flat File Ingestion Notebook
# 
# This notebook processes flat files (CSV, JSON, Parquet, Avro, XML) and loads them into delta tables based on configuration metadata.

# PARAMETERS CELL ********************

# Default parameters
config_id = ""
execution_group = 1
environment = "development"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Import Libraries and Initialize

# CELL ********************

import sys
import uuid
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, BooleanType
from pyspark.sql.functions import lit, current_timestamp, col, when, coalesce
from delta.tables import DeltaTable

# Mount configuration files
if "notebookutils" in sys.modules:
    notebookutils.fs.mount(
        "abfss://3a4fc13c-f7c5-463e-a9de-57c4754699ff@onelake.dfs.fabric.microsoft.com/.Lakehouse/Files/",
        "/config_files"
    )
    new_path = notebookutils.fs.getMountPath("/config_files")
    sys.path.insert(0, new_path)
else:
    print("NotebookUtils not available, using local imports")
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.create_instance()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔧 Configuration and Setup

# CELL ********************

from ingen_fab.python_libs.common.config_utils import get_configs_as_object, ConfigsObject
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

# Initialize configuration
configs: ConfigsObject = get_configs_as_object()
spark = SparkSession.builder.appName("FlatFileIngestion").getOrCreate()
execution_id = str(uuid.uuid4())

# Initialize lakehouse utilities
config_lakehouse = lakehouse_utils(
    target_workspace_id=configs.config_lakehouse_workspace_id,
    target_lakehouse_id=configs.config_lakehouse_id
)

print(f"Execution ID: {execution_id}")
print(f"Config ID: {config_id}")
print(f"Execution Group: {execution_group}")
print(f"Environment: {environment}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📋 Load Configuration Data

# CELL ********************

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

# Load configuration
config_df = config_lakehouse.read_table("config_flat_file_ingestion").to_pandas()

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🚀 File Processing Functions

# CELL ********************

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
        elif config.source_file_format.lower() == "avro":
            return self._read_avro(config)
        elif config.source_file_format.lower() == "xml":
            return self._read_xml(config)
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
    
    def _read_avro(self, config: FlatFileIngestionConfig) -> DataFrame:
        """Read Avro file"""
        return self.spark.read.format("avro").load(config.source_file_path)
    
    def _read_xml(self, config: FlatFileIngestionConfig) -> DataFrame:
        """Read XML file"""
        reader = self.spark.read.format("xml")
        
        if config.custom_schema_json:
            schema = StructType.fromJson(json.loads(config.custom_schema_json))
            reader = reader.schema(schema)
            
        return reader.load(config.source_file_path)
    
    def validate_data(self, df: DataFrame, config: FlatFileIngestionConfig) -> DataFrame:
        """Apply data validation rules"""
        if not config.data_validation_rules:
            return df
            
        try:
            validation_rules = json.loads(config.data_validation_rules)
            # Apply validation rules here
            # This is a placeholder for custom validation logic
            return df
        except Exception as e:
            if config.error_handling_strategy == "fail":
                raise e
            elif config.error_handling_strategy == "log":
                print(f"Validation error: {e}")
                return df
            else:  # skip
                return df.limit(0)
    
    def write_data(self, df: DataFrame, config: FlatFileIngestionConfig) -> Dict[str, Any]:
        """Write data to target table"""
        
        # Initialize target lakehouse
        target_lakehouse = lakehouse_utils(
            target_workspace_id=config.target_lakehouse_workspace_id,
            target_lakehouse_id=config.target_lakehouse_id
        )
        
        full_table_name = f"{config.target_schema_name}.{config.target_table_name}"
        
        # Add metadata columns
        df_with_metadata = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                            .withColumn("_ingestion_execution_id", lit(execution_id)) \
                            .withColumn("_source_file_path", lit(config.source_file_path))
        
        write_stats = {
            "records_processed": df_with_metadata.count(),
            "records_inserted": 0,
            "records_updated": 0,
            "records_deleted": 0
        }
        
        if config.write_mode == "overwrite":
            df_with_metadata.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
            write_stats["records_inserted"] = write_stats["records_processed"]
            
        elif config.write_mode == "append":
            df_with_metadata.write.format("delta").mode("append").saveAsTable(full_table_name)
            write_stats["records_inserted"] = write_stats["records_processed"]
            
        elif config.write_mode == "merge":
            if not config.merge_keys:
                raise ValueError("Merge keys must be specified for merge write mode")
                
            # Create delta table if it doesn't exist
            try:
                delta_table = DeltaTable.forName(self.spark, full_table_name)
            except Exception:
                # Table doesn't exist, create it
                df_with_metadata.write.format("delta").saveAsTable(full_table_name)
                write_stats["records_inserted"] = write_stats["records_processed"]
                return write_stats
            
            # Perform merge
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in config.merge_keys])
            
            merge_result = delta_table.alias("target").merge(
                df_with_metadata.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            
            # Parse merge statistics
            if 'operationMetrics' in merge_result:
                metrics = merge_result['operationMetrics']
                write_stats["records_inserted"] = int(metrics.get('numTargetRowsInserted', 0))
                write_stats["records_updated"] = int(metrics.get('numTargetRowsUpdated', 0))
                write_stats["records_deleted"] = int(metrics.get('numTargetRowsDeleted', 0))
        
        return write_stats

# Initialize processor
processor = FlatFileProcessor(spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📊 Process Files

# CELL ********************

def log_execution(config: FlatFileIngestionConfig, status: str, write_stats: Dict[str, Any] = None, 
                 error_message: str = None, error_details: str = None, 
                 start_time: datetime = None, end_time: datetime = None):
    """Log execution details"""
    
    duration = None
    if start_time and end_time:
        duration = int((end_time - start_time).total_seconds())
    
    log_data = {
        "log_id": str(uuid.uuid4()),
        "config_id": config.config_id,
        "execution_id": execution_id,
        "job_start_time": start_time,
        "job_end_time": end_time,
        "status": status,
        "source_file_path": config.source_file_path,
        "target_table_name": f"{config.target_schema_name}.{config.target_table_name}",
        "records_processed": write_stats.get("records_processed", 0) if write_stats else 0,
        "records_inserted": write_stats.get("records_inserted", 0) if write_stats else 0,
        "records_updated": write_stats.get("records_updated", 0) if write_stats else 0,
        "records_deleted": write_stats.get("records_deleted", 0) if write_stats else 0,
        "records_failed": write_stats.get("records_failed", 0) if write_stats else 0,
        "error_message": error_message,
        "error_details": error_details,
        "execution_duration_seconds": duration,
        "spark_application_id": spark.sparkContext.applicationId,
        "created_date": datetime.now(),
        "created_by": "system"
    }
    
    # Insert log record
    log_df = spark.createDataFrame([log_data])
    log_df.write.format("delta").mode("append").saveAsTable("log_flat_file_ingestion")

# Process each configuration
results = []
for _, config_row in config_df.iterrows():
    config = FlatFileIngestionConfig(config_row)
    start_time = datetime.now()
    
    try:
        print(f"\n=== Processing {config.config_name} ===")
        print(f"Source: {config.source_file_path}")
        print(f"Target: {config.target_schema_name}.{config.target_table_name}")
        print(f"Format: {config.source_file_format}")
        print(f"Write Mode: {config.write_mode}")
        
        # Log start
        log_execution(config, "running", start_time=start_time)
        
        # Read file
        df = processor.read_file(config)
        print(f"Read {df.count()} records from source file")
        
        # Validate data
        df_validated = processor.validate_data(df, config)
        
        # Write data
        write_stats = processor.write_data(df_validated, config)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"Processing completed in {duration:.2f} seconds")
        print(f"Records processed: {write_stats['records_processed']}")
        print(f"Records inserted: {write_stats['records_inserted']}")
        print(f"Records updated: {write_stats['records_updated']}")
        
        # Log success
        log_execution(config, "completed", write_stats, start_time=start_time, end_time=end_time)
        
        results.append({
            "config_id": config.config_id,
            "config_name": config.config_name,
            "status": "success",
            "duration": duration,
            "records_processed": write_stats['records_processed']
        })
        
    except Exception as e:
        end_time = datetime.now()
        error_message = str(e)
        error_details = str(e.__class__.__name__)
        
        print(f"Error processing {config.config_name}: {error_message}")
        
        # Log error
        log_execution(config, "failed", error_message=error_message, error_details=error_details,
                     start_time=start_time, end_time=end_time)
        
        results.append({
            "config_id": config.config_id,
            "config_name": config.config_name,
            "status": "failed",
            "error": error_message
        })
        
        if config.error_handling_strategy == "fail":
            raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📈 Execution Summary

# CELL ********************

# Print summary
print("\n=== EXECUTION SUMMARY ===")
print(f"Execution ID: {execution_id}")
print(f"Total configurations processed: {len(results)}")

successful = [r for r in results if r["status"] == "success"]
failed = [r for r in results if r["status"] == "failed"]

print(f"Successful: {len(successful)}")
print(f"Failed: {len(failed)}")

if successful:
    print("\nSuccessful configurations:")
    for result in successful:
        print(f"  - {result['config_name']}: {result['records_processed']} records in {result['duration']:.2f}s")

if failed:
    print("\nFailed configurations:")
    for result in failed:
        print(f"  - {result['config_name']}: {result['error']}")

# Create summary dataframe for potential downstream use
summary_df = spark.createDataFrame(results)
summary_df.show(truncate=False)

print(f"\nExecution completed at: {datetime.now()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }