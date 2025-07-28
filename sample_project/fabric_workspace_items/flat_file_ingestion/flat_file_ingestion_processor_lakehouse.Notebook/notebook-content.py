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


# MARKDOWN ********************

# ## 『』Parameters

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

# ## ## 📄 Flat File Ingestion Notebook

# CELL ********************


# This notebook processes flat files (CSV, JSON, Parquet, Avro, XML) and loads them into delta tables based on configuration metadata.




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Load Python Libraries and Initialize Environment

# CELL ********************

import sys

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
        
    spark = None
    
    mount_path = None
    run_mode = "local"

import traceback

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

    print("\n✅ Successfully loaded:")
    for f in success_files:
        print(f" - {f}")

    if failed_files:
        print("\n⚠️ Failed to load:")
        for f in failed_files:
            print(f" - {f}")

def clear_module_cache(prefix: str):
    """Clear module cache for specified prefix"""
    for mod in list(sys.modules):
        if mod.startswith(prefix):
            print("deleting..." + mod)
            del sys.modules[mod]

# Clear the module cache only when running in Fabric environment
# When running locally, module caching conflicts can occur in parallel execution
if run_mode == "fabric":
    # Check if ingen_fab modules are present in cache (indicating they need clearing)
    ingen_fab_modules = [mod for mod in sys.modules.keys() if mod.startswith(('ingen_fab.python_libs', 'ingen_fab'))]
    
    if ingen_fab_modules:
        print(f"Found {len(ingen_fab_modules)} ingen_fab modules to clear from cache")
        clear_module_cache("ingen_fab.python_libs")
        clear_module_cache("ingen_fab")
        print("✓ Module cache cleared for ingen_fab libraries")
    else:
        print("ℹ No ingen_fab modules found in cache - already cleared or first load")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 🔧 Load Configuration and Initialize

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔧 Load Configuration and Initialize Utilities

# CELL ********************


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
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType, BooleanType, FloatType
from pyspark.sql.functions import lit, current_timestamp, col, when, coalesce

execution_id = str(uuid.uuid4())

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

# ## ## 📋 Load Configuration Data

# CELL ********************


class FlatFileIngestionConfig:
    """Configuration class for flat file ingestion"""
    
    def __init__(self, config_row):
        self.config_id = config_row["config_id"]
        self.config_name = config_row["config_name"]
        self.source_file_path = config_row["source_file_path"]
        self.source_file_format = config_row["source_file_format"]
        self.target_workspace_id = config_row["target_workspace_id"]
        self.target_datastore_id = config_row["target_datastore_id"]
        self.target_datastore_type = config_row["target_datastore_type"]
        self.target_schema_name = config_row["target_schema_name"]
        self.target_table_name = config_row["target_table_name"]
        self.staging_table_name = config_row.get("staging_table_name")
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

# Initialize config lakehouse utilities
config_lakehouse = lakehouse_utils(
    target_workspace_id=configs.config_workspace_id,
    target_lakehouse_id=configs.config_lakehouse_id,
    spark=spark
)

# Initialize raw data lakehouse utilities for file access
raw_lakehouse = lakehouse_utils(
    target_workspace_id=configs.raw_workspace_id,
    target_lakehouse_id=configs.raw_datastore_id,
    spark=spark
)

# Load configuration
config_df = config_lakehouse.read_table("config_flat_file_ingestion").toPandas()

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

# ## ## 🚀 File Processing Functions

# CELL ********************


class FlatFileProcessor:
    """Main processor for flat file ingestion using python_libs abstractions"""
    
    def __init__(self, spark_session: SparkSession, raw_lakehouse_utils):
        self.spark = spark_session
        self.raw_lakehouse = raw_lakehouse_utils
        
    def read_file(self, config: FlatFileIngestionConfig) -> tuple[DataFrame, Dict[str, Any]]:
        """Read file based on format and configuration using abstracted file access"""
        
        # Track read performance
        read_start = time.time()
        
        # Build options dictionary for the abstracted read_file method
        options = {}
        
        if config.source_file_format.lower() == "csv":
            options["header"] = config.has_header
            options["delimiter"] = config.file_delimiter
            options["encoding"] = config.encoding
            options["inferSchema"] = config.schema_inference
            options["dateFormat"] = config.date_format
            options["timestampFormat"] = config.timestamp_format
            
        elif config.source_file_format.lower() == "json":
            options["dateFormat"] = config.date_format
            options["timestampFormat"] = config.timestamp_format
            
        # Add custom schema if provided
        if config.custom_schema_json:
            import json
            from pyspark.sql.types import StructType
            schema = StructType.fromJson(json.loads(config.custom_schema_json))
            options["schema"] = schema
            
        # Use the abstracted read_file method from raw lakehouse_utils
        df = self.raw_lakehouse.read_file(
            file_path=config.source_file_path,
            file_format=config.source_file_format,
            options=options
        )
        
        # Calculate read duration
        read_duration_ms = int((time.time() - read_start) * 1000)
        
        # Get source row count
        source_row_count = df.count()
        
        read_stats = {
            "data_read_duration_ms": read_duration_ms,
            "source_row_count": source_row_count
        }
        
        return df, read_stats
    
    
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
    
    def write_data(self, df: DataFrame, config: FlatFileIngestionConfig, target_lakehouse: Any, source_row_count: int) -> Dict[str, Any]:
        """Write data to target table using lakehouse_utils abstraction"""
        
        # Track write performance
        write_start = time.time()
        
        # Get target row count before write
        full_table_name = f"{config.target_schema_name}_{config.target_table_name}" if config.target_schema_name else config.target_table_name
        
        try:
            target_df_before = target_lakehouse.read_table(full_table_name)
            target_count_before = target_df_before.count()
        except:
            target_count_before = 0  # Table doesn't exist yet
        
        # Add metadata columns for traceability
        df_with_metadata = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                            .withColumn("_ingestion_execution_id", lit(execution_id)) \
                            .withColumn("_source_file_path", lit(config.source_file_path))
        
        staging_row_count = df_with_metadata.count()
        
        # Handle different write modes using lakehouse_utils
        write_options = {}
        if config.partition_columns:
            write_options["partitionBy"] = config.partition_columns
        
        # Use the abstracted write_to_table method
        target_lakehouse.write_to_table(
            df=df_with_metadata,
            table_name=full_table_name,
            mode=config.write_mode,
            options=write_options
        )
        
        write_duration_ms = int((time.time() - write_start) * 1000)
        
        # Get target row count after write
        target_df_after = target_lakehouse.read_table(full_table_name)
        target_count_after = target_df_after.count()
        
        # Calculate actual records inserted/updated
        if config.write_mode == "overwrite":
            records_inserted = target_count_after
            records_updated = 0
            records_deleted = target_count_before
        elif config.write_mode == "append":
            records_inserted = target_count_after - target_count_before
            records_updated = 0
            records_deleted = 0
        else:  # merge or other modes
            records_inserted = max(0, target_count_after - target_count_before)
            records_updated = min(staging_row_count, target_count_before)
            records_deleted = 0
        
        write_stats = {
            "records_processed": target_count_after,
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_deleted": records_deleted,
            "staging_row_count": staging_row_count,
            "target_row_count_before": target_count_before,
            "target_row_count_after": target_count_after,
            "staging_write_duration_ms": write_duration_ms
        }
        
        return write_stats

# Initialize processor
processor = FlatFileProcessor(spark, raw_lakehouse)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 📊 Process Files

# CELL ********************


def log_execution(config: FlatFileIngestionConfig, status: str, write_stats: Dict[str, Any] = None,
                 read_stats: Dict[str, Any] = None, error_message: str = None, error_details: str = None, 
                 start_time: datetime = None, end_time: datetime = None):
    """Log execution details with performance metrics using lakehouse_utils abstraction"""
    
    duration = None
    total_duration_ms = None
    if start_time and end_time:
        duration = int((end_time - start_time).total_seconds())
        total_duration_ms = int((end_time - start_time).total_seconds() * 1000)
    
    # Get file information using abstracted method
    file_info = raw_lakehouse.get_file_info(config.source_file_path)
    
    # Calculate performance metrics
    source_row_count = read_stats.get("source_row_count", 0) if read_stats else 0
    staging_row_count = write_stats.get("staging_row_count", 0) if write_stats else 0
    target_count_before = write_stats.get("target_row_count_before", 0) if write_stats else 0
    target_count_after = write_stats.get("target_row_count_after", 0) if write_stats else 0
    
    # Row count reconciliation
    row_count_reconciliation_status = "not_verified"
    row_count_difference = None
    if status == "completed" and source_row_count > 0:
        if config.write_mode == "overwrite" and source_row_count == target_count_after:
            row_count_reconciliation_status = "matched"
            row_count_difference = 0
        elif config.write_mode == "append" and (target_count_after - target_count_before) == source_row_count:
            row_count_reconciliation_status = "matched"
            row_count_difference = 0
        else:
            row_count_reconciliation_status = "mismatched"
            row_count_difference = abs(source_row_count - (target_count_after - target_count_before))
    
    # Calculate throughput
    avg_rows_per_second = None
    data_size_mb = None
    throughput_mb_per_second = None
    if total_duration_ms and total_duration_ms > 0:
        avg_rows_per_second = (source_row_count / total_duration_ms) * 1000 if source_row_count else 0
        if file_info.get("size"):
            data_size_mb = file_info["size"] / (1024 * 1024)
            throughput_mb_per_second = (data_size_mb / total_duration_ms) * 1000
    
    log_data = {
        "log_id": str(uuid.uuid4()),
        "config_id": config.config_id,
        "execution_id": execution_id,
        "job_start_time": start_time,
        "job_end_time": end_time,
        "status": status,
        "source_file_path": config.source_file_path,
        "source_file_size_bytes": file_info.get("size"),
        "source_file_modified_time": file_info.get("modified_time"),
        "target_table_name": f"{config.target_schema_name}.{config.target_table_name}",
        "records_processed": write_stats.get("records_processed", 0) if write_stats else 0,
        "records_inserted": write_stats.get("records_inserted", 0) if write_stats else 0,
        "records_updated": write_stats.get("records_updated", 0) if write_stats else 0,
        "records_deleted": write_stats.get("records_deleted", 0) if write_stats else 0,
        "records_failed": write_stats.get("records_failed", 0) if write_stats else 0,
        # Performance metrics
        "source_row_count": source_row_count,
        "staging_row_count": staging_row_count,
        "target_row_count_before": target_count_before,
        "target_row_count_after": target_count_after,
        "row_count_reconciliation_status": row_count_reconciliation_status,
        "row_count_difference": row_count_difference,
        "data_read_duration_ms": read_stats.get("data_read_duration_ms") if read_stats else None,
        "staging_write_duration_ms": write_stats.get("staging_write_duration_ms") if write_stats else None,
        "merge_duration_ms": None,  # Not applicable for lakehouse direct writes
        "total_duration_ms": total_duration_ms,
        "avg_rows_per_second": avg_rows_per_second,
        "data_size_mb": data_size_mb,
        "throughput_mb_per_second": throughput_mb_per_second,
        # Error tracking
        "error_message": error_message,
        "error_details": error_details,
        "execution_duration_seconds": duration,
        "spark_application_id": getattr(spark, "sparkContext", None) and spark.sparkContext.applicationId or "unknown",
        "created_date": datetime.now(),
        "created_by": "system"
    }
    
    # Use lakehouse_utils abstraction for logging
    # Define log schema to match the DDL
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
    
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
        # Performance metrics
        StructField("source_row_count", LongType(), nullable=True),
        StructField("staging_row_count", LongType(), nullable=True),
        StructField("target_row_count_before", LongType(), nullable=True),
        StructField("target_row_count_after", LongType(), nullable=True),
        StructField("row_count_reconciliation_status", StringType(), nullable=True),
        StructField("row_count_difference", LongType(), nullable=True),
        StructField("data_read_duration_ms", LongType(), nullable=True),
        StructField("staging_write_duration_ms", LongType(), nullable=True),
        StructField("merge_duration_ms", LongType(), nullable=True),
        StructField("total_duration_ms", LongType(), nullable=True),
        StructField("avg_rows_per_second", FloatType(), nullable=True),
        StructField("data_size_mb", FloatType(), nullable=True),
        StructField("throughput_mb_per_second", FloatType(), nullable=True),
        # Error tracking
        StructField("error_message", StringType(), nullable=True),
        StructField("error_details", StringType(), nullable=True),
        StructField("execution_duration_seconds", IntegerType(), nullable=True),
        StructField("spark_application_id", StringType(), nullable=True),
        StructField("created_date", TimestampType(), nullable=False),
        StructField("created_by", StringType(), nullable=False)
    ])
    
    # Use config_lakehouse to create DataFrame with abstraction
    log_df = config_lakehouse.get_connection.createDataFrame([log_data], log_schema)
    config_lakehouse.write_to_table(
        df=log_df,
        table_name="log_flat_file_ingestion",
        mode="append"
    )

# Process each configuration
results = []
for _, config_row in config_df.iterrows():
    config = FlatFileIngestionConfig(config_row)
    start_time = datetime.now()
    
    # Initialize target lakehouse for this configuration
    target_lakehouse = lakehouse_utils(
        target_workspace_id=config.target_workspace_id,
        target_lakehouse_id=config.target_datastore_id,
        spark=spark
    )
    
    try:
        print(f"\n=== Processing {config.config_name} ===")
        print(f"Source: {config.source_file_path}")
        print(f"Target: {config.target_schema_name}.{config.target_table_name}")
        print(f"Format: {config.source_file_format}")
        print(f"Write Mode: {config.write_mode}")
        
        # Log start
        log_execution(config, "running", start_time=start_time)
        
        # Read file with performance tracking
        df, read_stats = processor.read_file(config)
        print(f"Read {read_stats['source_row_count']} records from source file in {read_stats['data_read_duration_ms']}ms")
        
        # Validate data
        df_validated = processor.validate_data(df, config)
        
        # Write data using abstraction with performance tracking
        write_stats = processor.write_data(df_validated, config, target_lakehouse, read_stats['source_row_count'])
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\nProcessing completed in {duration:.2f} seconds")
        print(f"Records processed: {write_stats['records_processed']}")
        print(f"Records inserted: {write_stats['records_inserted']}")
        print(f"Records updated: {write_stats['records_updated']}")
        print(f"Source rows: {read_stats['source_row_count']}")
        print(f"Target rows before: {write_stats['target_row_count_before']}")
        print(f"Target rows after: {write_stats['target_row_count_after']}")
        
        # Check reconciliation
        recon_status = "matched"
        if config.write_mode == "overwrite" and read_stats['source_row_count'] != write_stats['target_row_count_after']:
            recon_status = "mismatched"
        elif config.write_mode == "append" and read_stats['source_row_count'] != (write_stats['target_row_count_after'] - write_stats['target_row_count_before']):
            recon_status = "mismatched"
        print(f"Row count reconciliation: {recon_status}")
        
        # Log success with all metrics
        log_execution(config, "completed", write_stats, read_stats, start_time=start_time, end_time=end_time)
        
        results.append({
            "config_id": config.config_id,
            "config_name": config.config_name,
            "status": "success",
            "duration": duration,
            "records_processed": write_stats['records_processed'],
            "reconciliation_status": recon_status,
            "performance": {
                "source_rows": read_stats['source_row_count'],
                "avg_rows_per_second": (read_stats['source_row_count'] / duration) if duration > 0 else 0,
                "read_duration_ms": read_stats['data_read_duration_ms'],
                "write_duration_ms": write_stats['staging_write_duration_ms']
            }
        })
        
    except Exception as e:
        end_time = datetime.now()
        error_message = str(e)
        error_details = str(e.__class__.__name__)
        
        print(f"Error processing {config.config_name}: {error_message}")
        
        # Log error
        log_execution(config, "failed", 
                     read_stats=read_stats if 'read_stats' in locals() else None,
                     error_message=error_message, error_details=error_details,
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

# ## ## 📈 Execution Summary

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
        if 'performance' in result:
            perf = result['performance']
            print(f"    Performance: {perf['avg_rows_per_second']:.0f} rows/sec")
            print(f"    Read time: {perf['read_duration_ms']}ms, Write time: {perf['write_duration_ms']}ms")
        print(f"    Row count reconciliation: {result.get('reconciliation_status', 'N/A')}")

if failed:
    print("\nFailed configurations:")
    for result in failed:
        print(f"  - {result['config_name']}: {result['error']}")

# Create summary dataframe for potential downstream use
if results:
    summary_df = config_lakehouse.get_connection.createDataFrame(results)
    summary_df.show(truncate=False)

print(f"\nExecution completed at: {datetime.now()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✔️ Exit notebook with result

# CELL ********************


notebookutils.mssparkutils.notebook.exit("success")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

