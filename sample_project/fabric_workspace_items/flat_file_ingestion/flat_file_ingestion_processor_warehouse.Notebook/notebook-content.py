# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_python",
# META     "display_name": "Python (Synapse)"
# META   },
# META   "language_info": {
# META     "name": "python",
# META     "language_group": "synapse_python"
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
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📄 Flat File Ingestion Notebook (Warehouse)

# CELL ********************


# This notebook processes flat files (CSV, JSON, Parquet, Avro, XML) and loads them into warehouse tables using COPY INTO operations.




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_python"
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

    
    # Python environment - no spark session needed
    spark = None
    
else:
    print("NotebookUtils not available, assumed running in local mode.")
    from ingen_fab.python_libs.python.notebook_utils_abstraction import (
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

# Always clear the module cache - We may remove this once the libs are stable
clear_module_cache("ingen_fab.python_libs")
clear_module_cache("ingen_fab")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔧 Load Configuration and Initialize

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_python"
# META }

# MARKDOWN ********************

# ## 🔧 Load Configuration and Initialize Utilities

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import get_configs_as_object, ConfigsObject
    from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils
    
    from ingen_fab.python_libs.python.ddl_utils import ddl_utils
    
    from ingen_fab.python_libs.python.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.get_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/python/lakehouse_utils.py",
        
        "ingen_fab/python_libs/python/ddl_utils.py",
        
        "ingen_fab/python_libs/python/notebook_utils_abstraction.py"
    ]
    load_python_modules_from_path(mount_path, files_to_load)


# Initialize configuration
configs: ConfigsObject = get_configs_as_object()



# Additional imports for flat file ingestion
import uuid
import json
from datetime import datetime
from typing import Dict, List, Optional, Any

execution_id = str(uuid.uuid4())

print(f"Execution ID: {execution_id}")
print(f"Config ID: {config_id}")
print(f"Execution Group: {execution_group}")
print(f"Environment: {environment}")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📋 Load Configuration Data

# CELL ********************


class FlatFileIngestionConfig:
    """Configuration class for flat file ingestion to warehouse"""
    
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
        self.staging_table_name = config_row.get("staging_table_name", f"staging_{config_row['target_table_name']}")
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

# Initialize config warehouse utilities for configuration data
config_warehouse = warehouse_utils(
    target_workspace_id=configs.config_workspace_id,
    target_warehouse_id=configs.config_warehouse_id
)

# Initialize raw data lakehouse utilities for file access (files still come from lakehouse)
raw_lakehouse = lakehouse_utils(
    target_workspace_id=configs.raw_workspace_id,
    target_lakehouse_id=configs.raw_datastore_id
)

# Load configuration using DDL utils
config_df_result = ddl_utils.read_table("config_flat_file_ingestion")
config_df = config_df_result.to_pandas()

# Filter configurations
if config_id:
    config_df = config_df[config_df["config_id"] == config_id]
else:
    config_df = config_df[
        (config_df["execution_group"] == execution_group) & 
        (config_df["active_yn"] == "Y") &
        (config_df["target_datastore_type"] == "warehouse")
    ]

if config_df.empty:
    raise ValueError(f"No active warehouse configurations found for config_id: {config_id}, execution_group: {execution_group}")

print(f"Found {len(config_df)} warehouse configurations to process")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🚀 File Processing Functions

# CELL ********************


class FlatFileWarehouseProcessor:
    """Main processor for flat file ingestion to warehouse using COPY INTO operations"""
    
    def __init__(self, raw_lakehouse_utils, target_warehouse_utils, ddl_utils):
        self.raw_lakehouse = raw_lakehouse_utils
        self.target_warehouse = target_warehouse_utils
        self.ddl = ddl_utils
        
    def get_copy_into_sql(self, config: FlatFileIngestionConfig) -> str:
        """Generate COPY INTO SQL statement based on file format and configuration"""
        
        # Build file format options
        format_options = []
        
        if config.source_file_format.lower() == "csv":
            format_options.append(f"FIELDTERMINATOR = '{config.file_delimiter}'")
            format_options.append(f"FIRSTROW = {2 if config.has_header else 1}")
            format_options.append(f"ENCODING = '{config.encoding.upper()}'")
            
        elif config.source_file_format.lower() == "parquet":
            # Parquet format has fewer options
            pass
            
        elif config.source_file_format.lower() == "json":
            # JSON format options
            pass
        
        # Build the COPY INTO statement
        full_staging_table = f"{config.target_schema_name}.{config.staging_table_name}"
        
        copy_sql = f"""
        COPY INTO {full_staging_table}
        FROM '{config.source_file_path}'
        WITH (
            FILE_TYPE = '{config.source_file_format.upper()}'
        """
        
        if format_options:
            copy_sql += ",\n            " + ",\n            ".join(format_options)
        
        copy_sql += "\n        )"
        
        return copy_sql
    
    def create_staging_table(self, config: FlatFileIngestionConfig) -> bool:
        """Create staging table with appropriate schema"""
        
        try:
            # First, try to read a sample to infer schema
            sample_df = self.raw_lakehouse.read_file(
                file_path=config.source_file_path,
                file_format=config.source_file_format,
                options={"header": config.has_header} if config.source_file_format == "csv" else {}
            )
            
            # Convert PySpark schema to SQL DDL for warehouse
            schema_fields = []
            for field in sample_df.schema.fields:
                sql_type = self._spark_to_sql_type(field.dataType)
                nullable = "NULL" if field.nullable else "NOT NULL"
                schema_fields.append(f"{field.name} {sql_type} {nullable}")
            
            # Add metadata columns
            schema_fields.extend([
                "_ingestion_timestamp DATETIME2 NOT NULL",
                "_ingestion_execution_id NVARCHAR(50) NOT NULL", 
                "_source_file_path NVARCHAR(500) NOT NULL"
            ])
            
            schema_sql = ",\n    ".join(schema_fields)
            
            # Create staging table
            full_staging_table = f"{config.target_schema_name}.{config.staging_table_name}"
            
            create_table_sql = f"""
            DROP TABLE IF EXISTS {full_staging_table};
            
            CREATE TABLE {full_staging_table} (
                {schema_sql}
            );
            """
            
            self.ddl.execute_sql(create_table_sql)
            print(f"Created staging table: {full_staging_table}")
            return True
            
        except Exception as e:
            print(f"Error creating staging table: {e}")
            return False
    
    def _spark_to_sql_type(self, spark_type):
        """Convert Spark data types to SQL Server data types"""
        type_mapping = {
            "StringType": "NVARCHAR(500)",
            "IntegerType": "INT",
            "LongType": "BIGINT", 
            "DoubleType": "FLOAT",
            "BooleanType": "BIT",
            "DateType": "DATE",
            "TimestampType": "DATETIME2",
            "DecimalType": "DECIMAL(18,2)"
        }
        
        spark_type_name = type(spark_type).__name__
        return type_mapping.get(spark_type_name, "NVARCHAR(500)")
    
    def execute_copy_into(self, config: FlatFileIngestionConfig) -> Dict[str, Any]:
        """Execute COPY INTO operation"""
        
        try:
            # Create staging table
            if not self.create_staging_table(config):
                raise Exception("Failed to create staging table")
            
            # Execute COPY INTO
            copy_sql = self.get_copy_into_sql(config)
            print(f"Executing COPY INTO:\n{copy_sql}")
            
            result = self.ddl.execute_sql(copy_sql)
            
            # Get row count from staging table
            full_staging_table = f"{config.target_schema_name}.{config.staging_table_name}"
            count_result = self.ddl.execute_sql(f"SELECT COUNT(*) as row_count FROM {full_staging_table}")
            records_loaded = count_result.iloc[0]['row_count'] if not count_result.empty else 0
            
            return {
                "records_loaded": records_loaded,
                "copy_status": "success"
            }
            
        except Exception as e:
            print(f"COPY INTO failed: {e}")
            return {
                "records_loaded": 0,
                "copy_status": "failed",
                "error": str(e)
            }
    
    def merge_to_target(self, config: FlatFileIngestionConfig) -> Dict[str, Any]:
        """Merge data from staging to target table"""
        
        full_staging_table = f"{config.target_schema_name}.{config.staging_table_name}"
        full_target_table = f"{config.target_schema_name}.{config.target_table_name}"
        
        try:
            if config.write_mode == "overwrite":
                # Simple truncate and insert
                merge_sql = f"""
                TRUNCATE TABLE {full_target_table};
                
                INSERT INTO {full_target_table}
                SELECT *,
                       GETDATE() as _ingestion_timestamp,
                       '{execution_id}' as _ingestion_execution_id,
                       '{config.source_file_path}' as _source_file_path
                FROM {full_staging_table};
                """
                
            elif config.write_mode == "append":
                # Simple insert
                merge_sql = f"""
                INSERT INTO {full_target_table}
                SELECT *,
                       GETDATE() as _ingestion_timestamp,
                       '{execution_id}' as _ingestion_execution_id,
                       '{config.source_file_path}' as _source_file_path
                FROM {full_staging_table};
                """
                
            elif config.write_mode == "merge" and config.merge_keys:
                # MERGE operation
                merge_conditions = " AND ".join([f"target.{key} = source.{key}" for key in config.merge_keys])
                
                merge_sql = f"""
                MERGE {full_target_table} AS target
                USING (
                    SELECT *,
                           GETDATE() as _ingestion_timestamp,
                           '{execution_id}' as _ingestion_execution_id,
                           '{config.source_file_path}' as _source_file_path
                    FROM {full_staging_table}
                ) AS source
                ON {merge_conditions}
                WHEN MATCHED THEN
                    UPDATE SET /* Update all columns */
                WHEN NOT MATCHED THEN
                    INSERT VALUES (/* Insert all columns */);
                """
            else:
                raise ValueError(f"Unsupported write mode: {config.write_mode}")
            
            print(f"Executing merge operation:\n{merge_sql}")
            result = self.ddl.execute_sql(merge_sql)
            
            # Get final row count
            count_result = self.ddl.execute_sql(f"SELECT COUNT(*) as row_count FROM {full_target_table}")
            final_count = count_result.iloc[0]['row_count'] if not count_result.empty else 0
            
            return {
                "records_processed": final_count,
                "records_inserted": final_count if config.write_mode in ["overwrite", "append"] else 0,
                "records_updated": 0,
                "records_deleted": 0,
                "merge_status": "success"
            }
            
        except Exception as e:
            print(f"Merge operation failed: {e}")
            return {
                "records_processed": 0,
                "records_inserted": 0,
                "records_updated": 0,
                "records_deleted": 0,
                "merge_status": "failed",
                "error": str(e)
            }
    
    def cleanup_staging(self, config: FlatFileIngestionConfig):
        """Clean up staging table"""
        try:
            full_staging_table = f"{config.target_schema_name}.{config.staging_table_name}"
            self.ddl.execute_sql(f"DROP TABLE IF EXISTS {full_staging_table}")
            print(f"Cleaned up staging table: {full_staging_table}")
        except Exception as e:
            print(f"Warning: Could not clean up staging table: {e}")

# Initialize processor
processor = FlatFileWarehouseProcessor(raw_lakehouse, config_warehouse, ddl_utils)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Process Files

# CELL ********************


def log_execution(config: FlatFileIngestionConfig, status: str, write_stats: Dict[str, Any] = None, 
                 error_message: str = None, error_details: str = None, 
                 start_time: datetime = None, end_time: datetime = None):
    """Log execution details using warehouse utilities"""
    
    duration = None
    if start_time and end_time:
        duration = int((end_time - start_time).total_seconds())
    
    # Get file information
    try:
        file_info = raw_lakehouse.get_file_info(config.source_file_path)
    except:
        file_info = {"size": None, "modified_time": None}
    
    log_data = {
        "log_id": str(uuid.uuid4()),
        "config_id": config.config_id,
        "execution_id": execution_id,
        "job_start_time": start_time.strftime('%Y-%m-%d %H:%M:%S') if start_time else None,
        "job_end_time": end_time.strftime('%Y-%m-%d %H:%M:%S') if end_time else None,
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
        "error_message": error_message,
        "error_details": error_details,
        "execution_duration_seconds": duration,
        "spark_application_id": "N/A",  # Not applicable for warehouse/Python runtime
        "created_date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "created_by": "system"
    }
    
    # Insert log record using warehouse utilities
    try:
        # Convert to DataFrame and insert
        import pandas as pd
        log_df = pd.DataFrame([log_data])
        
        # Use warehouse_utils to insert log data
        ddl_utils.insert_dataframe(
            df=log_df,
            table_name="log_flat_file_ingestion",
            if_exists="append"
        )
    except Exception as e:
        print(f"Warning: Could not log execution: {e}")

# Process each configuration
results = []
for _, config_row in config_df.iterrows():
    config = FlatFileIngestionConfig(config_row.to_dict())
    start_time = datetime.now()
    
    try:
        print(f"\n=== Processing {config.config_name} ===")
        print(f"Source: {config.source_file_path}")
        print(f"Target: {config.target_schema_name}.{config.target_table_name}")
        print(f"Format: {config.source_file_format}")
        print(f"Write Mode: {config.write_mode}")
        print(f"Target Datastore: {config.target_datastore_type}")
        
        # Log start
        log_execution(config, "running", start_time=start_time)
        
        # Execute COPY INTO to staging
        copy_stats = processor.execute_copy_into(config)
        
        if copy_stats["copy_status"] == "success":
            print(f"COPY INTO completed: {copy_stats['records_loaded']} records loaded to staging")
            
            # Merge from staging to target
            merge_stats = processor.merge_to_target(config)
            
            if merge_stats["merge_status"] == "success":
                # Clean up staging table
                processor.cleanup_staging(config)
                
                end_time = datetime.now()
                duration = (end_time - start_time).total_seconds()
                
                print(f"Processing completed in {duration:.2f} seconds")
                print(f"Records processed: {merge_stats['records_processed']}")
                print(f"Records inserted: {merge_stats['records_inserted']}")
                
                # Log success
                log_execution(config, "completed", merge_stats, start_time=start_time, end_time=end_time)
                
                results.append({
                    "config_id": config.config_id,
                    "config_name": config.config_name,
                    "status": "success",
                    "duration": duration,
                    "records_processed": merge_stats['records_processed']
                })
            else:
                raise Exception(f"Merge failed: {merge_stats.get('error', 'Unknown error')}")
        else:
            raise Exception(f"COPY INTO failed: {copy_stats.get('error', 'Unknown error')}")
        
    except Exception as e:
        end_time = datetime.now()
        error_message = str(e)
        error_details = str(e.__class__.__name__)
        
        print(f"Error processing {config.config_name}: {error_message}")
        
        # Clean up staging table on error
        processor.cleanup_staging(config)
        
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
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

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
if results:
    import pandas as pd
    summary_df = pd.DataFrame(results)
    print("\nDetailed Results:")
    print(summary_df.to_string(index=False))

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
# META   "language_group": "synapse_python"
# META }

