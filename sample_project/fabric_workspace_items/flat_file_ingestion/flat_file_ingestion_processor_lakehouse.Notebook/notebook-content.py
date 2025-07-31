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
execution_group = None
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
        # Advanced CSV handling options
        self.quote_character = config_row.get("quote_character", '"')
        self.escape_character = config_row.get("escape_character", "\\")
        self.multiline_values = config_row.get("multiline_values", True)
        self.ignore_leading_whitespace = config_row.get("ignore_leading_whitespace", False)
        self.ignore_trailing_whitespace = config_row.get("ignore_trailing_whitespace", False)
        self.null_value = config_row.get("null_value", "")
        self.empty_value = config_row.get("empty_value", "")
        self.comment_character = config_row.get("comment_character", None)
        self.max_columns = config_row.get("max_columns", 20480)
        self.max_chars_per_column = config_row.get("max_chars_per_column", -1)
        self.custom_schema_json = config_row.get("custom_schema_json")
        self.partition_columns = config_row.get("partition_columns", "").split(",") if config_row.get("partition_columns") else []
        self.sort_columns = config_row.get("sort_columns", "").split(",") if config_row.get("sort_columns") else []
        self.write_mode = config_row.get("write_mode", "overwrite")
        self.merge_keys = config_row.get("merge_keys", "").split(",") if config_row.get("merge_keys") else []
        self.data_validation_rules = config_row.get("data_validation_rules")
        self.error_handling_strategy = config_row.get("error_handling_strategy", "fail")
        self.execution_group = config_row["execution_group"]
        self.active_yn = config_row["active_yn"]
        # New fields for incremental synthetic data import support
        self.import_pattern = config_row.get("import_pattern", "single_file")
        self.date_partition_format = config_row.get("date_partition_format", "YYYY/MM/DD")
        self.table_relationship_group = config_row.get("table_relationship_group")
        self.batch_import_enabled = config_row.get("batch_import_enabled", False)
        self.file_discovery_pattern = config_row.get("file_discovery_pattern")
        self.import_sequence_order = config_row.get("import_sequence_order", 1)
        self.date_range_start = config_row.get("date_range_start")
        self.date_range_end = config_row.get("date_range_end")
        self.skip_existing_dates = config_row.get("skip_existing_dates", True)

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
    # If execution_group is not set or is empty, process all execution groups
    if execution_group and str(execution_group).strip():
        config_df = config_df[
            (config_df["execution_group"] == execution_group) & 
            (config_df["active_yn"] == "Y")
        ]
    else:
        config_df = config_df[config_df["active_yn"] == "Y"]

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
    
    def discover_date_partitioned_files(self, config: FlatFileIngestionConfig) -> List[Dict[str, str]]:
        """Discover files in date-partitioned directory structure using lakehouse_utils abstraction"""
        
        if config.import_pattern != "date_partitioned":
            # For non-date-partitioned imports, return single file path
            return [{"file_path": config.source_file_path, "date_partition": None}]
        
        discovered_files = []
        base_path = config.source_file_path
        
        try:
            # Parse date partition format to understand directory structure
            # Support formats like "YYYY/MM/DD" or "YYYY-MM-DD"
            date_format = config.date_partition_format.replace("YYYY", "*").replace("MM", "*").replace("DD", "*")
            
            # Build search pattern for file discovery
            if config.file_discovery_pattern:
                # Use custom discovery pattern
                search_pattern = config.file_discovery_pattern
            else:
                # Build pattern based on base path and date format
                search_pattern = f"{base_path}/{date_format}/**/*.{config.source_file_format}"
            
            print(f"🔍 Discovering files with pattern: {search_pattern}")
            
            # Use lakehouse_utils to list files matching the pattern
            # Split the pattern to extract directory and file pattern
            import os
            if config.file_discovery_pattern:
                # For custom patterns, we need to handle them differently
                # Extract the base directory and pattern
                if '**' in search_pattern:
                    # Split at the first **
                    parts = search_pattern.split('**', 1)
                    dir_path = parts[0].rstrip('/')
                    file_pattern = '**' + parts[1]
                else:
                    dir_path = os.path.dirname(search_pattern)
                    file_pattern = os.path.basename(search_pattern)
            else:
                # For standard patterns, use the base path as directory
                dir_path = base_path
                file_pattern = f"{date_format}/**/*.{config.source_file_format}"
            
            print(f"📂 Searching in directory: {dir_path}")
            print(f"🔍 With pattern: {file_pattern}")
            
            file_list = self.raw_lakehouse.list_files(dir_path, pattern=file_pattern, recursive=True)
            
            for file_path in file_list:
                # Convert string path to dictionary format
                file_info = {"path": file_path}
                
                # Extract date partition from file path
                date_partition = self._extract_date_from_path(file_path, base_path, config.date_partition_format)
                
                # Apply date range filtering if specified
                if self._is_date_in_range(date_partition, config.date_range_start, config.date_range_end):
                    discovered_files.append({
                        "file_path": file_path,
                        "date_partition": date_partition,
                        "size": 0,  # Size info not available from list_files
                        "modified_time": None  # Modified time not available from list_files
                    })
            
            # Sort by date partition for consistent processing order
            discovered_files.sort(key=lambda x: x["date_partition"] or "")
            
            print(f"📁 Discovered {len(discovered_files)} files for processing")
            
        except Exception as e:
            print(f"⚠️ Warning: File discovery failed: {e}")
            # Fallback to single file processing
            discovered_files = [{"file_path": config.source_file_path, "date_partition": None}]
        
        return discovered_files
    
    def _extract_date_from_path(self, file_path: str, base_path: str, date_format: str) -> str:
        """Extract date partition from file path based on format"""
        try:
            # Remove base path to get relative path
            relative_path = file_path.replace(base_path, "").strip("/")
            
            # Parse date based on format
            if date_format == "YYYY/MM/DD":
                # Extract YYYY/MM/DD pattern
                path_parts = relative_path.split("/")
                if len(path_parts) >= 3:
                    return f"{path_parts[0]}-{path_parts[1]}-{path_parts[2]}"
            elif date_format == "YYYY-MM-DD":
                # Extract YYYY-MM-DD pattern
                path_parts = relative_path.split("/")
                for part in path_parts:
                    if len(part) == 10 and part.count("-") == 2:
                        return part
            
            # Fallback: try to extract any date-like pattern
            import re
            date_match = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", relative_path)
            if date_match:
                return f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            
        except Exception as e:
            print(f"⚠️ Could not extract date from path {file_path}: {e}")
        
        return None
    
    def _is_date_in_range(self, date_str: str, start_date: str, end_date: str) -> bool:
        """Check if date is within specified range"""
        if not date_str:
            return True  # Include files without date partitions
        
        try:
            from datetime import datetime
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            if start_date:
                start = datetime.strptime(start_date, "%Y-%m-%d").date()
                if file_date < start:
                    return False
            
            if end_date:
                end = datetime.strptime(end_date, "%Y-%m-%d").date()
                if file_date > end:
                    return False
            
            return True
        except Exception as e:
            print(f"⚠️ Date range check failed for {date_str}: {e}")
            return True
        
    def read_file(self, config: FlatFileIngestionConfig) -> tuple[DataFrame, Dict[str, Any]]:
        """Read file based on format and configuration using abstracted file access"""
        
        # Track read performance
        read_start = time.time()
        
        # Check if this is a date-partitioned import that needs special handling
        if getattr(config, 'import_pattern', None) == "date_partitioned" and getattr(config, 'batch_import_enabled', None):
            print(f"🔍 Detected date-partitioned import for {config.config_name}")
            return self._read_date_partitioned_files(config, read_start)
        
        # Build options dictionary for the abstracted read_file method
        options = {}
        
        if config.source_file_format.lower() == "csv":
            # Basic CSV options
            options["header"] = config.has_header
            options["delimiter"] = config.file_delimiter
            options["encoding"] = config.encoding
            options["inferSchema"] = config.schema_inference
            options["dateFormat"] = config.date_format
            options["timestampFormat"] = config.timestamp_format
            
            # Advanced CSV options for handling complex scenarios
            options["quote"] = config.quote_character
            options["escape"] = config.escape_character
            options["multiLine"] = config.multiline_values  # Allows newlines within quoted fields
            options["ignoreLeadingWhiteSpace"] = config.ignore_leading_whitespace
            options["ignoreTrailingWhiteSpace"] = config.ignore_trailing_whitespace
            options["nullValue"] = config.null_value
            options["emptyValue"] = config.empty_value
            
            # Handle comment character (only if specified)
            if config.comment_character:
                options["comment"] = config.comment_character
            
            # Set column and character limits for safety
            options["maxColumns"] = config.max_columns
            if config.max_chars_per_column > 0:
                options["maxCharsPerColumn"] = config.max_chars_per_column
            
            # Additional safety options for complex CSV files
            options["unescapedQuoteHandling"] = "STOP_AT_DELIMITER"  # Handle unescaped quotes safely
            options["enforceSchema"] = False  # Allow flexible schema when inferring
            options["columnNameOfCorruptRecord"] = "_corrupt_record"  # Track corrupt records
            
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
        
        # For CSV files with complex parsing, check for corrupt records
        corrupt_records_count = 0
        if config.source_file_format.lower() == "csv" and "_corrupt_record" in df.columns:
            corrupt_records_count = df.filter(col("_corrupt_record").isNotNull()).count()
            if corrupt_records_count > 0:
                print(f"⚠️ Warning: Found {corrupt_records_count} corrupt records in CSV file")
                if config.error_handling_strategy == "fail":
                    # Show a sample of corrupt records for debugging
                    corrupt_sample = df.filter(col("_corrupt_record").isNotNull()).select("_corrupt_record").limit(5)
                    print("Sample corrupt records:")
                    corrupt_sample.show(truncate=False)
                    raise ValueError(f"CSV parsing failed with {corrupt_records_count} corrupt records")
                elif config.error_handling_strategy == "log":
                    print("Continuing with corrupt records logged...")
                else:  # skip corrupt records
                    df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
                    print(f"Skipped {corrupt_records_count} corrupt records")
        
        # Calculate read duration
        read_duration_ms = int((time.time() - read_start) * 1000)
        
        # Get source row count (after filtering corrupt records if applicable)
        source_row_count = df.count()
        
        read_stats = {
            "data_read_duration_ms": read_duration_ms,
            "source_row_count": source_row_count,
            "corrupt_records_count": corrupt_records_count
        }
        
        return df, read_stats
    
    def _read_date_partitioned_files(self, config: FlatFileIngestionConfig, read_start: float) -> tuple[DataFrame, Dict[str, Any]]:
        """Read date-partitioned files using the discovery method"""
        from pyspark.sql.functions import lit
        
        # Discover files to process
        discovered_files = self.discover_date_partitioned_files(config)
        
        if not discovered_files:
            print(f"⚠️ No files discovered for date-partitioned import {config.config_name}")
            # Return empty DataFrame with proper schema
            from pyspark.sql.types import StructType
            empty_df = self.raw_lakehouse.get_connection.createDataFrame([], StructType([]))
            return empty_df, {
                "data_read_duration_ms": int((time.time() - read_start) * 1000),
                "source_row_count": 0,
                "corrupt_records_count": 0,
                "files_processed": 0
            }
        
        print(f"📁 Processing {len(discovered_files)} date-partitioned files")
        
        # Build options for file reading
        options = {}
        if config.source_file_format.lower() == "csv":
            options["header"] = config.has_header
            options["delimiter"] = config.file_delimiter
            options["encoding"] = config.encoding
            options["inferSchema"] = config.schema_inference
            options["dateFormat"] = config.date_format
            options["timestampFormat"] = config.timestamp_format
            options["quote"] = config.quote_character
            options["escape"] = config.escape_character
            options["multiLine"] = config.multiline_values
            options["ignoreLeadingWhiteSpace"] = config.ignore_leading_whitespace
            options["ignoreTrailingWhiteSpace"] = config.ignore_trailing_whitespace
            options["nullValue"] = config.null_value
            options["emptyValue"] = config.empty_value
            if config.comment_character:
                options["comment"] = config.comment_character
            options["maxColumns"] = config.max_columns
            if config.max_chars_per_column > 0:
                options["maxCharsPerColumn"] = config.max_chars_per_column
        elif config.source_file_format.lower() == "json":
            options["dateFormat"] = config.date_format
            options["timestampFormat"] = config.timestamp_format
        
        # Add custom schema if provided
        if config.custom_schema_json:
            import json
            from pyspark.sql.types import StructType
            schema = StructType.fromJson(json.loads(config.custom_schema_json))
            options["schema"] = schema
        
        # Read and union all discovered files
        dataframes = []
        total_files_processed = 0
        
        for file_info in discovered_files:
            file_path = file_info["file_path"]
            date_partition = file_info["date_partition"]
            
            try:
                # Validate that the file format matches the configuration
                actual_format = self._detect_file_format_from_path(file_path)
                if actual_format and actual_format != config.source_file_format:
                    error_msg = f"Format mismatch: Configuration expects {config.source_file_format} but found {actual_format} file: {file_path}"
                    print(f"❌ {error_msg}")
                    raise ValueError(error_msg)
                
                print(f"  📄 Reading {file_path} (date: {date_partition or 'N/A'})")
                
                # Read the individual file using configured format
                df_file = self.raw_lakehouse.read_file(
                    file_path=file_path,
                    file_format=config.source_file_format,
                    options=options
                )
                
                # Add date partition as a column if available
                if date_partition:
                    df_file = df_file.withColumn("_partition_date", lit(date_partition))
                
                dataframes.append(df_file)
                total_files_processed += 1
                
            except Exception as e:
                print(f"⚠️ Failed to read {file_path}: {e}")
                if config.error_handling_strategy == "fail":
                    raise
                # Continue with other files for 'log' or 'skip' strategies
        
        if not dataframes:
            print(f"⚠️ No files successfully read for {config.config_name}")
            from pyspark.sql.types import StructType
            empty_df = self.raw_lakehouse.get_connection.createDataFrame([], StructType([]))
            return empty_df, {
                "data_read_duration_ms": int((time.time() - read_start) * 1000),
                "source_row_count": 0,
                "corrupt_records_count": 0,
                "files_processed": 0
            }
        
        # Union all dataframes
        combined_df = dataframes[0]
        for df in dataframes[1:]:
            combined_df = combined_df.union(df)
        
        # Calculate final stats
        read_duration_ms = int((time.time() - read_start) * 1000)
        source_row_count = combined_df.count()
        
        print(f"✅ Successfully processed {total_files_processed} files with {source_row_count} total records")
        
        return combined_df, {
            "data_read_duration_ms": read_duration_ms,
            "source_row_count": source_row_count,
            "corrupt_records_count": 0,  # TODO: Handle corrupt records in batch processing
            "files_processed": total_files_processed
        }
    
    def _detect_file_format_from_path(self, file_path: str) -> str:
        """Detect file format from file extension"""
        file_path_lower = file_path.lower()
        
        if file_path_lower.endswith('.parquet'):
            return 'parquet'
        elif file_path_lower.endswith('.json'):
            return 'json'
        elif file_path_lower.endswith(('.csv', '.tsv')):
            return 'csv'
        elif file_path_lower.endswith('.avro'):
            return 'avro'
        elif file_path_lower.endswith('.xml'):
            return 'xml'
        else:
            # Return None for unknown formats - will use configured format
            return None
    
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
    
    def perform_merge(self, df: DataFrame, config: FlatFileIngestionConfig, target_lakehouse: Any, full_table_name: str) -> Dict[str, Any]:
        """Perform merge operation using Delta merge"""
        from delta.tables import DeltaTable
        
        # Build the table path using lakehouse_utils abstraction
        table_path = f"{target_lakehouse.lakehouse_tables_uri()}{full_table_name}"
        
        # Check if table exists by trying to read it
        table_exists = False
        try:
            target_lakehouse.read_table(full_table_name)
            table_exists = True
        except:
            pass
        
        if not table_exists:
            # First load - create table
            target_lakehouse.write_to_table(
                df=df,
                table_name=full_table_name,
                mode="overwrite",
                options={"partitionBy": config.partition_columns} if config.partition_columns else {}
            )
            return {"matched": 0, "inserted": df.count(), "updated": 0}
        
        # Build merge condition
        if not config.merge_keys:
            raise ValueError(f"Merge keys must be specified for merge write mode in config: {config.config_id}")
        
        merge_conditions = " AND ".join([
            f"target.{key.strip()} = source.{key.strip()}" 
            for key in config.merge_keys if key.strip()
        ])
        
        # Get the Delta table reference
        deltaTable = DeltaTable.forPath(self.spark, table_path)
        
        # Perform merge
        merge_builder = deltaTable.alias("target").merge(
            df.alias("source"),
            merge_conditions
        )
        
        # Update all columns when matched
        merge_builder = merge_builder.whenMatchedUpdateAll()
        
        # Insert all columns when not matched
        merge_builder = merge_builder.whenNotMatchedInsertAll()
        
        # Execute merge and get metrics
        merge_builder.execute()
        
        # Get merge metrics from Delta history
        history_df = deltaTable.history(1)
        operation_metrics = history_df.select("operationMetrics").collect()[0][0]
        
        return {
            "matched": operation_metrics.get("numTargetRowsMatched", 0),
            "inserted": operation_metrics.get("numTargetRowsInserted", 0),
            "updated": operation_metrics.get("numTargetRowsUpdated", 0)
        }
    
    def perform_partition_swap(self, df: DataFrame, config: FlatFileIngestionConfig, target_lakehouse: Any, full_table_name: str) -> Dict[str, Any]:
        """Perform partition swap operation - replace entire partitions atomically"""
        
        if not config.partition_columns:
            raise ValueError(f"Partition columns must be specified for partition_swap write mode in config: {config.config_id}")
        
        # Build the table path using lakehouse_utils abstraction
        table_path = f"{target_lakehouse.lakehouse_tables_uri()}{full_table_name}"
        
        # Check if table exists by trying to read it
        table_exists = False
        try:
            target_lakehouse.read_table(full_table_name)
            table_exists = True
        except:
            pass
        
        if not table_exists:
            # First load - create partitioned table
            target_lakehouse.write_to_table(
                df=df,
                table_name=full_table_name,
                mode="overwrite",
                options={"partitionBy": config.partition_columns}
            )
            return {"partitions_affected": 0, "rows_deleted": 0, "rows_inserted": df.count()}
        
        # Get unique partition values from source data
        partition_cols = [col.strip() for col in config.partition_columns if col.strip()]
        partition_df = df.select(*partition_cols).distinct()
        partition_values = partition_df.collect()
        
        rows_deleted = 0
        partitions_affected = len(partition_values)
        
        # Delete matching partitions from target table
        for row in partition_values:
            conditions = []
            for col in partition_cols:
                value = row[col]
                if value is None:
                    conditions.append(f"{col} IS NULL")
                elif isinstance(value, str):
                    conditions.append(f"{col} = '{value}'")
                else:
                    conditions.append(f"{col} = {value}")
            
            delete_condition = " AND ".join(conditions)
            
            # Count rows before deletion
            count_before = self.spark.sql(f"SELECT COUNT(*) FROM {full_table_name} WHERE {delete_condition}").collect()[0][0]
            rows_deleted += count_before
            
            # Delete the partition
            self.spark.sql(f"DELETE FROM {full_table_name} WHERE {delete_condition}")
        
        # Append new data for the affected partitions
        target_lakehouse.write_to_table(
            df=df,
            table_name=full_table_name,
            mode="append"
        )
        
        return {
            "partitions_affected": partitions_affected,
            "rows_deleted": rows_deleted,
            "rows_inserted": df.count()
        }
    
    def write_data(self, df: DataFrame, config: FlatFileIngestionConfig, target_lakehouse: Any, source_row_count: int) -> Dict[str, Any]:
        """Write data to target table using lakehouse_utils abstraction"""
        
        # Track write performance
        write_start = time.time()
        
        # Get target row count before write
        full_table_name = f"{config.target_schema_name}_{config.target_table_name}" if config.target_schema_name else config.target_table_name
        
        try:
            target_df_before = target_lakehouse.read_table(full_table_name)
            target_count_before = target_df_before.count()
            table_exists = True
        except:
            target_count_before = 0  # Table doesn't exist yet
            table_exists = False
        
        # Add metadata columns for traceability
        df_with_metadata = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                            .withColumn("_ingestion_execution_id", lit(execution_id)) \
                            .withColumn("_source_file_path", lit(config.source_file_path))
        
        staging_row_count = df_with_metadata.count()
        
        # Handle different write modes
        if config.write_mode == "merge":
            # Perform merge operation
            merge_stats = self.perform_merge(df_with_metadata, config, target_lakehouse, full_table_name)
            
            # Get target count after merge
            target_df_after = target_lakehouse.read_table(full_table_name)
            target_count_after = target_df_after.count()
            
            write_stats = {
                "records_processed": target_count_after,
                "records_inserted": merge_stats["inserted"],
                "records_updated": merge_stats["updated"],
                "records_deleted": 0,
                "records_matched": merge_stats["matched"],
                "staging_row_count": staging_row_count,
                "target_row_count_before": target_count_before,
                "target_row_count_after": target_count_after,
                "write_mode_details": merge_stats
            }
            
        elif config.write_mode == "partition_swap":
            # Perform partition swap operation
            swap_stats = self.perform_partition_swap(df_with_metadata, config, target_lakehouse, full_table_name)
            
            # Get target count after swap
            target_df_after = target_lakehouse.read_table(full_table_name)
            target_count_after = target_df_after.count()
            
            write_stats = {
                "records_processed": target_count_after,
                "records_inserted": swap_stats["rows_inserted"],
                "records_updated": 0,
                "records_deleted": swap_stats["rows_deleted"],
                "partitions_affected": swap_stats["partitions_affected"],
                "staging_row_count": staging_row_count,
                "target_row_count_before": target_count_before,
                "target_row_count_after": target_count_after,
                "write_mode_details": swap_stats
            }
            
        else:
            # Standard overwrite/append modes using lakehouse_utils
            write_options = {}
            if config.partition_columns:
                write_options["partitionBy"] = config.partition_columns
            
            target_lakehouse.write_to_table(
                df=df_with_metadata,
                table_name=full_table_name,
                mode=config.write_mode,
                options=write_options
            )
            
            # Get target count after write
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
            else:
                # Unknown mode - default behavior
                records_inserted = max(0, target_count_after - target_count_before)
                records_updated = 0
                records_deleted = 0
            
            write_stats = {
                "records_processed": target_count_after,
                "records_inserted": records_inserted,
                "records_updated": records_updated,
                "records_deleted": records_deleted,
                "staging_row_count": staging_row_count,
                "target_row_count_before": target_count_before,
                "target_row_count_after": target_count_after
            }
        
        # Calculate write duration
        write_duration_ms = int((time.time() - write_start) * 1000)
        write_stats["staging_write_duration_ms"] = write_duration_ms
        
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
    if status == "completed" and source_row_count == 0:
        # No source data - reconciliation is skipped
        row_count_reconciliation_status = "skipped"
        row_count_difference = 0
    elif status == "completed" and source_row_count > 0:
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
        avg_rows_per_second = float((source_row_count / total_duration_ms) * 1000) if source_row_count else 0.0
        if file_info.get("size"):
            data_size_mb = float(file_info["size"] / (1024 * 1024))
            throughput_mb_per_second = float((data_size_mb / total_duration_ms) * 1000)
    
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
        
        # Handle case where no source data was found
        if read_stats['source_row_count'] == 0:
            print(f"⚠️ No source data found for {config.config_name}. Skipping write and reconciliation operations.")
            
            # Create minimal write stats for logging
            write_stats = {
                "records_processed": 0,
                "records_inserted": 0,
                "records_updated": 0,
                "records_deleted": 0,
                "staging_row_count": 0,
                "target_row_count_before": 0,
                "target_row_count_after": 0,
                "staging_write_duration_ms": 0
            }
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"Processing completed in {duration:.2f} seconds (no data processed)")
            print("Row count reconciliation: skipped (no source data)")
            
            # Log completion with no data processed
            log_execution(config, "completed", write_stats, read_stats, start_time=start_time, end_time=end_time)
            
            results.append({
                "config_id": config.config_id,
                "config_name": config.config_name,
                "status": "no_data",
                "duration": duration,
                "records_processed": 0,
                "reconciliation_status": "skipped",
                "performance": {
                    "source_rows": 0,
                    "avg_rows_per_second": 0.0,
                    "read_duration_ms": read_stats['data_read_duration_ms'],
                    "write_duration_ms": 0
                }
            })
            
            continue  # Skip to next configuration
        
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
        if write_stats.get('records_deleted', 0) > 0:
            print(f"Records deleted: {write_stats['records_deleted']}")
        if config.write_mode == "merge" and 'records_matched' in write_stats:
            print(f"Records matched: {write_stats['records_matched']}")
        if config.write_mode == "partition_swap" and 'partitions_affected' in write_stats:
            print(f"Partitions affected: {write_stats['partitions_affected']}")
        print(f"Source rows: {read_stats['source_row_count']}")
        print(f"Target rows before: {write_stats['target_row_count_before']}")
        print(f"Target rows after: {write_stats['target_row_count_after']}")
        
        # Check reconciliation based on write mode
        recon_status = "matched"
        if config.write_mode == "overwrite" and read_stats['source_row_count'] != write_stats['target_row_count_after']:
            recon_status = "mismatched"
        elif config.write_mode == "append" and read_stats['source_row_count'] != (write_stats['target_row_count_after'] - write_stats['target_row_count_before']):
            recon_status = "mismatched"
        elif config.write_mode == "merge":
            # For merge, check if all source rows were processed (either matched or inserted)
            total_processed = write_stats.get('records_matched', 0) + write_stats.get('records_inserted', 0)
            if read_stats['source_row_count'] != total_processed:
                recon_status = "mismatched"
        elif config.write_mode == "partition_swap":
            # For partition swap, check if all source rows were inserted
            if read_stats['source_row_count'] != write_stats.get('records_inserted', 0):
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
                "avg_rows_per_second": float(read_stats['source_row_count'] / duration) if duration > 0 else 0.0,
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
no_data = [r for r in results if r["status"] == "no_data"]

print(f"Successful: {len(successful)}")
print(f"Failed: {len(failed)}")
print(f"No data found: {len(no_data)}")

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

if no_data:
    print("\nConfigurations with no data found:")
    for result in no_data:
        print(f"  - {result['config_name']}: No source files discovered")
        if 'performance' in result:
            perf = result['performance']
            print(f"    Read time: {perf['read_duration_ms']}ms")
        print(f"    Row count reconciliation: {result.get('reconciliation_status', 'N/A')}")

# Create summary dataframe for potential downstream use
#if results:
#    summary_df = config_lakehouse.get_connection.createDataFrame(results)
#    summary_df.show(truncate=False)

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

