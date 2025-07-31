# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark",
# META     "display_name": "Synapse PySpark"
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
execution_group = ""  # Optional: filter by execution group, empty = process all
environment = "development"
run_type = "FULL"  # FULL or INCREMENTAL



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📤 Extract Generation Notebook (Lakehouse)

# CELL ********************


# This notebook generates extract files from lakehouse tables and views
# based on configuration metadata, supporting multiple formats and compression options.




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
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔧 Load Configuration and Initialize

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
    
    from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
    
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.get_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        
        "ingen_fab/python_libs/pyspark/ddl_utils.py",
        
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py"
    ]
    load_python_modules_from_path(mount_path, files_to_load)


# Initialize configuration
configs: ConfigsObject = get_configs_as_object()



# Additional imports for extract generation
import uuid
import json
import io
import gzip
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Import lakehouse_utils and sql_templates for lakehouse operations
if run_mode == "local":
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.python.sql_templates import SQLTemplates
else:
    files_to_load = ["ingen_fab/python_libs/pyspark/lakehouse_utils.py", "ingen_fab/python_libs/python/sql_templates.py"]
    load_python_modules_from_path(mount_path, files_to_load)

import time

run_id = str(uuid.uuid4())
start_time = datetime.utcnow()

print(f"Run ID: {run_id}")
print(f"Execution Group Filter: {execution_group or 'ALL'}")
print(f"Environment: {environment}")
print(f"Run Type: {run_type}")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📋 Load Extract Configuration

# CELL ********************


class ExtractConfiguration:
    """Configuration class for extract generation"""
    
    def __init__(self, config_row, details_row):
        # Main configuration
        self.extract_name = config_row["extract_name"]
        self.is_active = config_row["is_active"]
        self.trigger_name = config_row.get("trigger_name")
        self.extract_pipeline_name = config_row.get("extract_pipeline_name")
        
        # Source configuration
        self.extract_table_name = config_row.get("extract_table_name")
        self.extract_table_schema = config_row.get("extract_table_schema")
        self.extract_view_name = config_row.get("extract_view_name")
        self.extract_view_schema = config_row.get("extract_view_schema")
        
        # Note: Stored procedures not supported in lakehouse
        self.extract_sp_name = None
        self.extract_sp_schema = None
        
        # Validation configuration (simplified for lakehouse)
        self.validation_table_sp_name = None
        self.validation_table_sp_schema = None
        
        # Load configuration
        self.is_full_load = config_row["is_full_load"]
        self.execution_group = config_row.get("execution_group")
        
        # File details configuration
        self.file_generation_group = details_row.get("file_generation_group")
        self.extract_container = details_row.get("extract_container", "extracts")
        self.extract_directory = details_row.get("extract_directory", "")
        
        # File naming
        self.extract_file_name = details_row.get("extract_file_name", self.extract_name)
        self.extract_file_name_timestamp_format = details_row.get("extract_file_name_timestamp_format")
        # Convert to int if not None and not NaN to handle float values from DataFrame
        import math
        period_end_day = details_row.get("extract_file_name_period_end_day")
        self.extract_file_name_period_end_day = int(period_end_day) if period_end_day is not None and not (isinstance(period_end_day, float) and math.isnan(period_end_day)) else None
        self.extract_file_name_extension = details_row.get("extract_file_name_extension", "csv")
        # Convert to int if not None and not NaN to handle float values from DataFrame  
        ordering = details_row.get("extract_file_name_ordering", 1)
        self.extract_file_name_ordering = int(ordering) if ordering is not None and not (isinstance(ordering, float) and math.isnan(ordering)) else 1
        
        # File properties
        self.file_properties_column_delimiter = details_row.get("file_properties_column_delimiter", ",")
        self.file_properties_row_delimiter = details_row.get("file_properties_row_delimiter", "\\n")
        self.file_properties_encoding = details_row.get("file_properties_encoding", "UTF-8")
        self.file_properties_quote_character = details_row.get("file_properties_quote_character", '"')
        self.file_properties_escape_character = details_row.get("file_properties_escape_character", "\\")
        self.file_properties_header = details_row.get("file_properties_header", True)
        self.file_properties_null_value = details_row.get("file_properties_null_value", "")
        # Convert to int if not None and not NaN to handle float values from DataFrame
        max_rows = details_row.get("file_properties_max_rows_per_file")
        self.file_properties_max_rows_per_file = int(max_rows) if max_rows is not None and not (isinstance(max_rows, float) and math.isnan(max_rows)) else None
        
        # Output format
        self.output_format = details_row.get("output_format", "csv")
        
        # Trigger file
        self.is_trigger_file = details_row.get("is_trigger_file", False)
        self.trigger_file_extension = details_row.get("trigger_file_extension", ".done")
        
        # Compression
        self.is_compressed = details_row.get("is_compressed", False)
        self.compressed_type = details_row.get("compressed_type")
        self.compressed_level = details_row.get("compressed_level", "NORMAL")
        self.compressed_file_name = details_row.get("compressed_file_name")
        self.compressed_extension = details_row.get("compressed_extension", ".zip")
        
        # Fabric paths
        self.fabric_lakehouse_path = details_row.get("fabric_lakehouse_path")
        
        # Performance options
        # Default to True for backward compatibility - most extracts expect single file output
        self.force_single_file = details_row.get("force_single_file", True)
        
    def get_source_query(self) -> Tuple[str, str]:
        """Get the Spark SQL query to extract data and the source type"""
        if self.extract_table_name:
            # For lakehouse, we don't use schema prefixes typically
            table_ref = self.extract_table_name
            if self.extract_table_schema and self.extract_table_schema != "default":
                table_ref = f"{self.extract_table_schema}.{self.extract_table_name}"
            return f"SELECT * FROM {table_ref}", "TABLE"
        elif self.extract_view_name:
            view_ref = self.extract_view_name
            if self.extract_view_schema and self.extract_view_schema != "default":
                view_ref = f"{self.extract_view_schema}.{self.extract_view_name}"
            return f"SELECT * FROM {view_ref}", "VIEW"
        else:
            raise ValueError("No source object defined for extract")
    
    def _convert_datetime_format(self, format_str: str) -> str:
        """Convert Java/C# datetime format to Python strftime format"""
        if not format_str:
            return format_str
            
        # Common datetime format conversions
        conversions = {
            'yyyy': '%Y',
            'yy': '%y',
            'MM': '%m',
            'dd': '%d',
            'HH': '%H',
            'mm': '%M',
            'ss': '%S',
            'SSS': '%f',  # milliseconds to microseconds (will need truncation)
        }
        
        result = format_str
        # Replace in order of longest to shortest to avoid partial replacements
        for old, new in sorted(conversions.items(), key=lambda x: -len(x[0])):
            result = result.replace(old, new)
            
        return result
    
    def generate_file_name(self, sequence: int = 1) -> str:
        """Generate file name based on configuration"""
        parts = []
        
        # Base name
        parts.append(self.extract_file_name)
        
        # Timestamp
        if self.extract_file_name_timestamp_format:
            # Convert Java/C# format to Python format
            python_format = self._convert_datetime_format(self.extract_file_name_timestamp_format)
            
            if self.extract_file_name_period_end_day:
                # Calculate period end date
                today = datetime.utcnow()
                if today.day <= self.extract_file_name_period_end_day:
                    # Previous month
                    period_end = today.replace(day=1) - timedelta(days=1)
                    period_end = period_end.replace(day=self.extract_file_name_period_end_day)
                else:
                    # Current month
                    period_end = today.replace(day=self.extract_file_name_period_end_day)
                timestamp_str = period_end.strftime(python_format)
            else:
                timestamp_str = datetime.utcnow().strftime(python_format)
            parts.append(timestamp_str)
        
        # Sequence number for file splitting
        if sequence > 1:
            parts.append(f"part{sequence:04d}")
        
        # Join parts and add extension
        file_name = "_".join(parts)
        if not file_name.endswith(f".{self.extract_file_name_extension}"):
            file_name = f"{file_name}.{self.extract_file_name_extension}"
        
        return file_name

# Initialize lakehouse utilities
lakehouse = lakehouse_utils(
    target_workspace_id=configs.config_workspace_id,
    target_lakehouse_id=configs.config_lakehouse_id
)

# Load extract configurations from lakehouse configuration tables
print("📋 Loading extract configurations from lakehouse tables...")

# Check if configuration tables exist
if not lakehouse.check_if_table_exists("config_extract_generation"):
    raise ValueError("Configuration table 'config_extract_generation' not found. Please run DDL scripts first.")

if not lakehouse.check_if_table_exists("config_extract_generation_details"):
    raise ValueError("Configuration table 'config_extract_generation_details' not found. Please run DDL scripts first.")

# Build query to load active extract configurations
config_query = """
SELECT 
    c.extract_name,
    c.is_active,
    c.trigger_name,
    c.extract_pipeline_name,
    c.extract_table_name,
    c.extract_table_schema,
    c.extract_view_name,
    c.extract_view_schema,
    c.is_full_load,
    c.execution_group,
    d.file_generation_group,
    d.extract_container,
    d.extract_directory,
    d.extract_file_name,
    d.extract_file_name_timestamp_format,
    d.extract_file_name_period_end_day,
    d.extract_file_name_extension,
    d.extract_file_name_ordering,
    d.file_properties_column_delimiter,
    d.file_properties_row_delimiter,
    d.file_properties_encoding,
    d.file_properties_quote_character,
    d.file_properties_escape_character,
    d.file_properties_header,
    d.file_properties_null_value,
    d.file_properties_max_rows_per_file,
    d.output_format,
    d.is_trigger_file,
    d.trigger_file_extension,
    d.is_compressed,
    d.compressed_type,
    d.compressed_level,
    d.compressed_file_name,
    d.compressed_extension,
    d.fabric_lakehouse_path
FROM config_extract_generation c
INNER JOIN config_extract_generation_details d ON c.extract_name = d.extract_name
WHERE c.is_active = true
"""

# Add execution group filter if specified
if execution_group:
    config_query += f" AND c.execution_group = '{execution_group}'"

config_query += " ORDER BY c.extract_name"

print(f"Executing query: {config_query[:100]}...")
configs_df = lakehouse.execute_query(query=config_query)

if configs_df.count() == 0:
    filter_msg = f" for execution group '{execution_group}'" if execution_group else ""
    raise ValueError(f"No active extract configurations found{filter_msg}")

# Convert to Pandas for easier processing
configs_pandas_df = configs_df.toPandas()

# Create extract configuration objects
extract_configs = []
for _, row in configs_pandas_df.iterrows():
    row_dict = row.to_dict()
    
    # Split into config and details based on expected columns
    config_columns = ['extract_name', 'is_active', 'trigger_name', 'extract_pipeline_name',
                     'extract_table_name', 'extract_table_schema', 'extract_view_name', 'extract_view_schema',
                     'is_full_load', 'execution_group']
    
    config_data = {col: row_dict.get(col) for col in config_columns if col in row_dict}
    details_data = {col: val for col, val in row_dict.items() if col not in config_columns}
    
    config = ExtractConfiguration(config_data, details_data)
    extract_configs.append(config)

print(f"Found {len(extract_configs)} active extracts to process")
for config in extract_configs:
    print(f"  - {config.extract_name} ({config.get_source_query()[1]}, {config.output_format})")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 💾 Helper Functions

# CELL ********************


def write_dataframe_to_lakehouse_file(df: pd.DataFrame, file_path: str, output_format: str, config: ExtractConfiguration) -> dict:
    """Write DataFrame to lakehouse Files using lakehouse_utils abstraction"""
    
    # Convert Pandas DataFrame back to Spark DataFrame for lakehouse operations
    spark_df = lakehouse.spark.createDataFrame(df)
    
    # Check if single file output is required (default to True for backward compatibility)
    force_single_file = getattr(config, 'force_single_file', True)
    
    if force_single_file:
        # Coalesce to 1 partition to ensure single file output
        # This creates a directory with only one part file inside
        spark_df = spark_df.coalesce(1)
        print(f"📄 Using single file output (coalesce to 1 partition)")
    else:
        # Allow Spark to use its default partitioning for better performance on large datasets
        print(f"📁 Using distributed file output (multiple part files)")
    
    # Prepare write options based on configuration
    options = {
        "mode": "overwrite"
    }
    
    # Add compression options if configured
    if config.is_compressed:
        if config.compressed_type == "GZIP":
            if output_format.lower() in ["csv", "tsv", "json", "text"]:
                options["compression"] = "gzip"
            elif output_format.lower() == "parquet":
                options["compression"] = "gzip"
        elif config.compressed_type == "SNAPPY":
            if output_format.lower() == "parquet":
                options["compression"] = "snappy"
        elif config.compressed_type == "LZ4":
            if output_format.lower() == "parquet":
                options["compression"] = "lz4"
        elif config.compressed_type == "BROTLI":
            if output_format.lower() == "parquet":
                options["compression"] = "brotli"
    
    if output_format.lower() == "csv":
        options.update({
            "header": str(config.file_properties_header).lower(),
            "delimiter": config.file_properties_column_delimiter,
            "encoding": config.file_properties_encoding,
            "quote": config.file_properties_quote_character,
            "escape": config.file_properties_escape_character,
            "nullValue": config.file_properties_null_value
        })
    elif output_format.lower() == "parquet":
        # Parquet has built-in compression support
        if not config.is_compressed:
            options["compression"] = "snappy"  # Default for Parquet
    elif output_format.lower() == "tsv":
        options.update({
            "header": str(config.file_properties_header).lower(),
            "delimiter": "\t",
            "encoding": config.file_properties_encoding,
            "nullValue": config.file_properties_null_value
        })
    
    # Use lakehouse_utils to write the file
    try:
        # Note: Spark will create a directory with the file_path name containing part files
        # With coalesce(1), there will be only one part file (e.g., part-00000-xxx.csv)
        # This is standard Spark behavior and expected for lakehouse environments
        lakehouse.write_file(
            df=spark_df,
            file_path=file_path,
            file_format=output_format,
            options=options
        )
        
        # Return metadata about the written file
        return {
            "file_path": file_path,
            "rows": len(df),
            "format": output_format,
            "success": True,
            "compressed": config.is_compressed,
            "compression_type": config.compressed_type if config.is_compressed else None,
            "note": "Output is a directory containing a single part file due to Spark's distributed nature"
        }
    except Exception as e:
        print(f"❌ Error writing file {file_path}: {str(e)}")
        return {
            "file_path": file_path,
            "rows": len(df),
            "format": output_format,
            "success": False,
            "error": str(e)
        }

def create_trigger_file(file_path: str) -> bool:
    """Create a trigger file using lakehouse_utils"""
    try:
        # Create an empty DataFrame for the trigger file
        empty_df = lakehouse.spark.createDataFrame([], "dummy STRING")
        
        # Write empty file as trigger
        lakehouse.write_file(
            df=empty_df,
            file_path=file_path,
            file_format="text",
            options={"mode": "overwrite"}
        )
        return True
    except Exception as e:
        print(f"❌ Error creating trigger file {file_path}: {str(e)}")
        return False



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔍 Process All Active Extracts

# CELL ********************


def log_extract_run(extract_config, status: str, extract_start_time, error_message: str = None, **kwargs):
    """Log extract run (simplified for lakehouse - just print)"""
    log_data = {
        "extract_name": extract_config.extract_name,
        "execution_group": extract_config.execution_group,
        "run_id": run_id,
        "run_timestamp": datetime.utcnow(),
        "run_status": status,
        "run_type": run_type,
        "start_time": extract_start_time,
        "end_time": datetime.utcnow(),
        "duration_seconds": int((datetime.utcnow() - extract_start_time).total_seconds()),
        "error_message": error_message,
        "workspace_id": configs.config_workspace_id,
        "lakehouse_id": configs.config_lakehouse_id,
        "created_by": "extract_generation_lakehouse_notebook"
    }
    
    # Add optional fields
    log_data.update(kwargs)
    
    # For lakehouse, just print the log data
    print(f"📝 LOG: {extract_config.extract_name} - {status}")
    if error_message:
        print(f"    Error: {error_message}")
    if kwargs:
        print(f"    Details: {kwargs}")

# Process each extract configuration
total_extracts = len(extract_configs)
successful_extracts = 0
failed_extracts = 0

print(f"\n🚀 Starting batch extract processing for {total_extracts} extracts...")

for extract_index, config in enumerate(extract_configs, 1):
    extract_start_time = datetime.utcnow()
    
    print(f"\n{'='*60}")
    print(f"Processing Extract {extract_index}/{total_extracts}: {config.extract_name}")
    print(f"{'='*60}")
    print(f"Source: {config.get_source_query()[1]} - {config.get_source_query()[0][:100]}...")
    print(f"Output: {config.output_format} | Compression: {'Yes' if config.is_compressed else 'No'}")
    
    try:
        # Get source query
        source_query, source_type = config.get_source_query()
        
        # Execute query using lakehouse_utils abstraction
        print(f"Executing query using lakehouse_utils: {source_query[:100]}...")
        spark_df = lakehouse.execute_query(query=source_query)
        
        # Get row count from PySpark DataFrame
        total_rows = spark_df.count()
        print(f"Retrieved {total_rows:,} rows from source")
        
        # Convert to Pandas DataFrame for file generation
        data_df = spark_df.toPandas()
        
        # Log initial status
        log_extract_run(
            config, "IN_PROGRESS", extract_start_time,
            source_type=source_type,
            source_object=config.extract_table_name or config.extract_view_name,
            source_schema=config.extract_table_schema or config.extract_view_schema,
            rows_extracted=total_rows
        )
        
        # Generate extract files for this extract using lakehouse_utils
        files_generated = []
        total_rows_written = 0
        
        # Show compression status
        if config.is_compressed:
            print(f"🗜️ Compression enabled: {config.compressed_type} ({config.compressed_level})")
        else:
            print(f"📄 No compression specified")
        
        # Check if we need to split files
        max_rows = config.file_properties_max_rows_per_file
        
        if max_rows and total_rows > max_rows:
            # Split into multiple files
            num_files = (total_rows + max_rows - 1) // max_rows
            print(f"Splitting into {num_files} files ({max_rows:,} rows each)")
            
            for i in range(num_files):
                start_idx = i * max_rows
                end_idx = min((i + 1) * max_rows, total_rows)
                df_chunk = data_df.iloc[start_idx:end_idx]
                
                # Generate file name
                file_name = config.generate_file_name(sequence=i+1)
                
                # Build full file path for lakehouse Files
                file_path = f"{config.extract_container}/{config.extract_directory}/{file_name}".replace("//", "/")
                
                compression_info = f" ({config.compressed_type})" if config.is_compressed else ""
                print(f"📄 Writing file chunk {i+1}/{num_files}: {file_path}{compression_info}")
                
                # Write using lakehouse_utils abstraction
                write_result = write_dataframe_to_lakehouse_file(
                    df=df_chunk,
                    file_path=file_path,
                    output_format=config.output_format,
                    config=config
                )
                
                if write_result["success"]:
                    files_generated.append({
                        "file_name": file_name,
                        "file_path": file_path,
                        "file_size": 0,  # Size not available with lakehouse_utils
                        "rows": len(df_chunk),
                        "compressed": write_result.get("compressed", False),
                        "compression_type": write_result.get("compression_type")
                    })
                else:
                    print(f"❌ Failed to write file chunk {i+1}: {write_result.get('error', 'Unknown error')}")
                
                total_rows_written += len(df_chunk)
        else:
            # Single file
            file_name = config.generate_file_name()
            
            # Build full file path for lakehouse Files
            file_path = f"{config.extract_container}/{config.extract_directory}/{file_name}".replace("//", "/")
            
            compression_info = f" ({config.compressed_type})" if config.is_compressed else ""
            print(f"📄 Writing single file: {file_path}{compression_info}")
            
            # Write using lakehouse_utils abstraction
            write_result = write_dataframe_to_lakehouse_file(
                df=data_df,
                file_path=file_path,
                output_format=config.output_format,
                config=config
            )
            
            if write_result["success"]:
                files_generated.append({
                    "file_name": file_name,
                    "file_path": file_path,
                    "file_size": 0,  # Size not available with lakehouse_utils
                    "rows": total_rows,
                    "compressed": write_result.get("compressed", False),
                    "compression_type": write_result.get("compression_type")
                })
                total_rows_written = total_rows
            else:
                print(f"❌ Failed to write file: {write_result.get('error', 'Unknown error')}")
        
        # Generate trigger file if configured
        if config.is_trigger_file:
            trigger_file_name = f"{config.extract_file_name}{config.trigger_file_extension}"
            trigger_file_path = f"{config.extract_container}/{config.extract_directory}/{trigger_file_name}".replace("//", "/")
            
            print(f"🎯 Creating trigger file: {trigger_file_path}")
            trigger_success = create_trigger_file(trigger_file_path)
            if not trigger_success:
                print(f"⚠️  Warning: Failed to create trigger file {trigger_file_path}")
        
        # Log successful completion
        log_extract_run(
            config, "SUCCESS", extract_start_time,
            source_type=source_type,
            source_object=config.extract_table_name or config.extract_view_name,
            source_schema=config.extract_table_schema or config.extract_view_schema,
            rows_extracted=total_rows,
            rows_written=total_rows_written,
            files_generated=len(files_generated),
            output_format=config.output_format,
            output_file_path=files_generated[0]["file_path"] if files_generated else None,
            output_file_name=files_generated[0]["file_name"] if files_generated else None,
            output_file_size_bytes=sum(f["file_size"] for f in files_generated),
            is_compressed=config.is_compressed,
            compression_type=config.compressed_type if config.is_compressed else None,
            trigger_file_created=config.is_trigger_file,
            trigger_file_path=trigger_file_path if config.is_trigger_file else None
        )
        
        print(f"✅ Extract {config.extract_name} completed successfully!")
        print(f"   Files: {len(files_generated)} | Rows: {total_rows_written:,} | Size: {sum(f['file_size'] for f in files_generated):,} bytes")
        successful_extracts += 1
        
    except Exception as e:
        # Enhanced error reporting with line numbers and stack trace
        import traceback
        import sys
        
        # Get the current frame info for line number
        exc_type, exc_value, exc_traceback = sys.exc_info()
        
        # Format the full traceback
        full_traceback = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        
        # Get the line number where the error occurred
        if exc_traceback:
            line_number = exc_traceback.tb_lineno
            filename = exc_traceback.tb_frame.f_code.co_filename
        else:
            line_number = "Unknown"
            filename = "Unknown"
        
        error_msg = f"Error extracting data from {config.extract_name}: {str(e)}"
        detailed_error = f"""
❌ EXTRACT GENERATION ERROR DETAILS (LAKEHOUSE):
   Extract Name: {config.extract_name}
   Error Type: {type(e).__name__}
   Error Message: {str(e)}
   File: {filename}
   Line Number: {line_number}
   
📋 Full Traceback:
{full_traceback}
        """
        
        print(detailed_error)
        log_extract_run(config, "FAILED", extract_start_time, error_message=f"{str(e)} (Line: {line_number})")
        failed_extracts += 1
        continue

# End of extract processing loop
print(f"\n🎉 Batch extract processing completed!")
print(f"📊 Summary:")
print(f"   Total Extracts: {total_extracts}")
print(f"   ✅ Successful: {successful_extracts}")
print(f"   ❌ Failed: {failed_extracts}")
print(f"   ⏱️ Total Duration: {int((datetime.utcnow() - start_time).total_seconds())} seconds")

if failed_extracts > 0:
    print(f"\n⚠️ WARNING: {failed_extracts} extracts failed. Check logs for details.")
else:
    print(f"\n🎯 All extracts completed successfully!")




# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Final Summary

# CELL ********************


# Display final batch summary
print(f"🔄 Batch Extract Run Complete (Lakehouse)")
print(f"Run ID: {run_id}")
print(f"⏱️ Total Duration: {int((datetime.utcnow() - start_time).total_seconds())} seconds")
print(f"📈 Execution Summary:")
print(f"   • Total Extracts Processed: {total_extracts}")
print(f"   • ✅ Successfully Completed: {successful_extracts}")
print(f"   • ❌ Failed: {failed_extracts}")
print(f"   • 📊 Success Rate: {(successful_extracts/total_extracts*100):.1f}%" if total_extracts > 0 else "   • 📊 Success Rate: 0%")

if execution_group:
    print(f"   • 🎯 Execution Group Filter: {execution_group}")

print(f"\n🏁 Lakehouse extract generation batch processing completed at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")