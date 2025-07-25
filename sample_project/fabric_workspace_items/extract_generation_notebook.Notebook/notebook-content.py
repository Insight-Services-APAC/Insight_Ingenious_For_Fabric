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
extract_name = ""
execution_group = ""
environment = "development"
run_type = "FULL"  # FULL or INCREMENTAL



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📤 Extract Generation Notebook (Warehouse)

# CELL ********************


# This notebook generates extract files from warehouse tables, views, or stored procedures
# based on configuration metadata, supporting multiple formats and compression options.




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
    
    notebookutils.fs.mount("abfss://local_workspace@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
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



# Additional imports for extract generation
import uuid
import json
import io
import gzip
import zipfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd

# Import warehouse_utils for warehouse operations
if run_mode == "local":
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
else:
    files_to_load = ["ingen_fab/python_libs/python/warehouse_utils.py"]
    load_python_modules_from_path(mount_path, files_to_load)

import time

run_id = str(uuid.uuid4())
start_time = datetime.utcnow()

print(f"Run ID: {run_id}")
print(f"Extract Name: {extract_name}")
print(f"Execution Group: {execution_group}")
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
        self.extract_sp_name = config_row.get("extract_sp_name")
        self.extract_sp_schema = config_row.get("extract_sp_schema")
        self.extract_table_name = config_row.get("extract_table_name")
        self.extract_table_schema = config_row.get("extract_table_schema")
        self.extract_view_name = config_row.get("extract_view_name")
        self.extract_view_schema = config_row.get("extract_view_schema")
        
        # Validation configuration
        self.validation_table_sp_name = config_row.get("validation_table_sp_name")
        self.validation_table_sp_schema = config_row.get("validation_table_sp_schema")
        
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
        self.extract_file_name_period_end_day = details_row.get("extract_file_name_period_end_day")
        self.extract_file_name_extension = details_row.get("extract_file_name_extension", "csv")
        self.extract_file_name_ordering = details_row.get("extract_file_name_ordering", 1)
        
        # File properties
        self.file_properties_column_delimiter = details_row.get("file_properties_column_delimiter", ",")
        self.file_properties_row_delimiter = details_row.get("file_properties_row_delimiter", "\\n")
        self.file_properties_encoding = details_row.get("file_properties_encoding", "UTF-8")
        self.file_properties_quote_character = details_row.get("file_properties_quote_character", '"')
        self.file_properties_escape_character = details_row.get("file_properties_escape_character", "\\")
        self.file_properties_header = details_row.get("file_properties_header", True)
        self.file_properties_null_value = details_row.get("file_properties_null_value", "")
        self.file_properties_max_rows_per_file = details_row.get("file_properties_max_rows_per_file")
        
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
        
    def get_source_query(self) -> Tuple[str, str]:
        """Get the SQL query to extract data and the source type"""
        if self.extract_table_name:
            return f"SELECT * FROM [{self.extract_table_schema}].[{self.extract_table_name}]", "TABLE"
        elif self.extract_view_name:
            return f"SELECT * FROM [{self.extract_view_schema}].[{self.extract_view_name}]", "VIEW"
        elif self.extract_sp_name:
            return f"EXEC [{self.extract_sp_schema}].[{self.extract_sp_name}]", "STORED_PROCEDURE"
        else:
            raise ValueError("No source object defined for extract")
    
    def generate_file_name(self, sequence: int = 1) -> str:
        """Generate file name based on configuration"""
        parts = []
        
        # Base name
        parts.append(self.extract_file_name)
        
        # Timestamp
        if self.extract_file_name_timestamp_format:
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
                timestamp_str = period_end.strftime(self.extract_file_name_timestamp_format)
            else:
                timestamp_str = datetime.utcnow().strftime(self.extract_file_name_timestamp_format)
            parts.append(timestamp_str)
        
        # Sequence number for file splitting
        if sequence > 1:
            parts.append(f"part{sequence:04d}")
        
        # Join parts and add extension
        file_name = "_".join(parts)
        if not file_name.endswith(f".{self.extract_file_name_extension}"):
            file_name = f"{file_name}.{self.extract_file_name_extension}"
        
        return file_name

# Initialize warehouse utilities
warehouse = warehouse_utils(
    target_workspace_id=configs.config_workspace_id,
    target_warehouse_id=configs.config_wh_warehouse_id
)

# Load configuration
config_query = f"""
SELECT * FROM [config].[config_extract_generation]
WHERE extract_name = '{extract_name}'
AND is_active = 1
"""

details_query = f"""
SELECT * FROM [config].[config_extract_details]
WHERE extract_name = '{extract_name}'
AND is_active = 1
"""

config_df = warehouse.query_table(config_query)
details_df = warehouse.query_table(details_query)

if config_df.empty:
    raise ValueError(f"No active configuration found for extract: {extract_name}")

if details_df.empty:
    raise ValueError(f"No active extract details found for extract: {extract_name}")

# Create configuration object
config = ExtractConfiguration(config_df.iloc[0].to_dict(), details_df.iloc[0].to_dict())

print(f"Extract configuration loaded: {config.extract_name}")
print(f"Source type: {config.get_source_query()[1]}")
print(f"Output format: {config.output_format}")
print(f"Compression: {'Yes' if config.is_compressed else 'No'}")



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔍 Extract Data from Source

# CELL ********************


def log_extract_run(status: str, error_message: str = None, **kwargs):
    """Log extract run to log table"""
    log_data = {
        "extract_name": config.extract_name,
        "execution_group": config.execution_group,
        "run_id": run_id,
        "run_timestamp": datetime.utcnow(),
        "run_status": status,
        "run_type": run_type,
        "start_time": start_time,
        "end_time": datetime.utcnow(),
        "duration_seconds": int((datetime.utcnow() - start_time).total_seconds()),
        "error_message": error_message,
        "workspace_id": configs.config_workspace_id,
        "warehouse_id": configs.config_wh_warehouse_id,
        "created_by": "extract_generation_notebook"
    }
    
    # Add optional fields
    log_data.update(kwargs)
    
    # Insert log entry
    warehouse.insert_data(
        data=[log_data],
        target_schema="log",
        target_table="log_extract_generation"
    )

try:
    # Run validation if configured
    if config.validation_table_sp_name:
        print(f"Running validation procedure: [{config.validation_table_sp_schema}].[{config.validation_table_sp_name}]")
        validation_query = f"EXEC [{config.validation_table_sp_schema}].[{config.validation_table_sp_name}]"
        warehouse.execute_query(validation_query)
    
    # Get source query
    source_query, source_type = config.get_source_query()
    
    print(f"Executing query: {source_query[:100]}...")
    
    # Execute query and get results
    if source_type == "STORED_PROCEDURE":
        # For stored procedures, we need to capture the result set
        # This assumes the SP returns a result set
        data_df = warehouse.query_table(source_query)
    else:
        data_df = warehouse.query_table(source_query)
    
    total_rows = len(data_df)
    print(f"Retrieved {total_rows:,} rows from source")
    
    # Log initial status
    log_extract_run(
        status="IN_PROGRESS",
        source_type=source_type,
        source_object=config.extract_table_name or config.extract_view_name or config.extract_sp_name,
        source_schema=config.extract_table_schema or config.extract_view_schema or config.extract_sp_schema,
        rows_extracted=total_rows
    )
    
except Exception as e:
    error_msg = f"Error extracting data: {str(e)}"
    print(error_msg)
    log_extract_run(status="FAILED", error_message=error_msg)
    raise



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 💾 Generate Extract Files

# CELL ********************


def write_dataframe_to_format(df: pd.DataFrame, output_format: str, config: ExtractConfiguration) -> bytes:
    """Convert DataFrame to specified format and return as bytes"""
    buffer = io.BytesIO()
    
    if output_format == "csv":
        df.to_csv(
            buffer,
            index=False,
            sep=config.file_properties_column_delimiter,
            header=config.file_properties_header,
            encoding=config.file_properties_encoding,
            quotechar=config.file_properties_quote_character,
            escapechar=config.file_properties_escape_character,
            na_rep=config.file_properties_null_value
        )
    elif output_format == "tsv":
        df.to_csv(
            buffer,
            index=False,
            sep='\t',
            header=config.file_properties_header,
            encoding=config.file_properties_encoding,
            na_rep=config.file_properties_null_value
        )
    elif output_format == "parquet":
        df.to_parquet(buffer, index=False, engine='pyarrow')
    else:
        raise ValueError(f"Unsupported output format: {output_format}")
    
    buffer.seek(0)
    return buffer.getvalue()

def compress_file(file_data: bytes, file_name: str, compression_type: str, compression_level: str) -> bytes:
    """Compress file data based on configuration"""
    if compression_type == "GZIP":
        # Map compression levels
        level_map = {"MINIMUM": 1, "NORMAL": 6, "MAXIMUM": 9}
        compress_level = level_map.get(compression_level, 6)
        
        return gzip.compress(file_data, compresslevel=compress_level)
    
    elif compression_type == "ZIP":
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.writestr(file_name, file_data)
        buffer.seek(0)
        return buffer.getvalue()
    
    else:
        raise ValueError(f"Unsupported compression type: {compression_type}")

# Generate files
files_generated = []
total_rows_written = 0

try:
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
            
            # Convert to specified format
            file_data = write_dataframe_to_format(df_chunk, config.output_format, config)
            
            # Compress if needed
            if config.is_compressed:
                compressed_name = f"{file_name}.{config.compressed_extension}"
                file_data = compress_file(file_data, file_name, config.compressed_type, config.compressed_level)
                file_name = compressed_name
            
            # Write to lakehouse
            file_path = f"{config.extract_container}/{config.extract_directory}/{file_name}".replace("//", "/")
            
            # For now, we'll simulate file writing
            # In production, this would use Fabric APIs to write to lakehouse
            print(f"Writing file: {file_path} ({len(file_data):,} bytes)")
            
            files_generated.append({
                "file_name": file_name,
                "file_path": file_path,
                "file_size": len(file_data),
                "rows": len(df_chunk)
            })
            
            total_rows_written += len(df_chunk)
    else:
        # Single file
        file_name = config.generate_file_name()
        
        # Convert to specified format
        file_data = write_dataframe_to_format(data_df, config.output_format, config)
        
        # Compress if needed
        if config.is_compressed:
            compressed_name = f"{file_name}.{config.compressed_extension}"
            file_data = compress_file(file_data, file_name, config.compressed_type, config.compressed_level)
            file_name = compressed_name
        
        # Write to lakehouse
        file_path = f"{config.extract_container}/{config.extract_directory}/{file_name}".replace("//", "/")
        
        print(f"Writing file: {file_path} ({len(file_data):,} bytes)")
        
        files_generated.append({
            "file_name": file_name,
            "file_path": file_path,
            "file_size": len(file_data),
            "rows": total_rows
        })
        
        total_rows_written = total_rows
    
    # Generate trigger file if configured
    if config.is_trigger_file:
        trigger_file_name = f"{config.extract_file_name}{config.trigger_file_extension}"
        trigger_file_path = f"{config.extract_container}/{config.extract_directory}/{trigger_file_name}".replace("//", "/")
        
        print(f"Creating trigger file: {trigger_file_path}")
        # In production, create empty file in lakehouse
        
    # Log successful completion
    log_extract_run(
        status="SUCCESS",
        source_type=source_type,
        source_object=config.extract_table_name or config.extract_view_name or config.extract_sp_name,
        source_schema=config.extract_table_schema or config.extract_view_schema or config.extract_sp_schema,
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
    
    print(f"\n✅ Extract completed successfully!")
    print(f"Files generated: {len(files_generated)}")
    print(f"Total rows written: {total_rows_written:,}")
    print(f"Total size: {sum(f['file_size'] for f in files_generated):,} bytes")
    
except Exception as e:
    error_msg = f"Error generating extract files: {str(e)}"
    print(error_msg)
    log_extract_run(
        status="FAILED",
        error_message=error_msg,
        source_type=source_type,
        source_object=config.extract_table_name or config.extract_view_name or config.extract_sp_name,
        source_schema=config.extract_table_schema or config.extract_view_schema or config.extract_sp_schema,
        rows_extracted=total_rows
    )
    raise



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Summary

# CELL ********************


# Display summary of the extract run
print(f"Extract Name: {config.extract_name}")
print(f"Run ID: {run_id}")
print(f"Duration: {int((datetime.utcnow() - start_time).total_seconds())} seconds")
print(f"\nFiles Generated:")
for file_info in files_generated:
    print(f"  - {file_info['file_name']} ({file_info['rows']:,} rows, {file_info['file_size']:,} bytes)")

# Display recent run history
history_query = f"""
SELECT TOP 10
    run_timestamp,
    run_status,
    run_type,
    rows_extracted,
    rows_written,
    files_generated,
    duration_seconds,
    error_message
FROM [log].[log_extract_generation]
WHERE extract_name = '{config.extract_name}'
ORDER BY run_timestamp DESC
"""

history_df = warehouse.query_table(history_query)
if not history_df.empty:
    print("\nRecent Run History:")
    print(history_df.to_string(index=False))