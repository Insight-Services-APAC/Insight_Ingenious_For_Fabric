# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "language_info": {
# META     "name": "python"
# META   }
# META }



# ## 『』Parameters



# Default parameters  
# Add default parameters here


# METADATA ********************

# META {
# META   "language": "python"
# META }



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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

# ## 🗂️ Now Load the Custom Python Libraries

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils.py import *
    from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.python.ddl_utils import ddl_utils
    from ingen_fab.python_libs.python.notebook_utils_abstraction import NotebookUtilsFactory
    from ingen_fab.python_libs.python.sql_templates import sql_templates
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
    from ingen_fab.python_libs.python.pipeline_utils import pipeline_utils
    notebookutils = NotebookUtilsFactory.create_instance() 
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/python/lakehouse_utils.py",
        "ingen_fab/python_libs/python/ddl_utils.py",
        "ingen_fab/python_libs/python/notebook_utils_abstraction.py",
        "ingen_fab/python_libs/python/sql_templates.py",
        "ingen_fab/python_libs/python/warehouse_utils.py",
        "ingen_fab/python_libs/python/pipeline_utils.py"
    ]

    load_python_modules_from_path(mount_path, files_to_load)



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🆕 Instantiate Required Classes 

# CELL ********************


target_lakehouse_config_prefix = "Config"
configs: ConfigsObject = get_configs_as_object()
target_warehouse_id = get_config_value(f"{target_lakehouse_config_prefix.lower()}_warehouse_id")
target_workspace_id = get_config_value(f"{target_lakehouse_config_prefix.lower()}_workspace_id")

du = ddl_utils(
    target_workspace_id=target_workspace_id,
    target_warehouse_id=target_warehouse_id,
    notebookutils=notebookutils
)




# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🏃‍♂️‍➡️ Run DDL Cells 

# CELL ********************


# DDL cells are injected below:



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 001_config_flat_file_ingestion_create.sql

# CELL ********************

guid = "c84bee3df682"
def work():
    sql = """

-- Configuration table for flat file ingestion metadata - Warehouse version
CREATE TABLE config_flat_file_ingestion (
    config_id NVARCHAR(50) NOT NULL,
    config_name NVARCHAR(255) NOT NULL,
    source_file_path NVARCHAR(500) NOT NULL,
    source_file_format NVARCHAR(50) NOT NULL, -- csv, json, parquet, avro, xml
    target_lakehouse_workspace_id NVARCHAR(50) NOT NULL,
    target_lakehouse_id NVARCHAR(50) NOT NULL,
    target_schema_name NVARCHAR(128) NOT NULL,
    target_table_name NVARCHAR(128) NOT NULL,
    file_delimiter NVARCHAR(10) NULL, -- for CSV files
    has_header BIT NULL, -- for CSV files
    encoding NVARCHAR(50) NULL, -- utf-8, latin-1, etc.
    date_format NVARCHAR(50) NULL, -- for date columns
    timestamp_format NVARCHAR(50) NULL, -- for timestamp columns
    schema_inference BIT NOT NULL, -- whether to infer schema
    custom_schema_json NVARCHAR(MAX) NULL, -- custom schema definition
    partition_columns NVARCHAR(500) NULL, -- comma-separated list
    sort_columns NVARCHAR(500) NULL, -- comma-separated list
    write_mode NVARCHAR(50) NOT NULL, -- overwrite, append, merge
    merge_keys NVARCHAR(500) NULL, -- for merge operations
    data_validation_rules NVARCHAR(MAX) NULL, -- JSON validation rules
    error_handling_strategy NVARCHAR(50) NOT NULL, -- fail, skip, log
    execution_group INT NOT NULL,
    active_yn NVARCHAR(1) NOT NULL,
    created_date DATETIME2 NOT NULL,
    modified_date DATETIME2 NULL,
    created_by NVARCHAR(100) NOT NULL,
    modified_by NVARCHAR(100) NULL,
    CONSTRAINT PK_config_flat_file_ingestion PRIMARY KEY (config_id),
    CONSTRAINT CHK_active_yn CHECK (active_yn IN ('Y', 'N')),
    CONSTRAINT CHK_source_file_format CHECK (source_file_format IN ('csv', 'json', 'parquet', 'avro', 'xml')),
    CONSTRAINT CHK_write_mode CHECK (write_mode IN ('overwrite', 'append', 'merge')),
    CONSTRAINT CHK_error_handling_strategy CHECK (error_handling_strategy IN ('fail', 'skip', 'log'))
);

    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"001_config_flat_file_ingestion_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 002_log_flat_file_ingestion_create.sql

# CELL ********************

guid = "e6b0ef5f7a70"
def work():
    sql = """

-- Log table for flat file ingestion execution tracking - Warehouse version
CREATE TABLE log_flat_file_ingestion (
    log_id NVARCHAR(50) NOT NULL,
    config_id NVARCHAR(50) NOT NULL,
    execution_id NVARCHAR(50) NOT NULL,
    job_start_time DATETIME2 NOT NULL,
    job_end_time DATETIME2 NULL,
    status NVARCHAR(50) NOT NULL, -- running, completed, failed, cancelled
    source_file_path NVARCHAR(500) NOT NULL,
    source_file_size_bytes BIGINT NULL,
    source_file_modified_time DATETIME2 NULL,
    target_table_name NVARCHAR(128) NOT NULL,
    records_processed BIGINT NULL,
    records_inserted BIGINT NULL,
    records_updated BIGINT NULL,
    records_deleted BIGINT NULL,
    records_failed BIGINT NULL,
    error_message NVARCHAR(MAX) NULL,
    error_details NVARCHAR(MAX) NULL,
    execution_duration_seconds INT NULL,
    spark_application_id NVARCHAR(255) NULL,
    created_date DATETIME2 NOT NULL,
    created_by NVARCHAR(100) NOT NULL,
    CONSTRAINT PK_log_flat_file_ingestion PRIMARY KEY (log_id),
    CONSTRAINT CHK_status CHECK (status IN ('running', 'completed', 'failed', 'cancelled')),
    CONSTRAINT FK_log_config FOREIGN KEY (config_id) REFERENCES config_flat_file_ingestion(config_id)
);

    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"002_log_flat_file_ingestion_create", guid)






# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📇 Print the execution log

# CELL ********************


du.print_log() 



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ✔️ If we make it to the end return a successful result

# CELL ********************


import sys
sys.exit("success")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

