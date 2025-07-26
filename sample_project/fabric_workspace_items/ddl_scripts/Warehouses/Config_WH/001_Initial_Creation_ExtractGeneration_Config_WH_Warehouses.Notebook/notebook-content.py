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



# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************



# Default parameters  
# Add default parameters here




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
    
    notebookutils.fs.mount("abfss://REPLACE_WITH_CONFIG_WORKSPACE_NAME@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
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

# ## 🗂️ Now Load the Custom Python Libraries

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import *
    from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.python.ddl_utils import ddl_utils
    from ingen_fab.python_libs.python.notebook_utils_abstraction import NotebookUtilsFactory
    from ingen_fab.python_libs.python.sql_templates import SQLTemplates
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
    from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils
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

# ## ⚙️ Configuration Settings

# CELL ********************


# variableLibraryInjectionStart: var_lib

# All variables as a dictionary
configs_dict = {'fabric_environment': 'local', 'fabric_deployment_workspace_id': 'REPLACE_WITH_YOUR_WORKSPACE_GUID', 'config_workspace_name': 'REPLACE_WITH_CONFIG_WORKSPACE_NAME', 'config_workspace_id': 'REPLACE_WITH_CONFIG_WORKSPACE_GUID', 'config_wh_workspace_id': 'REPLACE_WITH_CONFIG_WH_WORKSPACE_GUID', 'config_lakehouse_name': 'config', 'config_lakehouse_id': 'REPLACE_WITH_CONFIG_LAKEHOUSE_GUID', 'config_wh_warehouse_name': 'config_wh', 'config_wh_warehouse_id': 'REPLACE_WITH_CONFIG_WAREHOUSE_GUID', 'sample_lakehouse_name': 'sample', 'sample_lakehouse_id': 'REPLACE_WITH_SAMPLE_LAKEHOUSE_GUID', 'sample_wh_workspace_id': 'REPLACE_WITH_SAMPLE_WH_WORKSPACE_GUID', 'sample_wh_warehouse_name': 'sample_wh', 'sample_wh_warehouse_id': 'REPLACE_WITH_SAMPLE_WAREHOUSE_GUID', 'raw_workspace_id': 'REPLACE_WITH_RAW_WORKSPACE_GUID', 'raw_datastore_id': 'REPLACE_WITH_RAW_DATASTORE_GUID', 'edw_workspace_id': 'REPLACE_WITH_EDW_WORKSPACE_GUID', 'edw_lakehouse_name': 'edw', 'edw_lakehouse_id': 'REPLACE_WITH_EDW_LAKEHOUSE_GUID', 'edw_warehouse_name': 'edw', 'edw_warehouse_id': 'REPLACE_WITH_EDW_WAREHOUSE_GUID'}
# All variables as an object
from dataclasses import dataclass
@dataclass
class ConfigsObject:
    fabric_environment: str 
    fabric_deployment_workspace_id: str 
    config_workspace_name: str 
    config_workspace_id: str 
    config_wh_workspace_id: str 
    config_lakehouse_name: str 
    config_lakehouse_id: str 
    config_wh_warehouse_name: str 
    config_wh_warehouse_id: str 
    sample_lakehouse_name: str 
    sample_lakehouse_id: str 
    sample_wh_workspace_id: str 
    sample_wh_warehouse_name: str 
    sample_wh_warehouse_id: str 
    raw_workspace_id: str 
    raw_datastore_id: str 
    edw_workspace_id: str 
    edw_lakehouse_name: str 
    edw_lakehouse_id: str 
    edw_warehouse_name: str 
    edw_warehouse_id: str 
configs_object: ConfigsObject = ConfigsObject(**configs_dict)
# variableLibraryInjectionEnd: var_lib



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🆕 Instantiate Required Classes 

# CELL ********************


target_lakehouse_config_prefix = "Config_WH"
configs: ConfigsObject = get_configs_as_object()
target_warehouse_id = get_config_value(f"{target_lakehouse_config_prefix.lower()}_warehouse_id")
target_workspace_id = get_config_value(f"{target_lakehouse_config_prefix.lower()}_workspace_id")

du = ddl_utils(
    target_workspace_id=target_workspace_id,
    target_warehouse_id=target_warehouse_id,
    notebookutils=notebookutils
)

wu = warehouse_utils(
    target_workspace_id=target_workspace_id,
    target_warehouse_id=target_warehouse_id
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

# ## 𝄜 Cell for 000_schema_creation.sql

# CELL ********************

guid = "670b6a9a2f22"
def work():
    sql = """

-- Create schemas for Extract Generation package
-- This script creates the necessary schemas if they don't exist

-- Create config schema for configuration tables
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'config')
BEGIN
    EXEC('CREATE SCHEMA config')
END;


-- Create log schema for logging tables
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'log')
BEGIN
    EXEC('CREATE SCHEMA log')
END;

    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"000_schema_creation", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 001_config_extract_details_create.sql

# CELL ********************

guid = "baecee7038c8"
def work():
    sql = """

-- Create config_extract_details table
-- Maps to legacy ConfigurationExtractDetails table structure

CREATE TABLE [config].[config_extract_details] (
    -- Core columns matching legacy structure
    creation_time DATETIME2(6) NOT NULL,
    is_active BIT NOT NULL,
    extract_name VARCHAR(100) NOT NULL,
    file_generation_group VARCHAR(100),
    
    -- Azure Data Lake settings
    kv_azure_data_lake_url VARCHAR(1000),
    kv_azure_data_lake_secret_name VARCHAR(100),
    extract_container VARCHAR(100),
    extract_directory VARCHAR(100),
    
    -- File naming configuration
    extract_file_name VARCHAR(100),
    extract_file_name_timestamp_format VARCHAR(100),
    extract_file_name_period_end_day INT,
    extract_file_name_extension VARCHAR(100),
    extract_file_name_ordering INT,
    
    -- File properties
    file_properties_column_delimiter VARCHAR(5),
    file_properties_row_delimiter VARCHAR(5),
    file_properties_encoding VARCHAR(50),
    file_properties_quote_character VARCHAR(1),
    file_properties_escape_character VARCHAR(1),
    file_properties_header BIT NOT NULL,
    file_properties_null_value VARCHAR(5),
    file_properties_max_rows_per_file INT,
    
    -- Trigger file settings
    is_trigger_file BIT NOT NULL,
    trigger_file_extension VARCHAR(100),
    
    -- Compression settings
    is_compressed BIT NOT NULL,
    compressed_type VARCHAR(50),
    compressed_level VARCHAR(50),
    compressed_is_compress_multiple_files BIT NOT NULL,
    compressed_is_delete_old_files BIT NOT NULL,
    compressed_file_name VARCHAR(100),
    compressed_timestamp_format VARCHAR(100),
    compressed_period_end_day INT,
    compressed_extension VARCHAR(100),
    compressed_name_ordering INT,
    compressed_is_trigger_file BIT NOT NULL,
    compressed_trigger_file_extension VARCHAR(100),
    compressed_extract_container VARCHAR(100),
    compressed_extract_directory VARCHAR(100),
    
    -- Encryption API settings
    is_encrypt_api BIT NOT NULL,
    encrypt_api_is_encrypt BIT NOT NULL,
    encrypt_api_is_create_trigger_file BIT NOT NULL,
    encrypt_api_trigger_file_extension VARCHAR(10),
    encrypt_api_kv_token_authorisation VARCHAR(100),
    encrypt_api_kv_azure_data_lake_connection_string VARCHAR(1000),
    encrypt_api_kv_azure_data_lake_account_name VARCHAR(100),
    
    -- Validation settings
    is_validation_table BIT NOT NULL,
    is_validation_table_external_data_source VARCHAR(100),
    is_validation_table_external_file_format VARCHAR(100),
    
    -- Additional Fabric-specific columns
    output_format VARCHAR(50), -- 'csv', 'tsv', 'parquet'
    fabric_lakehouse_path VARCHAR(500),
    
    -- Metadata columns
    created_by VARCHAR(100),
    created_timestamp DATETIME2(6),
    modified_by VARCHAR(100),
    modified_timestamp DATETIME2(6),
    
);

    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"001_config_extract_details_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 002_config_extract_generation_create.sql

# CELL ********************

guid = "5616ca805070"
def work():
    sql = """

-- Create config_extract_generation table
-- Maps to legacy ConfigurationExtract table structure

CREATE TABLE [config].[config_extract_generation] (
    -- Core columns matching legacy structure
    creation_time DATETIME2(6) NOT NULL,
    is_active BIT NOT NULL,
    trigger_name VARCHAR(100),
    extract_name VARCHAR(100) NOT NULL,
    extract_pipeline_name VARCHAR(100),
    extract_sp_name VARCHAR(100),
    extract_sp_schema VARCHAR(100),
    extract_table_name VARCHAR(100),
    extract_table_schema VARCHAR(100),
    extract_view_name VARCHAR(100),
    extract_view_schema VARCHAR(100),
    validation_table_sp_name VARCHAR(100),
    validation_table_sp_schema VARCHAR(100),
    is_full_load BIT NOT NULL,
    
    -- Additional Fabric-specific columns
    workspace_id VARCHAR(100),
    lakehouse_id VARCHAR(100),
    warehouse_id VARCHAR(100),
    execution_group VARCHAR(50),
    
    -- Metadata columns
    created_by VARCHAR(100),
    created_timestamp DATETIME2(6),
    modified_by VARCHAR(100),
    modified_timestamp DATETIME2(6),
    
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"002_config_extract_generation_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 003_log_extract_generation_create.sql

# CELL ********************

guid = "aa0e791045c7"
def work():
    sql = """

-- Create log_extract_generation table
-- Tracks execution history of extract generation runs
-- NOTE: IDENTITY columns are not supported in Microsoft Fabric SQL Warehouse
-- Applications must generate unique log_id values manually (e.g., using NEWID(), timestamps, or sequence logic)
CREATE TABLE [log].[log_extract_generation] (
    -- Primary key (Note: IDENTITY not supported in Fabric Warehouse - app must provide unique values)
    log_id BIGINT NOT NULL,
    
    -- Extract identification
    extract_name VARCHAR(100) NOT NULL,
    execution_group VARCHAR(50),
    
    -- Run information
    run_id VARCHAR(100) NOT NULL,
    run_timestamp DATETIME2(6) NOT NULL,
    run_status VARCHAR(50) NOT NULL,
    run_type VARCHAR(50), -- 'FULL', 'INCREMENTAL'
    
    -- Source information
    source_type VARCHAR(50), -- 'TABLE', 'VIEW', 'STORED_PROCEDURE'
    source_object VARCHAR(200),
    source_schema VARCHAR(100),
    
    -- File generation details
    output_format VARCHAR(50),
    output_file_path VARCHAR(1000),
    output_file_name VARCHAR(500),
    output_file_size_bytes BIGINT,
    
    -- Record counts
    rows_extracted BIGINT,
    rows_written BIGINT,
    files_generated INT,
    
    -- Compression details
    is_compressed BIT,
    compressed_file_path VARCHAR(1000),
    compressed_file_name VARCHAR(500),
    compressed_file_size_bytes BIGINT,
    compression_type VARCHAR(50),
    
    -- Trigger file details
    trigger_file_created BIT,
    trigger_file_path VARCHAR(1000),
    
    -- Timing information
    start_time DATETIME2(6),
    end_time DATETIME2(6),
    duration_seconds INT,
    
    -- Error handling
    error_message VARCHAR(8000),
    error_details VARCHAR(8000),
    
    -- Fabric specific
    workspace_id VARCHAR(100),
    lakehouse_id VARCHAR(100),
    warehouse_id VARCHAR(100),
    notebook_run_id VARCHAR(100),
    
    -- Metadata
    created_by VARCHAR(100),
    created_timestamp DATETIME2(6),
    
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"003_log_extract_generation_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 004_config_extract_details_constraints.sql

# CELL ********************

guid = "3352f7a24eec"
def work():
    sql = """

-- Add primary key constraint for config_extract_details table
-- Note: Must be done after table creation in Microsoft Fabric SQL Warehouse
-- This step may fail in Fabric due to transaction isolation but is included for completeness
-- For PostgreSQL: This becomes a standard PRIMARY KEY constraint

-- Drop constraint if it exists (for re-run scenarios)
ALTER TABLE [config].[config_extract_details] DROP CONSTRAINT IF EXISTS PK_config_extract_details;

-- Add the primary key constraint
ALTER TABLE [config].[config_extract_details] 
ADD CONSTRAINT PK_config_extract_details 
PRIMARY KEY NONCLUSTERED (extract_name) NOT ENFORCED;

    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"004_config_extract_details_constraints", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 005_config_extract_generation_constraints.sql

# CELL ********************

guid = "bc690b97cd64"
def work():
    sql = """

-- Add primary key constraint for config_extract_generation table
-- Note: Must be done after table creation in Microsoft Fabric SQL Warehouse
-- This step may fail in Fabric due to transaction isolation but is included for completeness
-- For PostgreSQL: This becomes a standard PRIMARY KEY constraint

-- Drop constraint if it exists (for re-run scenarios)
ALTER TABLE [config].[config_extract_generation] DROP CONSTRAINT IF EXISTS PK_config_extract_generation;

-- Add the primary key constraint
ALTER TABLE [config].[config_extract_generation] 
ADD CONSTRAINT PK_config_extract_generation 
PRIMARY KEY NONCLUSTERED (extract_name) NOT ENFORCED;

    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"005_config_extract_generation_constraints", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 006_log_extract_generation_constraints.sql

# CELL ********************

guid = "4237817f44f7"
def work():
    sql = """

-- Add primary key constraint for log_extract_generation table
-- Note: Must be done after table creation in Microsoft Fabric SQL Warehouse
-- This step may fail in Fabric due to transaction isolation but is included for completeness
-- For PostgreSQL: This becomes a standard PRIMARY KEY constraint

-- Drop constraint if it exists (for re-run scenarios)
ALTER TABLE [log].[log_extract_generation] DROP CONSTRAINT IF EXISTS PK_log_extract_generation;

-- Add the primary key constraint
ALTER TABLE [log].[log_extract_generation] 
ADD CONSTRAINT PK_log_extract_generation 
PRIMARY KEY NONCLUSTERED (log_id) NOT ENFORCED;

    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"006_log_extract_generation_constraints", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 008_vw_extract_generation_latest_runs.sql

# CELL ********************

guid = "5fd58cd46d86"
def work():
    sql = """


CREATE VIEW [log].[vw_extract_generation_latest_runs] AS
WITH LatestRuns AS (
    SELECT 
        extract_name,
        MAX(run_timestamp) as latest_run_timestamp
    FROM [log].[log_extract_generation]
    GROUP BY extract_name
)
SELECT 
    l.*
FROM [log].[log_extract_generation] l
INNER JOIN LatestRuns lr 
    ON l.extract_name = lr.extract_name 
    AND l.run_timestamp = lr.latest_run_timestamp;

    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"008_vw_extract_generation_latest_runs", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 009_sample_data_insert.sql

# CELL ********************

guid = "e185dd711363"
def work():
    sql = """

-- Sample data for Extract Generation package
-- This script inserts sample configurations for testing various extract scenarios

-- Clear existing sample data
DELETE FROM [config].[config_extract_details] WHERE extract_name LIKE 'SAMPLE_%';
DELETE FROM [config].[config_extract_generation] WHERE extract_name LIKE 'SAMPLE_%';

-- Sample 1: Simple table extract to CSV
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_CUSTOMERS_DAILY', 1, 'customers', 'dbo', 
    1, 'DAILY_EXTRACTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_row_delimiter,
    file_properties_encoding, file_properties_header, output_format,
    is_trigger_file, is_compressed, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_CUSTOMERS_DAILY', 1, 'CUSTOMER_DATA',
    'extracts', 'customers', 'customers',
    'yyyyMMdd_HHmmss', 'csv',
    ',', '\n',
    'UTF-8', 1, 'csv',
    0, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 2: View extract with ZIP compression
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_view_name, extract_view_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_SALES_SUMMARY_MONTHLY', 1, 'v_sales_summary', 'reporting', 
    1, 'MONTHLY_REPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    is_compressed, compressed_type, compressed_level,
    compressed_file_name, compressed_extension, output_format,
    is_trigger_file, is_encrypt_api, is_validation_table,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_SALES_SUMMARY_MONTHLY', 1, 'SALES_REPORTS',
    'extracts', 'sales/monthly', 'sales_summary',
    'yyyyMM', 'csv',
    ',', 1,
    1, 'ZIP', 'NORMAL',
    'sales_summary_archive', '.zip', 'csv',
    0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 3: Stored procedure extract with trigger file  
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_sp_name, extract_sp_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_FINANCIAL_REPORT', 1, 'sp_generate_financial_report', 'finance', 
    0, 'FINANCIAL_REPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    is_trigger_file, trigger_file_extension,
    output_format, file_properties_max_rows_per_file,
    is_compressed, is_encrypt_api, is_validation_table,
    file_properties_header,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_FINANCIAL_REPORT', 1, 'FINANCE',
    'extracts', 'finance/reports', 'financial_report',
    'yyyyMMdd', 'parquet',
    1, '.done',
    'parquet', 1000000,
    0, 0, 0,
    1,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 4: Large table extract with file splitting and GZIP compression
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema, 
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_TRANSACTIONS_EXPORT', 1, 'transactions', 'dbo', 
    1, 'LARGE_EXPORTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_header,
    file_properties_max_rows_per_file,
    is_compressed, compressed_type, compressed_level,
    compressed_is_compress_multiple_files, output_format,
    is_trigger_file, is_encrypt_api, is_validation_table,
    compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_TRANSACTIONS_EXPORT', 1, 'TRANSACTIONS',
    'extracts', 'transactions/daily', 'transactions',
    'yyyyMMdd_HHmmss', 'tsv',
    '\t', 1,
    500000,
    1, 'GZIP', 'MAXIMUM',
    1, 'tsv',
    0, 0, 0,
    1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);

-- Sample 5: Incremental extract with validation
INSERT INTO [config].[config_extract_generation] (
    creation_time, extract_name, is_active, extract_table_name, extract_table_schema,
    validation_table_sp_name, validation_table_sp_schema,
    is_full_load, execution_group, created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDERS_INCREMENTAL', 1, 'orders', 'dbo',
    'sp_validate_orders_extract', 'dbo',
    0, 'INCREMENTAL_EXTRACTS', SYSTEM_USER, GETDATE()
);

INSERT INTO [config].[config_extract_details] (
    creation_time, extract_name, is_active, file_generation_group,
    extract_container, extract_directory, extract_file_name,
    extract_file_name_timestamp_format, extract_file_name_extension,
    file_properties_column_delimiter, file_properties_quote_character,
    file_properties_escape_character, file_properties_header,
    file_properties_null_value, output_format,
    is_validation_table, is_trigger_file, is_compressed, is_encrypt_api,
    compressed_is_compress_multiple_files, compressed_is_delete_old_files, compressed_is_trigger_file,
    encrypt_api_is_encrypt, encrypt_api_is_create_trigger_file,
    created_by, created_timestamp
) VALUES (
    GETDATE(), 'SAMPLE_ORDERS_INCREMENTAL', 1, 'ORDERS',
    'extracts', 'orders/incremental', 'orders_delta',
    'yyyyMMdd_HHmmss', 'csv',
    ',', '"',
    '|', 1,
    'NULL', 'csv',
    1, 0, 0, 0,
    0, 1, 0,
    0, 0,
    SYSTEM_USER, GETDATE()
);



    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"009_sample_data_insert", guid)






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


notebookutils.exit_notebook("success")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

