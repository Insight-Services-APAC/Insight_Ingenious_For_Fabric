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
configs_dict = {'fabric_environment': 'local', 'fabric_deployment_workspace_id': '#####', 'synapse_source_database_1': 'test1', 'config_workspace_id': '#####', 'synapse_source_sql_connection': 'sansdaisyn-ondemand.sql.azuresynapse.net', 'config_lakehouse_name': 'config', 'edw_warehouse_name': 'edw', 'config_lakehouse_id': '2629d4cc-685c-458a-866b-b4705dde71a7', 'edw_workspace_id': '###', 'edw_warehouse_id': '###', 'edw_lakehouse_id': '6adb67d6-c8eb-4612-9053-890cae3a55d7', 'edw_lakehouse_name': 'edw', 'legacy_synapse_connection_name': 'synapse_connection', 'synapse_export_shortcut_path_in_onelake': 'exports/', 'raw_workspace_id': 'local_raw_workspace', 'raw_datastore_id': 'local_raw_datastore', 'config_warehouse_id': 'local-config-warehouse-id', 'config_workspace_name': 'local_workspace'}
# All variables as an object
from dataclasses import dataclass
@dataclass
class ConfigsObject:
    fabric_environment: str 
    fabric_deployment_workspace_id: str 
    synapse_source_database_1: str 
    config_workspace_id: str 
    synapse_source_sql_connection: str 
    config_lakehouse_name: str 
    edw_warehouse_name: str 
    config_lakehouse_id: str 
    edw_workspace_id: str 
    edw_warehouse_id: str 
    edw_lakehouse_id: str 
    edw_lakehouse_name: str 
    legacy_synapse_connection_name: str 
    synapse_export_shortcut_path_in_onelake: str 
    raw_workspace_id: str 
    raw_datastore_id: str 
    config_warehouse_id: str 
    config_workspace_name: str 
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


target_lakehouse_config_prefix = "Config"
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

# ## 𝄜 Cell for 003_sample_data_insert.sql

# CELL ********************

guid = "a8c5702c3e8c"
def work():
    sql = """

-- Sample configuration data for flat file ingestion testing - Universal schema (Warehouse version)

INSERT INTO config.config_flat_file_ingestion (
    config_id,
    config_name,
    source_file_path,
    source_file_format,
    target_workspace_id,
    target_datastore_id,
    target_datastore_type,
    target_schema_name,
    target_table_name,
    staging_table_name,
    file_delimiter,
    has_header,
    encoding,
    date_format,
    timestamp_format,
    schema_inference,
    custom_schema_json,
    partition_columns,
    sort_columns,
    write_mode,
    merge_keys,
    data_validation_rules,
    error_handling_strategy,
    execution_group,
    active_yn,
    created_date,
    modified_date,
    created_by,
    modified_by
) VALUES 
(
    'csv_test_001',
    'CSV Sales Data Test',
    'Files/sample_data/sales_data.csv',
    'csv',
    '#####',
    '2629d4cc-685c-458a-866b-b4705dde71a7',
    'lakehouse',
    'raw',
    'sales_data',
    NULL,
    ',',
    1,
    'utf-8',
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    '',
    'date',
    'overwrite',
    '',
    NULL,
    'fail',
    1,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL
),
(
    'parquet_test_002',
    'Parquet Products Data Test',
    'Files/sample_data/products.parquet',
    'parquet',
    '#####',
    '2629d4cc-685c-458a-866b-b4705dde71a7',
    'lakehouse',
    'raw',
    'products',
    NULL,
    NULL,
    NULL,
    NULL,
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    'category',
    'product_id',
    'overwrite',
    '',
    NULL,
    'log',
    1,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL
),
(
    'csv_warehouse_test_001',
    'CSV Sales Data Test (Warehouse)',
    'Files/sample_data/sales_data.csv',
    'csv',
    '#####',
    'local-config-warehouse-id',
    'warehouse',
    'raw',
    'sales_data',
    'staging_sales_data',
    ',',
    1,
    'utf-8',
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    '',
    'date',
    'overwrite',
    '',
    NULL,
    'fail',
    2,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL
),
(
    'parquet_warehouse_test_002',
    'Parquet Products Data Test (Warehouse)',
    'Files/sample_data/products.parquet',
    'parquet',
    '#####',
    'local-config-warehouse-id',
    'warehouse',
    'raw',
    'products',
    'staging_products',
    NULL,
    NULL,
    NULL,
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    '',
    'product_id',
    'overwrite',
    '',
    NULL,
    'log',
    2,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL
);

PRINT '✓ Inserted 4 sample configuration records (including warehouse targets)';

    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"003_sample_data_insert", guid)






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

