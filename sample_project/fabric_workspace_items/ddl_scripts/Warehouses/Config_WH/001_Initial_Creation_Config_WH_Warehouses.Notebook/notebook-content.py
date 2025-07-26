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

# ## 𝄜 Cell for 000_config_schema_create.sql

# CELL ********************

guid = "705373a5afe8"
def work():
    sql = """

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'config')
    exec('CREATE SCHEMA config;')



    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"000_config_schema_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 001_config_parquet_loads_create.sql

# CELL ********************

guid = "e77f9d31e86b"
def work():
    sql = """


CREATE TABLE config.config_parquet_loads (
    cfg_target_lakehouse_workspace_id VARCHAR(300) NOT NULL,
    cfg_target_lakehouse_id VARCHAR(300) NOT NULL,
    target_partition_columns VARCHAR(300) NOT NULL,
    target_sort_columns VARCHAR(300) NOT NULL,
    target_replace_where VARCHAR(300) NOT NULL,
    cfg_source_lakehouse_workspace_id VARCHAR(300) NOT NULL,
    cfg_source_lakehouse_id VARCHAR(300) NOT NULL,
    cfg_source_file_path VARCHAR(300) NOT NULL,
    source_file_path VARCHAR(300) NOT NULL,
    source_file_name VARCHAR(300) NOT NULL,
    cfg_legacy_synapse_connection_name VARCHAR(300) NOT NULL,
    synapse_source_schema_name VARCHAR(300) NOT NULL,
    synapse_source_table_name VARCHAR(300) NOT NULL,
    synapse_partition_clause VARCHAR(1000) NOT NULL,
    execution_group INT NOT NULL,
    active_yn VARCHAR(1) NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"001_config_parquet_loads_create", guid)






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

