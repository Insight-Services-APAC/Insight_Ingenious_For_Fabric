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
    
    notebookutils.fs.mount("abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
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
configs_dict = {'fabric_environment': 'local', 'fabric_deployment_workspace_id': '#####', 'synapse_source_database_1': 'test1', 'config_workspace_id': '#####', 'synapse_source_sql_connection': 'sansdaisyn-ondemand.sql.azuresynapse.net', 'config_lakehouse_name': 'config', 'edw_warehouse_name': 'edw', 'config_lakehouse_id': '2629d4cc-685c-458a-866b-b4705dde71a7', 'edw_workspace_id': '###', 'edw_warehouse_id': '###', 'edw_lakehouse_id': '6adb67d6-c8eb-4612-9053-890cae3a55d7', 'edw_lakehouse_name': 'edw', 'legacy_synapse_connection_name': 'synapse_connection', 'synapse_export_shortcut_path_in_onelake': 'exports/', 'raw_workspace_id': 'local_raw_workspace', 'raw_datastore_id': 'local_raw_datastore', 'config_warehouse_id': 'local-config-warehouse-id'}
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

# ## 𝄜 Cell for 001_config_parquet_loads_create.sql

# CELL ********************

guid = "6937eb26c16c"
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
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 002_config_synapse_loads_create.sql

# CELL ********************

guid = "bc17803f3701"
def work():
    sql = """


CREATE TABLE config.config_synapse_extracts (
    cfg_legacy_synapse_connection_name VARCHAR(300) NOT NULL,
    source_schema_name VARCHAR(300) NOT NULL,
    source_table_name VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000) NOT NULL,
    execution_group INT NOT NULL,
    active_yn VARCHAR(1) NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"002_config_synapse_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 003_log_parquet_loads_create.sql

# CELL ********************

guid = "0a6390965eba"
def work():
    sql = """


CREATE TABLE log.log_parquet_loads (
    execution_id VARCHAR(300) NOT NULL,
    cfg_target_lakehouse_workspace_id VARCHAR(300) NOT NULL,
    cfg_target_lakehouse_id VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000),
    status VARCHAR(300) NOT NULL,
    error_messages VARCHAR(4000),
    start_date BIGINT NOT NULL,
    finish_date BIGINT NOT NULL,
    update_date BIGINT NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"003_log_parquet_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 004_log_synapse_loads_create.sql

# CELL ********************

guid = "755109da42d0"
def work():
    sql = """


CREATE TABLE log.log_synapse_extracts (
    execution_id VARCHAR(300) NOT NULL,
    cfg_synapse_connection_name VARCHAR(300) NOT NULL,
    source_schema_name VARCHAR(300) NOT NULL,
    source_table_name VARCHAR(300) NOT NULL,
    extract_file_name VARCHAR(300) NOT NULL,
    partition_clause VARCHAR(1000) NOT NULL,
    status VARCHAR(300) NOT NULL,
    error_messages VARCHAR(4000),
    start_date BIGINT NOT NULL,
    finish_date BIGINT NOT NULL,
    update_date BIGINT NOT NULL
);


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"004_log_synapse_loads_create", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 005_config_synapse_loads_insert.sql

# CELL ********************

guid = "7461d6d52cd5"
def work():
    sql = """


INSERT INTO config.config_synapse_extracts (
    cfg_legacy_synapse_connection_name,
    source_schema_name,
    source_table_name,
    partition_clause,
    execution_group,
    active_yn
)
VALUES
('legacy_synapse_connection_name', 'dbo', 'dim_customer', '', 1, 'Y'),
('legacy_synapse_connection_name', 'dbo', 'fact_transactions', 'where year = @year and month = @month', 1, 'Y');


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"005_config_synapse_loads_insert", guid)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 006_config_parquet_loads_insert.sql

# CELL ********************

guid = "6f95f2886b11"
def work():
    sql = """


INSERT INTO config.config_parquet_loads (
    cfg_target_lakehouse_workspace_id,
    cfg_target_lakehouse_id,
    target_partition_columns,
    target_sort_columns,
    target_replace_where,
    cfg_source_lakehouse_workspace_id,
    cfg_source_lakehouse_id,
    cfg_source_file_path,
    source_file_path,
    source_file_name,
    cfg_legacy_synapse_connection_name,
    synapse_source_schema_name,
    synapse_source_table_name,
    synapse_partition_clause,
    execution_group,
    active_yn
)
VALUES
('edw_workspace_id', 'edw_lakehouse_id', '', '', '', 'edw_lakehouse_workspace_id', 'edw_lakehouse_id', 'synapse_export_shortcut_path_in_onelake', 'dbo_dim_customer', '', 'legacy_synapse_connection_name', 'dbo', 'dim_customer', '', 1, 'Y'),
('edw_workspace_id', 'edw_lakehouse_id', 'year, month', 'year, month', 'WHERE year = @year AND month = @month', 'edw_workspace_id', 'edw_lakehouse_id', 'synapse_export_shortcut_path_in_onelake', 'dbo_dim_customer', '', 'legacy_synapse_connection_name', 'dbo', 'fact_transactions', 'WHERE year = @year AND month = @month', 1, 'Y');


    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"006_config_parquet_loads_insert", guid)






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

