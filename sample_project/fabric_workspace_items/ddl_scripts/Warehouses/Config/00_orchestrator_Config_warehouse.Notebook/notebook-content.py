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

# ## 🆕 Instantiate Required Classes 

# CELL ********************



configs: ConfigsObject = get_configs_as_object()




# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🏃‍♂️‍➡️ Run All Warehouse DDL

# CELL ********************

# Import required libraries
import sys
from datetime import datetime

# Initialize variables
success_count = 0
failed_notebook = None
start_time = datetime.now()

# Define execution function
def execute_notebook(notebook_name, index, total, timeout_seconds=3600):
    """Execute a single notebook and handle success/failure."""
    global success_count
    
    try:
        
        print(f"{'='*60}")
        print(f"Executing notebook {index}/{total}:{notebook_name}")
        print(f"{'='*60}")
        params = {}
        
        # Use notebook utils abstraction for cross-environment compatibility
        result = notebookutils.notebook.run(
            notebook_name,
            timeout_seconds,
            params
        )
        
        if (result == 'success'):
            success_count += 1
        else: 
            raise Exception({"result": result}) 

        print(f"✓ Successfully executed: {notebook_name}")
        print(f"Exit value: {result}")
        return True
        
    except Exception as e:
        print(f"✗ Failed to execute: {notebook_name}")
        print(f"Error: {str(e)}")
        
        # Stop execution on failure
        error_msg = f"Orchestration stopped due to failure in notebook: {notebook_name}. Error: {str(e)}"
        notebookutils.notebook.exit(error_msg)
        return False

print(f"Starting orchestration for  warehouse")
print(f"Start time: {start_time}")
print(f"Total notebooks to execute: 8")
print("="*60)
execute_notebook("000_Initial_Schema_Creation_Config_Warehouses_ddl_scripts", 1, 8)
execute_notebook("001_Initial_Creation_Ingestion_Config_Warehouses_ddl_scripts", 2, 8)
execute_notebook("001_Initial_Creation_Config_Warehouses_ddl_scripts", 3, 8)
execute_notebook("001_Initial_Creation_Synapse_Sync_Config_Warehouses_ddl_scripts", 4, 8)
execute_notebook("001_Initial_Creation_ExtractGeneration_Config_Warehouses_ddl_scripts", 5, 8)
execute_notebook("002_Sample_Data_Ingestion_Config_Warehouses_ddl_scripts", 6, 8)
execute_notebook("002_Parquet_Load_Update_Config_Warehouses_ddl_scripts", 7, 8)
execute_notebook("002_Sample_Data_Synapse_Sync_Config_Warehouses_ddl_scripts", 8, 8)

# Final Summary
end_time = datetime.now()
duration = end_time - start_time

print(f"{'='*60}")
print(f"Orchestration Complete!")
print(f"{'='*60}")
print(f"End time: {end_time}")
print(f"Duration: {duration}")
print(f"Total notebooks: 8")
print(f"Successfully executed: {success_count}")
print(f"Failed: 8 - {success_count}")

if success_count == 8:
    print("✓ All notebooks executed successfully!")
    notebookutils.notebook.exit("success")
else:
    print(f"✗ Orchestration completed with failures")
    notebookutils.notebook.exit(f"Orchestration completed with {success_count}/8 successful executions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

