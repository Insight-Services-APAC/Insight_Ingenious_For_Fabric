# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions


# CELL ********************


import sys


if "notebookutils" in sys.modules:
    import sys
    
    notebookutils.fs.mount("abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_lakehouse_name}}.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
    mount_path = notebookutils.fs.getMountPath("/config_files")  # type: ignore # noqa: F821
    
    sys.path.insert(0, mount_path)
else:
    print("NotebookUtils not available, assumed running in local mode.")
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )
    notebookutils = NotebookUtilsFactory.create_instance()
    spark = None
    mount_path = None


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
            #traceback.print_exc()
            #print(notebookutils.fs.head(full_path, max_chars))

    print("\n✅ Successfully loaded:")
    for f in success_files:
        print(f" - {f}")

    if failed_files:
        print("\n⚠️ Failed to load:")
        for f in failed_files:
            print(f" - {f}")

import sys

def clear_module_cache(prefix: str):
    for mod in list(sys.modules):
        if mod.startswith(prefix):
            print("deleting..." + mod)
            del sys.modules[mod]

# Always clear the module cache - We may remove this once the libs are stable
clear_module_cache("ingen_fab.python_libs")
clear_module_cache("ingen_fab")






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "{language group | required}"
# META }

# MARKDOWN ********************

# ## Instantiate the Helper Classes

# CELL ********************



files_to_load = [
    "ingen_fab/python_libs/common/config_utils.py",
    "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
    "ingen_fab/python_libs/pyspark/ddl_utils.py",
    "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py",
    "ingen_fab/python_libs/pyspark/parquet_load_utils.py"
]

load_python_modules_from_path(pathPrefix, files_to_load)

target_lakehouse_config_prefix = "Config"

configs: ConfigsObject = get_configs_as_object()
config_lakehouse = lakehouse_utils(
    target_workspace_id=configs.edw_workspace_id,
    target_lakehouse_id=configs.edw_lakehouse_id
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "{language group | required}"
# META }

# MARKDOWN ********************

# ## Run the lakehouse DDL Notebooks

# CELL ********************



# Import required libraries
import sys
from datetime import datetime

# Initialize variables
workspace_id = mssparkutils.runtime.context.get("currentWorkspaceId")
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
        
        # Run the notebook
        result = mssparkutils.notebook.run(
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
        mssparkutils.notebook.exit(error_msg)
        return False

print(f"Starting orchestration for Config lakehouse")
print(f"Start time: {start_time}")
print(f"Workspace ID: {workspace_id}")
print(f"Total notebooks to execute: 1")
print("="*60)
execute_notebook("001_Initial_Creation_Config_Lakehouses", 1, 1)

# Final Summary
end_time = datetime.now()
duration = end_time - start_time

print(f"{'='*60}")
print(f"Orchestration Complete!")
print(f"{'='*60}")
print(f"End time: {end_time}")
print(f"Duration: {duration}")
print(f"Total notebooks: 1")
print(f"Successfully executed: {success_count}")
print(f"Failed: 1 - {success_count}")

if success_count == 1:
    print("✓ All notebooks executed successfully!")
    mssparkutils.notebook.exit("success")
else:
    print(f"✗ Orchestration completed with failures")
    mssparkutils.notebook.exit(f"Orchestration completed with {success_count}/1 successful executions")



