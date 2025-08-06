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
        if base_path.startswith("file:") or base_path.startswith("abfss:"):
            full_path = f"{base_path}/{relative_path}"
        else:
            full_path = f"file:{base_path}/{relative_path}"
        try:
            print(f"🔄 Loading: {full_path}")
            code = notebookutils.fs.head(full_path, max_chars)
            exec(code, globals())  # Use globals() to share context across modules
            success_files.append(relative_path)
        except Exception as e:
            failed_files.append(relative_path)
            print(f"❌ Error loading {relative_path}")
            print(f"   Error type: {type(e).__name__}")
            print(f"   Error message: {str(e)}")
            print(f"   Stack trace:")
            traceback.print_exc()

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
    from ingen_fab.python_libs.python.ddl_utils import ddl_utils
    from ingen_fab.python_libs.python.notebook_utils_abstraction import NotebookUtilsFactory
    from ingen_fab.python_libs.python.sql_templates import SQLTemplates
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
    from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils
    notebookutils = NotebookUtilsFactory.create_instance() 
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
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
failed_notebooks = []
start_time = datetime.now()

# Define execution function
def execute_notebook(notebook_name, index, total, timeout_seconds=3600):
    """Execute a single notebook and handle success/failure."""
    global success_count, failed_notebooks
    
    try:
        
        print(f"{'='*60}")
        print(f"Executing notebook {index}/{total}: {notebook_name}")
        print(f"{'='*60}")
        params = {}
        
        # Use notebook utils abstraction for cross-environment compatibility
        result = notebookutils.mssparkutils.notebook.run(
            notebook_name,
            timeout=timeout_seconds,
            params=params
        )
        
        if (result == 'success'):
            success_count += 1
            print(f"✓ Successfully executed: {notebook_name}")
            print(f"Exit value: {result}")
            return True
        else: 
            # Record the failure but continue execution to get full picture
            failed_notebooks.append({
                'name': notebook_name,
                'error': f"Notebook returned: {result}",
                'index': index
            })
            print(f"✗ Failed to execute: {notebook_name}")
            print(f"Result: {result}")
            return False

    except Exception as e:
        # Record the failure but continue execution to get full picture
        failed_notebooks.append({
            'name': notebook_name,
            'error': str(e),
            'index': index
        })
        print(f"✗ Failed to execute: {notebook_name}")
        print(f"Error: {str(e)}")
        return False

print(f"Starting orchestration for  warehouse")
print(f"Start time: {start_time}")
print(f"Total notebooks to execute: 1")
print("="*60)
execute_notebook("001_Initial_Creation_Config_WH_Warehouses_ddl_scripts", 1, 1)

# Final Summary
end_time = datetime.now()
duration = end_time - start_time
failed_count = 1 - success_count

print(f"{'='*60}")
print(f"Orchestration Complete!")
print(f"{'='*60}")
print(f"End time: {end_time}")
print(f"Duration: {duration}")
print(f"Total notebooks: 1")
print(f"Successfully executed: {success_count}")
print(f"Failed: {failed_count}")

# Show details of failed notebooks if any
if failed_notebooks:
    print(f"\n{'='*60}")
    print("FAILED NOTEBOOKS DETAILS:")
    print(f"{'='*60}")
    for i, failure in enumerate(failed_notebooks, 1):
        print(f"{i}. {failure['name']}")
        print(f"   Error: {failure['error']}")
        print()

if success_count == 1:
    print("\n✓ All notebooks executed successfully!")
    notebookutils.notebook.exit("success")
else:
    print(f"\n✗ Orchestration completed with {failed_count} failure(s)")
    # Exit with failure status - this will be caught by parent orchestrator as non-"success"
    error_summary = f"failed: {failed_count} of 1 notebooks failed"
    notebookutils.notebook.exit(error_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

