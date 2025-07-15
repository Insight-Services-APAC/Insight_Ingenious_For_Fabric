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

# ## 🏃‍♂️‍➡️ Run All Lakehouse Orchestrators in Parallel

# CELL ********************



# Define the lakehouses and their orchestrators
lakehouses_to_run = [
    {'name': 'Config', 'orchestrator': '0_orchestrator_Config_warehouse_ddl_scripts'},
]


# Import required libraries
from notebookutils import mssparkutils
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time

# Initialize variables
start_time = datetime.now()
results = {}

print(f"Starting parallel orchestration for all lakehouses")
print(f"Start time: {start_time}")

print(f"Total lakehouses to process: 1")
print("="*60)


# Define function to run a single lakehouse orchestrator
def run_lakehouse_orchestrator(lakehouse_name, orchestrator_name):
    """Run a single lakehouse orchestrator and return the result."""
    result = {
        'lakehouse': lakehouse_name,
        'orchestrator': orchestrator_name,
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None,
        'status': 'Running',
        'error': None,
        'exit_value': None
    }
    
    try:
        print(f"[{result['start_time']}] Starting orchestrator for {lakehouse_name}")
        
        params = {
            "fabric_environment": fabric_environment,
            "config_workspace_id": config_workspace_id,
            "config_lakehouse_id": config_lakehouse_id,
            "target_lakehouse_config_prefix": f"{lakehouse_name}",
            "full_reset": full_reset
        }

        # Run the lakehouse orchestrator
        notebook_result = mssparkutils.notebook.run(
            f"{orchestrator_name}_Lakehouses_ddl_scripts",
            timeout_seconds=7200,  # 2 hour timeout per lakehouse
            arguments=params
        )
        
        result['end_time'] = datetime.now()
        result['duration'] = result['end_time'] - result['start_time']
        if notebook_result == 'success':
            result['status'] = 'Success'
        else:
            raise Exception(f"Notebook returned unexpected result: {notebook_result}")
        
        print(f"[{result['end_time']}] ✓ Completed {lakehouse_name} in {result['duration']}")
        
    except Exception as e:
        result['end_time'] = datetime.now()
        result['duration'] = result['end_time'] - result['start_time']
        result['status'] = 'Failed'
        result['error'] = str(e)
        
        print(f"[{result['end_time']}] ✗ Failed {lakehouse_name} after {result['duration']}")
        print(f"  Error: {result['error']}")
    
    return result


# Execute all lakehouse orchestrators in parallel
print("\nStarting parallel execution of lakehouse orchestrators...")
print("="*60)


# Run orchestrators in parallel using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=1) as executor:
    # Submit all tasks
    future_to_lakehouse = {
        executor.submit(
            run_lakehouse_orchestrator, 
            lakehouse['name'], 
            lakehouse['orchestrator']
        ): lakehouse['name'] 
        for lakehouse in lakehouses_to_run
    }
    
    # Process completed tasks as they finish
    for future in as_completed(future_to_lakehouse):
        lakehouse_name = future_to_lakehouse[future]
        try:
            result = future.result()
            results[lakehouse_name] = result
        except Exception as exc:
            print(f'Lakehouse {lakehouse_name} generated an exception: {exc}')
            results[lakehouse_name] = {
                'lakehouse': lakehouse_name,
                'status': 'Exception',
                'error': str(exc)
            }


# Generate detailed summary report
end_time = datetime.now()
total_duration = end_time - start_time

print("\n" + "="*60)
print("ORCHESTRATION SUMMARY REPORT")
print("="*60)
print(f"Total execution time: {total_duration}")
print(f"Total lakehouses: 1")

# Count results
success_count = sum(1 for r in results.values() if r['status'] == 'Success')
failed_count = sum(1 for r in results.values() if r['status'] == 'Failed')
exception_count = sum(1 for r in results.values() if r['status'] == 'Exception')

print(f"Successful: {success_count}")
print(f"Failed: {failed_count}")
print(f"Exceptions: {exception_count}")
print("\n" + "-"*60)


# Detailed results for each lakehouse
print("\nDETAILED RESULTS BY LAKEHOUSE:")
print("="*60)

for lakehouse_name in sorted(results.keys()):
    result = results[lakehouse_name]
    print(f"\n{lakehouse_name}:")
    print(f"  Status: {result['status']}")
    
    if 'duration' in result and result['duration']:
        print(f"  Duration: {result['duration']}")
    
    if result['status'] == 'Success' and 'exit_value' in result:
        print(f"  Exit value: {result['exit_value']}")
    elif result['status'] in ['Failed', 'Exception'] and 'error' in result:
        print(f"  Error: {result['error']}")


# Create a summary table using markdown
summary_data = []
for lakehouse_name in sorted(results.keys()):
    result = results[lakehouse_name]
    status_icon = "✓" if result['status'] == 'Success' else "✗"
    duration_str = str(result.get('duration', 'N/A'))
    summary_data.append(f"| {lakehouse_name} | {status_icon} {result['status']} | {duration_str} |")

markdown_table = f"""
## Execution Summary Table

| Lakehouse | Status | Duration |
|-----------|--------|----------|
{''.join(summary_data)}

**Total Execution Time:** {total_duration}
"""

print(markdown_table)


# Final status and exit
if failed_count == 0 and exception_count == 0:
    final_message = f"✓ All {success_count} lakehouses processed successfully!"
    print(f"\n{final_message}")
    mssparkutils.notebook.exit(final_message)
else:
    final_message = f"Completed with {failed_count + exception_count} failures out of 1 lakehouses"
    print(f"\n✗ {final_message}")
    mssparkutils.notebook.exit(final_message)
    raise Exception(final_message)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

