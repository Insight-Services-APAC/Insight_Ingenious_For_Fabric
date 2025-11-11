# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark",
# META     "display_name": "PySpark (Synapse)"
# META   },
# META   "language_info": {
# META     "name": "python",
# META     "language_group": "synapse_pyspark"
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
# META   "language_group": "synapse_pyspark"
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

    
    # PySpark environment - spark session should be available
    
else:
    print("NotebookUtils not available, assumed running in local mode.")
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
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
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    from ingen_fab.python_libs.python.sql_templates import SQLTemplates
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
    from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils
    notebookutils = NotebookUtilsFactory.create_instance() 
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        "ingen_fab/python_libs/pyspark/ddl_utils.py",
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py",
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

# ## 🏃‍♂️‍➡️ Run All Warehouse Orchestrators in Parallel

# CELL ********************



# Define the warehouses and their orchestrators
warehouses_to_run = [
    {'name': 'wh_gold', 'orchestrator': '00_orchestrator_wh_gold_warehouse_ddl_scripts'},
]


# Import required libraries
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time

# Initialize variables
start_time = datetime.now()
results = {}

print(f"Starting parallel orchestration for all warehouses")
print(f"Start time: {start_time}")

print(f"Total warehouses to process: 1")
print("="*60)


# Define function to run a single warehouse orchestrator
def run_warehouse_orchestrator(warehouse_name, orchestrator_name, stagger_delay=0):
    """Run a single warehouse orchestrator and return the result."""
    result = {
        'warehouse': warehouse_name,
        'orchestrator': orchestrator_name,
        'orchestrator_path': f"ddl_scripts/Warehouses/{warehouse_name}/{orchestrator_name}.Notebook/notebook-content.py",
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None,
        'status': 'Running',
        'error': None,
        'exit_value': None
    }
    
    try:
        # Add staggered delay to avoid concurrent update issues
        if stagger_delay > 0:
            print(f"[{result['start_time']}] Waiting {stagger_delay:.1f}s before starting {warehouse_name} (staggered execution)")
            time.sleep(stagger_delay)
            
        print(f"[{result['start_time']}] Starting orchestrator for {warehouse_name}")
        
        params = {}

        # Run the warehouse orchestrator using mssparkutils
        notebook_result = notebookutils.mssparkutils.notebook.run(
            f"{orchestrator_name}",
            7200,  # 2 hour timeout per warehouse
            params
        )
        
        result['end_time'] = datetime.now()
        result['duration'] = result['end_time'] - result['start_time']
        if notebook_result == 'success':
            result['status'] = 'Success'
        else:
            raise Exception(f"Notebook returned unexpected result: {notebook_result}")
        
        print(f"[{result['end_time']}] ✓ Completed {warehouse_name} in {result['duration']}")
        
    except Exception as e:
        result['end_time'] = datetime.now()
        result['duration'] = result['end_time'] - result['start_time']
        result['status'] = 'Failed'
        result['error'] = str(e)
        
        print(f"[{result['end_time']}] ✗ Failed {warehouse_name} after {result['duration']}")
        print(f"  Error: {result['error']}")
    
    return result


# Execute all warehouse orchestrators in parallel
print("\nStarting parallel execution of warehouse orchestrators...")
print("="*60)


# Run orchestrators in parallel using ThreadPoolExecutor with staggered execution
with ThreadPoolExecutor(max_workers=1) as executor:
    # Submit all tasks with staggered delays (0.2 seconds apart)
    future_to_warehouse = {}
    for i, warehouse in enumerate(warehouses_to_run):
        stagger_delay = i * 0.2  # 0.2-second stagger between executions
        future = executor.submit(
            run_warehouse_orchestrator, 
            warehouse['name'], 
            warehouse['orchestrator'],
            stagger_delay
        )
        future_to_warehouse[future] = warehouse['name']
    
    # Process completed tasks as they finish
    for future in as_completed(future_to_warehouse):
        warehouse_name = future_to_warehouse[future]
        try:
            result = future.result()
            results[warehouse_name] = result
        except Exception as exc:
            print(f'Warehouse {warehouse_name} generated an exception: {exc}')
            results[warehouse_name] = {
                'warehouse': warehouse_name,
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
print(f"Total warehouses: 1")

# Count results
success_count = sum(1 for r in results.values() if r['status'] == 'Success')
failed_count = sum(1 for r in results.values() if r['status'] == 'Failed')
exception_count = sum(1 for r in results.values() if r['status'] == 'Exception')

print(f"Successful: {success_count}")
print(f"Failed: {failed_count}")
print(f"Exceptions: {exception_count}")
print("\n" + "-"*60)


# Detailed results for each warehouse
print("\nDETAILED RESULTS BY WAREHOUSE:")
print("="*60)

for warehouse_name in sorted(results.keys()):
    result = results[warehouse_name]
    print(f"\n{warehouse_name}:")
    print(f"  Status: {result['status']}")
    
    if 'duration' in result and result['duration']:
        print(f"  Duration: {result['duration']}")
    
    if result['status'] == 'Success' and 'exit_value' in result:
        print(f"  Exit value: {result['exit_value']}")
    elif result['status'] in ['Failed', 'Exception'] and 'error' in result:
        print(f"  Error: {result['error']}")


# Create a summary table using markdown
summary_data = []
for warehouse_name in sorted(results.keys()):
    result = results[warehouse_name]
    status_icon = "✓" if result['status'] == 'Success' else "✗"
    duration_str = str(result.get('duration', 'N/A'))
    orchestrator_path = result.get('orchestrator_path', 'N/A')
    summary_data.append(f"| {warehouse_name} | {status_icon} {result['status']} | {duration_str} | {orchestrator_path} |")

markdown_table = f"""
## Execution Summary Table

| Warehouse | Status | Duration | Orchestrator Path |
|-----------|--------|----------|-------------------|
{chr(10).join(summary_data)}

**Total Execution Time:** {total_duration}
"""

print(markdown_table)


# Final status and exit
if failed_count == 0 and exception_count == 0:
    final_message = f"✓ All {success_count} warehouses processed successfully!"
    print(f"\n{final_message}")
    notebookutils.notebook.exit(final_message)
else:
    final_message = f"Completed with {failed_count + exception_count} failures out of 1 warehouses"
    print(f"\n✗ {final_message}")
    notebookutils.notebook.exit(final_message)
    raise Exception(final_message)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

