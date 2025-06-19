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


# Define the lakehouses and their orchestrators
lakehouses_to_run = [
    {'name': 'Config', 'orchestrator': '0_orchestrator_Config'},
]

# Fabric Configurations 
fabric_environment: str = "development" 
config_lakehouse_name = "LH1"  # name of your Lakehouse
config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
config_lakehouse_id = "f0121783-34cc-4e64-bfb5-6b44b1a7f04b"
edw_workspace_id = '50fbcab0-7d56-46f7-90f6-80ceb00ac86d'
edw_warehouse_id = '8a26f137-fd0b-47f6-92e1-e4551593c751'
edw_warehouse_name = 'WH'
edw_lakehouse_id = '8a26f137-fd0b-47f6-92e1-e4551593c751'
edw_lakehouse_name = 'LH'
legacy_synapse_connection_name = 'synapse_connection'
synapse_export_shortcut_path_in_onelake = 'exports/'
full_reset = False  




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions

# CELL ********************








# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Insert the Configs Into The Configuration Table

# CELL ********************



cu = config_utils(config_workspace_id,config_lakehouse_id)

cr = config_utils.FabricConfig(
    fabric_environment,
    config_workspace_id,
    config_lakehouse_id,
    edw_workspace_id,
    edw_warehouse_id,
    edw_warehouse_name,    
    edw_lakehouse_id,
    edw_lakehouse_name,
    legacy_synapse_connection_name,
    synapse_export_shortcut_path_in_onelake,
    full_reset
)

cu.merge_config_record(cr)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run All Lakehouse Orchestrators in Parallel

# CELL ********************




# Import required libraries
from notebookutils import mssparkutils
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time

# Initialize variables
workspace_id = mssparkutils.runtime.context.get("currentWorkspaceId")
start_time = datetime.now()
results = {}

print(f"Starting parallel orchestration for all lakehouses")
print(f"Start time: {start_time}")
print(f"Workspace ID: {workspace_id}")
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



