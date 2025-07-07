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
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions

# CELL ********************



import sys

if "notebookutils" in sys.modules:
    import sys
    
    notebookutils.fs.mount("abfss://{{varlib:config_workspace_id}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_workspace_name}}.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
    new_Path = notebookutils.fs.getMountPath("/config_files")  # type: ignore # noqa: F821
    
    sys.path.insert(0, new_Path)
else:
    print("NotebookUtils not available, skipping config files mount.")
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )
    notebookutils = NotebookUtilsFactory.create_instance()





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Instantiate the Helper Classes

# CELL ********************





from ingen_fab.python_libs.common.config_utils import ConfigUtils
from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

target_lakehouse_config_prefix = "Config"

config_utils = ConfigUtils()
configs: ConfigUtils.ConfigsObject = config_utils.get_configs_as_object
config_lakehouse = lakehouse_utils(
    target_workspace_id=configs.edw_workspace_id,
    target_lakehouse_id=configs.edw_lakehouse_id
)




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run the lakehouse DDL Notebooks

# CELL ********************


mssparkutils = notebookutils.mssparkutils  # type: ignore # noqa: F821

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
        
        # Run the notebook
        result = mssparkutils.notebook.run(
            notebook_name,
            timeout_seconds
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
print(f"Total notebooks to execute: 1")
print("="*60)
execute_notebook("001_Initial_Creation_Config_Lakehouses_ddl_scripts", 1, 1)

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



