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



#lakehouse_name = "LH"  # name of your Lakehouse
#config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
#config_lakehouse_id = "a29c9d15-c24e-4779-8344-0c7c237b3990"

# Target for DDL scripts
target_lakehouse_config_prefix = "config"

# Fabric Configurations 
fabric_environment: str = "development" 
config_lakehouse_name = "LH1"  # name of your Lakehouse
config_workspace_id = "50fbcab0-7d56-46f7-90f6-80ceb00ac86d"
config_lakehouse_id = "f0121783-34cc-4e64-bfb5-6b44b1a7f04b"


# ONLY USE THIS IN DEV
full_reset = False # ⚠️ Full reset -- DESTRUCTIVE - will drop all tables






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

# ## Instantiate the Helper Classes

# CELL ********************



cu = config_utils(config_workspace_id,config_lakehouse_id)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Drop all Tables in Target LH

# CELL ********************



if full_reset == True:
    from ipywidgets import ToggleButtons, VBox, Label, Button
    from IPython.display import display as pdisplay
    from IPython.display import clear_output

    
    configs = cu.get_configs_as_object(fabric_environment)

    target_workspace_id = configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_workspace_id")
    target_lakehouse_id =  configs.get_attribute(f"{target_lakehouse_config_prefix.lower()}_lakehouse_id")
    
    # Build the widget
    prompt = Label("❗ This will drop all tables — do you want to proceed?")
    yesno = ToggleButtons(options=["No","Yes"], description="Confirm Destructive Action:")
    go = Button(description="Submit", button_style="warning")

    out = VBox([prompt, yesno, go])
    pdisplay(out)

    # Define what happens on click
    def on_click(b):
        clear_output()  # hide the widget after click
        if yesno.value == "Yes":
            print("Dropping tables…")
            lu = lakehouse_utils(target_workspace_id, target_lakehouse_id)
            lu.drop_all_tables()
        else:
            print("Operation cancelled.")

    go.on_click(on_click)





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run the lakehouse DDL Notebooks

# CELL ********************



# Import required libraries
from notebookutils import mssparkutils
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
        params = {
            "fabric_environment": fabric_environment,
            "config_workspace_id": config_workspace_id,
            "config_lakehouse_id": config_lakehouse_id,
            "target_lakehouse_config_prefix": target_lakehouse_config_prefix,
            'useRootDefaultLakehouse': True
        }
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
execute_notebook("001_Initial_Creation_Config_Warehouses_ddl_scripts", 1, 1)

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



