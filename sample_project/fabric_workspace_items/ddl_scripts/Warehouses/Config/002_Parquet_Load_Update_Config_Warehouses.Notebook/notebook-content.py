# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META }

# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python",
# META }

# MARKDOWN ********************

# ## 📦 Inject Reusable Classes and Functions


# CELL ********************


import sys


if "notebookutils" in sys.modules:
    import sys
    
    notebookutils.fs.mount("abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_lakehouse_name}}.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
    mount_path = notebookutils.fs.getMountPath("/config_files")  # type: ignore # noqa: F821
    
    run_mode = "fabric"
    sys.path.insert(0, mount_path)

    
    spark = None # Assuming Python mode does not require a Spark session
    
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

# ## 🗂️ Now Load the Custom Python Libraries

# CELL ********************



if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils.py import *
    from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.python.ddl_utils import ddl_utils
    from ingen_fab.python_libs.python.notebook_utils_abstraction import notebookutils
    from ingen_fab.python_libs.python.sql_templates import sql_templates
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils
    from ingen_fab.python_libs.python.pipeline_utils import pipeline_utils 
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
# META   "language": "python",
# META   "language_group": "{language group | required}"
# META }

# MARKDOWN ********************

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





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "{language group | required}"
# META }

# MARKDOWN ********************

# ## 🏃‍♂️‍➡️ Run DDL Cells 

# CELL ********************



# DDL cells are injected below:



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 001_parquet_load_upate.sql

# CELL ********************

guid = "de43b7e48326"
def work():
    sql = """



    """

    wu.execute_query(wu.get_connection(), sql)

du.run_once(work,"001_parquet_load_upate", guid)







# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "{language group | required}"
# META }

# MARKDOWN ********************

# ## 📇 Print the execution log

# CELL ********************



du.print_log() 




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "{language group | required}"
# META }

# MARKDOWN ********************

# ## ✔️ If we make it to the end return a successful result

# CELL ********************



import sys
sys.exit("success")



