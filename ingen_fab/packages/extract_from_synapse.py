# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# CELL ********************


# MARKDOWN ********************

# ## 『』Parameters


# PARAMETERS CELL ********************



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
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
    from ingen_fab.python_libs.python.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )
    notebookutils = NotebookUtilsFactory.create_instance()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 🧪🧪 Testing Scripts Start


# CELL ********************

from rich.console import Console

from ingen_fab.python_libs.common.config_utils import get_configs_as_object, ConfigsObject
from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils
from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils

console = Console()  # Keep for tables but use regular print for most output


configs: ConfigsObject = get_configs_as_object()
config_lakehouse = lakehouse_utils(
    target_workspace_id=configs.edw_lakehouse_id,
    target_lakehouse_id=configs.edw_lakehouse_id
)


extract_config_df = config_lakehouse.read_table("config_synapse_extracts").to_pandas()


# Get datasource details for Synapse connection
datasource_name = extract_config_df.loc[0, "synapse_datasource_name"]
datasource_location = extract_config_df.loc[0, "synapse_datasource_location"]
pipeline_id = extract_config_df.loc[0, "pipeline_id"]
