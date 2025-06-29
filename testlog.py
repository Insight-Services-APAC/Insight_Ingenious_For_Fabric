
from fabric_cicd import (
    FabricWorkspace,
    publish_all_items,
)

pu = FabricWorkspace(
    workspace_id="3a4fc13c-f7c5-463e-a9de-57c4754699ff",
    repository_directory="sample_project/fabric_workspace_items",
    items_to_include=["var_lib"],
    item_type_in_scope=[
        "VariableLibrary",
        "DataPipeline",
        "Environment",
        "Notebook",
        "Report",
        "SemanticModel",
        "Lakehouse",
        "MirroredDatabase",
        "CopyJob",
        "Eventhouse",
        "Reflex",
        "Eventstream",
        "Warehouse",
        "SQLDatabase",
    ],
    environment="development",
)

status_entries = publish_all_items(pu)


# status_entries = fabric_cicd_log.read_log_status_entries("fabric_cicd.error.log")
# fabric_cicd_log.print_status_summary(status_entries)
