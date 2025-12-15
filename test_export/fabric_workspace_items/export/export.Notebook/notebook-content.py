# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark",
# META     "display_name": "Synapse PySpark"
# META   },
# META   "language_info": {
# META     "name": "python"
# META   }
# META }


# MARKDOWN ********************

# ## Parameters

# PARAMETERS CELL ********************

EXPORT_GROUP_NAME = None  # Optional: filter by export group (None = all groups)
EXPORT_NAME = None  # Optional: filter by export name within group (None = all exports)
EXECUTION_GROUP = None  # Optional: filter by execution group
EXECUTION_ID = None  # auto-UUID if None
IS_RETRY = False  # If True, only reprocess failed exports
MAX_CONCURRENCY = 4

# METADATA ********************

# META {
# META   "language": "python"
# META }

# MARKDOWN ********************

# ## Load Environment and Python Libraries

# CELL ********************

import logging
import sys
import traceback

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True
)

# Check if running in Fabric environment
if "notebookutils" in sys.modules:
    notebookutils.fs.mount("abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/", "/config_files")  # type: ignore # noqa: F821
    mount_path = notebookutils.fs.getMountPath("/config_files")  # type: ignore # noqa: F821
    run_mode = "fabric"
    sys.path.insert(0, mount_path)
else:
    print("NotebookUtils not available, assumed running in local mode.")
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )
    notebookutils = NotebookUtilsFactory.create_instance()
    spark = None
    mount_path = None
    run_mode = "local"


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
            print(f"Loading: {full_path}")
            code = notebookutils.fs.head(full_path, max_chars)
            exec(code, globals())  # Use globals() to share context across modules
            success_files.append(relative_path)
        except Exception as e:
            failed_files.append(relative_path)
            print(f"Error loading {relative_path}")
            print(f"   Error type: {type(e).__name__}")
            print(f"   Error message: {str(e)}")
            print(f"   Stack trace:")
            traceback.print_exc()

    print("\nSuccessfully loaded:")
    for f in success_files:
        print(f" - {f}")

    if failed_files:
        print("\nFailed to load:")
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
        print("Module cache cleared for ingen_fab libraries")
    else:
        print("No ingen_fab modules found in cache - already cleared or first load")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Configuration

# CELL ********************

if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import get_configs_as_object, ConfigsObject
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.get_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/common/fsspec_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        "ingen_fab/python_libs/common/export_resource_config_schema.py",
        # Export common
        "ingen_fab/python_libs/pyspark/export/common/constants.py",
        "ingen_fab/python_libs/pyspark/export/common/exceptions.py",
        "ingen_fab/python_libs/pyspark/export/common/config.py",
        "ingen_fab/python_libs/pyspark/export/common/results.py",
        "ingen_fab/python_libs/pyspark/export/common/config_manager.py",
        # Export components
        "ingen_fab/python_libs/pyspark/export/export_logger.py",
        "ingen_fab/python_libs/pyspark/export/exporters/base_exporter.py",
        "ingen_fab/python_libs/pyspark/export/exporters/lakehouse_exporter.py",
        "ingen_fab/python_libs/pyspark/export/exporters/warehouse_exporter.py",
        "ingen_fab/python_libs/pyspark/export/writers/file_writer.py",
        "ingen_fab/python_libs/pyspark/export/export_orchestrator.py",
    ]
    load_python_modules_from_path(mount_path, files_to_load)


# Initialize configuration
configs: ConfigsObject = get_configs_as_object()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Initialize

# CELL ********************

config_lakehouse = lakehouse_utils(
    target_workspace_name=configs.fabric_workspace_name,
    target_lakehouse_name=configs.config_lakehouse_name,
    spark=spark,
)

config_mgr = ConfigExportManager(
    config_lakehouse=config_lakehouse,
    spark=spark
)

export_logger = ExportLogger(config_lakehouse, auto_create_tables=True)
export_orchestrator = ExportOrchestrator(
    spark=spark,
    export_logger=export_logger,
    max_concurrency=MAX_CONCURRENCY
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load Export Configs

# CELL ********************

export_configs = config_mgr.get_configs(
    export_group_name=EXPORT_GROUP_NAME,
    export_name=EXPORT_NAME,
    execution_group=EXECUTION_GROUP
)

print(f"Loaded {len(export_configs)} export configuration(s)")
for cfg in export_configs:
    print(f"  - {cfg.export_group_name}/{cfg.export_name} (execution_group: {cfg.execution_group})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Exports

# CELL ********************

import uuid
execution_id = EXECUTION_ID if EXECUTION_ID else str(uuid.uuid4())
results = export_orchestrator.process_exports(
    configs=export_configs,
    execution_id=execution_id,
    is_retry=IS_RETRY
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check Results

# CELL ********************

# Extract warnings from results
warnings = [
    {
        "export_name": r.get('export_name'),
        "message": r.get('error_message')
    }
    for r in results.get('results', [])
    if r.get('status') == 'warning'
]

# Check if export failed
if not results.get('success'):
    failed_exports = [r.get('export_name') for r in results.get('results', []) if r.get('status') == 'error']
    raise Exception(f"Export failed for: {failed_exports}. Execution ID: {execution_id}")

print(f"Export completed successfully!")
print(f"  Total: {results['total_exports']}")
print(f"  Successful: {results['successful']}")
print(f"  Failed: {results['failed']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Exit Notebook

# CELL ********************

import json

summary = {
    "execution_id": execution_id,
    "success": results["success"],
    "total_exports": results["total_exports"],
    "successful": results["successful"],
    "failed": results["failed"],
    "warnings": warnings
}

notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }