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
MASTER_EXECUTION_ID = None  # auto-UUID if None
WORK_ITEMS_JSON     = None  # JSON-encoded list for historical mode
MAX_CONCURRENCY     = 10
INCLUDE_SNAPSHOTS   = True  # sets whether to include snapshot tables in the extraction
DAILY_RUN_START_DATE = None  # will be set to current date - 4 days
DAILY_RUN_END_DATE   = None  # will be set to current date



# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔄 Synapse Sync Processor

# CELL ********************


# This notebook orchestrates extraction of data from Azure Synapse Analytics to Parquet files stored in ADLS.
# 
# The Synapse extraction process automates the extraction of data from Azure Synapse Analytics to Parquet files stored in ADLS. It supports two primary modes:
# 
# 1. **Daily Extraction** - Runs for a specified date range (4-day sliding window) for incremental extracts whilst capturing a snapshot of all other objects
# 2. **Historical Extraction** - Processes specific tables/dates as defined in JSON input parameter `WORK_ITEMS_JSON`
# 
# The process leverages Microsoft Fabric's REST API to trigger pipelines that execute CETAS (CREATE EXTERNAL TABLE AS SELECT) operations in Synapse.




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "python"
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

# ## 🔧 Load Configuration and Initialize

# CELL ********************

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "python"
# META }

# MARKDOWN ********************

# ## 🔧 Load Configuration and Initialize Utilities

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import get_configs_as_object, ConfigsObject
    from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils
    
    from ingen_fab.python_libs.python.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.get_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/python/lakehouse_utils.py",
        
        "ingen_fab/python_libs/python/notebook_utils_abstraction.py"
    ]
    load_python_modules_from_path(mount_path, files_to_load)


# Initialize configuration
configs: ConfigsObject = get_configs_as_object()



# Additional imports for synapse sync
import asyncio
import nest_asyncio
import json
import logging
import uuid
import time
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any

# Third-party imports
import sempy.fabric as fabric
import numpy as np
import pyarrow as pa
from delta.tables import DeltaTable

import pyspark.sql.functions as F
from pyspark.sql.types import *

# ## Configure logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ## Define parameters

MASTER_EXECUTION_ID = None  # auto-UUID if None
WORK_ITEMS_JSON     = None  # JSON-encoded list for historical mode
MAX_CONCURRENCY     = 10

INCLUDE_SNAPSHOTS   = True  # sets whether to include snapshot tables in the extraction
DAILY_RUN_START_DATE : date = (datetime.utcnow() - timedelta(days=4)).date()
DAILY_RUN_END_DATE : date = datetime.utcnow().date()

# ## Define constants

POLL_INTERVAL    = 30
FAST_RETRY_SEC   = 3
TRANSIENT_HTTP   = {429, 500, 503, 504, 408}

MASTER_EXECUTION_PARAMETERS = {
    "start_date": {
        "year": DAILY_RUN_START_DATE.year,
        "month": DAILY_RUN_START_DATE.month,
        "day": DAILY_RUN_START_DATE.day
    },
    "end_date": {
        "year": DAILY_RUN_END_DATE.year,
        "month": DAILY_RUN_END_DATE.month,
        "day": DAILY_RUN_END_DATE.day
    }
}

TRIGGER_TYPE = "Manual"

# ## Initialize utilities from python libs

# Get lakehouse utilities
lakehouse = lakehouse_utils(spark=spark, lakehouse_id=lakehouse_id, workspace_id=workspace_id)

# Check if DDL tables exist
if not lakehouse.check_if_table_exists("config_synapse_extract_objects"):
    logger.error("Configuration table 'config_synapse_extract_objects' not found. Please run DDL scripts first.")
    raise ValueError("Configuration table not found")

if not lakehouse.check_if_table_exists("log_synapse_extract_run_log"):
    logger.error("Log table 'log_synapse_extract_run_log' not found. Please run DDL scripts first.")
    raise ValueError("Log table not found")

# ## Load Synapse-Specific Libraries

# Load pipeline utils and synapse-specific libraries
if run_mode == "local":
    from ingen_fab.python_libs.python.pipeline_utils import PipelineUtils
    from ingen_fab.python_libs.python.synapse_extract_utils import SynapseExtractUtils
    from ingen_fab.python_libs.python.synapse_orchestrator import SynapseOrchestrator
else:
    synapse_files_to_load = [
        "ingen_fab/python_libs/python/pipeline_utils.py",
        "ingen_fab/python_libs/python/synapse_extract_utils.py",
        "ingen_fab/python_libs/python/synapse_orchestrator.py"
    ]
    load_python_modules_from_path(mount_path, synapse_files_to_load)

# ## Import Helper Functions from Libraries

# Import the helper functions from synapse_extract_utils
if run_mode == "local":
    from ingen_fab.python_libs.python.synapse_extract_utils import (
        build_path_components,
        build_partition_clause, 
        create_sql_script,
        enrich_work_item,
        prepare_extract_payloads
    )
else:
    # Functions are already loaded via load_python_modules_from_path above
    pass

# ## Initialize Synapse Extract Configuration

# Read configuration from the synapse extract objects table
synapse_extract_objects_df = lakehouse.read_table("config_synapse_extract_objects").filter("active_yn = 'Y'")

# Get data source and pipeline configuration
synapse_config_row = synapse_extract_objects_df.limit(1).collect()[0]
synapse_datasource_name = synapse_config_row["synapse_datasource_name"]
synapse_datasource_location = synapse_config_row["synapse_datasource_location"]
synapse_extract_pipeline_id = synapse_config_row["pipeline_id"]

logger.info(f"Datasource: {synapse_datasource_name}")
logger.info(f"Datasource Location: {synapse_datasource_location}")
logger.info(f"Pipeline ID: {synapse_extract_pipeline_id}")

synapse_extract_utils = SynapseExtractUtils(
    datasource_name=synapse_datasource_name,
    datasource_location=synapse_datasource_location,
    lakehouse_util=lakehouse
)

# ## Construct extract work items

if WORK_ITEMS_JSON:
    # Historical extraction mode
    work_items: List[Dict] = json.loads(WORK_ITEMS_JSON)
    master_execution_id = MASTER_EXECUTION_ID or work_items[0]["master_execution_id"]
    mode = "Historical"

    logger.info(f"HISTORICAL EXTRACTION MODE - Master Execution ID: {master_execution_id}")
    logger.info(f"Tables to process: {len(work_items)}")
    
    # Add the mode to each work item if not already present
    for item in work_items:
        item["mode"] = "Historical"

        for field in ["extract_start", "extract_end", "single_date_filter", "date_range_filter", "partition_clause"]:
            if field in item and item[field] == "None":
                item[field] = None
    
    # Filter out snapshots if INCLUDE_SNAPSHOTS is False
    if not INCLUDE_SNAPSHOTS:
        original_count = len(work_items)
        work_items = [item for item in work_items if item["extract_mode"] != "snapshot"]
        filtered_count = original_count - len(work_items)
        if filtered_count > 0:
            logger.info(f"Excluded {filtered_count} snapshot tables based on INCLUDE_SNAPSHOTS=False")
else:
    # Daily extraction
    master_execution_id = MASTER_EXECUTION_ID or str(uuid.uuid4())

    logger.info(f"DAILY EXTRACTION MODE - Master Execution ID: {master_execution_id}")
    logger.info(f"Date Range: {DAILY_RUN_START_DATE} to {DAILY_RUN_END_DATE}")

    # Create extract work items for each active table in the objects table
    table_configs = synapse_extract_objects_df.collect()

    # Filter out snapshots if INCLUDE_SNAPSHOTS is False
    if not INCLUDE_SNAPSHOTS:
        original_count = len(table_configs)
        table_configs = [config for config in table_configs if config["extract_mode"] != "snapshot"]
        filtered_count = original_count - len(table_configs)
        if filtered_count > 0:
            logger.info(f"Excluded {filtered_count} snapshot tables based on INCLUDE_SNAPSHOTS=False")
    
    # Create work items from the filtered table configs
    work_items = [
        dict(
            master_execution_id = master_execution_id,
            execution_group     = table_config["execution_group"],
            source_schema       = table_config["source_schema_name"],
            source_table        = table_config["source_table_name"],
            single_date_filter  = table_config["single_date_filter"] if "single_date_filter" in table_config else None,
            date_range_filter   = table_config["date_range_filter"] if "date_range_filter" in table_config else None,
            extract_mode        = table_config["extract_mode"],
            extract_start       = str(DAILY_RUN_START_DATE) if table_config["extract_mode"] == "incremental" else None,
            extract_end         = str(DAILY_RUN_END_DATE) if table_config["extract_mode"] == "incremental" else None,
            mode                = "Daily"
        )
        for table_config in table_configs
    ]
    mode = "Daily"

# Get the SQL template from the SynapseExtractUtils
sql_template = synapse_extract_utils.get_extract_sql_template()

# Prepare extract payloads for all work items
extraction_payloads = prepare_extract_payloads(work_items, sql_template)

# Sort extraction payloads by execution_group to ensure they are processed in order
extraction_payloads.sort(key=lambda x: x.get('execution_group', float('inf')))
logger.info(f"Extraction payloads sorted by execution_group for ordered processing")

# ## Orchestrate extraction using simplified modular approach

# Initialize the orchestrator with PipelineUtils integration
orchestrator = SynapseOrchestrator(
    synapse_extract_utils=synapse_extract_utils,
    workspace_id=workspace_id,
    pipeline_id=synapse_extract_pipeline_id,
    max_concurrency=MAX_CONCURRENCY
)

def run_async_orchestration():
    """
    Run the complete extraction orchestration process using the modular orchestrator.
    
    Returns:
        Dict: Extraction summary with execution statistics
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        nest_asyncio.apply()

    # Run orchestration using the modular orchestrator
    return loop.run_until_complete(
        orchestrator.orchestrate_extractions(
            extraction_payloads=extraction_payloads,
            master_execution_id=master_execution_id,
            master_execution_parameters=MASTER_EXECUTION_PARAMETERS,
            trigger_type=TRIGGER_TYPE
        )
    )

# Start extraction orchestration process
logger.info("Starting extraction orchestration process")
summary = run_async_orchestration()
logger.info("Extraction orchestration process completed")

# Display summary
logger.info(f"EXTRACTION SUMMARY - Master Execution ID: {summary['master_execution_id']}")
logger.info(f"Mode: {summary['extraction_mode']} • Completed: {summary['completion_time']}")
logger.info(f"Total: {summary['total_tables']}, Succeeded: {summary['successful_extractions']}, "
            f"Failed: {summary['failed_extractions']}, Skipped: {summary['skipped_extractions']}")
logger.info(f"Success Rate: {summary['success_rate']}")

if summary['failed_extractions'] > 0:
    logger.warning(f"FAILED TASKS DETAILS: {summary['failed_extractions']} extractions failed")
    
    for i, task in enumerate(summary['failed_details'], 1):
        if task.get('error'):
            logger.error(f"Failed extraction {i}: {task['table']} - Status: {task['status']} - Error: {task['error']}")
        else:
            logger.error(f"Failed extraction {i}: {task['table']} - Status: {task['status']}")
    
    # If high failure rate, log warning
    if summary['failed_extractions'] / summary['total_tables'] > 0.25:
        logger.warning(f"HIGH FAILURE RATE DETECTED! Success rate: {summary['success_rate']}")
        logger.warning("Review failed tasks and consider retrying.")
else:
    logger.info("All extractions completed successfully!")