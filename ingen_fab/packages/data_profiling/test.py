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




# MARKDOWN ********************

# ## 『』Parameters

# PARAMETERS CELL ********************




# Tiered Profiling Scan Level Control
# The TieredProfiler uses 4 progressive scan levels:
# Level 1: Discovery - Table metadata and basic stats
# Level 2: Schema - Column metadata and data types  
# Level 3: Profile - Detailed column statistics and distributions
# Level 4: Advanced - Cross-column correlations and relationship discovery
scan_levels = [1, 2, 3, 4]  # Which scan levels to run (default: all 4 levels)
max_scan_level = 4  # Maximum scan level to execute (1-4)
min_scan_level = 1  # Minimum scan level to execute (1-4)

# Incremental scanning parameters
enable_incremental = True  # Skip tables already scanned at requested levels
resume_from_level = None  # Resume from specific level (None = auto-detect)
force_rescan = True  # Force rescan even if results exist
only_scan_new_tables = False  # Only scan tables not in metadata

# Original parameters (for backward compatibility with profile_dataset method)
profile_type = "full"  # full_with_relationships triggers Level 4
save_to_catalog = True  # Whether to save results to catalog tables
generate_report = True  # Whether to generate HTML/Markdown report
generate_yaml_output = True  # Whether to generate YAML output files
output_format = "yaml"  # Report format: yaml, html, markdown, json
sample_size = None  # Sample fraction for Level 3+ (0-1) or None for full
target_tables = []  # List of specific tables to profile, empty for all

# Performance and filtering parameters
exclude_views = True  # Exclude views from profiling (recommended)
max_tables_per_batch = 10  # Process tables in batches for large workspaces



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 📊 Data Profiling Notebook (Lakehouse)

# CELL ********************



# This notebook profiles datasets and generates data quality reports based on configuration metadata.
# Uses modularized components from python_libs for maintainable and reusable code.




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
    
    if not mount_path:
        raise Exception("Error mounting python libraries. Mount path for config lakehouse not found - ensure lakehouse is provisioned and accessible")
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
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 🔧 Load Configuration and Initialize

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔧 Load Configuration and Initialize Utilities

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import get_configs_as_object, ConfigsObject
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.packages.data_profiling.runtime.profilers.tiered import tiered_profiler
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import NotebookUtilsFactory
    notebookutils = NotebookUtilsFactory.get_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py"
    ]
    load_python_modules_from_path(mount_path, files_to_load)


# Initialize configuration
configs: ConfigsObject = get_configs_as_object()



# Additional imports for data profiling
import json
import time
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from ingen_fab.packages.data_profiling.runtime.core.enums import ProfileType
from ingen_fab.packages.data_profiling.runtime.profilers.tiered.tiered_profiler import (
    TieredProfiler,
)

print("✅ Using new restructured imports")
USE_NEW_STRUCTURE = True

# Import cross-profile analyzer for relationship discovery
try:
    from ingen_fab.python_libs.common.cross_profile_analyzer import CrossProfileAnalyzer
    cross_profile_available = True
except ImportError:
    cross_profile_available = False
    print("⚠️ Cross-profile analyzer not available - relationship discovery limited to single tables")


from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


execution_id = str(uuid.uuid4())

print(f"Execution ID: {execution_id}")
print(f"Profile Type: {profile_type}")
print(f"Save to Catalog: {save_to_catalog}")
print(f"Generate Report: {generate_report}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ## 🚀 Main Execution

# CELL ********************




print("=" * 80)
print(f"Data Profiling Processor - Lakehouse")
print(f"Execution Time: {datetime.now()}")
print("=" * 80)

# Initialize lakehouse utils (it creates its own spark session)
config_lakehouse = lakehouse_utils(
    target_workspace_id=configs.config_workspace_id,
    target_lakehouse_id=configs.config_lakehouse_id
)

target_lakehouse = lakehouse_utils(
    target_workspace_id=configs.config_workspace_id,
    target_lakehouse_id=configs.config_lakehouse_id
)

#drop all tables in target lakehouse starting with tiered_profile_
tables_to_drop:list[str] = target_lakehouse.list_tables()
for table in tables_to_drop:
    if table.startswith("tiered_profile_"):
        print(f"Dropping table: {table}")
        target_lakehouse.spark.sql(f"DROP TABLE IF EXISTS {table}")

# Map profile type string to enum
profile_type_map = {
    "basic": ProfileType.BASIC,
    "statistical": ProfileType.STATISTICAL,
    "data_quality": ProfileType.DATA_QUALITY,
    "relationship": ProfileType.RELATIONSHIP,
    "full": ProfileType.FULL
}
selected_profile_type = profile_type_map.get(profile_type.lower(), ProfileType.FULL)

# Initialize TieredProfiler (the only profiler available)
print("⚡ Using TieredProfiler - progressive multi-level profiling for optimal performance")
# TieredProfiler already imported above
profiler = TieredProfiler(
    config_lakehouse=config_lakehouse, 
    target_lakehouse=target_lakehouse,
    spark=config_lakehouse.spark,
    exclude_views=exclude_views,
    force_rescan=force_rescan
)

# Determine which scan levels to run based on parameters
if isinstance(scan_levels, str):
    scan_levels = eval(scan_levels)  # Convert string "[1,2,3,4]" to list
levels_to_run = [level for level in scan_levels if min_scan_level <= level <= max_scan_level]

print(f"\n📊 Scan Level Configuration:")
print(f"  • Levels to run: {levels_to_run}")
print(f"  • Incremental scanning: {'Enabled' if enable_incremental else 'Disabled'}")
print(f"  • Force rescan: {'Yes' if force_rescan else 'No'}")
print(f"  • Sample size for Level 3+: {sample_size if sample_size else 'Full dataset'}")

# Get list of tables to profile
tables_to_profile =profiler.table_discovery.discover_tables_to_profile()

# Run progressive scan levels
execution_id = str(uuid.uuid4())
run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

print(f"\n🚀 Starting TieredProfiler Progressive Scanning")
print(f"   Execution ID: {execution_id}")
print(f"   Tables to process: {len(tables_to_profile)}")
print(f"   Timestamp: {run_timestamp}")

for table in tables_to_profile:
    profile_l3 = profiler.profile_dataset(
        dataset=table,
        profile_type=ProfileType.FULL  # L3 level
    )


