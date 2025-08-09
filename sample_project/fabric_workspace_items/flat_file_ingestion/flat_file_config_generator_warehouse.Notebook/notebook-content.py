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
# Add default parameters here


# METADATA ********************

# META {
# META   "language": "python"
# META }

# MARKDOWN ********************

# ## 📦 Load Python Libraries and Initialize Environment

# CELL ********************

import sys

# Check if running in Fabric environment
if "notebookutils" in sys.modules:
    import sys

    notebookutils.fs.mount(
        "abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_lakehouse_name}}.Lakehouse/Files/",
        "/config_files",
    )  # type: ignore # noqa: F821
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


def load_python_modules_from_path(
    base_path: str, relative_files: list[str], max_chars: int = 1_000_000_000
):
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
            print("   Stack trace:")
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
    ingen_fab_modules = [
        mod
        for mod in sys.modules.keys()
        if mod.startswith(("ingen_fab.python_libs", "ingen_fab"))
    ]

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

# ## 🗂️ Load Custom Python Libraries

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import *
    from ingen_fab.python_libs.python.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.python.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )

    notebookutils = NotebookUtilsFactory.create_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/python/lakehouse_utils.py",
        "ingen_fab/python_libs/python/notebook_utils_abstraction.py",
    ]
    load_python_modules_from_path(mount_path, files_to_load)


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ⚙️ Configuration Settings

# CELL ********************


# Configuration for file discovery
scan_folder_path = "synthetic_data"  # Folder to scan
recursive_scan = True  # Scan subfolders
max_files_to_sample = 5  # Max files to sample per pattern
target_schema = "raw"  # Default target schema
execution_group_start = 100  # Starting execution group number


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🆕 Initialize Components

# CELL ********************


import glob
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# Get configurations
configs: ConfigsObject = get_configs_as_object()

# Initialize lakehouse utils
target_lakehouse_id = get_config_value("config_lakehouse_id")
target_workspace_id = get_config_value("config_workspace_id")

lh_utils = lakehouse_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark,
)


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔍 File Discovery Functions

# CELL ********************


def discover_files(base_path: str, recursive: bool = True) -> Dict[str, List[str]]:
    """Discover files in the specified path and group by pattern

    This function understands Spark output conventions where files are actually folders
    containing part files (e.g., customers.csv/ contains part-00000-*.csv files)
    """
    file_groups = {}

    # Prepare full path for scanning
    full_base_path = f"{base_path}" if not base_path.startswith("Files/") else base_path

    try:
        # Get all directories and files in the base path using the new list_all method
        all_items = lh_utils.list_all(full_base_path, recursive=recursive)

        # Process each item to identify file types
        for item_path in all_items:
            # Extract relative path from full path - handle absolute paths properly
            # item_path looks like: /workspaces/ingen_fab/tmp/spark/Files/synthetic_data/csv/single/customers.csv
            # We want: csv/single/customers.csv

            # Find the part after synthetic_data (or the base path)
            synthetic_data_marker = f"/tmp/spark/Files/{base_path}/"
            if synthetic_data_marker in item_path:
                rel_path = item_path.split(synthetic_data_marker)[1]
            else:
                # Fallback - try other patterns
                rel_path = item_path.replace(full_base_path + "/", "").replace(
                    full_base_path, ""
                )
                if rel_path.startswith("/"):
                    rel_path = rel_path[1:]

            # Check if this looks like a file format folder (e.g., customers.csv, data.parquet)
            path_parts = rel_path.split("/")
            for part in path_parts:
                if "." in part:
                    file_ext = part.split(".")[-1].lower()
                    if file_ext in ["csv", "parquet", "json", "avro"]:
                        # Check if this is actually a folder containing part files
                        # Build the relative path to the folder
                        folder_rel_path_parts = path_parts[: path_parts.index(part) + 1]
                        folder_rel_path = "/".join(folder_rel_path_parts)

                        # Convert back to absolute path for checking
                        absolute_folder_path = item_path.replace(
                            "/" + "/".join(path_parts), "/" + folder_rel_path
                        )

                        try:
                            # Check if this absolute path exists and contains part files
                            from pathlib import Path as PathLib

                            abs_path = PathLib(absolute_folder_path)
                            if abs_path.exists() and abs_path.is_dir():
                                part_files = list(abs_path.glob("part-*"))
                                has_part_files = len(part_files) > 0

                                if has_part_files:
                                    if file_ext not in file_groups:
                                        file_groups[file_ext] = []
                                    if folder_rel_path not in file_groups[file_ext]:
                                        file_groups[file_ext].append(folder_rel_path)
                        except:
                            # If we can't list contents, it might be a regular file
                            pass
                        break

    except Exception as e:
        print(f"Warning: Error discovering files in {full_base_path}: {str(e)}")
        # Fallback to glob for local testing
        if run_mode == "local":
            file_patterns = {
                "csv": ["**/*.csv"] if recursive else ["*.csv"],
                "parquet": ["**/*.parquet"] if recursive else ["*.parquet"],
                "json": ["**/*.json"] if recursive else ["*.json"],
                "avro": ["**/*.avro"] if recursive else ["*.avro"],
                "delta": ["**/*/_delta_log"] if recursive else ["*/_delta_log"],
            }

            for file_type, patterns in file_patterns.items():
                found_files = []
                for pattern in patterns:
                    local_path = Path(base_path) / pattern.replace("**/", "")
                    found_files.extend(glob.glob(str(local_path), recursive=recursive))

                if found_files:
                    # Convert to relative paths
                    rel_files = [
                        str(Path(f).relative_to(base_path)) for f in found_files
                    ]
                    file_groups[file_type] = list(set(rel_files))

    return file_groups


def analyze_file_structure(file_path: str) -> Dict[str, any]:
    """Analyze a file to determine its structure and properties"""
    file_info = {
        "path": file_path,
        "format": Path(file_path).suffix.lstrip("."),
        "size_mb": 0,
        "is_folder": False,
        "has_header": None,
        "delimiter": None,
        "encoding": "utf-8",
        "columns": [],
        "sample_data": None,
        "date_pattern": None,
        "is_partitioned": False,
    }

    try:
        # Check if it's a folder with part files (Spark output)
        full_file_path = (
            f"Files/{file_path}" if not file_path.startswith("Files/") else file_path
        )
        try:
            folder_contents = lh_utils.list_files(full_file_path)
            part_files = [f for f in folder_contents if "part-" in f]
            if part_files:
                file_info["is_folder"] = True
                file_info["part_file_count"] = len(part_files)
            else:
                file_info["is_folder"] = False
        except:
            file_info["is_folder"] = False

        # Analyze based on file type
        if file_info["format"] in ["csv", "parquet", "json", "avro"]:
            try:
                # Read a sample of the data
                read_options = {}
                if file_info["format"] == "csv":
                    read_options = {"header": True, "inferSchema": True}
                    file_info["has_header"] = True  # Assume header for CSV
                    file_info["delimiter"] = ","  # Default delimiter
                elif file_info["format"] == "json":
                    read_options = {"multiLine": True}

                df_sample = lh_utils.read_file(
                    file_path=full_file_path,
                    file_format=file_info["format"],
                    options=read_options,
                ).limit(100)

                file_info["columns"] = df_sample.columns
                # Convert to pandas for sampling
                try:
                    sample_pandas = df_sample.limit(5).toPandas()
                    file_info["sample_data"] = sample_pandas.to_dict(orient="records")
                except:
                    file_info["sample_data"] = None

            except Exception as read_error:
                file_info["error"] = f"Could not read file: {str(read_error)}"
                file_info["columns"] = []
                file_info["sample_data"] = None

        # Check for date partitioning patterns
        path_parts = file_path.split("/")
        date_patterns = [
            "YYYY/MM/DD",
            "YYYY-MM-DD",
            "YYYYMMDD",
            "year=YYYY/month=MM/day=DD",
        ]
        for pattern in date_patterns:
            if any(p.isdigit() for p in path_parts[-3:]):
                file_info["date_pattern"] = pattern
                file_info["is_partitioned"] = True
                break

    except Exception as e:
        file_info["error"] = str(e)

    return file_info


def infer_table_relationships(file_groups: Dict[str, List[Dict]]) -> Dict[str, str]:
    """Infer relationships between tables based on naming patterns"""
    relationships = {}

    # Common relationship patterns
    fact_patterns = ["fact_", "f_", "sales", "transactions", "events", "orders"]
    dimension_patterns = ["dim_", "d_", "customer", "product", "location", "date"]

    for group_name, files in file_groups.items():
        table_names = [f["inferred_table_name"] for f in files]

        # Check if it's a fact or dimension table group
        if any(pattern in group_name.lower() for pattern in fact_patterns):
            relationships[group_name] = "fact_tables"
        elif any(pattern in group_name.lower() for pattern in dimension_patterns):
            relationships[group_name] = "dimension_tables"
        else:
            relationships[group_name] = "general_tables"

    return relationships


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Discover Files in Target Folder

# CELL ********************


print(f"🔍 Scanning folder: {scan_folder_path}")
print(f"📁 Recursive scan: {recursive_scan}")
print("=" * 60)

# Discover files
discovered_files = discover_files(scan_folder_path, recursive_scan)

# Display summary
total_files = sum(len(files) for files in discovered_files.values())
print(f"✅ Found {total_files} files across {len(discovered_files)} file types")
print("")
print("File Type Summary:")
for file_type, files in discovered_files.items():
    print(f"  - {file_type.upper()}: {len(files)} files")


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔬 Analyze File Structures

# CELL ********************


# Analyze sample files from each group
analyzed_files = {}

for file_type, file_list in discovered_files.items():
    print("")
    print(f"📋 Analyzing {file_type.upper()} files...")
    analyzed_files[file_type] = []

    # Sample up to max_files_to_sample files
    sample_files = file_list[:max_files_to_sample]

    for file_path in sample_files:
        print(f"  - Analyzing: {file_path}")
        file_info = analyze_file_structure(file_path)

        # Infer table name from file path
        path_parts = file_path.split("/")
        file_name = path_parts[-1].replace(f".{file_type}", "")

        # Handle folder names (remove part file references)
        if file_info["is_folder"]:
            file_name = path_parts[-2] if len(path_parts) > 1 else file_name

        file_info["inferred_table_name"] = (
            file_name.replace("-", "_").replace(" ", "_").lower()
        )
        analyzed_files[file_type].append(file_info)


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🔧 Generate Ingestion Configurations

# CELL ********************


def generate_config_id(table_name: str, file_type: str) -> str:
    """Generate a unique config ID"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"auto_{table_name}_{file_type}_{timestamp}"


def determine_execution_group(file_info: Dict, base_group: int) -> int:
    """Determine execution group based on file characteristics"""
    # Assign groups based on dependencies and file types
    if (
        "dim_" in file_info["inferred_table_name"]
        or "dimension" in file_info["inferred_table_name"]
    ):
        return base_group  # Dimensions first
    elif "fact_" in file_info["inferred_table_name"]:
        return base_group + 10  # Facts after dimensions
    elif file_info["is_partitioned"]:
        return base_group + 20  # Partitioned data later
    else:
        return base_group + 5  # General tables in between


def generate_ingestion_config(file_info: Dict, config_counter: int) -> Dict:
    """Generate a flat file ingestion configuration from file analysis"""

    config = {
        "config_id": generate_config_id(
            file_info["inferred_table_name"], file_info["format"]
        ),
        "config_name": f"Auto-discovered: {file_info['inferred_table_name']} ({file_info['format']})",
        "source_file_path": file_info["path"],
        "source_file_format": file_info["format"],
        "source_workspace_id": "{{varlib:config_workspace_id}}",
        "source_datastore_id": "{{varlib:config_lakehouse_id}}",
        "source_datastore_type": "lakehouse",
        "source_file_root_path": "Files",
        "target_workspace_id": "{{varlib:config_workspace_id}}",
        "target_datastore_id": "{{varlib:config_lakehouse_id}}",
        "target_datastore_type": "lakehouse",
        "target_schema_name": target_schema,
        "target_table_name": file_info["inferred_table_name"],
        "staging_table_name": None,
        "file_delimiter": file_info.get("delimiter", ",")
        if file_info["format"] == "csv"
        else None,
        "has_header": file_info.get("has_header", True)
        if file_info["format"] == "csv"
        else None,
        "encoding": file_info.get("encoding", "utf-8")
        if file_info["format"] == "csv"
        else None,
        "date_format": "yyyy-MM-dd",
        "timestamp_format": "yyyy-MM-dd HH:mm:ss",
        "schema_inference": True,
        "custom_schema_json": None,
        "partition_columns": "",
        "sort_columns": file_info["columns"][0] if file_info.get("columns") else "",
        "write_mode": "append" if file_info["is_partitioned"] else "overwrite",
        "merge_keys": "",
        "data_validation_rules": None,
        "error_handling_strategy": "log",
        "execution_group": determine_execution_group(file_info, execution_group_start),
        "active_yn": "Y",
        "created_date": datetime.now().strftime("%Y-%m-%d"),
        "modified_date": None,
        "created_by": "auto_discovery",
        "modified_by": None,
        "quote_character": '"' if file_info["format"] == "csv" else None,
        "escape_character": '"' if file_info["format"] == "csv" else None,
        "multiline_values": True if file_info["format"] == "csv" else None,
        "ignore_leading_whitespace": False if file_info["format"] == "csv" else None,
        "ignore_trailing_whitespace": False if file_info["format"] == "csv" else None,
        "null_value": "" if file_info["format"] == "csv" else None,
        "empty_value": "" if file_info["format"] == "csv" else None,
        "comment_character": None,
        "max_columns": 100 if file_info["format"] == "csv" else None,
        "max_chars_per_column": 50000 if file_info["format"] == "csv" else None,
        "import_pattern": "date_partitioned"
        if file_info["is_partitioned"]
        else "single_file",
        "date_partition_format": file_info.get("date_pattern"),
        "table_relationship_group": f"auto_discovered_{datetime.now().strftime('%Y%m%d')}",
        "batch_import_enabled": file_info["is_partitioned"],
        "file_discovery_pattern": f"**/*.{file_info['format']}"
        if file_info["is_partitioned"]
        else None,
        "import_sequence_order": config_counter,
        "date_range_start": None,
        "date_range_end": None,
        "skip_existing_dates": True if file_info["is_partitioned"] else None,
        "source_is_folder": file_info["is_folder"],
    }

    return config


# Generate configurations
all_configs = []
config_counter = 1

for file_type, file_infos in analyzed_files.items():
    for file_info in file_infos:
        if "error" not in file_info:
            config = generate_ingestion_config(file_info, config_counter)
            all_configs.append(config)
            config_counter += 1

print("")
print(f"✅ Generated {len(all_configs)} ingestion configurations")


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 💾 Save Configurations

# CELL ********************


# Import the schema definition
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType(
    [
        StructField("config_id", StringType(), nullable=False),
        StructField("config_name", StringType(), nullable=False),
        StructField("source_file_path", StringType(), nullable=False),
        StructField(
            "source_file_format", StringType(), nullable=False
        ),  # csv, json, parquet, avro, xml
        # Source location fields (optional - defaults to target or raw workspace)
        StructField(
            "source_workspace_id", StringType(), nullable=True
        ),  # Source workspace (defaults to target if null)
        StructField(
            "source_datastore_id", StringType(), nullable=True
        ),  # Source lakehouse/warehouse (defaults to raw if null)
        StructField(
            "source_datastore_type", StringType(), nullable=True
        ),  # 'lakehouse' or 'warehouse' (defaults to lakehouse)
        StructField(
            "source_file_root_path", StringType(), nullable=True
        ),  # Root path override (e.g., "Files", "Tables")
        # Target location fields
        StructField(
            "target_workspace_id", StringType(), nullable=False
        ),  # Universal field for workspace
        StructField(
            "target_datastore_id", StringType(), nullable=False
        ),  # Universal field for lakehouse/warehouse
        StructField(
            "target_datastore_type", StringType(), nullable=False
        ),  # 'lakehouse' or 'warehouse'
        StructField("target_schema_name", StringType(), nullable=False),
        StructField("target_table_name", StringType(), nullable=False),
        StructField(
            "staging_table_name", StringType(), nullable=True
        ),  # For warehouse COPY INTO staging
        StructField("file_delimiter", StringType(), nullable=True),  # for CSV files
        StructField("has_header", BooleanType(), nullable=True),  # for CSV files
        StructField("encoding", StringType(), nullable=True),  # utf-8, latin-1, etc.
        StructField("date_format", StringType(), nullable=True),  # for date columns
        StructField(
            "timestamp_format", StringType(), nullable=True
        ),  # for timestamp columns
        StructField(
            "schema_inference", BooleanType(), nullable=False
        ),  # whether to infer schema
        StructField(
            "custom_schema_json", StringType(), nullable=True
        ),  # custom schema definition
        StructField(
            "partition_columns", StringType(), nullable=True
        ),  # comma-separated list
        StructField(
            "sort_columns", StringType(), nullable=True
        ),  # comma-separated list
        StructField(
            "write_mode", StringType(), nullable=False
        ),  # overwrite, append, merge
        StructField("merge_keys", StringType(), nullable=True),  # for merge operations
        StructField(
            "data_validation_rules", StringType(), nullable=True
        ),  # JSON validation rules
        StructField(
            "error_handling_strategy", StringType(), nullable=False
        ),  # fail, skip, log
        StructField("execution_group", IntegerType(), nullable=False),
        StructField("active_yn", StringType(), nullable=False),
        StructField("created_date", StringType(), nullable=False),
        StructField("modified_date", StringType(), nullable=True),
        StructField("created_by", StringType(), nullable=False),
        StructField("modified_by", StringType(), nullable=True),
        # Advanced CSV configuration fields
        StructField("quote_character", StringType(), nullable=True),  # Default: '"'
        StructField(
            "escape_character", StringType(), nullable=True
        ),  # Default: '"' (Excel style)
        StructField("multiline_values", BooleanType(), nullable=True),  # Default: True
        StructField(
            "ignore_leading_whitespace", BooleanType(), nullable=True
        ),  # Default: False
        StructField(
            "ignore_trailing_whitespace", BooleanType(), nullable=True
        ),  # Default: False
        StructField("null_value", StringType(), nullable=True),  # Default: ""
        StructField("empty_value", StringType(), nullable=True),  # Default: ""
        StructField("comment_character", StringType(), nullable=True),  # Default: None
        StructField("max_columns", IntegerType(), nullable=True),  # Default: 100
        StructField(
            "max_chars_per_column", IntegerType(), nullable=True
        ),  # Default: 50000
        # New fields for incremental synthetic data import support
        StructField(
            "import_pattern", StringType(), nullable=True
        ),  # 'single_file', 'date_partitioned', 'wildcard_pattern'
        StructField(
            "date_partition_format", StringType(), nullable=True
        ),  # Date partition format (e.g., 'YYYY/MM/DD')
        StructField(
            "table_relationship_group", StringType(), nullable=True
        ),  # Group for related table imports
        StructField(
            "batch_import_enabled", BooleanType(), nullable=True
        ),  # Enable batch processing
        StructField(
            "file_discovery_pattern", StringType(), nullable=True
        ),  # Pattern for automatic file discovery
        StructField(
            "import_sequence_order", IntegerType(), nullable=True
        ),  # Order for related table imports
        StructField(
            "date_range_start", StringType(), nullable=True
        ),  # Start date for batch import
        StructField(
            "date_range_end", StringType(), nullable=True
        ),  # End date for batch import
        StructField(
            "skip_existing_dates", BooleanType(), nullable=True
        ),  # Skip already imported dates
        StructField(
            "source_is_folder", BooleanType(), nullable=True
        ),  # True for folder with part files, False for single file
    ]
)

# Convert configs to Spark DataFrame
from pyspark.sql import Row

config_rows = []
for config in all_configs:
    # Convert dict to Row, ensuring all required fields are present
    row_data = {
        "config_id": config["config_id"],
        "config_name": config["config_name"],
        "source_file_path": config["source_file_path"],
        "source_file_format": config["source_file_format"],
        "source_workspace_id": config.get("source_workspace_id"),
        "source_datastore_id": config.get("source_datastore_id"),
        "source_datastore_type": config.get("source_datastore_type"),
        "source_file_root_path": config.get("source_file_root_path"),
        "target_workspace_id": config["target_workspace_id"],
        "target_datastore_id": config["target_datastore_id"],
        "target_datastore_type": config["target_datastore_type"],
        "target_schema_name": config["target_schema_name"],
        "target_table_name": config["target_table_name"],
        "staging_table_name": config.get("staging_table_name"),
        "file_delimiter": config.get("file_delimiter"),
        "has_header": config.get("has_header"),
        "encoding": config.get("encoding"),
        "date_format": config.get("date_format"),
        "timestamp_format": config.get("timestamp_format"),
        "schema_inference": config["schema_inference"],
        "custom_schema_json": config.get("custom_schema_json"),
        "partition_columns": config.get("partition_columns", ""),
        "sort_columns": config.get("sort_columns", ""),
        "write_mode": config["write_mode"],
        "merge_keys": config.get("merge_keys", ""),
        "data_validation_rules": config.get("data_validation_rules"),
        "error_handling_strategy": config["error_handling_strategy"],
        "execution_group": config["execution_group"],
        "active_yn": config["active_yn"],
        "created_date": config["created_date"],
        "modified_date": config.get("modified_date"),
        "created_by": config["created_by"],
        "modified_by": config.get("modified_by"),
        "quote_character": config.get("quote_character"),
        "escape_character": config.get("escape_character"),
        "multiline_values": config.get("multiline_values"),
        "ignore_leading_whitespace": config.get("ignore_leading_whitespace"),
        "ignore_trailing_whitespace": config.get("ignore_trailing_whitespace"),
        "null_value": config.get("null_value"),
        "empty_value": config.get("empty_value"),
        "comment_character": config.get("comment_character"),
        "max_columns": config.get("max_columns"),
        "max_chars_per_column": config.get("max_chars_per_column"),
        "import_pattern": config.get("import_pattern"),
        "date_partition_format": config.get("date_partition_format"),
        "table_relationship_group": config.get("table_relationship_group"),
        "batch_import_enabled": config.get("batch_import_enabled"),
        "file_discovery_pattern": config.get("file_discovery_pattern"),
        "import_sequence_order": config.get("import_sequence_order"),
        "date_range_start": config.get("date_range_start"),
        "date_range_end": config.get("date_range_end"),
        "skip_existing_dates": config.get("skip_existing_dates"),
        "source_is_folder": config.get("source_is_folder"),
    }
    config_rows.append(Row(**row_data))

# Create DataFrame with the schema
configs_df = lh_utils.get_connection.createDataFrame(config_rows, schema)

# Option 1: Append to existing config table
print("")
print("📝 Option 1: Append configurations to existing table")
print("To append these configurations, uncomment and run:")
print(
    "# lh_utils.write_to_table(df=configs_df, table_name='config_flat_file_ingestion', mode='append')"
)

# Option 2: Save to a temporary table for review
temp_table_name = f"temp_auto_configs_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
lh_utils.write_to_table(df=configs_df, table_name=temp_table_name, mode="overwrite")
print("")
print(f"✅ Saved configurations to temporary table: {temp_table_name}")

# Display sample configurations
print("")
print("📋 Sample Configurations (first 3):")
configs_df.select(
    "config_id", "config_name", "source_file_path", "target_table_name"
).show(3, truncate=False)


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📊 Configuration Summary Report

# CELL ********************


# Generate summary report
print("=" * 80)
print("FILE DISCOVERY AND CONFIGURATION GENERATION SUMMARY")
print("=" * 80)
print(f"Scan Path: {scan_folder_path}")
print(f"Total Files Discovered: {total_files}")
print(f"Configurations Generated: {len(all_configs)}")
print(f"Temporary Table: {temp_table_name}")
print("")
print("File Types:")
for file_type, count in [(ft, len(files)) for ft, files in discovered_files.items()]:
    print(f"  - {file_type.upper()}: {count} files")

print("")
print("Execution Groups:")
exec_groups = {}
for config in all_configs:
    group = config["execution_group"]
    exec_groups[group] = exec_groups.get(group, 0) + 1

for group in sorted(exec_groups.keys()):
    print(f"  - Group {group}: {exec_groups[group]} configurations")

print("")
print("Table Relationship Groups:")
rel_groups = {}
for config in all_configs:
    group = config["table_relationship_group"]
    rel_groups[group] = rel_groups.get(group, 0) + 1

for group, count in rel_groups.items():
    print(f"  - {group}: {count} tables")

print("")
print("✅ Configuration generation complete!")
print(f"Review the configurations in table: {temp_table_name}")
print("Once reviewed, append to config_flat_file_ingestion table to activate.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✔️ Exit notebook with result

# CELL ********************


notebookutils.mssparkutils.notebook.exit("success")


# METADATA ********************

# META {
# META   "language": "python"
# META }
