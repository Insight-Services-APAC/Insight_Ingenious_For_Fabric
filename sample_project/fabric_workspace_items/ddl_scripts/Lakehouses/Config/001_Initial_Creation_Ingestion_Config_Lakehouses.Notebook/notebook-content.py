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

    notebookutils.fs.mount(
        "abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_lakehouse_name}}.Lakehouse/Files/",
        "/config_files",
    )  # type: ignore # noqa: F821
    mount_path = notebookutils.fs.getMountPath("/config_files")  # type: ignore # noqa: F821

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

# ## 🗂️ Now Load the Custom Python Libraries

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import *
    from ingen_fab.python_libs.pyspark.ddl_utils import ddl_utils
    from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
    from ingen_fab.python_libs.pyspark.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )

    notebookutils = NotebookUtilsFactory.create_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/pyspark/lakehouse_utils.py",
        "ingen_fab/python_libs/pyspark/ddl_utils.py",
        "ingen_fab/python_libs/pyspark/notebook_utils_abstraction.py",
    ]

    load_python_modules_from_path(mount_path, files_to_load)


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🆕 Instantiate Required Classes

# CELL ********************


target_lakehouse_config_prefix = "Config"
configs: ConfigsObject = get_configs_as_object()
target_lakehouse_id = get_config_value(
    f"{target_lakehouse_config_prefix.lower()}_lakehouse_id"
)
target_workspace_id = get_config_value(
    f"{target_lakehouse_config_prefix.lower()}_workspace_id"
)

target_lakehouse = lakehouse_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark,  # Pass the Spark session if available
)

du = ddl_utils(
    target_workspace_id=target_workspace_id,
    target_lakehouse_id=target_lakehouse_id,
    spark=spark,  # Pass the Spark session if available
)

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 🏃‍♂️‍➡️ Run DDL Cells

# CELL ********************


# DDL cells are injected below:


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 001_config_flat_file_ingestion_create.py

# CELL ********************

guid = "6acfb4b93bae"
object_name = "001_config_flat_file_ingestion_create"


def script_to_execute():
    # Configuration table for flat file ingestion metadata - Universal schema (Lakehouse version)
    from pyspark.sql.types import (
        BooleanType,
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
            StructField(
                "encoding", StringType(), nullable=True
            ),  # utf-8, latin-1, etc.
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
            StructField(
                "merge_keys", StringType(), nullable=True
            ),  # for merge operations
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
            StructField(
                "multiline_values", BooleanType(), nullable=True
            ),  # Default: True
            StructField(
                "ignore_leading_whitespace", BooleanType(), nullable=True
            ),  # Default: False
            StructField(
                "ignore_trailing_whitespace", BooleanType(), nullable=True
            ),  # Default: False
            StructField("null_value", StringType(), nullable=True),  # Default: ""
            StructField("empty_value", StringType(), nullable=True),  # Default: ""
            StructField(
                "comment_character", StringType(), nullable=True
            ),  # Default: None
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

    target_lakehouse.create_table(
        table_name="config_flat_file_ingestion",
        schema=schema,
        mode="overwrite",
        options={"parquet.vorder.default": "true"},
    )


du.run_once(script_to_execute, "001_config_flat_file_ingestion_create", "6acfb4b93bae")


def script_to_execute():
    print("Script block is empty. No action taken.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📄 Cell for 002_log_flat_file_ingestion_create.py

# CELL ********************

guid = "af23899189d1"
object_name = "002_log_flat_file_ingestion_create"


def script_to_execute():
    # Log table for flat file ingestion execution tracking - Lakehouse version
    # Import centralized schema to ensure consistency
    from ingen_fab.python_libs.common.flat_file_logging_schema import (
        get_flat_file_ingestion_log_schema,
    )

    schema = get_flat_file_ingestion_log_schema()

    target_lakehouse.create_table(
        table_name="log_flat_file_ingestion",
        schema=schema,
        mode="overwrite",
        options={"parquet.vorder.default": "true"},
    )


du.run_once(script_to_execute, "002_log_flat_file_ingestion_create", "af23899189d1")


def script_to_execute():
    print("Script block is empty. No action taken.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 📇 Print the execution log

# CELL ********************


du.print_log()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ✔️ If we make it to the end return a successful result

# CELL ********************


notebookutils.mssparkutils.notebook.exit("success")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
