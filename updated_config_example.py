"""
Updated configuration examples using new regex-based date extraction parameters.

Changes:
- date_pattern → date_regex (flexible regex-based extraction)
- use_current_date → use_process_date (clearer naming)
- Added partition_depth for explicit control of folder nesting level

Partition Depth Examples:
- partition_depth=3: YYYY/MM/DD (day-level)
- partition_depth=4: YYYY/MM/DD/HH (hour-level)
- partition_depth=5: YYYY/MM/DD/HH/mm (minute-level)
- partition_depth=6: YYYY/MM/DD/HH/mm/ss (second-level)

Benefits of partition_depth:
✓ Explicit - you control exactly which level to process
✓ Efficient - no wasted time checking intermediate folders
✓ Clear - configuration documents the expected structure
"""

from ingen_fab.python_libs.pyspark.ingestion.config import (
    SourceConfig,
    ResourceConfig,
    FileSystemExtractionParams
)

# Assuming you have a configs object with workspace/lakehouse names
# configs.fabric_workspace_name
# configs.config_lakehouse_name

# ============================================================================
# EXAMPLE 1: Daily Sales - File-level batching with filename date extraction
# ============================================================================

source_config = SourceConfig(
    source_type="filesystem",
    connection_params={
        "workspace_name": configs.fabric_workspace_name,
        "lakehouse_name": configs.config_lakehouse_name,
    },
    description="Sales data from vendor XYZ"
)

extraction_params = FileSystemExtractionParams(
    inbound_path="Files/inbound/sales/",  # Note: Should include "Files/" prefix for Fabric lakehouse paths

    discovery_pattern="daily_sales_*.csv",
    recursive=False,
    batch_by="file",

    has_header=True,
    file_delimiter=",",
    encoding="utf-8",
    quote_character='"',
    escape_character="\\",
    multiline_values=True,

    # Validation
    require_control_file=False,
    duplicate_handling="skip",
    require_files=True,  # Fail extraction if no files found (default: False)

    # Date extraction (NEW: Regex-based)
    date_regex=r"daily_sales_(\d{8})",  # Matches: daily_sales_20250107.csv → "20250107"
    output_structure="{YYYY}/{MM}/{DD}/",  # Organize raw as: 2025/01/07/
    use_process_date=False  # Use date from filename, not execution date
)

daily_sales_config = ResourceConfig(
    resource_name="sales_daily_sales",
    source_name="sales",
    source_config=source_config,

    extraction_params=extraction_params.to_dict(),

    raw_file_path="Files/raw/sales/daily_sales/",  # Note: Should include "Files/" prefix
    file_format="csv",

    import_mode="incremental",
    batch_by="folder",

    target_workspace=configs.fabric_workspace_name,
    target_lakehouse=configs.config_lakehouse_name,
    target_schema="bronze",  # Optional schema prefix
    target_table="sales_daily_sales",

    # Write strategy
    write_mode="append",
    enable_schema_evolution=True,

    # Execution control
    execution_group=1,
    active=True,
)

# ============================================================================
# EXAMPLE 2: Invoice Data - Folder-level batching with folder date extraction
# ============================================================================

extraction_params_invoice = FileSystemExtractionParams(
    inbound_path="Files/exports_copy/incremental/EDL_HANA_FCT_DIRECT_INVOICE_LN/",
    discovery_pattern="*.parq.snappy",
    recursive=True,
    batch_by="folder",

    # Validation
    duplicate_handling="allow",
    require_control_file=False,
    require_files=False,  # Don't fail if no new data (incremental pattern)

    # Partition configuration - only check folders at day-level (3 levels deep: YYYY/MM/DD)
    partition_depth=3,  # Look exactly 3 levels deep from inbound_path
    date_regex=r"(\d{4})/(\d{2})/(\d{2})",  # Validate folder structure is YYYY/MM/DD
    output_structure="{YYYY}/{MM}/{DD}",  # Preserve structure in raw
)

source_config_invoice = SourceConfig(
    source_type="filesystem",
    connection_params={
        "workspace_name": configs.fabric_workspace_name,
        "lakehouse_name": configs.config_lakehouse_name
    },
)

invoice_config = ResourceConfig(
    resource_name="edl_hana_fct_direct_invoice_ln",
    source_name="edl_hana",
    source_config=source_config_invoice,
    extraction_params=extraction_params_invoice.to_dict(),

    raw_file_path="Files/raw/edl_hana/fct_direct_invoice_ln/",
    file_format="parquet",

    import_mode="incremental",
    batch_by="folder",

    target_workspace=configs.fabric_workspace_name,
    target_lakehouse=configs.config_lakehouse_name,
    target_schema="bronze",
    target_table="edl_hana_fct_direct_invoice_ln",

    write_mode="append",
    partition_columns=[],
    enable_schema_evolution=True,

    execution_group=1,
    active=True,
)

# ============================================================================
# EXAMPLE 3: Dimension Snapshot - All-batch with process date
# ============================================================================

extraction_params_dim = FileSystemExtractionParams(
    inbound_path="Files/exports_copy/snapshot/EDL_DIM_TIMEPERIOD_GROUP/",
    discovery_pattern="*.parq.snappy",
    recursive=True,
    batch_by="all",  # All part files = ONE batch

    # Validation
    duplicate_handling="allow",  # Won't reprocess same date twice
    require_control_file=False,
    require_files=True,  # Fail if snapshot is missing - critical data!

    # Date: Use execution date for snapshots (not extracted from path)
    use_process_date=True,  # Uses today's date for partitioning
    output_structure="{YYYY}/{MM}/{DD}",  # Creates dated folders in raw
)

source_config_dim = SourceConfig(
    source_type="filesystem",
    connection_params={
        "workspace_name": configs.fabric_workspace_name,
        "lakehouse_name": configs.config_lakehouse_name
    },
)

dim_timeperiod_config = ResourceConfig(
    resource_name="edl_dim_timeperiod_group",
    source_name="edl",
    source_config=source_config_dim,
    extraction_params=extraction_params_dim.to_dict(),

    # Extraction writes here with date folders
    raw_file_path="Files/raw/edl/dim_timeperiod_group/",
    file_format="parquet",

    # Loading settings
    import_mode="incremental",  # Only process NEW date folders
    batch_by="folder",          # Each date folder = one batch

    # Target settings
    target_workspace=configs.fabric_workspace_name,
    target_lakehouse=configs.config_lakehouse_name,
    target_schema="bronze",
    target_table="edl_dim_timeperiod_group",

    # Write settings (overwrite table with latest snapshot)
    write_mode="overwrite",  # Replace table completely
    partition_columns=[],
    enable_schema_evolution=True,

    execution_group=1,
    active=True,
)

# ============================================================================
# ADDITIONAL EXAMPLES: Different Partition Depths
# ============================================================================

# Hour-level partitioning (YYYY/MM/DD/HH)
extraction_params_hourly = FileSystemExtractionParams(
    inbound_path="Files/exports_copy/incremental/hourly_data/",
    discovery_pattern="*.parquet",
    batch_by="folder",
    partition_depth=4,  # 4 levels: YYYY/MM/DD/HH
    date_regex=r"(\d{4})/(\d{2})/(\d{2})/(\d{2})",  # Extract hour too
    output_structure="{YYYY}/{MM}/{DD}",  # But organize raw by day only
    duplicate_handling="skip",
)

# Minute-level partitioning (YYYY/MM/DD/HH/mm)
extraction_params_minutely = FileSystemExtractionParams(
    inbound_path="Files/exports_copy/incremental/streaming_data/",
    discovery_pattern="*.json",
    batch_by="folder",
    partition_depth=5,  # 5 levels: YYYY/MM/DD/HH/mm
    date_regex=r"(\d{4})/(\d{2})/(\d{2})/(\d{2})/(\d{2})",
    output_structure="{YYYY}/{MM}/{DD}",  # Still organize raw by day
    duplicate_handling="skip",
)

# No date extraction - flat structure
extraction_params_flat = FileSystemExtractionParams(
    inbound_path="Files/exports_copy/manual_uploads/",
    discovery_pattern="*.csv",
    batch_by="file",
    # No partition_depth, no date_regex - files go directly to raw
    use_process_date=True,  # Use execution date for organizing
    output_structure="{YYYY}/{MM}/{DD}/",
    duplicate_handling="skip",
)
