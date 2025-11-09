"""
Updated configuration examples using new regex-based date extraction parameters.

Changes:
- date_pattern → date_regex (flexible regex-based extraction)
- use_current_date → use_process_date (clearer naming)
- raw_file_path → raw_landing_path, raw_archive_path, raw_quarantined_path
"""

from ingen_fab.python_libs.pyspark.ingestion.config import (
    SourceConfig,
    ResourceConfig,
    FileSystemExtractionParams
)

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
    inbound_path="inbound/sales/",

    discovery_pattern="daily_sales_*.csv",
    recursive=False,
    batch_by="file",
    require_files=True,

    has_header=True,
    file_delimiter=",",
    encoding="utf-8",
    quote_character='"',
    escape_character="\\",
    multiline_values=True,

    require_control_file=False,
    duplicate_handling="skip",

    date_regex=r"daily_sales_(\d{8})",
    output_structure="{YYYY}/{MM}/{DD}/",
    use_process_date=False
)

daily_sales_config = ResourceConfig(
    resource_name="sales_daily_sales",
    source_name="sales",
    source_config=source_config,

    extraction_params=extraction_params.to_dict(),

    raw_landing_path="raw/landing/sales/daily_sales/",
    raw_archive_path="raw/archive/sales/daily_sales/",
    raw_quarantined_path="raw/quarantined/sales/daily_sales/",
    file_format="csv",

    import_mode="incremental",
    batch_by="folder",

    target_workspace=configs.fabric_workspace_name,
    target_lakehouse=configs.config_lakehouse_name,
    target_schema="bronze",  # Optional schema prefix
    target_table="sales_daily_sales",

    write_mode="append",
    enable_schema_evolution=True,

    execution_group=1,
    active=True,
)

# ============================================================================
# EXAMPLE 2: Invoice Data - Folder-level batching with folder date extraction
# ============================================================================

extraction_params_invoice = FileSystemExtractionParams(
    inbound_path="exports_copy/incremental/EDL_HANA_FCT_DIRECT_INVOICE_LN/",
    discovery_pattern="*.parq.snappy",
    recursive=True,
    batch_by="folder",
    require_files=True,

    date_regex=r"(\d{4})/(\d{2})/(\d{2})",
    output_structure="{YYYY}/{MM}/{DD}",

    duplicate_handling="allow",
    require_control_file=False,
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

    raw_landing_path="raw/landing/edl_hana/fct_direct_invoice_ln/",
    raw_archive_path="raw/archive/edl_hana/fct_direct_invoice_ln/",
    raw_quarantined_path="raw/quarantined/edl_hana/fct_direct_invoice_ln/",
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


extraction_params_dim = FileSystemExtractionParams(
    inbound_path="exports_copy/snapshot/EDL_DIM_TIMEPERIOD_GROUP/",
    discovery_pattern="*.parq.snappy",
    recursive=True,
    batch_by="all",
    use_process_date=True,
    output_structure="{YYYY}/{MM}/{DD}",
    require_files=True,

    duplicate_handling="allow",
    require_control_file=False,
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

    raw_landing_path="raw/landing/edl/dim_timeperiod_group/",
    raw_archive_path="raw/archive/edl/dim_timeperiod_group/",
    raw_quarantined_path="raw/quarantined/edl/dim_timeperiod_group/",
    file_format="parquet",

    import_mode="incremental",
    batch_by="folder",

    target_workspace=configs.fabric_workspace_name,
    target_lakehouse=configs.config_lakehouse_name,
    target_schema="bronze",
    target_table="edl_dim_timeperiod_group",

    write_mode="overwrite",
    partition_columns=[],
    enable_schema_evolution=True,

    execution_group=1,
    active=True,
)
