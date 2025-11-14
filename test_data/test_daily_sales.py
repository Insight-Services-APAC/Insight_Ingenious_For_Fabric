"""
Test script for daily sales file loading.

This script demonstrates how to configure and run the file loading framework
for daily sales CSV files.

Usage:
1. Upload the CSV files from test_data/ to your lakehouse at: Files/inbound/sales/
2. Update the workspace and lakehouse names below
3. Run this script in a Fabric notebook
"""

import uuid
from ingen_fab.python_libs.pyspark.ingestion.config import (
    SourceConfig,
    ResourceConfig,
    FileSystemExtractionParams
)
from ingen_fab.python_libs.pyspark.ingestion.orchestrator import Orchestrator

# ============================================================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================================================
WORKSPACE_NAME = "your_workspace_name"  # TODO: Change this
LAKEHOUSE_NAME = "your_lakehouse_name"  # TODO: Change this

# ============================================================================
# SOURCE CONFIGURATION
# ============================================================================
source_config = SourceConfig(
    source_type="filesystem",
    connection_params={
        "workspace_name": WORKSPACE_NAME,
        "lakehouse_name": LAKEHOUSE_NAME,
    },
    description="Daily sales data from vendor"
)

# ============================================================================
# EXTRACTION PARAMETERS (Inbound → Raw)
# ============================================================================
extraction_params = FileSystemExtractionParams(
    # Source location
    inbound_path="Files/inbound/sales/",

    # File discovery
    discovery_pattern="daily_sales_*.csv",  # Match daily_sales_20250107.csv
    recursive=False,
    batch_by="file",  # Each CSV file is one batch

    # CSV format
    has_header=True,
    file_delimiter=",",
    encoding="utf-8",
    quote_character='"',
    escape_character="\\",
    multiline_values=True,

    # Validation
    duplicate_handling="skip",  # Skip already processed files
    require_files=False,  # Don't fail if no files (default: False)

    # Date extraction and partitioning
    date_regex=r"daily_sales_(\d{8})",  # Extract from filename: daily_sales_20250107.csv → 20250107
    output_structure="{YYYY}/{MM}/{DD}/",  # Organize raw as: 2025/01/07/
    use_process_date=False,  # Use date from filename, not execution date
)

# ============================================================================
# RESOURCE CONFIGURATION (Complete config for extraction + loading)
# ============================================================================
daily_sales_config = ResourceConfig(
    # Identity
    resource_name="sales_daily_sales",
    source_name="sales",
    source_config=source_config,

    # Extraction settings
    extraction_params=extraction_params.to_dict(),

    # Raw layer
    raw_file_path="Files/raw/sales/daily_sales/",
    file_format="csv",

    # Loading settings (Raw → Bronze)
    import_mode="incremental",  # Only process new files
    batch_by="folder",  # Each date folder is one batch

    # Target bronze table
    target_workspace=WORKSPACE_NAME,
    target_lakehouse=LAKEHOUSE_NAME,
    target_schema="",  # Optional schema prefix
    target_table="sales_daily_sales",

    # Write strategy
    write_mode="append",  # Append new data
    partition_columns=["sale_date"],  # Partition bronze by sale_date
    enable_schema_evolution=True,

    # Execution
    execution_group=1,
    active=True,
)

# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """Run the daily sales ingestion pipeline."""

    print("=" * 80)
    print("DAILY SALES INGESTION TEST")
    print("=" * 80)

    # Step 1: Create orchestrator
    print("\n1. Creating orchestrator...")
    orchestrator = Orchestrator(
        spark=spark,  # spark is available in Fabric notebooks
        configs=[daily_sales_config],
        execution_id=str(uuid.uuid4()),
    )

    # Step 2: Execute extraction + loading
    print("\n2. Running extraction and loading...")
    result = orchestrator.execute()

    # Step 3: Print results
    print("\n" + "=" * 80)
    print("EXECUTION RESULTS")
    print("=" * 80)
    print(f"Status: {result.status}")
    print(f"Batches processed: {result.batches_processed}")
    print(f"Batches failed: {result.batches_failed}")
    print(f"Records processed: {result.metrics.records_processed}")
    print(f"Records inserted: {result.metrics.records_inserted}")
    print(f"Total duration: {result.metrics.total_duration_ms}ms")

    if result.error_message:
        print(f"\nError: {result.error_message}")

    print("\n" + "=" * 80)

    return result

# ============================================================================
# VERIFICATION QUERIES
# ============================================================================
def verify_results():
    """Run verification queries to check the results."""

    print("\n" + "=" * 80)
    print("VERIFICATION")
    print("=" * 80)

    # Check bronze table
    print("\n1. Bronze table summary:")
    try:
        bronze_summary = spark.sql("""
            SELECT
                sale_date,
                COUNT(*) as num_sales,
                SUM(total_amount) as total_revenue,
                MIN(sale_id) as first_sale_id,
                MAX(sale_id) as last_sale_id
            FROM sales_daily_sales
            GROUP BY sale_date
            ORDER BY sale_date
        """)
        bronze_summary.show()
    except Exception as e:
        print(f"   Error: {e}")

    # Check log entries
    print("\n2. Recent log entries:")
    try:
        log_entries = spark.sql("""
            SELECT
                config_id,
                status,
                source_file_path,
                records_processed,
                started_at
            FROM log_file_load
            WHERE config_id = 'sales_daily_sales'
            ORDER BY started_at DESC
            LIMIT 10
        """)
        log_entries.show(truncate=False)
    except Exception as e:
        print(f"   Error: {e}")

    # Check raw layer
    print("\n3. Raw layer structure:")
    try:
        spark.sql("LIST 'Files/raw/sales/daily_sales/'").show(truncate=False)
    except Exception as e:
        print(f"   Error: {e}")

# ============================================================================
# RUN THE TEST
# ============================================================================
if __name__ == "__main__":
    # Before running: Make sure you've uploaded the CSV files to Files/inbound/sales/

    # Run the ingestion
    result = main()

    # Verify the results
    if result.status == "completed":
        verify_results()

    print("\nTest completed!")
