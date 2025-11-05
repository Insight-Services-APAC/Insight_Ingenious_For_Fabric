"""
Example usage patterns for the File Loading Framework

This file demonstrates how to use the new file loading framework to:
1. Load files from lakehouse Files storage into Delta tables
2. Handle incremental loading with duplicate detection
3. Orchestrate multiple resources with execution groups
4. Track state with logging
"""

import uuid
from pyspark.sql import SparkSession

from ingen_fab.python_libs.pyspark.ingestion import (
    FileLoader,
    FileLoadingLogger,
    FileLoadingOrchestrator,
    FileSystemLoadingParams,
    ResourceConfig,
    SourceConfig,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils


# ============================================================================
# EXAMPLE 1: Basic Single File Loading
# ============================================================================

def example_single_file_loading(spark: SparkSession):
    """Load a single CSV file into a Delta table"""

    # Step 1: Create SourceConfig (connection info for source lakehouse)
    source_config = SourceConfig(
        source_name="my_source_lakehouse",
        source_type="filesystem",
        connection_params={
            "workspace_name": "MyWorkspace",
            "lakehouse_name": "SourceLakehouse",
        },
    )

    # Step 2: Create FileSystemLoadingParams (file-specific settings)
    loading_params = FileSystemLoadingParams(
        import_pattern="single_file",
        file_delimiter=",",
        has_header=True,
        encoding="utf-8",
    )

    # Step 3: Create ResourceConfig (complete configuration)
    resource_config = ResourceConfig(
        resource_name="load_orders_csv",
        source_config=source_config,
        source_file_path="raw/orders/orders.csv",
        source_file_format="csv",
        loading_params=loading_params.to_dict(),
        target_workspace_name="MyWorkspace",
        target_datastore_name="TargetLakehouse",
        target_datastore_type="lakehouse",
        target_schema_name="bronze",
        target_table_name="orders",
        write_mode="overwrite",
        enable_schema_evolution=True,
    )

    # Step 4: Create FileLoader and process
    loader = FileLoader(spark=spark, config=resource_config)

    # Discover and read files
    batches = loader.discover_and_read_files()

    # Process each batch
    for batch_info, df, metrics in batches:
        print(f"Batch {batch_info.batch_id}: {metrics.source_row_count} rows")
        df.show(5)


# ============================================================================
# EXAMPLE 2: Incremental File Loading with Duplicate Detection
# ============================================================================

def example_incremental_file_loading(spark: SparkSession):
    """Load files incrementally with duplicate detection"""

    # Source configuration
    source_config = SourceConfig(
        source_name="my_source_lakehouse",
        source_type="filesystem",
        connection_params={
            "workspace_name": "MyWorkspace",
            "lakehouse_name": "SourceLakehouse",
        },
    )

    # File loading parameters with duplicate handling
    loading_params = FileSystemLoadingParams(
        import_pattern="incremental_files",
        discovery_pattern="*.csv",              # Match all CSV files
        file_delimiter=",",
        has_header=True,
        duplicate_handling="skip",              # Skip duplicates (or "allow", "fail")
        date_pattern="YYYYMMDD",                # Extract date from filename
        date_range_start="2024-01-01",          # Optional: filter by date range
        date_range_end="2024-12-31",
    )

    # Resource configuration
    resource_config = ResourceConfig(
        resource_name="load_daily_sales",
        source_config=source_config,
        source_file_path="raw/sales/daily",     # Directory containing files
        source_file_format="csv",
        loading_params=loading_params.to_dict(),
        target_workspace_name="MyWorkspace",
        target_datastore_name="TargetLakehouse",
        target_datastore_type="lakehouse",
        target_schema_name="bronze",
        target_table_name="sales_daily",
        write_mode="append",                    # Append new data
        enable_schema_evolution=True,
        state_tracking=True,                    # Track processed files
    )

    # Use FileLoader to discover files
    loader = FileLoader(spark=spark, config=resource_config)
    batches = loader.discover_and_read_files()

    print(f"Discovered {len(batches)} new file(s) to process")

    # Check for duplicates
    if loader.last_duplicate_items:
        print(f"Skipped {len(loader.last_duplicate_items)} duplicate file(s)")


# ============================================================================
# EXAMPLE 3: Folder-Based Loading (Batch Processing)
# ============================================================================

def example_folder_based_loading(spark: SparkSession):
    """Load files organized in folders (e.g., daily batches)"""

    source_config = SourceConfig(
        source_name="my_source_lakehouse",
        source_type="filesystem",
        connection_params={
            "workspace_name": "MyWorkspace",
            "lakehouse_name": "SourceLakehouse",
        },
    )

    # Folder-based loading parameters
    loading_params = FileSystemLoadingParams(
        import_pattern="incremental_folders",
        discovery_pattern="batch_*",            # Match folders like "batch_001", "batch_002"
        file_delimiter=",",
        has_header=True,
        duplicate_handling="skip",
        require_control_file=True,              # Require control file per folder
        control_file_pattern="_SUCCESS",        # Look for _SUCCESS file in each folder
    )

    resource_config = ResourceConfig(
        resource_name="load_batch_folders",
        source_config=source_config,
        source_file_path="raw/batches",
        source_file_format="csv",
        loading_params=loading_params.to_dict(),
        target_workspace_name="MyWorkspace",
        target_datastore_name="TargetLakehouse",
        target_datastore_type="lakehouse",
        target_table_name="batch_data",
        write_mode="append",
        enable_schema_evolution=True,
    )

    loader = FileLoader(spark=spark, config=resource_config)
    batches = loader.discover_and_read_files()

    for batch_info, df, metrics in batches:
        print(f"Folder: {batch_info.folder_name}, Rows: {metrics.source_row_count}")


# ============================================================================
# EXAMPLE 4: Full Orchestration with Multiple Resources
# ============================================================================

def example_orchestrated_loading(spark: SparkSession):
    """Orchestrate loading of multiple resources with execution groups"""

    # Common source configuration
    source_config = SourceConfig(
        source_name="my_source_lakehouse",
        source_type="filesystem",
        connection_params={
            "workspace_name": "MyWorkspace",
            "lakehouse_name": "SourceLakehouse",
        },
    )

    # Resource 1: Orders (execution group 1)
    orders_config = ResourceConfig(
        resource_name="load_orders",
        source_config=source_config,
        source_file_path="raw/orders/orders.csv",
        source_file_format="csv",
        loading_params=FileSystemLoadingParams(
            import_pattern="single_file",
            has_header=True,
        ).to_dict(),
        target_workspace_name="MyWorkspace",
        target_datastore_name="TargetLakehouse",
        target_datastore_type="lakehouse",
        target_table_name="orders",
        write_mode="overwrite",
        execution_group=1,                      # Load first
    )

    # Resource 2: Customers (execution group 1 - parallel with orders)
    customers_config = ResourceConfig(
        resource_name="load_customers",
        source_config=source_config,
        source_file_path="raw/customers/customers.csv",
        source_file_format="csv",
        loading_params=FileSystemLoadingParams(
            import_pattern="single_file",
            has_header=True,
        ).to_dict(),
        target_workspace_name="MyWorkspace",
        target_datastore_name="TargetLakehouse",
        target_datastore_type="lakehouse",
        target_table_name="customers",
        write_mode="overwrite",
        execution_group=1,                      # Load first (parallel)
    )

    # Resource 3: Order Items (execution group 2 - depends on orders)
    order_items_config = ResourceConfig(
        resource_name="load_order_items",
        source_config=source_config,
        source_file_path="raw/order_items",
        source_file_format="csv",
        loading_params=FileSystemLoadingParams(
            import_pattern="incremental_files",
            discovery_pattern="*.csv",
            has_header=True,
            duplicate_handling="skip",
        ).to_dict(),
        target_workspace_name="MyWorkspace",
        target_datastore_name="TargetLakehouse",
        target_datastore_type="lakehouse",
        target_table_name="order_items",
        write_mode="append",
        execution_group=2,                      # Load second (after group 1)
    )

    # Create orchestrator
    orchestrator = FileLoadingOrchestrator(
        spark=spark,
        max_concurrency=4,                      # Max 4 parallel workers per group
    )

    # Process all resources
    execution_id = str(uuid.uuid4())
    results = orchestrator.process_resources(
        configs=[orders_config, customers_config, order_items_config],
        execution_id=execution_id,
    )

    # Print summary
    print(f"\nExecution ID: {results['execution_id']}")
    print(f"Total resources: {results['total_resources']}")
    print(f"Successful: {results['successful']}")
    print(f"Failed: {results['failed']}")
    print(f"No data: {results['no_data']}")


# ============================================================================
# EXAMPLE 5: With Logging and State Tracking
# ============================================================================

def example_with_logging(spark: SparkSession):
    """Use logging service for state tracking"""

    # Create lakehouse utils for logging lakehouse
    log_lakehouse = lakehouse_utils(
        target_workspace_name="MyWorkspace",
        target_lakehouse_name="LogsLakehouse",
        spark=spark,
    )

    # Create logger
    logger = FileLoadingLogger(lakehouse_utils_instance=log_lakehouse)

    # Create orchestrator with logger
    orchestrator = FileLoadingOrchestrator(
        spark=spark,
        logger_instance=logger,                 # Enable state tracking
        max_concurrency=4,
    )

    # Configure resource
    source_config = SourceConfig(
        source_name="my_source",
        source_type="filesystem",
        connection_params={
            "workspace_name": "MyWorkspace",
            "lakehouse_name": "SourceLakehouse",
        },
    )

    resource_config = ResourceConfig(
        resource_name="load_transactions",
        source_config=source_config,
        source_file_path="raw/transactions",
        source_file_format="csv",
        loading_params=FileSystemLoadingParams(
            import_pattern="incremental_files",
            discovery_pattern="*.csv",
            has_header=True,
            duplicate_handling="skip",
        ).to_dict(),
        target_workspace_name="MyWorkspace",
        target_datastore_name="TargetLakehouse",
        target_datastore_type="lakehouse",
        target_table_name="transactions",
        write_mode="append",
        state_tracking=True,
    )

    # Process with logging
    execution_id = str(uuid.uuid4())
    results = orchestrator.process_resources(
        configs=[resource_config],
        execution_id=execution_id,
    )

    # Query logs
    batch_logs = spark.sql(f"""
        SELECT *
        FROM log_file_load
        WHERE execution_id = '{execution_id}'
        ORDER BY started_at
    """)
    batch_logs.show()

    config_logs = spark.sql(f"""
        SELECT *
        FROM log_config_execution
        WHERE execution_id = '{execution_id}'
    """)
    config_logs.show()


# ============================================================================
# EXAMPLE 6: Merge Mode (Upsert Pattern)
# ============================================================================

def example_merge_mode(spark: SparkSession):
    """Load data using merge mode for incremental updates"""

    source_config = SourceConfig(
        source_name="my_source",
        source_type="filesystem",
        connection_params={
            "workspace_name": "MyWorkspace",
            "lakehouse_name": "SourceLakehouse",
        },
    )

    resource_config = ResourceConfig(
        resource_name="load_products_merge",
        source_config=source_config,
        source_file_path="raw/products/products.csv",
        source_file_format="csv",
        loading_params=FileSystemLoadingParams(
            import_pattern="single_file",
            has_header=True,
        ).to_dict(),
        target_workspace_name="MyWorkspace",
        target_datastore_name="TargetLakehouse",
        target_datastore_type="lakehouse",
        target_table_name="products",
        write_mode="merge",                     # Upsert mode
        merge_keys=["product_id"],              # Key columns for merge
        enable_schema_evolution=True,
    )

    orchestrator = FileLoadingOrchestrator(spark=spark)
    execution_id = str(uuid.uuid4())

    results = orchestrator.process_resources(
        configs=[resource_config],
        execution_id=execution_id,
    )

    # Check results
    for result in results['resources']:
        if result.metrics:
            print(f"Resource: {result.resource_name}")
            print(f"  Records inserted: {result.metrics.records_inserted}")
            print(f"  Records updated: {result.metrics.records_updated}")


# ============================================================================
# EXAMPLE 7: Hierarchical Date Folders (e.g., 2025/01/15)
# ============================================================================

def example_hierarchical_date_folders(spark: SparkSession):
    """Load files organized in hierarchical date folders"""

    source_config = SourceConfig(
        source_name="my_source",
        source_type="filesystem",
        connection_params={
            "workspace_name": "MyWorkspace",
            "lakehouse_name": "SourceLakehouse",
        },
    )

    loading_params = FileSystemLoadingParams(
        import_pattern="incremental_folders",
        discovery_pattern="*/*/*",              # Match YYYY/MM/DD structure
        date_pattern="YYYY/MM/DD",              # Extract date from folder path
        date_range_start="2025-01-01",
        has_header=True,
        duplicate_handling="skip",
    )

    resource_config = ResourceConfig(
        resource_name="load_daily_logs",
        source_config=source_config,
        source_file_path="raw/logs",
        source_file_format="json",
        loading_params=loading_params.to_dict(),
        target_workspace_name="MyWorkspace",
        target_datastore_name="TargetLakehouse",
        target_datastore_type="lakehouse",
        target_table_name="logs",
        write_mode="append",
        partition_columns=["date_partition"],   # Partition by extracted date
    )

    loader = FileLoader(spark=spark, config=resource_config)
    batches = loader.discover_and_read_files()

    for batch_info, df, metrics in batches:
        print(f"Date: {batch_info.date_partition}, Rows: {metrics.source_row_count}")


# ============================================================================
# MAIN: Run Examples
# ============================================================================

if __name__ == "__main__":
    # Note: These examples require a Spark session and proper configuration
    # Uncomment to run specific examples:

    # spark = SparkSession.builder.getOrCreate()
    # example_single_file_loading(spark)
    # example_incremental_file_loading(spark)
    # example_folder_based_loading(spark)
    # example_orchestrated_loading(spark)
    # example_with_logging(spark)
    # example_merge_mode(spark)
    # example_hierarchical_date_folders(spark)

    print("Example usage patterns defined. Uncomment in __main__ to run.")
