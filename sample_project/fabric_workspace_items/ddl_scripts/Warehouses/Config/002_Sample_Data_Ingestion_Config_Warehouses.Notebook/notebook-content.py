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
# META   "language": "python",
# META   "language_group": "jupyter_python"
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

# ## 🗂️ Now Load the Custom Python Libraries

# CELL ********************


if run_mode == "local":
    from ingen_fab.python_libs.common.config_utils import *
    from ingen_fab.python_libs.python.ddl_utils import ddl_utils
    from ingen_fab.python_libs.python.notebook_utils_abstraction import (
        NotebookUtilsFactory,
    )
    from ingen_fab.python_libs.python.warehouse_utils import warehouse_utils

    notebookutils = NotebookUtilsFactory.create_instance()
else:
    files_to_load = [
        "ingen_fab/python_libs/common/config_utils.py",
        "ingen_fab/python_libs/python/lakehouse_utils.py",
        "ingen_fab/python_libs/python/ddl_utils.py",
        "ingen_fab/python_libs/python/notebook_utils_abstraction.py",
        "ingen_fab/python_libs/python/sql_templates.py",
        "ingen_fab/python_libs/python/warehouse_utils.py",
        "ingen_fab/python_libs/python/pipeline_utils.py",
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


# variableLibraryInjectionStart: var_lib


# variableLibraryInjectionEnd: var_lib


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
target_warehouse_id = get_config_value(
    f"{target_lakehouse_config_prefix.lower()}_warehouse_id"
)
target_workspace_id = get_config_value(
    f"{target_lakehouse_config_prefix.lower()}_workspace_id"
)

du = ddl_utils(
    target_workspace_id=target_workspace_id,
    target_warehouse_id=target_warehouse_id,
    notebookutils=notebookutils,
)

wu = warehouse_utils(
    target_workspace_id=target_workspace_id, target_warehouse_id=target_warehouse_id
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
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## 𝄜 Cell for 003_sample_data_insert.sql

# CELL ********************

guid = "a8c5702c3e8c"


def work():
    sql = """

-- Sample configuration data for flat file ingestion testing - Universal schema (Warehouse version)
-- Using synthetic data generator parquet files

INSERT INTO config.config_flat_file_ingestion (
    config_id,
    config_name,
    source_file_path,
    source_file_format,
    target_workspace_id,
    target_datastore_id,
    target_datastore_type,
    target_schema_name,
    target_table_name,
    staging_table_name,
    file_delimiter,
    has_header,
    encoding,
    date_format,
    timestamp_format,
    schema_inference,
    -- Advanced CSV handling options
    quote_character,
    escape_character,
    multiline_values,
    ignore_leading_whitespace,
    ignore_trailing_whitespace,
    null_value,
    empty_value,
    comment_character,
    max_columns,
    max_chars_per_column,
    custom_schema_json,
    partition_columns,
    sort_columns,
    write_mode,
    merge_keys,
    data_validation_rules,
    error_handling_strategy,
    execution_group,
    active_yn,
    created_date,
    modified_date,
    created_by,
    modified_by,
    -- New fields for incremental synthetic data import support
    import_pattern,
    date_partition_format,
    table_relationship_group,
    batch_import_enabled,
    file_discovery_pattern,
    import_sequence_order,
    date_range_start,
    date_range_end,
    skip_existing_dates
) VALUES 
(
    'synthetic_customers_001',
    'Synthetic Data - Customers (Retail OLTP Small)',
    'Files/synthetic_data/parquet/single/retail_oltp_small/customers.parquet',
    'parquet',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_lakehouse_id}}',
    'lakehouse',
    'raw',
    'synthetic_customers',
    NULL,
    NULL,
    NULL,
    NULL,
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    '',
    'customer_id',
    'overwrite',
    '',
    NULL,
    'fail',
    1,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL,
    -- New fields for incremental synthetic data import support
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
),
(
    'synthetic_products_002',
    'Synthetic Data - Products (Retail OLTP Small)',
    'Files/synthetic_data/parquet/single/retail_oltp_small/products.parquet',
    'parquet',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_lakehouse_id}}',
    'lakehouse',
    'raw',
    'synthetic_products',
    NULL,
    NULL,
    NULL,
    NULL,
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    'category',
    'product_id',
    'overwrite',
    '',
    NULL,
    'log',
    1,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL,
    -- New fields for incremental synthetic data import support
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
),
(
    'synthetic_orders_003',
    'Synthetic Data - Orders (Retail OLTP Small)',
    'Files/synthetic_data/parquet/single/retail_oltp_small/orders.parquet',
    'parquet',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_lakehouse_id}}',
    'lakehouse',
    'raw',
    'synthetic_orders',
    NULL,
    NULL,
    NULL,
    NULL,
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    '',
    'order_date',
    'overwrite',
    '',
    NULL,
    'fail',
    1,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL,
    -- New fields for incremental synthetic data import support
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
),
(
    'synthetic_order_items_004',
    'Synthetic Data - Order Items (Retail OLTP Small)',
    'Files/synthetic_data/parquet/single/retail_oltp_small/order_items.parquet',
    'parquet',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_lakehouse_id}}',
    'lakehouse',
    'raw',
    'synthetic_order_items',
    NULL,
    NULL,
    NULL,
    NULL,
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    '',
    'order_id',
    'overwrite',
    '',
    NULL,
    'log',
    1,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL,
    -- New fields for incremental synthetic data import support
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
),
(
    'synthetic_customers_warehouse_001',
    'Synthetic Data - Customers (Warehouse)',
    'Files/synthetic_data/parquet/single/retail_oltp_small/customers.parquet',
    'parquet',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_wh_warehouse_id}}',
    'warehouse',
    'raw',
    'synthetic_customers',
    'staging_synthetic_customers',
    NULL,
    NULL,
    NULL,
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    '',
    'customer_id',
    'overwrite',
    '',
    NULL,
    'fail',
    2,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL,
    -- New fields for incremental synthetic data import support
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
),
(
    'synthetic_products_warehouse_002',
    'Synthetic Data - Products (Warehouse)',
    'Files/synthetic_data/parquet/single/retail_oltp_small/products.parquet',
    'parquet',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_wh_warehouse_id}}',
    'warehouse',
    'raw',
    'synthetic_products',
    'staging_synthetic_products',
    NULL,
    NULL,
    NULL,
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    '',
    'product_id',
    'overwrite',
    '',
    NULL,
    'log',
    2,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL,
    -- New fields for incremental synthetic data import support
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
),
(
    'synthetic_orders_warehouse_003',
    'Synthetic Data - Orders (Warehouse)',
    'Files/synthetic_data/parquet/single/retail_oltp_small/orders.parquet',
    'parquet',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_wh_warehouse_id}}',
    'warehouse',
    'raw',
    'synthetic_orders',
    'staging_synthetic_orders',
    NULL,
    NULL,
    NULL,
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    '',
    'order_date',
    'overwrite',
    '',
    NULL,
    'fail',
    2,
    'Y',
    '2024-01-15',
    NULL,
    'system',
    NULL
);

-- Insert additional complex CSV configuration examples
INSERT INTO config.config_flat_file_ingestion (
    config_id, config_name, source_file_path, source_file_format,
    target_workspace_id, target_datastore_id, target_datastore_type,
    target_schema_name, target_table_name, staging_table_name,
    file_delimiter, has_header, encoding, date_format, timestamp_format, schema_inference,
    -- Advanced CSV handling options
    quote_character, escape_character, multiline_values, ignore_leading_whitespace, ignore_trailing_whitespace,
    null_value, empty_value, comment_character, max_columns, max_chars_per_column,
    custom_schema_json, partition_columns, sort_columns, write_mode, merge_keys,
    data_validation_rules, error_handling_strategy, execution_group, active_yn,
    created_date, modified_date, created_by, modified_by,
    -- New fields for incremental synthetic data import support
    import_pattern, date_partition_format, table_relationship_group,
    batch_import_enabled, file_discovery_pattern, import_sequence_order,
    date_range_start, date_range_end, skip_existing_dates
) VALUES 
(
    'complex_csv_warehouse_001',
    'Complex CSV - Customer Comments with Newlines (Warehouse)',
    'Files/sample_data/complex_customer_feedback.csv',
    'csv',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_wh_warehouse_id}}',
    'warehouse',
    'raw',
    'customer_feedback_complex',
    'staging_customer_feedback_complex',
    ',',
    1,
    'utf-8',
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    -- Advanced CSV configuration for multiline and comments
    '"', '\', 1, 1, 1,
    'NULL', '', '#', 50, 10000,
    NULL, '', 'feedback_id', 'overwrite', '',
    NULL, 'fail', 5, 'N',
    '2024-01-15', NULL, 'system', NULL,
    -- New fields for incremental synthetic data import support
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
),
(
    'complex_csv_warehouse_002',
    'Complex CSV - Product Descriptions with Quotes (Warehouse)',
    'Files/sample_data/products_with_complex_descriptions.csv',
    'csv',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_wh_warehouse_id}}',
    'warehouse',
    'raw',
    'products_complex_descriptions',
    'staging_products_complex_descriptions',
    ',',
    1,
    'utf-8',
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    -- Advanced CSV configuration for handling quotes and special characters
    '"', '"', 1, 0, 0,
    '', '', NULL, 100, 50000,
    NULL, 'category', 'product_id', 'merge', 'product_id',
    NULL, 'log', 5, 'N',
    '2024-01-15', NULL, 'system', NULL,
    -- New fields for incremental synthetic data import support
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
);

-- Insert incremental synthetic data import configurations
-- These demonstrate the new date-partitioned import capabilities
INSERT INTO config.config_flat_file_ingestion (
    config_id, config_name, source_file_path, source_file_format,
    target_workspace_id, target_datastore_id, target_datastore_type,
    target_schema_name, target_table_name, staging_table_name,
    file_delimiter, has_header, encoding, date_format, timestamp_format,
    schema_inference, custom_schema_json, partition_columns, sort_columns,
    write_mode, merge_keys, data_validation_rules, error_handling_strategy,
    execution_group, active_yn, created_date, created_by,
    quote_character, escape_character, multiline_values, ignore_leading_whitespace,
    ignore_trailing_whitespace, null_value, empty_value, comment_character,
    max_columns, max_chars_per_column,
    import_pattern, date_partition_format, table_relationship_group,
    batch_import_enabled, file_discovery_pattern, import_sequence_order,
    date_range_start, date_range_end, skip_existing_dates
) VALUES 
(
    'retail_customers_incremental_wh',
    'Retail Customers Incremental Import (Warehouse)',
    'Files/synthetic_data/csv/series/retail_oltp_small/nested/snapshot_customers',
    'csv',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_wh_warehouse_id}}',
    'warehouse',
    'bronze',
    'customers',
    'customers_staging',
    ',',
    1,
    'utf-8',
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    'year_month',
    'customer_id',
    'merge',
    'customer_id',
    '{"required_columns": ["customer_id", "first_name", "last_name"]}',
    'log',
    10,
    'Y',
    GETDATE(),
    'admin',
    '"',
    '"',
    1,
    0,
    0,
    '',
    '',
    '#',
    100,
    50000,
    'date_partitioned',
    'YYYY/MM/DD',
    'retail_oltp',
    1,
    '**/customers/*.csv',
    1,
    '2024-01-01',
    '2024-01-30',
    1
),
(
    'retail_products_incremental_wh',
    'Retail Products Incremental Import (Warehouse)',
    'Files/synthetic_data/csv/series/retail_oltp_small/nested/snapshot_products',
    'csv',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_wh_warehouse_id}}',
    'warehouse',
    'bronze',
    'products',
    'products_staging',
    ',',
    1,
    'utf-8',
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    'category',
    'product_id',
    'merge',
    'product_id',
    '{"required_columns": ["product_id", "product_name", "category"]}',
    'log',
    10,
    'Y',
    GETDATE(),
    'admin',
    '"',
    '"',
    1,
    0,
    0,
    '',
    '',
    '#',
    100,
    50000,
    'date_partitioned',
    'YYYY/MM/DD',
    'retail_oltp',
    1,
    '**/products/*.csv',
    2,
    '2024-01-01',
    '2024-01-30',
    1
),
(
    'retail_orders_incremental_wh',
    'Retail Orders Incremental Import (Warehouse)',
    'Files/synthetic_data/csv/series/retail_oltp_small/nested/orders',
    'csv',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_wh_warehouse_id}}',
    'warehouse',
    'bronze',
    'orders',
    'orders_staging',
    ',',
    1,
    'utf-8',
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    'year_month',
    'order_id',
    'append',
    NULL,
    '{"required_columns": ["order_id", "customer_id", "order_date"]}',
    'log',
    11,
    'Y',
    GETDATE(),
    'admin',
    '"',
    '"',
    1,
    0,
    0,
    '',
    '',
    '#',
    100,
    50000,
    'date_partitioned',
    'YYYY/MM/DD',
    'retail_oltp',
    1,
    '**/orders/*.csv',
    4,
    '2024-01-01',
    '2024-01-30',
    0
),
(
    'retail_order_items_incremental_wh',
    'Retail Order Items Incremental Import (Warehouse)',
    'Files/synthetic_data/csv/series/retail_oltp_small/nested/order_items',
    'csv',
    '{{varlib:config_workspace_id}}',
    '{{varlib:config_wh_warehouse_id}}',
    'warehouse',
    'bronze',
    'order_items',
    'order_items_staging',
    ',',
    1,
    'utf-8',
    'yyyy-MM-dd',
    'yyyy-MM-dd HH:mm:ss',
    1,
    NULL,
    'year_month',
    'order_item_id',
    'append',
    NULL,
    '{"required_columns": ["order_item_id", "order_id", "product_id"]}',
    'log',
    11,
    'Y',
    GETDATE(),
    'admin',
    '"',
    '"',
    1,
    0,
    0,
    '',
    '',
    '#',
    100,
    50000,
    'date_partitioned',
    'YYYY/MM/DD',
    'retail_oltp',
    1,
    '**/order_items/*.csv',
    5,
    '2024-01-01',
    '2024-01-30',
    0
);

PRINT '✓ Inserted 13 sample configuration records (9 original + 4 incremental synthetic data)';
PRINT '✓ Includes incremental synthetic data import configurations demonstrating:';
PRINT '  - Snapshot tables (customers, products) with merge mode';
PRINT '  - Incremental tables (orders, order_items) with append mode';
PRINT '  - Date-partitioned processing with YYYY/MM/DD folder structure';
PRINT '  - All configurations use actual files from tmp/spark/Files/synthetic_data directory';

    """

    wu.execute_query(wu.get_connection(), sql)


du.run_once(work, "003_sample_data_insert", guid)


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## 📇 Print the execution log

# CELL ********************


du.print_log()


# METADATA ********************

# META {
# META   "language": "python"
# META }
# MARKDOWN ********************

# Add markdown content here

# ## ✔️ If we make it to the end return a successful result

# CELL ********************


notebookutils.notebook.exit("success")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
