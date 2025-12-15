# Export Framework

A configuration-driven data export framework for Microsoft Fabric that reads data from Lakehouse or Warehouse sources and writes to Lakehouse Files in various formats.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Configuration Schema](#configuration-schema)
- [Source Configuration](#source-configuration)
- [Extract Types](#extract-types)
- [File Formats](#file-formats)
- [Compression](#compression)
- [Placeholder Patterns](#placeholder-patterns)
- [Usage Examples](#usage-examples)
- [Advanced Features](#advanced-features)
- [State Management](#state-management)
- [Error Handling](#error-handling)

## Overview

The export framework reads data from Fabric Lakehouse tables or SQL Warehouse and writes to Lakehouse Files. It supports:

- Multiple source types (Lakehouse, Warehouse)
- Multiple extract types (full, incremental, period-based)
- Multiple file formats (CSV, Parquet, JSON)
- Multiple compression types (gzip, zipdeflate, snappy, etc.)
- File splitting by row count
- Trigger/control file creation
- Parallel execution within execution groups

## Architecture

```
┌─────────────────┐                      ┌──────────────────┐
│  Source Tables  │                      │  Lakehouse Files │
│   (Lakehouse    │  ──────────────────> │   (CSV, JSON,    │
│   or Warehouse) │                      │    Parquet)      │
└─────────────────┘                      └──────────────────┘
        ▲                                         │
        │                                         ▼
   SQL Query                               Trigger File
   (auto-generated                         (optional)
    or custom)
```

### Two Query Modes

| Mode | Behavior |
|------|----------|
| `source_table` | Framework builds query with auto-filtering based on extract type |
| `source_query` | User controls everything - framework just resolves `{placeholders}` |

## Configuration Schema

### `config_resource_export`

Central configuration table for exports. Each row represents one export definition.

**Primary Key**: (`export_group_name`, `export_name`)

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `export_group_name` | String | ✓ | Logical grouping of related exports (PK) | `"sales"` |
| `export_name` | String | ✓ | Unique identifier within group (PK) | `"daily_orders"` |
| `is_active` | Boolean | ✓ | Whether this export should be processed | `true` |
| `execution_group` | Integer | ✓ | Sequential execution order (group 1 before 2) | `1` |
| `source_type` | String | ✓ | Source datastore type | `"lakehouse"`, `"warehouse"` |
| `source_workspace` | String | ✓ | Workspace containing source data | `"DEV_WORKSPACE"` |
| `source_datastore` | String | ✓ | Lakehouse or Warehouse name | `"lh_gold"` |
| `source_schema` | String | | Schema name (required for warehouse, defaults to "dbo" for lakehouse) | `"dbo"` |
| `source_table` | String | † | Table to export (mutually exclusive with query) | `"orders"` |
| `source_query` | String | † | Custom SQL query (mutually exclusive with table) | `"SELECT * FROM..."` |
| `source_columns` | Array | | Columns to include (with source_table only) | `["id", "name"]` |
| `target_workspace` | String | ✓ | Workspace for output files | `"DEV_WORKSPACE"` |
| `target_lakehouse` | String | ✓ | Lakehouse for output files | `"lh_exports"` |
| `target_path` | String | ✓ | Path within Files/ | `"exports/sales/"` |
| `target_filename_pattern` | String | | Filename pattern with placeholders (default: `{export_name}_{timestamp}.{ext}`) | `"orders_{run_date:%Y%m%d}.csv"` |
| `file_format` | String | ✓ | Output format | `"csv"`, `"parquet"`, `"json"` |
| `compression` | String | | Compression type | `"gzip"`, `"zipdeflate"`, etc. |
| `compression_level` | Integer | | Compression level (format-specific) | `1`-`9` for gzip |
| `file_format_options` | Map<String,String> | | Spark write options (header, sep, quote, etc.) | `{"header": "true", "sep": "\|"}` |
| `max_rows_per_file` | Integer | | Split files at this row count | `1000000` |
| `trigger_file_pattern` | String | | Trigger file pattern (None = disabled) | `"orders_{run_date:%Y%m%d}.done"` |
| `extract_type` | String | | Extract type | `"full"`, `"incremental"`, `"period"` |
| `incremental_column` | String | | Column for watermark tracking | `"modified_date"` |
| `incremental_initial_watermark` | String | | Starting watermark for first run | `"2024-01-01"` |
| `period_filter_column` | String | | Column for period filtering (source_table) | `"order_date"` |
| `period_date_query` | String | | Query returning start_date, end_date | `"SELECT * FROM fn_GetPeriod('{run_date}')"` |
| `compressed_filename_pattern` | String | | Compressed file name pattern | `"orders_{run_date:%Y%m%d}.zip"` |
| `description` | String | | Human-readable description | `"Daily order export"` |

† One of `source_table` or `source_query` is required (mutually exclusive).

## Source Configuration

### Source Table Mode

Use `source_table` for straightforward table exports. The framework auto-generates queries with filtering.

```python
{
    "source_type": "lakehouse",
    "source_workspace": "DEV_WORKSPACE",
    "source_datastore": "lh_gold",
    "source_schema": "dbo",
    "source_table": "orders",
    "source_columns": ["order_id", "customer_id", "amount", "order_date"]
}
```

**Auto-filtering**: When using `source_table`, the framework automatically adds WHERE clauses based on extract type:

- **Incremental**: `WHERE {incremental_column} > '{watermark}'`
- **Period**: `WHERE {period_filter_column} BETWEEN '{start}' AND '{end}'`

### Source Query Mode

Use `source_query` for complex queries, joins, or custom filtering. The framework only resolves placeholders.

```python
{
    "source_type": "warehouse",
    "source_workspace": "DEV_WORKSPACE",
    "source_datastore": "wh_analytics",
    "source_query": """
        SELECT o.order_id, o.amount, c.customer_name
        FROM dbo.orders o
        JOIN dbo.customers c ON o.customer_id = c.customer_id
        WHERE o.order_date BETWEEN '{period_start_date:%Y-%m-%d}' AND '{period_end_date:%Y-%m-%d}'
    """
}
```

**Available placeholders in source_query**:
- `{run_date}` or `{run_date:format}` - Logical run date
- `{period_start_date}` or `{period_start_date:format}` - Period start
- `{period_end_date}` or `{period_end_date:format}` - Period end
- `{watermark}` - Last exported watermark value

## Extract Types

### Full Extract

Exports all data from source. No filtering applied.

```python
{
    "extract_type": "full"
}
```

### Incremental Extract

Tracks watermark and exports only new/changed records.

```python
{
    "extract_type": "incremental",
    "incremental_column": "modified_date",
    "incremental_initial_watermark": "2024-01-01T00:00:00"  # Optional: starting point
}
```

**Behavior**:
1. First run: No watermark → exports all data (or from `incremental_initial_watermark`)
2. After export: Framework stores max value of `incremental_column`
3. Next run: Filters `WHERE {incremental_column} > '{watermark}'`

### Period Extract

Uses a query to determine date range, then filters by that range.

**With source_table** (auto-filtering):
```python
{
    "extract_type": "period",
    "source_table": "orders",
    "period_filter_column": "order_date",
    "period_date_query": "SELECT start_date, end_date FROM dbo.fn_GetFiscalPeriod('{run_date:%Y-%m-%d}')"
}
```

**With source_query** (user controls filtering):
```python
{
    "extract_type": "period",
    "source_query": """
        SELECT * FROM orders
        WHERE order_date BETWEEN '{period_start_date:%Y-%m-%d}' AND '{period_end_date:%Y-%m-%d}'
    """,
    "period_date_query": "SELECT start_date, end_date FROM dbo.fn_GetFiscalPeriod('{run_date:%Y-%m-%d}')"
}
```

**Note**: `period_date_query` must return columns named `start_date` and `end_date`.

## File Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| `csv` | `.csv` | Comma-separated values |
| `parquet` | `.parquet` | Columnar binary format |
| `json` | `.json` | JSON records |

### file_format_options

All file format options go in the `file_format_options` map. These are passed directly to Spark's DataFrameWriter.

**Common CSV options:**

| Spark Option | Description | Example |
|--------------|-------------|---------|
| `header` | Include header row | `"true"`, `"false"` |
| `sep` | Field delimiter | `","`, `"\|"`, `"\t"` |
| `quote` | Quote character | `"\""` |
| `escape` | Escape character | `"\\"` |
| `nullValue` | NULL representation | `""` |
| `lineSep` | Line separator | `"\n"`, `"\r\n"` |
| `encoding` | Character encoding | `"UTF-8"` |

### Example: CSV with pipe delimiter

```python
{
    "file_format": "csv",
    "file_format_options": {
        "header": "true",
        "sep": "|",
        "quote": "\"",
        "nullValue": ""
    }
}
```

### Example: CSV without header, Windows line endings

```python
{
    "file_format": "csv",
    "file_format_options": {
        "header": "false",
        "sep": ",",
        "lineSep": "\r\n"
    }
}
```

## Compression

| Type | Extension | Levels | Notes |
|------|-----------|--------|-------|
| `none` | (none) | - | No compression |
| `gzip` | `.csv.gz` | 1-9 | Standard gzip |
| `zipdeflate` | `.zip` | 0-9 | ZIP archive with DEFLATE |
| `zip` | `.zip` | 0-9 | Same as zipdeflate |
| `snappy` | (embedded) | - | Parquet only (default) |
| `lz4` | (embedded) | - | Parquet/binary formats |
| `brotli` | `.br` | 0-11 | High compression ratio |

### Compression Modes

**Spark-native compression** (snappy, lz4 for Parquet):
- Applied during write
- Single compressed file

**Post-process compression** (gzip, zipdeflate):
- Framework writes uncompressed first
- Compresses file after write
- Supports custom compressed filename pattern

### Example: GZIP with level 6

```python
{
    "file_format": "csv",
    "compression": "gzip",
    "compression_level": 6,
    "file_format_options": {
        "header": "true",
        "sep": ","
    },
    "target_filename_pattern": "orders_{run_date:%Y%m%d}.csv"
    # Output: orders_20251212.csv.gz
}
```

### Example: ZIP with custom filename

```python
{
    "file_format": "csv",
    "compression": "zipdeflate",
    "file_format_options": {
        "header": "true",
        "sep": "|"
    },
    "target_filename_pattern": "orders_{run_date:%Y%m%d}.csv",
    "compressed_filename_pattern": "A0_ORDERS_{run_date:%Y%m%d}.zip"
}
```

## Placeholder Patterns

Placeholders are used in filename patterns, target paths, trigger file patterns, and source queries.

### Available Placeholders

| Placeholder | Description | Example |
|-------------|-------------|---------|
| `{run_date}` | Logical date for the export (ISO format). Defaults to today but can be overridden for backfills. | `2025-12-12 00:00:00` |
| `{run_date:format}` | Run date with strftime format | `{run_date:%Y%m%d}` → `20251212` |
| `{process_date}` | Actual time the pipeline ran, in the configured timezone | `2025-12-12 14:30:22` |
| `{process_date:format}` | Process date with format | `{process_date:%H%M%S}` → `143022` |
| `{period_start_date}` | Period start date (from `period_date_query`) | `2025-12-01 00:00:00` |
| `{period_start_date:format}` | Period start with format | `{period_start_date:%Y%m%d}` |
| `{period_end_date}` | Period end date (from `period_date_query`) | `2025-12-31 00:00:00` |
| `{period_end_date:format}` | Period end with format | `{period_end_date:%Y%m%d}` |
| `{export_name}` | Export name from config | `daily_orders` |
| `{run_id}` | First 8 chars of export run UUID | `a1b2c3d4` |
| `{watermark}` | Last watermark value (for incremental exports) | `2025-12-11T23:59:59` |
| `{part}` | Part number for split files | `1`, `2`, `3` |

**Note**: Both `run_date` and `process_date` use the `timezone` parameter configured in `process_exports()`. Use `run_date` for logical/business dates in filenames. Use `process_date` when you need the actual execution timestamp.

### Format Specifiers

Uses Python strftime format codes:

| Code | Output | Example |
|------|--------|---------|
| `%Y` | 4-digit year | `2025` |
| `%m` | 2-digit month | `12` |
| `%d` | 2-digit day | `12` |
| `%H` | 2-digit hour (24h) | `14` |
| `%M` | 2-digit minute | `30` |
| `%S` | 2-digit second | `22` |
| `%Y%m%d` | Date compact | `20251212` |
| `%Y-%m-%d` | Date ISO | `2025-12-12` |

### Examples

```python
# Filename with date
"target_filename_pattern": "orders_{run_date:%Y%m%d}.csv"
# → orders_20251212.csv

# Filename with period dates
"target_filename_pattern": "orders_{period_start_date:%Y%m%d}_{period_end_date:%Y%m%d}.csv"
# → orders_20251201_20251231.csv

# Trigger file
"trigger_file_pattern": "A0_ORDERS_{process_date:%Y%m%d%H%M%S}.done"
# → A0_ORDERS_20251212143022.done

# Dynamic target path
"target_path": "exports/{run_date:%Y}/{run_date:%m}/"
# → exports/2025/12/
```

## Usage Examples

### Example 1: Basic Export

```python
from ingen_fab.python_libs.pyspark.export.common.config_manager import ConfigExportManager
from ingen_fab.python_libs.pyspark.export.export_logger import ExportLogger
from ingen_fab.python_libs.pyspark.export.export_orchestrator import ExportOrchestrator

# Initialize components
config_mgr = ConfigExportManager(config_lakehouse=config_lakehouse, spark=spark)
export_logger = ExportLogger(config_lakehouse, auto_create_tables=True)
orchestrator = ExportOrchestrator(
    spark=spark,
    export_logger=export_logger,
    max_concurrency=4
)

# Load export configs
export_configs = config_mgr.get_configs(
    export_group_name="sales",
    export_name="daily_orders"  # Optional: filter to specific export
)

# Execute exports
import uuid
execution_id = str(uuid.uuid4())
results = orchestrator.process_exports(
    configs=export_configs,
    execution_id=execution_id,
    run_date="2025-12-12",  # Optional: defaults to today in timezone
    timezone="Australia/Sydney"  # Optional: timezone for run_date and process_date
)

# Check results
if not results['success']:
    failed = [r['export_name'] for r in results['results'] if r['status'] == 'error']
    raise Exception(f"Export failed for: {failed}")

print(f"Exported {results['successful']} of {results['total_exports']} configs")
```

### Example 2: Retry Failed Exports

```python
results = orchestrator.process_exports(
    configs=export_configs,
    execution_id=execution_id,
    is_retry=True  # Only process exports that failed previously
)
```

### Example 3: Period Export Configuration

```python
{
    "export_group_name": "finance",
    "export_name": "fiscal_transactions",
    "is_active": True,
    "execution_group": 1,

    "source_type": "warehouse",
    "source_workspace": "PROD_WORKSPACE",
    "source_datastore": "wh_finance",
    "source_schema": "dbo",
    "source_table": "transactions",

    "extract_type": "period",
    "period_filter_column": "transaction_date",
    "period_date_query": "SELECT start_date, end_date FROM dbo.fn_GetFiscalMonth('{run_date:%Y-%m-%d}')",

    "target_workspace": "PROD_WORKSPACE",
    "target_lakehouse": "lh_exports",
    "target_path": "finance/fiscal_transactions/{run_date:%Y}/",
    "target_filename_pattern": "transactions_{period_start_date:%Y%m%d}_{period_end_date:%Y%m%d}.csv",

    "file_format": "csv",
    "file_format_options": {
        "header": "true",
        "sep": "|"
    },
    "compression": "zipdeflate",
    "compressed_filename_pattern": "A0_TRANS_{period_start_date:%Y%m%d}.zip",
    "trigger_file_pattern": "A0_TRANS_{period_start_date:%Y%m%d}.done"
}
```

### Example 4: Large File with Splitting

```python
{
    "export_group_name": "analytics",
    "export_name": "events",
    "is_active": True,
    "execution_group": 1,

    "source_type": "lakehouse",
    "source_workspace": "PROD_WORKSPACE",
    "source_datastore": "lh_events",
    "source_table": "user_events",

    "extract_type": "incremental",
    "incremental_column": "event_timestamp",

    "target_workspace": "PROD_WORKSPACE",
    "target_lakehouse": "lh_exports",
    "target_path": "events/",
    "target_filename_pattern": "events_{run_date:%Y%m%d}.csv",

    "file_format": "csv",
    "file_format_options": {
        "header": "true",
        "sep": ","
    },
    "max_rows_per_file": 1000000,  # Split at 1M rows
    "compression": "zipdeflate",  # Split files bundled into single archive
    "compressed_filename_pattern": "events_{run_date:%Y%m%d}.zip"
}
```

## Advanced Features

### File Splitting

When `max_rows_per_file` is set and row count exceeds the limit:

1. DataFrame is split into chunks
2. Each chunk written as `{filename}_part0001.csv`, `{filename}_part0002.csv`, etc.
3. If compression is enabled, all parts are bundled into a single archive

### Trigger Files

Create empty trigger/control files after successful export:

```python
"trigger_file_pattern": "A0_ORDERS_{run_date:%Y%m%d}.done"
```

Trigger files are written to the same directory as data files.

### Execution Groups

Control processing order across exports:

```python
# Group 1 processes first
{"export_name": "dim_customers", "execution_group": 1}
{"export_name": "dim_products", "execution_group": 1}

# Group 2 processes after group 1 completes
{"export_name": "fact_orders", "execution_group": 2}
```

Within each group, exports run in parallel up to `max_concurrency`.

## State Management

### Log Tables

**`log_resource_export`**: Tracks export run metadata
- `export_run_id`, `master_execution_id`, `export_group_name`, `export_name`
- Status (`export_state`): `pending`, `running`, `success`, `warning`, `error`
- Source info: `source_type`, `source_workspace`, `source_datastore`, `source_table`
- Target info: `target_path`, `file_format`, `compression`
- Extract parameters: `watermark_value`, `period_start_date`, `period_end_date`
- Timing: `started_at`, `completed_at`, `duration_ms`
- Metrics: `rows_exported`, `files_created`, `total_bytes`
- Output: `file_paths`, `trigger_file_path`
- Error handling: `error_message`
- Audit: `updated_at`

**`log_resource_export_watermark`**: Tracks incremental watermarks
- `export_group_name`, `export_name`, `incremental_column`
- `watermark_value`, `updated_at`, `updated_by_run_id`

### Watermark Tracking

For incremental exports:

1. Before export: Query `log_resource_export_watermark` for last value
2. After successful export: Calculate max value from exported data
3. Update watermark in log table

```sql
-- Query watermarks
SELECT export_group_name, export_name, watermark_value, updated_at
FROM log_resource_export_watermark
WHERE export_group_name = 'sales';
```

## Error Handling

### Error States

| Status | Description |
|--------|-------------|
| `pending` | Export queued, not yet started |
| `running` | Export in progress |
| `success` | Export completed successfully |
| `warning` | Export completed with warnings |
| `error` | Export failed |

### Common Errors

**SourceReadError**: Failed to read from source
- Check source workspace/datastore accessibility
- Verify query syntax for `source_query` mode
- Check column names in `source_columns`

**Compression Errors**: Failed to compress file
- Verify compression_level is valid for compression type
- Check disk space in staging area

**Write Errors**: Failed to write output
- Check target_lakehouse accessibility
- Verify target_path exists or can be created

### Retry Strategy

Use `is_retry=True` to reprocess only failed exports:

```python
# Initial run
results = orchestrator.process_exports(configs=configs, execution_id=exec_id)

# Retry failed exports with same execution_id
if not results['success']:
    retry_results = orchestrator.process_exports(
        configs=configs,
        execution_id=exec_id,
        is_retry=True
    )
```
