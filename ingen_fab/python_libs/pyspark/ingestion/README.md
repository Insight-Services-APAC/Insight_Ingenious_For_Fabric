# Ingestion Framework

A configuration-driven, two-phase data ingestion framework for Microsoft Fabric that extracts data from external sources and loads it into Delta tables with validation and state management.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Configuration Table Schema](#configuration-table-schema)
- [Metadata Columns](#metadata-columns)
- [Extraction Framework](#extraction-framework)
- [Loading Framework](#loading-framework)
- [Key Configuration Options](#key-configuration-options)
- [Usage Examples](#usage-examples)
- [Advanced Features](#advanced-features)
- [State Management](#state-management)
- [Error Handling](#error-handling)

## Overview

The framework has two phases:

- **Extraction**: Move data from external sources (file systems) to raw storage
- **Loading**: Validate and load data from raw storage to Delta tables

## Architecture

### Two-Phase Pattern

```
┌─────────────┐     Extraction      ┌──────────────┐      Loading       ┌──────────────┐
│   External  │ ──────────────────> │  Raw Storage │ ─────────────────> │    Target    │
│   Sources   │  (FileSystemEx.)    │   (OneLake)  │   (LoadOrchest.)   │    Tables    │
└─────────────┘                     └──────────────┘                    └──────────────┘
  Inbound files                      ABFSS paths                         Delta tables
                                     Hive partitions                     with metadata
                                     State-tracked
```

### Loading: Two-Step Validation

The loading framework uses a two-step approach:

```
Step 1: Files → Staging Table
  - Read raw files (CSV, Parquet)
  - All columns cast to STRING
  - Structural validation only (PERMISSIVE mode)
  - Add _stg_* metadata columns
  - Write to staging Delta table

Step 2: Staging → Target Table
  - Read from staging table
  - Type casting with try_cast()
  - Business validation (duplicates, corruption limits)
  - Drop _stg_* columns, add _raw_* metadata
  - Write to target (merge/append/overwrite)
```

### State Tracking

All execution state is tracked in Delta tables:

- **Extraction**: `log_resource_extract_config`, `log_resource_extract_batch`
- **Loading**: `log_resource_execution`, `log_batch_load`
- **Load State**: `pending` → `processing` → `completed`/`failed`/`rejected`

## Configuration Table Schema

### `config_resource_ingestion`

Central configuration table that drives all ingestion processes. Each row represents one data resource.

**Primary Key**: (`source_name`, `resource_name`)

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `source_name` | String | ✓ | Logical grouping of related resources (PK) | `"pe"` |
| `resource_name` | String | ✓ | Unique identifier within source (PK) | `"pe_prom_codes"` |
| `source_type` | String | ✓ | Type of source to extract from | `"filesystem"` |
| `source_connection_params` | Map<String,String> | ✓ | Connection details for the source | `{"workspace_name": "DEV", "lakehouse_name": "inbound"}` |
| `source_extraction_params` | Map<String,String> | ✓ | Source-specific extraction parameters (see [Extraction Params](#extraction-parameters)) | `{"inbound_path": "Files/landing/pe/", "discovery_pattern": "*.dat"}` |
| `extract_path` | String | ✓ | Where extracted files land | `"Files/raw/pe/pe_prom_codes/"` |
| `extract_error_path` | String | ✓ | Where rejected files are moved | `"Files/error/pe/pe_prom_codes/"` |
| `extract_file_format` | String | ✓ | File format in raw layer | `"csv"`, `"parquet"` |
| `extract_storage_workspace` | String | ✓ | Workspace containing raw files | `"EDAA_INBOUND_DEV_017"` |
| `extract_storage_lakehouse` | String | ✓ | Lakehouse containing raw files | `"lh_storage"` |
| `stg_table_workspace` | String | ✓ | Workspace for staging table | `"EDAA_INBOUND_DEV_017"` |
| `stg_table_lakehouse` | String | ✓ | Lakehouse for staging table | `"lh_bronze"` |
| `stg_table_schema` | String | | Schema name for staging table | `"staging"` |
| `stg_table_name` | String | ✓ | Staging table name | `"stg_pe_prom_codes"` |
| `stg_table_write_mode` | String | | Write mode for staging: `append`, `overwrite` | `"overwrite"` |
| `stg_table_partition_columns` | Array<String> | ✓ | Partition columns for staging table | `["ds"]` |
| `target_workspace` | String | ✓ | Workspace for target table | `"EDAA_INBOUND_DEV_017"` |
| `target_lakehouse` | String | ✓ | Lakehouse for target table | `"lh_bronze"` |
| `target_schema` | String | | Schema name for target table | `"bronze"` |
| `target_table` | String | ✓ | Target table name | `"pe_prom_codes"` |
| `target_schema_columns` | Array<Struct> | ✓ | Schema definition: `[{"column_name": "id", "data_type": "integer"}, ...]` | `[{"column_name": "promo_code", "data_type": "string"}]` |
| `target_write_mode` | String | ✓ | Write mode: `merge`, `append`, `overwrite` | `"merge"` |
| `target_merge_keys` | Array<String> | | Columns to merge on (required for merge mode) | `["promo_code_id"]` |
| `target_partition_columns` | Array<String> | | Partition columns for target table | `["year", "month"]` |
| `target_soft_delete_enabled` | Boolean | | If true, DELETEs become `UPDATE SET _raw_is_deleted=True` | `true`, `false` |
| `target_cdc_config` | Struct | | CDC operation mapping (see [CDC](#cdc-processing)) | `{"operation_column": "op", "insert_values": ["c", "r"], ...}` |
| `target_load_type` | String | ✓ | Load type: `incremental` or `full` | `"incremental"`, `"full"` |
| `target_max_corrupt_records` | Integer | | Max corrupt records allowed (0 = reject any) | `0`, `100` |
| `target_fail_on_rejection` | Boolean | | If false, skip rejected batches instead of failing | `true`, `false` |
| `execution_group` | Integer | ✓ | Sequential execution order (group 1 before group 2) | `1`, `2`, `3` |
| `active` | Boolean | ✓ | Whether this resource should be processed | `true`, `false` |
| `created_at` | Timestamp | | When this config was created | `2025-11-17 10:30:00` |
| `updated_at` | Timestamp | | When this config was last updated | `2025-11-17 14:20:00` |
| `created_by` | String | | Who created this config | `"user@company.com"` |
| `updated_by` | String | | Who last updated this config | `"user@company.com"` |

### Extraction Parameters

The `source_extraction_params` field contains source-type specific parameters stored as Map<String,String>:

#### FileSystem Source

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `inbound_path` | String | Source file location | `"Files/landing/pe/"` |
| `discovery_pattern` | String | File pattern to match | `"*.csv"`, `"*.DAT"` |
| `recursive` | Boolean | Search subdirectories | `"true"`, `"false"` |
| `batch_by` | String | Batching strategy: `file` (one batch per file), `folder` (one batch per folder), `all` (one batch for all files) | `"file"` |
| `has_header` | Boolean | CSV has header row | `"true"` |
| `file_delimiter` | String | CSV delimiter | `","`, `"\|"` |
| `encoding` | String | File encoding | `"utf-8"` |
| `quote_character` | String | CSV quote character | `"\""` |
| `escape_character` | String | CSV escape character | `"\\\\"` |
| `multiline_values` | Boolean | Support multiline CSV values | `"true"`, `"false"` |
| `null_value` | String | String to treat as NULL | `""`, `" "` |
| `control_file_pattern` | String | Control file pattern (`{basename}` supported) | `"{basename}.CTL"` |
| `duplicate_handling` | String | How to handle duplicates: `fail`, `skip`, `allow` | `"skip"` |
| `require_files` | Boolean | Alert if no files found (doesn't fail pipeline) | `"true"`, `"false"` |
| `filename_metadata` | Array | Extract metadata from filenames | `[{"name": "file_date", "regex": "...", "type": "date", "format": "yyyyMMdd"}]` |
| `sort_by` | Array | Sort files by metadata fields | `["file_date"]` |
| `sort_order` | String | Sort direction | `"asc"`, `"desc"` |
| `raw_partition_columns` | Array | Hive partition structure for raw layer | `["ds"]`, `["year", "month", "day"]` |

## Metadata Columns

The framework adds metadata columns at two stages:

### Staging Table Metadata (`_stg_*` prefix)

Added in **Step 1** (Files → Staging):

| Column | Type | Description |
|--------|------|-------------|
| `_stg_created_load_id` | String | Batch ID that created this staging record |
| `_stg_file_path` | String | Source file path |
| `_stg_created_at` | Timestamp | When record was loaded to staging |
| `_stg_corrupt_record` | String | Raw corrupt record data (if any) from PERMISSIVE mode |

### Target Table Metadata (`_raw_*` prefix)

Added in **Step 2** (Staging → Target):

| Column | Type | Description |
|--------|------|-------------|
| `_raw_created_load_id` | String | Batch ID that **first created** this record (immutable on updates) |
| `_raw_updated_load_id` | String | Batch ID that **last updated** this record (updated on every load) |
| `_raw_file_path` | String | Source file path |
| `_raw_loaded_at` | Timestamp | When record was loaded (deprecated) |
| `_raw_corrupt_record` | String | Corrupt record tracking (deprecated) |
| `_raw_created_at` | Timestamp | When record was **first created** (immutable on merge) |
| `_raw_updated_at` | Timestamp | When record was **last updated** (updated on every load) |
| `_raw_is_deleted` | Boolean | Soft delete flag (if `target_soft_delete_enabled=True`) |
| `_raw_filename` | String | Original filename |

**Lineage Tracking:**
- `_raw_created_load_id` + `_raw_created_at`: Track which batch created the record (never changes)
- `_raw_updated_load_id` + `_raw_updated_at`: Track which batch last touched the record (updates on every load)

## Extraction Framework

### Purpose

Move data from external sources (inbound file systems) to raw storage in OneLake with batch tracking and state management.

### Components

- **`ExtractionOrchestrator`**: Manages execution groups, parallel processing, state tracking
- **`FileSystemExtractor`**: Extracts files from ABFSS file systems
- **`ExtractionLogger`**: Logs extraction events to Delta tables

### Supported Sources

- **filesystem**: ABFSS file systems (OneLake, ADLS)

### Key Features

- **Execution Groups**: Process resources sequentially by group, in parallel within group
- **Duplicate Detection**: Tracks processed files in `log_resource_extract_batch`
- **Batch Tracking**: Each extraction batch gets unique `extract_batch_id`
- **File Management**: Files moved from inbound → raw with Hive partitioning

### State Tracking

**`log_resource_extract_config`**: Tracks resource-level extraction runs
- `extract_run_id`, `execution_id`, `source_name`, `resource_name`
- Status: `pending`, `running`, `completed`, `failed`, `no_data`

**`log_resource_extract_batch`**: Tracks individual batch extractions
- `extract_batch_id`, `extract_run_id`, `source_path`, `extract_file_paths`
- `load_state`: Drives loading framework (`pending` → picked up by loader)

## Loading Framework

### Purpose

Load and validate data from raw storage to Delta tables.

### Components

- **`LoadingOrchestrator`**: Manages execution, discovery, state tracking
- **`FileLoader`**: Handles two-step file loading and validation
- **`LoadingLogger`**: Logs loading events to Delta tables

### Two-Step Loading Process

#### Step 1: Files → Staging Table

**Purpose**: Structural validation only, preserve all data

```python
# What happens:
1. Read files from raw storage using config schema (all columns as STRING)
2. Use Spark PERMISSIVE mode to capture corrupt rows in _stg_corrupt_record
3. Add staging metadata columns (_stg_created_load_id, _stg_file_path, _stg_created_at)
4. Write to staging Delta table
5. NO type validation, NO business rules, NO rejection
```

**Result**: All data preserved in staging table, ready for Step 2

#### Step 2: Staging → Target Table

**Purpose**: Type casting, business validation, write to target

```python
# What happens:
1. Read from staging table (filter by batch_id + partition)
2. Trim all string columns (removes leading/trailing whitespace for better merge key matching)
3. Type casting using try_cast() - identifies rows with cast failures
4. Validation checks:
   - Structural corruption (from Step 1's _stg_corrupt_record)
   - Type casting errors (columns that failed try_cast)
   - Duplicate records (on merge_keys)
5. Rejection handling:
   - if target_fail_on_rejection=True: Fail batch, file moved to error path
   - if target_fail_on_rejection=False: Filter bad records, continue
6. Add target metadata (_raw_created_load_id, _raw_updated_load_id, timestamps, etc.)
7. Write to target table (merge/append/overwrite)
```

**Result**: Validated, typed data in target table

### Key Features

- **Multi-Layer Validation**:
  - Structural: Malformed rows detected in Step 1
  - Type Casting: Invalid data types detected in Step 2
  - Business Rules: Duplicates, corrupt record limits
- **Batch Discovery**: Queries `log_resource_extract_batch.load_state='pending'`
- **Crash Recovery**: Automatically recovers stale batches stuck in `processing` state
- **Write Modes**: merge (upsert), append, overwrite
- **CDC Support**: Map operation columns to INSERT/UPDATE/DELETE
- **Soft Deletes**: DELETEs become `UPDATE SET _raw_is_deleted=True`
- **Full Load Deletes**: Mark missing records as deleted in snapshot loads

### State Tracking

**`log_resource_execution`**: Tracks resource-level loading runs
- `load_run_id`, `execution_id`, `source_name`, `resource_name`
- Status: `pending`, `running`, `completed`, `failed`, `no_data`
- Aggregated metrics

**`log_batch_load`**: Tracks individual batch loads
- `load_batch_id`, `load_run_id`, `extract_batch_id`
- Status: `pending`, `processing`, `completed`, `failed`, `rejected`
- Detailed metrics (rows inserted/updated/deleted, duration, etc.)

## Key Configuration Options

### `target_write_mode`

How to write data to the target table:

- **`merge`**: Upsert based on `target_merge_keys` (update if exists, insert if new)
- **`append`**: Always insert new rows (no deduplication)
- **`overwrite`**: Replace entire table/partition with new data

### `target_merge_keys`

Columns that uniquely identify a record for merge operations:

```python
"target_merge_keys": ["customer_id", "transaction_id"]
```

**Required when**: `target_write_mode = "merge"`

### `target_soft_delete_enabled`

Control delete behavior:

- **`true`**: DELETE operations become `UPDATE SET _raw_is_deleted=True` (soft delete)
- **`false`**: DELETE operations physically remove rows (hard delete)

**Use Cases**:
- Audit trails: Keep deleted records with `_raw_is_deleted=True`
- Compliance: Hard delete for GDPR/data retention policies

### `target_cdc_config`

Map CDC operation columns to INSERT/UPDATE/DELETE operations:

```python
# Debezium CDC format
"target_cdc_config": {
    "operation_column": "op",
    "insert_values": ["c", "r"],  # c=create, r=read (snapshot)
    "update_values": ["u"],
    "delete_values": ["d"]
}

# SQL Server CDC format
"target_cdc_config": {
    "operation_column": "__$operation",
    "insert_values": ["2"],  # 2=insert
    "update_values": ["4"],  # 4=update
    "delete_values": ["1"]   # 1=delete
}
```

### `target_load_type`

Handle full/snapshot loads vs incremental loads:

- **`incremental`** (default): Only process records in the batch (don't infer deletes)
- **`full`**: Each batch is a complete snapshot - mark missing records as deleted

**Example Use Case**: Daily full file from vendor
```python
"target_load_type": "full",
"target_soft_delete_enabled": true,  # Required for full load deletes
"target_write_mode": "merge"
```

**What happens**: Records in target but NOT in today's file → `_raw_is_deleted=True`

### `target_fail_on_rejection`

Control behavior when validation fails:

- **`true`** (default): Fail entire resource, file stays in landing for manual fix
- **`false`**: Skip rejected batch, filter corrupt records, continue processing

**Use Cases**:
- `true`: Critical data requiring manual review
- `false`: Best-effort loading, tolerate data quality issues

### `execution_group`

Control processing order:

```python
# Group 1 processes first (sequentially within group, parallel across resources)
Resource A: execution_group=1
Resource B: execution_group=1

# Group 2 processes after Group 1 completes
Resource C: execution_group=2
```

**Use Cases**: Enforce dependencies (e.g., load dimension tables before fact tables)

## Usage Examples

### Example 1: Extraction

```python
from ingen_fab.python_libs.pyspark.ingestion.config_manager import ConfigIngestionManager
from ingen_fab.python_libs.pyspark.ingestion.extraction_logger import ExtractionLogger
from ingen_fab.python_libs.pyspark.ingestion.extraction_orchestrator import ExtractionOrchestrator

# Initialize components
config_mgr = ConfigIngestionManager(lakehouse_utils_instance=config_lakehouse)
extraction_logger = ExtractionLogger(
    lakehouse_utils_instance=config_lakehouse,
    auto_create_tables=True  # Create log tables if they don't exist
)
orchestrator = ExtractionOrchestrator(
    spark=spark,
    extraction_logger=extraction_logger,
    max_concurrency=10  # Process up to 10 resources in parallel per execution group
)

# Load resource configs
resource_configs = config_mgr.get_configs(
    source_name="pe",           # Optional: filter by source
    resource_name="pe_prom_codes"  # Optional: filter by specific resource
)

# Extract data
import uuid
execution_id = str(uuid.uuid4())
results = orchestrator.process_resources(
    configs=resource_configs,
    execution_id=execution_id
)

# Check results
print(f"Successful: {results['successful']}")
print(f"Failed: {results['failed']}")
print(f"No Data: {results['no_data']}")
print(f"Missing Required Files: {results['missing_required_files']}")

# Fail on system errors
if results['failed'] > 0:
    raise Exception(f"Extraction failed for {results['failed']} resource(s)")

# Alert on missing required files (doesn't fail pipeline)
if results['missing_required_files'] > 0:
    send_alert(f"Expected files not found for {results['missing_required_files']} resource(s)")
```

### Example 2: Loading

```python
from ingen_fab.python_libs.pyspark.ingestion.config_manager import ConfigIngestionManager
from ingen_fab.python_libs.pyspark.ingestion.loading_logger import LoadingLogger
from ingen_fab.python_libs.pyspark.ingestion.loading_orchestrator import LoadingOrchestrator

# Initialize components
config_mgr = ConfigIngestionManager(lakehouse_utils_instance=config_lakehouse)
loading_logger = LoadingLogger(
    config_lakehouse,
    auto_create_tables=True  # Create log tables if they don't exist
)
orchestrator = LoadingOrchestrator(
    spark=spark,
    logger_instance=loading_logger,
    max_concurrency=10  # Process up to 10 resources in parallel per execution group
)

# Load resource configs
resource_configs = config_mgr.get_configs(
    source_name="pe",
    resource_name="pe_prom_codes"
)

# Load data
import uuid
execution_id = str(uuid.uuid4())
results = orchestrator.process_resources(
    configs=resource_configs,
    execution_id=execution_id
)

# Check results
print(f"Successful: {results['successful']}")
print(f"Failed: {results['failed']}")
print(f"No Data: {results['no_data']}")
print(f"Rejected Batches: {results['rejected_batches']}")

# Fail on system errors
if results['failed'] > 0:
    raise Exception(f"Loading failed for {results['failed']} resource(s)")

# Alert on rejected batches (doesn't fail pipeline)
if results['rejected_batches'] > 0:
    send_alert(f"{results['rejected_batches']} batches rejected - investigate data quality")
```

### Example 3: Custom Metadata Column Names

```python
# Override default metadata column names (dbt-style)
orchestrator = LoadingOrchestrator(
    spark=spark,
    logger_instance=loading_logger,
    metadata_columns={
        "_raw_created_load_id": "batch_id",
        "_raw_updated_load_id": "last_batch_id",
        "_raw_created_at": "created_ts",
        "_raw_updated_at": "modified_ts",
        "_raw_is_deleted": "is_deleted"
    }
)
```

## Advanced Features

### CDC Processing

The framework supports Change Data Capture (CDC) from various sources:

```python
# Configure CDC in config_resource_ingestion
"target_cdc_config": {
    "operation_column": "op",      # Column containing operation type
    "insert_values": ["c", "r"],   # Values meaning INSERT
    "update_values": ["u"],        # Values meaning UPDATE
    "delete_values": ["d"]         # Values meaning DELETE
}
```

**How it works**:
1. Loader reads `operation_column` from each row
2. Maps value to INSERT/UPDATE/DELETE operation
3. Executes appropriate merge logic
4. If `target_soft_delete_enabled=True`, DELETEs become soft deletes

### Full Load Deletes

For snapshot/full file loads where each batch represents the complete dataset:

```python
# Configure full load with delete inference
"target_load_type": "full",           # Each batch is complete snapshot
"target_soft_delete_enabled": true,   # Required for full load deletes
"target_write_mode": "merge",
"target_merge_keys": ["id"]
```

**What happens**:
1. Load today's full file (100 records)
2. Compare to target table (110 records)
3. Find missing records (10 records not in today's file)
4. Mark missing records: `_raw_is_deleted=True`, `_raw_updated_load_id=<current_batch>`
5. Merge remaining records normally

**Use Cases**:
- Daily vendor files with full refreshes

### Batch Lineage Tracking

Track which batches created vs updated each record:

```sql
-- Find records created by a specific batch
SELECT * FROM bronze.customers
WHERE _raw_created_load_id = '550e8400-e29b-41d4-a716-446655440000';

-- Find records updated by a specific batch
SELECT * FROM bronze.customers
WHERE _raw_updated_load_id = '550e8400-e29b-41d4-a716-446655440000'
  AND _raw_created_load_id != _raw_updated_load_id;  -- Exclude new inserts

-- Find records created today but updated yesterday
SELECT * FROM bronze.customers
WHERE DATE(_raw_created_at) = CURRENT_DATE()
  AND DATE(_raw_updated_at) = CURRENT_DATE() - INTERVAL 1 DAY;
```

### Three-Tier Storage Architecture

Raw storage, staging table, and target table can all be in different lakehouses:

```python
# Raw files in storage lakehouse
"extract_storage_workspace": "EDAA_INBOUND_DEV",
"extract_storage_lakehouse": "lh_storage",

# Staging table in bronze lakehouse (schema 'staging')
"stg_table_workspace": "EDAA_INBOUND_DEV",
"stg_table_lakehouse": "lh_bronze",
"stg_table_schema": "staging",
"stg_table_name": "stg_pe_prom_codes",

# Target table in bronze lakehouse (schema 'bronze')
"target_workspace": "EDAA_INBOUND_DEV",
"target_lakehouse": "lh_bronze",
"target_schema": "bronze",
"target_table": "pe_prom_codes"
```

## State Management

### Load State Flow

```
Extraction:
  extract_batch_id created → load_state = 'pending'

Loading:
  Batch discovered → load_state = 'processing'
  Batch completed → load_state = 'completed'
  Batch failed → load_state = 'failed'
  Batch rejected → load_state = 'rejected'
```

### Crash Recovery

The loading orchestrator automatically recovers stale batches:

```python
# Before processing batches
self._recover_stale_batches(config, config_logger)

# What it does:
# 1. Query log_batch_load for batches stuck in 'processing' > 1 hour
# 2. Reset them to 'pending'
# 3. Log recovery event
# 4. They'll be picked up in current execution
```

**Threshold**: 1 hour (configurable in code)

### Execution Logs

Query execution history:

```sql
-- Recent extraction runs
SELECT execution_id, extract_run_id, source_name, resource_name, status, created_at
FROM log_resource_extract_config
WHERE created_at >= CURRENT_DATE() - INTERVAL 7 DAY
ORDER BY created_at DESC;

-- Recent loading runs
SELECT execution_id, load_run_id, source_name, resource_name, status, created_at
FROM log_resource_execution
WHERE created_at >= CURRENT_DATE() - INTERVAL 7 DAY
ORDER BY created_at DESC;

-- Batch-level metrics
SELECT load_batch_id, extract_batch_id, status,
       records_processed, records_inserted, records_updated, records_deleted,
       total_duration_ms, created_at
FROM log_batch_load
WHERE load_run_id = '<your_load_run_id>'
ORDER BY created_at;
```

## Error Handling

### Rejection Types

**Structural Corruption** (Step 1):
- Malformed CSV rows (wrong number of columns, broken quotes)
- Detected by Spark PERMISSIVE mode
- Stored in `_stg_corrupt_record` column

**Type Casting Errors** (Step 2):
- String values that can't convert to target type
- Detected by `try_cast()` - returns NULL on failure
- Tracked in `_type_cast_error` temporary column

**Duplicate Records** (Step 2):
- Multiple rows with same `target_merge_keys`
- Detected by groupBy + count > 1
- Rejection includes duplicate count

### Error Behavior

Controlled by `target_fail_on_rejection`:

**`target_fail_on_rejection = True`** (default):
```
1. Validation fails (corruption/duplicates exceed tolerance)
2. Batch marked as 'rejected'
3. File stays in landing zone
4. Resource execution fails
5. Manual intervention required
```

**`target_fail_on_rejection = False`**:
```
1. Validation fails
2. Log warning
3. Filter out bad records
4. Continue processing with good records
5. Batch marked as 'completed' (with corrupt_records_count > 0)
```

### File Management

**On Success**: Files stay in `extract_path`
**On Failure**: Files stay in `extract_path` (transient errors retry automatically)
**On Rejection**: Files moved to `extract_error_path` (data quality issues require investigation)

**Why the distinction?**
- **Failures** (system errors, timeouts): Transient issues that may resolve on retry - keep files in landing
- **Rejections** (corruption, duplicates): Data quality issues that won't self-resolve - move to error path

**Retry Behavior for Failures**:
1. Transient error occurs, file stays in landing
2. Next run picks up the file automatically (load_state still 'pending')

**Investigation Pattern for Rejections**:
1. Data quality issue detected, file moved to `extract_error_path`
2. Ops team investigates file in error location
3. Fix source data and re-extract (file won't be picked up again from error path)