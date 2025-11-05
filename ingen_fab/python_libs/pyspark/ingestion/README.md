# File Loading Framework ✅ COMPLETE

**Purpose:** Load files from lakehouse Files storage into Delta tables

## What This Framework Does

1. **File Discovery**: Find files based on patterns (single_file, incremental_files, incremental_folders)
2. **File Reading**: Read files using Spark (CSV, Parquet, JSON, etc.)
3. **Data Loading**: Write data to Delta tables (lakehouse/warehouse)
4. **State Tracking**: Track processed files to avoid duplicates
5. **Execution Orchestration**: Handle multiple resources with execution groups and parallel processing

## What This Framework Does NOT Do

- ❌ Extract data from APIs (see `python/extraction/` - TODO)
- ❌ Extract data from databases (see `python/extraction/` - TODO)
- ❌ Write files to storage (use extraction frameworks for that)

## Components

### 1. Configuration (`config.py`)
- **SourceConfig**: Connection info for source lakehouse
- **ResourceConfig**: Complete resource configuration
- **FileSystemLoadingParams**: File-specific loading parameters

### 2. Results (`results.py`)
- **BatchInfo**: Information about discovered batches
- **ProcessingMetrics**: Performance metrics and row counts
- **ResourceExecutionResult**: Overall execution result

### 3. FileLoader (`loader.py`)
- Discovers files based on import_pattern
- Reads files into DataFrames
- Handles duplicate detection
- Supports:
  - single_file: Load one specific file
  - incremental_files: Load new files from directory
  - incremental_folders: Load new folders (batch processing)

### 4. FileLoadingOrchestrator (`orchestrator.py`)
- Orchestrates multiple resource loads
- Groups resources by execution_group
- Processes groups sequentially, resources within groups in parallel
- Writes to target Delta tables
- Integrates with logging service

### 5. FileLoadingLogger (`logging.py`)
- Logs to log_file_load table (batch-level)
- Logs to log_config_execution table (resource-level)
- Uses merge/upsert pattern for state tracking

### 6. Examples (`examples.py`)
- Comprehensive usage examples
- Shows all features and patterns

## Quick Start

```python
import uuid
from pyspark.sql import SparkSession
from ingen_fab.python_libs.pyspark.ingestion import (
    FileLoader,
    FileLoadingOrchestrator,
    FileLoadingLogger,
    ResourceConfig,
    SourceConfig,
    FileSystemLoadingParams,
)
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Configure source
source_config = SourceConfig(
    source_name="my_source_lakehouse",
    source_type="filesystem",
    connection_params={
        "workspace_name": "MyWorkspace",
        "lakehouse_name": "SourceLakehouse",
    },
)

# Configure file loading
loading_params = FileSystemLoadingParams(
    import_pattern="incremental_files",
    discovery_pattern="*.csv",
    has_header=True,
    duplicate_handling="skip",
)

# Create resource configuration
resource_config = ResourceConfig(
    resource_name="load_sales",
    source_config=source_config,
    source_file_path="raw/sales",
    source_file_format="csv",
    loading_params=loading_params.to_dict(),
    target_workspace_name="MyWorkspace",
    target_datastore_name="TargetLakehouse",
    target_datastore_type="lakehouse",
    target_table_name="sales",
    write_mode="append",
    enable_schema_evolution=True,
)

# Option 1: Use FileLoader directly (manual control)
loader = FileLoader(spark=spark, config=resource_config)
batches = loader.discover_and_read_files()
for batch_info, df, metrics in batches:
    print(f"Batch {batch_info.batch_id}: {metrics.source_row_count} rows")

# Option 2: Use Orchestrator (automatic processing with logging)
# Create logger
log_lakehouse = lakehouse_utils(
    target_workspace_name="MyWorkspace",
    target_lakehouse_name="LogsLakehouse",
    spark=spark,
)
logger = FileLoadingLogger(lakehouse_utils_instance=log_lakehouse)

# Create orchestrator
orchestrator = FileLoadingOrchestrator(
    spark=spark,
    logger_instance=logger,
    max_concurrency=4,
)

# Process resources
execution_id = str(uuid.uuid4())
results = orchestrator.process_resources(
    configs=[resource_config],
    execution_id=execution_id,
)

print(f"Success: {results['successful']}, Failed: {results['failed']}")
```

## Import Patterns

### single_file
Load one specific file:
```python
loading_params = FileSystemLoadingParams(
    import_pattern="single_file",
)
resource_config = ResourceConfig(
    source_file_path="raw/orders/orders.csv",  # Specific file
    ...
)
```

### incremental_files
Load new files from a directory:
```python
loading_params = FileSystemLoadingParams(
    import_pattern="incremental_files",
    discovery_pattern="*.csv",
    duplicate_handling="skip",  # skip, allow, or fail
    date_pattern="YYYYMMDD",    # Extract date from filename
)
resource_config = ResourceConfig(
    source_file_path="raw/daily_sales",  # Directory
    ...
)
```

### incremental_folders
Load new folders (batch processing):
```python
loading_params = FileSystemLoadingParams(
    import_pattern="incremental_folders",
    discovery_pattern="batch_*",
    duplicate_handling="skip",
    require_control_file=True,
    control_file_pattern="_SUCCESS",
)
resource_config = ResourceConfig(
    source_file_path="raw/batches",  # Directory containing folders
    ...
)
```

## Write Modes

### Overwrite
Replace entire table:
```python
resource_config = ResourceConfig(
    write_mode="overwrite",
    ...
)
```

### Append
Add new records:
```python
resource_config = ResourceConfig(
    write_mode="append",
    ...
)
```

### Merge (Upsert)
Update existing, insert new:
```python
resource_config = ResourceConfig(
    write_mode="merge",
    merge_keys=["product_id"],
    ...
)
```

## Execution Groups

Resources are grouped by `execution_group` number:
- Groups execute sequentially (group 1, then group 2, etc.)
- Resources within a group execute in parallel (up to max_concurrency)

```python
config1 = ResourceConfig(
    resource_name="load_customers",
    execution_group=1,  # Load first
    ...
)

config2 = ResourceConfig(
    resource_name="load_orders",
    execution_group=1,  # Load first (parallel with customers)
    ...
)

config3 = ResourceConfig(
    resource_name="load_order_items",
    execution_group=2,  # Load second (after group 1 completes)
    ...
)

results = orchestrator.process_resources(
    configs=[config1, config2, config3],
    execution_id=execution_id,
)
```

## State Tracking

The framework tracks processed files in `log_file_load` and `log_config_execution` tables:

```sql
-- View batch-level logs
SELECT *
FROM log_file_load
WHERE config_id = 'load_sales'
ORDER BY started_at DESC;

-- View resource-level logs
SELECT *
FROM log_config_execution
WHERE config_id = 'load_sales'
ORDER BY started_at DESC;
```

## Relationship to Extraction Framework

```
Extraction Framework (python/extraction/) [TODO]
    ↓ writes files to
Files/landing/
    ↓ read by
File Loading Framework (pyspark/ingestion/) [✅ COMPLETE]
    ↓ writes to
Delta Tables
```

Both frameworks share the same `ResourceConfig`:
- Extraction uses: `extraction_output_path`, `extraction_params`
- File Loading uses: `source_file_path`, `source_file_format`, `loading_params`, `target_*`

## See Also

- **examples.py**: Comprehensive usage examples
- **config.py**: Configuration classes and schemas
- **results.py**: Result objects and metrics
