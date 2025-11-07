# Test Data for File Loading Framework

This directory contains sample CSV files for testing the file loading framework with daily incremental sales data.

## Files

- `daily_sales_20250107.csv` - Day 1: 8 sales transactions
- `daily_sales_20250108.csv` - Day 2: 6 sales transactions
- `daily_sales_20250109.csv` - Day 3: 7 sales transactions

## Schema

All files have the same structure:

| Column | Type | Description |
|--------|------|-------------|
| sale_id | STRING | Unique sale identifier (S001, S002, etc.) |
| sale_date | DATE | Date of sale (YYYY-MM-DD format) |
| customer_id | STRING | Customer identifier (C12345, etc.) |
| customer_name | STRING | Customer full name |
| product_id | STRING | Product identifier (P100, P101, etc.) |
| product_name | STRING | Product name |
| quantity | INTEGER | Quantity purchased |
| unit_price | DECIMAL | Price per unit |
| total_amount | DECIMAL | Total sale amount (quantity × unit_price) |
| payment_method | STRING | Payment type (Credit Card, Cash, etc.) |
| store_location | STRING | Store city |

## How to Use

### Step 1: Upload to Lakehouse Inbound Folder

Upload these files to your Fabric lakehouse at:
```
Files/inbound/sales/
```

You can upload via:
- **Fabric UI**: Navigate to your Lakehouse → Files → Create folder structure → Upload
- **API**: Use Fabric REST API or notebookutils

### Step 2: Configure the Resource

```python
from ingen_fab.python_libs.pyspark.ingestion.config import (
    SourceConfig,
    ResourceConfig,
    FileSystemExtractionParams
)

source_config = SourceConfig(
    source_type="filesystem",
    connection_params={
        "workspace_name": "YOUR_WORKSPACE",
        "lakehouse_name": "YOUR_LAKEHOUSE",
    }
)

extraction_params = FileSystemExtractionParams(
    inbound_path="Files/inbound/sales/",
    discovery_pattern="daily_sales_*.csv",
    batch_by="file",
    has_header=True,
    file_delimiter=",",
    duplicate_handling="skip",
    require_files=False,  # Set to True to fail if no files found
    date_regex=r"daily_sales_(\d{8})",  # Extracts date from filename using regex
    output_structure="{YYYY}/{MM}/{DD}/",  # Creates date folders in raw
    use_process_date=False,  # Use date from filename, not execution date
)

config = ResourceConfig(
    resource_name="sales_daily_sales",
    source_name="sales",
    source_config=source_config,
    extraction_params=extraction_params.to_dict(),
    raw_file_path="Files/raw/sales/daily_sales/",
    file_format="csv",
    import_mode="incremental",
    batch_by="folder",
    target_workspace="YOUR_WORKSPACE",
    target_lakehouse="YOUR_LAKEHOUSE",
    target_table="sales_daily_sales",
    write_mode="append",
    partition_columns=["sale_date"],
    enable_schema_evolution=True,
)
```

### Step 3: Run Extraction + Loading

```python
from ingen_fab.python_libs.pyspark.ingestion.orchestrator import Orchestrator
import uuid

orchestrator = Orchestrator(
    spark=spark,
    configs=[config],
    execution_id=str(uuid.uuid4()),
)

result = orchestrator.execute()
print(f"Status: {result.status}")
print(f"Records processed: {result.metrics.records_inserted}")
```

## Expected Flow

### Day 1 (daily_sales_20250107.csv)
```
Inbound: Files/inbound/sales/daily_sales_20250107.csv
   ↓ EXTRACTION
Raw: Files/raw/sales/daily_sales/2025/01/07/daily_sales_20250107.csv
   ↓ LOADING
Bronze: sales_daily_sales (8 rows with sale_date = 2025-01-07)
```

### Day 2 (daily_sales_20250108.csv)
```
Inbound: Files/inbound/sales/daily_sales_20250108.csv
   ↓ EXTRACTION
Raw: Files/raw/sales/daily_sales/2025/01/08/daily_sales_20250108.csv
   ↓ LOADING
Bronze: sales_daily_sales (14 total rows: 8 from Day 1 + 6 from Day 2)
```

### Day 3 (daily_sales_20250109.csv)
```
Inbound: Files/inbound/sales/daily_sales_20250109.csv
   ↓ EXTRACTION
Raw: Files/raw/sales/daily_sales/2025/01/09/daily_sales_20250109.csv
   ↓ LOADING
Bronze: sales_daily_sales (21 total rows: 8 + 6 + 7)
```

## Verification Queries

After each run, verify the results:

```sql
-- Check bronze table
SELECT sale_date, COUNT(*) as num_sales, SUM(total_amount) as total_revenue
FROM sales_daily_sales
GROUP BY sale_date
ORDER BY sale_date;

-- Check log entries
SELECT config_id, status, source_file_path, records_processed, started_at
FROM log_file_load
ORDER BY started_at DESC;

-- Check raw layer structure
LIST 'Files/raw/sales/daily_sales/';
```

## Testing Incremental Loading

1. **First run**: Upload only `daily_sales_20250107.csv` → Process 8 records
2. **Second run**: Upload `daily_sales_20250108.csv` → Process 6 NEW records (skip Day 1)
3. **Third run**: Upload `daily_sales_20250109.csv` → Process 7 NEW records (skip Day 1 & 2)

The framework will automatically skip already-processed dates!

## Troubleshooting

If you encounter issues:

1. **Check inbound folder exists**: `LIST 'Files/inbound/sales/'`
2. **Check file uploaded**: Files should show in Fabric UI
3. **Check date extraction**: Filename MUST match pattern `daily_sales_YYYYMMDD.csv`
4. **Check logs**: Query `log_file_load` and `log_config_execution` tables
5. **Check permissions**: Ensure your notebook has read/write access to the lakehouse
