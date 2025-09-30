# Incremental Synthetic Data Generation

[Home](../index.md) > [Packages](index.md) > Incremental Synthetic Data Generation

This document describes the incremental synthetic data generation functionality for creating time-series test data.

## Overview

The incremental generation system creates synthetic data for specific date ranges, with each day's data generated independently. This is useful for:
- Testing time-based analytics and reporting
- Creating realistic historical data patterns
- Simulating daily operational data

## How It Works

### Batch Size

The `batch_size` parameter controls how many days are grouped together for processing:
- **Purpose**: Memory management and progress tracking
- **Example**: `batch_size=10` with 30 days creates 3 batches of 10 days each
- **Note**: Data is still generated day-by-day within each batch

### Configuration Parameters

#### Working Parameters

| Parameter | Description | Example Values |
|-----------|-------------|----------------|
| `dataset_id` | Dataset configuration to use | `"retail_oltp_small"` |
| `start_date` | Start date for generation | `"2024-01-01"` |
| `end_date` | End date for generation | `"2024-01-30"` |
| `batch_size` | Days per processing batch | `1`, `7`, `30` |
| `path_format` | File organization structure | `"flat"`, `"nested"` |
| `output_mode` | Output format | `"csv"`, `"parquet"`, `"table"` |
| `seed_value` | Random seed for reproducibility | `12345` |
| `enable_seasonal_patterns` | Apply day-of-week variations | `true`, `false` |

#### Currently Non-Functional Parameters

These parameters exist in the configuration but are not yet implemented:
- `parallel_workers` - No parallel processing implemented
- `chunk_size` - Not used in generation
- `enable_data_drift` / `drift_percentage` - Data drift not implemented
- `ignore_state` - State is not persisted between runs anyway

### Date Filtering

**Important**: The system generates new data for each date with the correct date values embedded. It does NOT filter pre-existing data. Each generated record will have its date columns set to the generation date.

## File Organization

### Nested Format
```
synthetic_data/parquet/series/retail_oltp_small/job_name/nested/
└── orders/
    └── 2024/
        └── 01/
            ├── 01/data.parquet  # January 1st
            ├── 02/data.parquet  # January 2nd
            └── 03/data.parquet  # January 3rd
```

### Flat Format
```
synthetic_data/csv/series/retail_oltp_small/job_name/flat/
└── orders/
    ├── orders_20240101.csv
    ├── orders_20240102.csv
    └── orders_20240103.csv
```

## Table Types

### Incremental Tables
- Generate new records for each day
- Examples: orders, order_items, transactions
- Date columns are set to the generation date

### Snapshot Tables
- Generate full table based on frequency
- Frequencies: `daily`, `weekly`, `monthly`
- Examples: customers, products (reference data)

## Example Configuration

```python
job_configs = {
    "job_name": "test_feb_2024",
    "dataset_id": "retail_oltp_small",
    "start_date": "2024-02-01",
    "end_date": "2024-02-07",
    "batch_size": 1,                    # Process day by day
    "path_format": "flat",
    "output_mode": "csv",
    "seed_value": 99999,
    "enable_seasonal_patterns": True
}
```

## Usage

The incremental generation is currently available through:
1. **Notebook Templates**: Use the generated notebooks in Fabric
2. **Python Scripts**: Run the notebook Python files locally

Note: CLI commands for synthetic data generation are not yet implemented.

## Seasonal Patterns

When `enable_seasonal_patterns` is enabled, the system applies day-of-week multipliers to vary the data volume:

```python
seasonal_multipliers = {
    "monday": 0.8,
    "tuesday": 0.9,
    "wednesday": 1.0,
    "thursday": 1.1,
    "friday": 1.3,
    "saturday": 1.2,
    "sunday": 0.7
}
```

## Date Column Mapping

Each table configuration includes date column definitions:
- `date_columns`: List of all date fields in the table
- `primary_date_column`: The main date field that gets set to the generation date

Example for orders table:
- `date_columns`: ["order_date", "shipped_date", "delivered_date"]
- `primary_date_column`: "order_date"

## Current Limitations

1. **No State Persistence**: Each run is independent; no state carried between runs
2. **No Growth/Churn Simulation**: Table sizes remain constant
3. **No Holiday Detection**: Holiday multipliers not implemented
4. **Sequential Processing**: No parallel processing of batches
5. **No CLI Commands**: Must use notebooks or Python scripts directly

## Best Practices

1. **Small Batch Sizes (1-7 days)**: Better for testing and debugging
2. **Larger Batch Sizes (30+ days)**: More efficient for generating long date ranges
3. **Use Seeds**: Set `seed_value` for reproducible test data
4. **Verify Dates**: Always check that generated data has correct date values

## Future Enhancements

Planned improvements include:
- CLI command support
- State persistence for consistent IDs across runs
- Growth and churn rate simulation
- Holiday detection and multipliers
- Parallel batch processing
- Data drift simulation
