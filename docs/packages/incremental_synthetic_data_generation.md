# Incremental Synthetic Data Generation - Enhanced Configuration Examples

This document provides comprehensive examples of how to configure and use the enhanced synthetic data generation package for incremental data generation.

## Key Features

### 1. Time-based Incremental Generation
- Generate data for specific dates or date ranges
- Support for both snapshot and incremental table types
- Consistent state management across generations

### 2. Flexible File Organization
- **Nested format**: `Files/Dataset/YYYY/MM/DD/table_name.parquet`
- **Flat format**: `Files/Dataset/YYYYMMDD_table_name.parquet`

### 3. Table Types
- **Snapshot tables**: Full table regenerated based on frequency (daily, weekly, monthly)
- **Incremental tables**: Only new data for the specific date

### 4. Advanced Configuration
- Seasonal patterns and multipliers
- Growth and churn rates for realistic data evolution
- State management for consistent cross-table relationships

## Example Configurations

### Retail OLTP with Incremental Capabilities

```python
{
    "dataset_id": "retail_oltp_incremental",
    "dataset_name": "Retail OLTP - Incremental",
    "dataset_type": "transactional",
    "schema_pattern": "oltp",
    "domain": "retail",
    "description": "Retail transactional system with incremental daily operations",
    
    "incremental_config": {
        "snapshot_frequency": "daily",
        "state_table_name": "synthetic_data_state",
        "enable_data_drift": True,
        "drift_percentage": 0.05,
        "enable_seasonal_patterns": True,
        "seasonal_multipliers": {
            "monday": 0.8,
            "tuesday": 0.9,
            "wednesday": 1.0,
            "thursday": 1.1,
            "friday": 1.3,
            "saturday": 1.2,
            "sunday": 0.7
        },
        "growth_rate": 0.001,  # 0.1% daily growth
        "churn_rate": 0.0005   # 0.05% daily churn
    },
    
    "table_configs": {
        "customers": {
            "type": "snapshot",
            "frequency": "daily",
            "growth_enabled": True,
            "churn_enabled": True,
            "base_rows": 50000,
            "daily_growth_rate": 0.002,
            "daily_churn_rate": 0.001
        },
        "products": {
            "type": "snapshot",
            "frequency": "weekly",
            "growth_enabled": True,
            "churn_enabled": False,
            "base_rows": 5000,
            "weekly_growth_rate": 0.01
        },
        "orders": {
            "type": "incremental",
            "frequency": "daily",
            "base_rows_per_day": 8000,
            "seasonal_multipliers_enabled": True,
            "weekend_multiplier": 1.4,
            "holiday_multiplier": 2.2
        },
        "order_items": {
            "type": "incremental",
            "frequency": "daily",
            "base_rows_per_day": 18000,
            "seasonal_multipliers_enabled": True,
            "weekend_multiplier": 1.4
        }
    }
}
```

### E-commerce Star Schema with Complex Incrementals

```python
{
    "dataset_id": "ecommerce_star_incremental",
    "dataset_name": "E-commerce Star Schema - Incremental",
    "dataset_type": "analytical",
    "schema_pattern": "star_schema",
    "domain": "ecommerce",
    "description": "E-commerce analytics with incremental facts and evolving dimensions",
    
    "incremental_config": {
        "enable_seasonal_patterns": True,
        "seasonal_multipliers": {
            "monday": 0.85,
            "tuesday": 0.90,
            "wednesday": 0.95,
            "thursday": 1.05,
            "friday": 1.40,
            "saturday": 1.30,
            "sunday": 0.75
        },
        "growth_rate": 0.0015,  # 0.15% daily growth
        "holiday_multipliers": {
            "black_friday": 3.5,
            "cyber_monday": 3.0,
            "christmas": 2.5
        }
    },
    
    "table_configs": {
        "dim_customer": {
            "type": "snapshot",
            "frequency": "daily",
            "growth_enabled": True,
            "base_rows": 500000,
            "daily_growth_rate": 0.003
        },
        "dim_product": {
            "type": "snapshot",
            "frequency": "weekly",
            "growth_enabled": True,
            "base_rows": 100000,
            "weekly_growth_rate": 0.02
        },
        "dim_session": {
            "type": "snapshot",
            "frequency": "daily",
            "growth_enabled": True,
            "base_rows": 1000000,
            "daily_growth_rate": 0.005
        },
        "dim_date": {
            "type": "snapshot",
            "frequency": "once",
            "base_rows": 3653
        },
        "fact_web_events": {
            "type": "incremental",
            "frequency": "daily",
            "base_rows_per_day": 2000000,
            "seasonal_multipliers_enabled": True,
            "weekend_multiplier": 0.8,
            "holiday_multiplier": 2.0
        },
        "fact_orders": {
            "type": "incremental",
            "frequency": "daily",
            "base_rows_per_day": 25000,
            "seasonal_multipliers_enabled": True,
            "weekend_multiplier": 1.2,
            "holiday_multiplier": 2.8
        }
    }
}
```

## Usage Examples

### 1. Generate Single Day Data

```bash
# Generate data for a specific date with nested directory structure
ingen_fab synthetic-data generate-incremental retail_oltp_incremental \
    --date 2024-01-15 \
    --path-format nested \
    --target-environment lakehouse \
    --generation-mode auto

# Generate with flat file structure
ingen_fab synthetic-data generate-incremental retail_oltp_incremental \
    --date 2024-01-15 \
    --path-format flat \
    --seed 42
```

### 2. Generate Date Range Series

```bash
# Generate 30 days of data
ingen_fab synthetic-data generate-series retail_oltp_incremental \
    --start-date 2024-01-01 \
    --end-date 2024-01-30 \
    --path-format nested \
    --batch-size 10

# Generate a full year with larger batches
ingen_fab synthetic-data generate-series ecommerce_star_incremental \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --batch-size 30 \
    --generation-mode pyspark
```

### 3. List Available Datasets

```bash
# See all available incremental datasets
ingen_fab synthetic-data list-incremental-datasets
```

## File Organization Examples

### Nested Directory Structure
```
Files/
└── synthetic_data/
    └── retail_oltp_incremental/
        ├── 2024/
        │   ├── 01/
        │   │   ├── 15/
        │   │   │   ├── customers.parquet      # Snapshot (if due)
        │   │   │   ├── orders.parquet         # Incremental
        │   │   │   └── order_items.parquet    # Incremental
        │   │   ├── 16/
        │   │   │   ├── orders.parquet
        │   │   │   └── order_items.parquet
        │   │   └── 17/
        │   │       ├── products.parquet       # Snapshot (weekly)
        │   │       ├── orders.parquet
        │   │       └── order_items.parquet
        │   └── 02/
        │       └── ...
        └── 2025/
            └── ...
```

### Flat File Structure
```
Files/
└── synthetic_data/
    └── retail_oltp_incremental/
        ├── 20240115_customers.parquet
        ├── 20240115_orders.parquet
        ├── 20240115_order_items.parquet
        ├── 20240116_orders.parquet
        ├── 20240116_order_items.parquet
        ├── 20240117_products.parquet
        ├── 20240117_orders.parquet
        ├── 20240117_order_items.parquet
        └── ...
```

## State Management

The incremental generation system maintains state to ensure:

1. **Consistent IDs**: Customer IDs, Product IDs remain consistent across generations
2. **Realistic Growth**: Gradual increase in dimension sizes over time
3. **Relationship Integrity**: Orders reference existing customers and products
4. **Seasonal Patterns**: Realistic variations based on day of week, holidays

## Best Practices

### 1. Path Format Selection
- **Nested**: Better for date-based querying and partitioning
- **Flat**: Simpler for basic file operations and smaller datasets

### 2. Generation Mode Selection
- **Python**: Best for < 1M rows per day, faster startup
- **PySpark**: Best for > 1M rows per day, better scalability
- **Auto**: Automatically selects based on expected data volume

### 3. Batch Size Considerations
- **Small batches (1-10 days)**: Better memory usage, more frequent progress updates
- **Large batches (30+ days)**: Better performance for large date ranges

### 4. Table Type Guidelines
- **Snapshot**: Use for slowly changing dimensions, reference data
- **Incremental**: Use for transaction tables, fact tables, event data

## Advanced Features

### 1. Seasonal Multipliers
Configure different patterns for different days of the week and special events.

### 2. Growth and Churn Rates
Realistic simulation of business growth and customer churn over time.

### 3. State Persistence
Automatic tracking of generation state for consistent incremental data.

### 4. Data Quality Validation
Built-in validation of generated files and row counts.

## Integration Examples

### Notebook Integration
The generated notebooks can be integrated into data pipelines for automated daily data generation.

### Pipeline Orchestration
Use with Fabric Pipelines or other orchestration tools to generate data on schedule.

### Testing Scenarios
Generate specific date ranges for testing time-based analytics and reporting.
