# Synthetic Data Generation Package

The Synthetic Data Generation package is a comprehensive framework for creating realistic test datasets at any scale. It supports multiple data patterns, runtime configuration, and both single-dataset and incremental time-series generation with sophisticated business logic.

## Overview

The Synthetic Data Generation package provides:

- **Multi-Scale Generation**: From thousands to billions of rows with automatic mode selection
- **Domain-Specific Datasets**: Pre-configured datasets for retail, finance, healthcare, and e-commerce
- **Flexible Schema Support**: OLTP (transactional) and OLAP (star schema) patterns
- **Incremental Generation**: Time-based data generation with state management
- **Runtime Configuration**: Modify parameters without recompilation
- **Enhanced Templates**: Generic templates with full runtime parameterization
- **Intelligent Defaults**: Auto-detection of optimal generation mode based on data size

## Architecture

### Core Components

1. **SyntheticDataGenerationCompiler**: Main compiler for single datasets
2. **IncrementalSyntheticDataGenerationCompiler**: Specialized for time-series data
3. **DatasetConfigurationRepository**: Centralized configuration management
4. **Enhanced Configuration System**: Runtime parameter modification (when available)
5. **Template System**: Unified Jinja2 templates supporting all scenarios

### Generation Modes

- **Python Mode**: For datasets < 1M rows using pandas/faker
- **PySpark Mode**: For large-scale generation using distributed computing
- **Auto Mode**: Automatically selects based on target rows

## Installation & Setup

### 1. List Available Datasets

```bash
# View all predefined dataset configurations
ingen_fab package synthetic-data list-datasets

# View incremental datasets
ingen_fab package synthetic-data list-incremental-datasets

# View enhanced templates (if available)
ingen_fab package synthetic-data list-enhanced
```

### 2. Compile Notebooks

#### Standard Compilation
```bash
# Compile for a specific dataset
ingen_fab package synthetic-data compile \
    --dataset-id retail_oltp_small \
    --target-rows 10000 \
    --target-environment lakehouse

# Compile with specific generation mode
ingen_fab package synthetic-data compile \
    --dataset-id finance_star_large \
    --target-rows 1000000 \
    --generation-mode pyspark
```

#### Enhanced Compilation
```bash
# Use enhanced template with runtime parameters
ingen_fab package synthetic-data compile \
    --enhanced \
    --config-template retail_oltp_enhanced \
    --path-pattern nested_daily

# List available enhanced templates
ingen_fab package synthetic-data compile \
    --enhanced \
    --config-template list
```

### 3. Deploy DDL Scripts

The package creates these configuration tables:

**Lakehouse Tables:**
```
config_synthetic_data_datasets
config_synthetic_data_generation_jobs
log_synthetic_data_generation
sample_dataset_configurations
```

**Warehouse Tables:**
```
config.config_synthetic_data_datasets
config.config_synthetic_data_generation_jobs
config.log_synthetic_data_generation
config.sample_dataset_configurations
```

## CLI Commands

### Core Commands

#### `compile`
Compile synthetic data generation notebooks and DDL scripts.

```bash
ingen_fab package synthetic-data compile [OPTIONS]

Options:
  --dataset-id, -d          Predefined dataset ID to compile
  --target-rows, -r         Number of rows to generate [default: 10000]
  --target-environment, -e  Target environment (lakehouse/warehouse) [default: lakehouse]
  --generation-mode, -m     Generation mode (python/pyspark/auto) [default: auto]
  --seed, -s               Seed value for reproducible generation
  --include-ddl            Include DDL scripts [default: True]
  --output-mode, -o        Output mode (table/parquet/csv) [default: table]
  --enhanced               Use enhanced template system
  --config-template, -t    Configuration template ID for enhanced mode
  --path-pattern, -p       File path pattern for outputs
```

#### `generate`
Generate synthetic data for a specific dataset.

```bash
ingen_fab package synthetic-data generate DATASET_ID [OPTIONS]

Options:
  --target-rows, -r         Number of rows to generate [default: 10000]
  --target-environment, -e  Target environment [default: lakehouse]
  --generation-mode, -m     Generation mode [default: auto]
  --seed, -s               Seed value for reproducibility
  --execute                Execute notebook after compilation
  --output-mode, -o        Output mode [default: table]
```

#### `generate-incremental`
Generate incremental synthetic data for a specific date.

```bash
ingen_fab package synthetic-data generate-incremental DATASET_ID [OPTIONS]

Options:
  --date, -d               Generation date (YYYY-MM-DD)
  --path-format, -p        Path format (nested/flat) [default: nested]
  --target-environment, -e  Target environment [default: lakehouse]
  --generation-mode, -m     Generation mode [default: auto]
  --seed, -s               Seed value
  --state-management       Enable state management [default: True]
  --execute                Execute notebook after compilation
```

#### `generate-series`
Generate a series of incremental data for a date range.

```bash
ingen_fab package synthetic-data generate-series DATASET_ID [OPTIONS]

Options:
  --start-date, -s         Start date (YYYY-MM-DD)
  --end-date, -e           End date (YYYY-MM-DD)
  --batch-size, -b         Days per batch [default: 10]
  --path-format, -p        Path format [default: nested]
  --target-environment, -t  Target environment [default: lakehouse]
  --generation-mode, -m     Generation mode [default: auto]
  --output-mode, -o        Output mode [default: parquet]
  --seed                   Seed value
  --ignore-state           Ignore existing state
  --execute                Execute notebook
```

### Generic Template Commands

#### `compile-generic-templates`
Compile runtime-parameterized generic templates.

```bash
ingen_fab package synthetic-data compile-generic-templates [OPTIONS]

Options:
  --target-environment, -t  Target environment [default: lakehouse]
  --force                  Force recompilation
  --template-type          Template type (all/series/single) [default: all]
```

#### `execute-with-parameters`
Execute generic templates with runtime parameters.

```bash
ingen_fab package synthetic-data execute-with-parameters NOTEBOOK_NAME [OPTIONS]

Options:
  --dataset-id             Dataset ID to generate
  --start-date             Start date for series
  --end-date               End date for series
  --batch-size             Days per batch
  --target-rows            Target rows for single dataset
  --scale-factor           Scale factor for size
  --path-format            Path format
  --output-mode            Output mode
  --seed                   Seed value
  --generation-mode        Generation mode
  --custom-schema          Custom schema JSON
  --enable-partitioning    Enable table partitioning
```

## Available Datasets

### Retail Domain

#### `retail_oltp_small` / `retail_oltp_large`
Transactional retail system with normalized schema:
- **customers**: Customer profiles with demographics
- **products**: Product catalog with categories and pricing
- **stores**: Physical store locations
- **orders**: Order transactions
- **order_items**: Order line items
- **inventory**: Stock levels by store and product

#### `retail_star_small` / `retail_star_large`
Star schema for retail analytics:
- **fact_sales**: Sales transactions with measures
- **dim_customer**: Customer dimension (SCD Type 2)
- **dim_product**: Product hierarchy
- **dim_store**: Store locations and attributes
- **dim_date**: Date dimension with fiscal calendar
- **dim_time**: Time of day analysis

### Financial Domain

#### `finance_oltp_small` / `finance_oltp_large`
Banking/financial services system:
- **customers**: Account holders
- **accounts**: Bank accounts
- **transactions**: Financial transactions
- **merchants**: Transaction endpoints
- **cards**: Payment instruments
- **branches**: Physical locations

#### `finance_star_small` / `finance_star_large`
Financial analytics star schema:
- **fact_transactions**: Transaction metrics
- **dim_account**: Account attributes
- **dim_customer**: Customer dimension (SCD Type 2)
- **dim_merchant**: Merchant categories
- **dim_transaction_type**: Transaction classifications
- **dim_date**: Banking calendar

### E-commerce Domain

#### `ecommerce_star_small` / `ecommerce_star_large`
E-commerce analytics schema:
- **fact_page_views**: Clickstream data
- **fact_orders**: Order transactions
- **dim_user**: User profiles
- **dim_product**: Product catalog
- **dim_session**: Session tracking
- **dim_campaign**: Marketing attribution

### Healthcare Domain

#### `healthcare_oltp_small`
Healthcare system (anonymized):
- **patients**: Patient records
- **providers**: Healthcare professionals
- **appointments**: Scheduled visits
- **diagnoses**: Medical diagnoses
- **prescriptions**: Medications
- **procedures**: Medical procedures

### Incremental Datasets

All standard datasets have incremental variants with `_incremental` suffix:
- `retail_oltp_small_incremental`
- `retail_star_large_incremental`
- `finance_oltp_small_incremental`
- etc.

## Configuration System

### Dataset Configuration Structure

```python
{
    "dataset_id": "retail_oltp_small",
    "dataset_name": "Retail OLTP - Small",
    "dataset_type": "transactional",
    "schema_pattern": "oltp",
    "domain": "retail",
    "tables": ["customers", "products", "orders", "order_items"],
    "target_rows": 10000,
    "generation_config": {
        "mode": "auto",
        "chunk_size": 10000,
        "parallel_tables": True
    },
    "relationships": [
        {
            "parent": "customers.customer_id",
            "child": "orders.customer_id",
            "type": "one_to_many"
        }
    ]
}
```

### Incremental Configuration

```python
{
    "incremental_config": {
        "snapshot_frequency": "daily",
        "file_path_pattern": "nested",
        "state_management": True,
        "table_types": {
            "customers": "snapshot",
            "orders": "incremental",
            "products": "slowly_changing_dimension"
        },
        "seasonal_patterns": {
            "monday": 0.8,
            "tuesday": 0.9,
            "wednesday": 1.0,
            "thursday": 1.1,
            "friday": 1.3,
            "saturday": 1.5,
            "sunday": 1.2
        }
    }
}
```

## File Path Patterns

### Built-in Patterns

1. **`nested_daily`**: `/YYYY/MM/DD/table_name/`
   ```
   /2024/03/15/orders/data.parquet
   ```

2. **`flat_with_date`**: `table_name_YYYYMMDD`
   ```
   orders_20240315.parquet
   ```

3. **`hive_partitioned`**: `/table_name/year=YYYY/month=MM/day=DD/`
   ```
   /orders/year=2024/month=03/day=15/data.parquet
   ```

4. **`custom`**: User-defined patterns using placeholders

## Enhanced Features

### Runtime Parameters

When using enhanced templates, parameters can be modified at runtime:

```python
runtime_params = {
    "dataset_id": "retail_oltp_small",
    "target_rows": 50000,
    "scale_factor": 2.0,
    "seed_value": 12345,
    "output_settings": {
        "output_mode": "parquet",
        "path_format": "hive_partitioned",
        "compression": "snappy"
    },
    "generation_settings": {
        "chunk_size": 10000,
        "parallel_workers": 4,
        "memory_fraction": 0.8
    }
}
```

### Data Quality Features

#### Referential Integrity
Maintains relationships between tables:
```python
"enable_relationships": True,
"relationship_null_percentage": 0.05  # 5% orphaned records
```

#### Data Distributions
Realistic value distributions:
```python
"distributions": {
    "order_amount": {
        "type": "lognormal",
        "mean": 100,
        "sigma": 50
    },
    "customer_age": {
        "type": "normal",
        "mean": 42,
        "stddev": 15,
        "min": 18,
        "max": 95
    }
}
```

#### Temporal Patterns
Time-based variations:
```python
"temporal_patterns": {
    "hourly_distribution": {
        "09-12": 1.2,  # Morning peak
        "12-14": 1.5,  # Lunch peak
        "14-17": 1.0,  # Afternoon
        "17-20": 1.8,  # Evening peak
        "default": 0.3
    },
    "seasonal_multipliers": {
        "black_friday": 5.0,
        "christmas_week": 3.0,
        "summer_sale": 2.0
    }
}
```

## Performance Optimization

### Chunking Strategy

For very large datasets:
```python
"chunking_config": {
    "enabled": True,
    "chunk_size": 1000000,
    "parallel_chunks": 10,
    "memory_fraction": 0.8,
    "spill_to_disk": True
}
```

### Partitioning

Optimize query performance:
```python
"partition_config": {
    "partition_columns": ["year", "month", "day"],
    "bucketing": {
        "columns": ["customer_id"],
        "buckets": 200
    },
    "sorting": {
        "columns": ["order_date", "customer_id"]
    }
}
```

### Delta Lake Optimizations

```python
"delta_config": {
    "optimize_write": True,
    "auto_compact": True,
    "z_order_columns": ["customer_id", "order_date"],
    "data_skipping": True,
    "stats_collection": True
}
```

## State Management

### Generation State Tracking

The system tracks generation history to prevent duplicates:

```sql
-- View generation state
SELECT 
    dataset_id,
    generation_date,
    tables_generated,
    total_rows,
    file_paths,
    status
FROM log_synthetic_data_generation
WHERE dataset_id = 'retail_oltp_small_incremental'
ORDER BY generation_date DESC;
```

### State Recovery

Resume interrupted generation:
```bash
# Continue from last successful date
ingen_fab package synthetic-data generate-series \
    retail_oltp_small_incremental \
    --start-date 2024-01-01 \
    --end-date 2024-12-31 \
    --resume  # Automatically detects last successful date
```

## Monitoring & Logging

### Performance Metrics

Track generation performance:
```sql
-- Average generation speed by dataset
SELECT 
    dataset_id,
    generation_mode,
    AVG(rows_per_second) as avg_rows_per_sec,
    AVG(duration_seconds) as avg_duration,
    MIN(rows_per_second) as min_speed,
    MAX(rows_per_second) as max_speed
FROM log_synthetic_data_generation
WHERE status = 'COMPLETED'
GROUP BY dataset_id, generation_mode;
```

### Resource Usage

Monitor resource consumption:
```sql
-- Peak memory usage by dataset size
SELECT 
    dataset_id,
    target_rows,
    MAX(peak_memory_gb) as max_memory,
    MAX(cpu_cores_used) as max_cores,
    AVG(duration_seconds) as avg_duration
FROM log_synthetic_data_generation
GROUP BY dataset_id, target_rows
ORDER BY target_rows;
```

## Best Practices

### Development

1. **Start Small**: Test with 1K-10K rows first
2. **Validate Schemas**: Check relationships and constraints
3. **Use Consistent Seeds**: Ensure reproducibility
4. **Profile Output**: Verify data distributions

### Testing

1. **Scale Gradually**: 10K → 100K → 1M → 10M → 100M
2. **Monitor Resources**: Track memory and CPU usage
3. **Validate Integrity**: Check referential constraints
4. **Test Edge Cases**: Null values, duplicates, outliers

### Production

1. **Resource Planning**: 
   - Estimate memory: ~10GB per billion rows
   - Plan cluster size: 4+ cores for > 100M rows
   - Schedule during off-peak hours

2. **Output Optimization**:
   - Partition by query patterns
   - Enable compression (70-90% reduction)
   - Use columnar formats (Parquet/Delta)

3. **Operational Excellence**:
   - Document configurations
   - Version control templates
   - Monitor generation logs
   - Set up alerting

## Troubleshooting

### Common Issues

#### Out of Memory
```bash
# Reduce chunk size
--chunk-size 100000

# Increase memory allocation
--memory-fraction 0.9

# Use disk spilling
--enable-spill
```

#### Slow Generation
- Check data complexity (relationships, calculations)
- Enable parallel processing
- Use PySpark for large datasets
- Optimize chunking strategy

#### Data Quality Issues
- Verify generator configurations
- Check seed consistency
- Review distribution parameters
- Validate temporal logic

## Examples

### Example 1: Quick Development Dataset

```bash
# Generate small OLTP dataset for testing
ingen_fab package synthetic-data generate \
    retail_oltp_small \
    --target-rows 1000 \
    --seed 42 \
    --output-mode table
```

### Example 2: Large Analytics Dataset

```bash
# Generate 100M row star schema
ingen_fab package synthetic-data compile \
    --dataset-id retail_star_large \
    --target-rows 100000000 \
    --generation-mode pyspark \
    --output-mode parquet
```

### Example 3: Incremental Time Series

```bash
# Generate daily data for Q1 2024
ingen_fab package synthetic-data generate-series \
    retail_oltp_small_incremental \
    --start-date 2024-01-01 \
    --end-date 2024-03-31 \
    --batch-size 7 \
    --path-format nested_daily
```

### Example 4: Enhanced Runtime Configuration

```bash
# Compile with enhanced template
ingen_fab package synthetic-data compile \
    --enhanced \
    --config-template retail_oltp_enhanced \
    --path-pattern hive_partitioned

# Execute with runtime parameters
ingen_fab package synthetic-data execute-with-parameters \
    generic_single_dataset_lakehouse \
    --dataset-id retail_oltp_small \
    --target-rows 50000 \
    --enable-partitioning \
    --output-mode parquet
```

## Integration Examples

### With Testing Frameworks

```python
# pytest fixture for test data
@pytest.fixture
def synthetic_test_data():
    """Generate test data for each test"""
    from ingen_fab.synthetic_data import SyntheticDataGenerator
    
    generator = SyntheticDataGenerator(
        dataset_id="retail_oltp_small",
        target_rows=100,
        seed=12345
    )
    return generator.generate()
```

### With CI/CD Pipelines

```yaml
# GitHub Actions workflow
name: Generate Test Data
on: [push]

jobs:
  generate-data:
    runs-on: ubuntu-latest
    steps:
      - name: Generate Synthetic Data
        run: |
          ingen_fab package synthetic-data generate \
            retail_oltp_small \
            --target-rows 10000 \
            --seed ${{ github.run_number }}
      
      - name: Run Tests
        run: pytest tests/ --test-data-path=./synthetic_data/
```

### With Data Pipelines

```python
# Airflow DAG for daily incremental generation
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    'synthetic_data_daily',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1)
)

generate_task = BashOperator(
    task_id='generate_daily_data',
    bash_command="""
    ingen_fab package synthetic-data generate-incremental \
        retail_oltp_small_incremental \
        --date {{ ds }} \
        --execute
    """,
    dag=dag
)
```

## Next Steps

- Review [incremental generation patterns](incremental_synthetic_data_generation.md)
- Explore [enhanced configuration options](synthetic_data_generation_enhancements.md)
- Learn about [custom data generators](../developer_guide/synthetic_data_generators.md)
- See [performance tuning guide](../user_guide/performance.md#synthetic-data)