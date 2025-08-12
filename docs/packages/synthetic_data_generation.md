# Synthetic Data Generation Package

[Home](../index.md) > [Packages](index.md) > Synthetic Data Generation

The Synthetic Data Generation package creates realistic test datasets at any scale. It supports multiple data patterns and both single-dataset and incremental time-series generation.

## Overview

The package provides:
- **Multi-Scale Generation**: From thousands to billions of rows
- **Domain-Specific Datasets**: Pre-configured datasets for retail, finance, healthcare, and e-commerce
- **Flexible Schema Support**: OLTP (transactional) and OLAP (star schema) patterns
- **Single and Series Generation**: One-time datasets or time-based incremental data
- **Runtime Configuration**: Modify parameters through notebook configuration

## Current CLI Commands

The package currently supports three main commands:

### 1. `generate` - Create Ready-to-Run Notebooks
```bash
ingen_fab package synthetic-data generate <config> [OPTIONS]

Options:
  --mode              Generation mode: 'single' or 'series' [default: single]
  --parameters        JSON string with generation parameters
  --output-path       Custom output path for generated files
  --dry-run          Preview without generating files
  --target-environment Target: 'lakehouse' or 'warehouse' [default: lakehouse]  
  --no-execute       Don't execute the notebook after generation
```

### 2. `list` - Show Available Configurations
```bash
ingen_fab package synthetic-data list [OPTIONS]

Options:
  --type, -t         What to list: 'datasets', 'templates', or 'all' [default: all]
  --format, -f       Output format: 'table' or 'json' [default: table]
```

### 3. `compile` - Create Template Notebooks
```bash
ingen_fab package synthetic-data compile [TEMPLATE] [OPTIONS]

Options:
  --runtime-config   JSON configuration for compilation
  --output-format    Output: 'notebook', 'ddl', or 'all' [default: all]
  --target-environment Target: 'lakehouse' or 'warehouse' [default: lakehouse]
```

## Available Datasets

### Retail Domain
- `retail_oltp_small` / `retail_oltp_large` - Transactional system
- `retail_star_small` / `retail_star_large` - Analytics star schema

### Financial Domain  
- `finance_oltp_small` / `finance_oltp_large` - Banking system
- `finance_star_small` / `finance_star_large` - Financial analytics

### E-commerce Domain
- `ecommerce_star_small` / `ecommerce_star_large` - E-commerce analytics

### Healthcare Domain
- `healthcare_oltp_small` - Healthcare system (anonymized)

## Single Dataset Generation

### Basic Usage

Generate a single dataset with default parameters:
```bash
# Generate small retail dataset
ingen_fab package synthetic-data generate retail_oltp_small

# Generate with custom parameters
ingen_fab package synthetic-data generate retail_oltp_small \
  --parameters '{"target_rows": 100000, "seed_value": 42}'
```

### Configuration Parameters

#### Working Parameters for Single Dataset

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `target_rows` | Number of rows to generate | 10000 | 100000 |
| `seed_value` | Random seed for reproducibility | None | 42 |
| `output_mode` | Output format | "table" | "parquet", "csv" |
| `generation_mode` | Generation engine | "auto" | "pyspark" |
| `scale_factor` | Multiplier for all table sizes | 1.0 | 2.5 |

#### Parameters That Exist But Don't Work

These parameters may appear in configurations but have no effect:
- `chunk_size` - Not implemented in generation logic
- `parallel_tables` - Tables are always generated sequentially
- `memory_fraction` - Memory management not configurable
- `enable_spill` - Disk spilling not implemented
- `compression` - Compression settings ignored
- `parallel_workers` - No parallel processing

### Generation Modes

- **`auto`**: Automatically selects PySpark (always uses PySpark currently)
- **`pyspark`**: Uses distributed Spark for generation
- **`python`**: Not actually available (falls back to PySpark)

## Series/Incremental Generation

For time-based data generation, see [Incremental Synthetic Data Generation](incremental_synthetic_data_generation.md).

Key points:
- Generates data day-by-day for date ranges
- `batch_size` groups days for memory management
- Each day's data has correct date values embedded

## Generic Templates

The package includes generic notebook templates that can be parameterized at runtime:

### Available Templates
- `generic_single_dataset_lakehouse` - Single dataset for lakehouse
- `generic_single_dataset_warehouse` - Single dataset for warehouse  
- `generic_incremental_series_lakehouse` - Time series for lakehouse
- `generic_incremental_series_warehouse` - Time series for warehouse

### Using Generic Templates

```bash
# Compile all generic templates
ingen_fab package synthetic-data compile

# Compile specific template
ingen_fab package synthetic-data compile generic_single_dataset_lakehouse

# Generate using generic template  
ingen_fab package synthetic-data generate generic_single_dataset_lakehouse \
  --parameters '{"dataset_id": "retail_oltp_small", "target_rows": 50000}'
```

## File Organization

### Table Output Mode
Creates Delta/Parquet tables in the lakehouse:
```
Tables/
├── customers
├── products  
├── orders
└── order_items
```

### Parquet/CSV Output Mode
Creates files in the Files section:
```
Files/synthetic_data/
└── retail_oltp_small/
    ├── customers.parquet
    ├── products.parquet
    ├── orders.parquet
    └── order_items.parquet
```

## DDL Scripts Created

The package creates configuration and logging tables:

**Lakehouse:**
- `config_synthetic_data_datasets` - Dataset configurations
- `log_synthetic_data_generation` - Generation history

**Warehouse:**
- `config.config_synthetic_data_datasets`
- `config.log_synthetic_data_generation`

## Data Characteristics

### Table Relationships
The generated data maintains referential integrity:
- Orders reference valid customer IDs
- Order items reference valid order and product IDs
- Foreign key relationships are preserved

### Data Distributions
- **Customer ages**: Normal distribution (18-95 years)
- **Order amounts**: Log-normal distribution  
- **Dates**: Evenly distributed or seasonal patterns
- **Categories**: Realistic category distributions

### Limitations
1. **No NULL handling**: All fields are populated (no realistic NULL patterns)
2. **No data evolution**: Customer/product attributes don't change over time
3. **Simple distributions**: No complex multi-modal distributions
4. **No data quality issues**: No duplicates, conflicts, or anomalies unless explicitly coded

## Performance Considerations

### Memory Usage
- Approximate memory needed: ~1GB per 10M rows
- Larger datasets may require cluster scaling
- No automatic memory management or spilling

### Generation Speed
- Small datasets (<100K rows): 1-5 seconds
- Medium datasets (1M rows): 10-30 seconds  
- Large datasets (100M rows): 5-15 minutes
- Very large (1B+ rows): 1-2 hours

### Best Practices
1. Start with small datasets for testing
2. Use consistent seeds for reproducibility
3. Monitor cluster resources for large generations
4. Use parquet format for better compression

## Common Issues and Solutions

### Issue: Out of Memory
**Solution**: 
- Reduce `target_rows`
- Scale up cluster
- Generate tables individually

### Issue: Slow Generation
**Solution**:
- Ensure sufficient cluster cores
- Use `generation_mode: "pyspark"`
- Reduce data complexity

### Issue: Cannot Find Dataset
**Solution**:
- Run `ingen_fab package synthetic-data list` to see available datasets
- Check spelling of dataset_id
- Ensure dataset is in configuration

## Examples

### Example 1: Quick Test Dataset
```bash
# Generate small dataset for unit testing
ingen_fab package synthetic-data generate retail_oltp_small \
  --parameters '{"target_rows": 1000, "seed_value": 42}'
```

### Example 2: Large Analytics Dataset  
```bash
# Generate large star schema
ingen_fab package synthetic-data generate retail_star_large \
  --parameters '{"target_rows": 10000000, "output_mode": "parquet"}'
```

### Example 3: Reproducible Dataset
```bash
# Always generates identical data
ingen_fab package synthetic-data generate finance_oltp_small \
  --parameters '{"seed_value": 12345, "target_rows": 50000}'
```

## Features Not Yet Implemented

The following features are mentioned in configurations but not functional:

### CLI Commands
- `generate-incremental` - Use `generate` with mode='series' instead
- `generate-series` - Use `generate` with mode='series' instead  
- `execute-with-parameters` - Not implemented
- `--resume` flag - No resume capability

### Configuration Options
- State management between runs
- Growth and churn rates
- Holiday detection and multipliers
- Data drift simulation
- Parallel table generation
- Memory management settings
- Custom schema definitions
- Data quality patterns (nulls, duplicates)
- Temporal patterns beyond day-of-week

### Performance Features  
- Chunking for very large datasets
- Disk spilling for memory management
- Parallel processing
- Delta Lake optimizations (Z-ordering, compaction)
- Adaptive query execution

## Next Steps

- For time-series data, see [Incremental Generation](incremental_synthetic_data_generation.md)
- Review available datasets with `ingen_fab package synthetic-data list`
- Start with small datasets to understand the data structure
- Scale up gradually for performance testing