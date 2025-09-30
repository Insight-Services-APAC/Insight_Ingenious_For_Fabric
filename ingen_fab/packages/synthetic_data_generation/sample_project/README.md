# Synthetic Data Generation Sample Project

This directory contains sample configurations and examples for using the Synthetic Data Generation package.

## Overview

The Synthetic Data Generation package provides tools for creating realistic synthetic datasets for testing, development, and demonstration purposes. It supports both transactional (OLTP) and analytical (OLAP/star schema) data patterns with configurable scale.

## Quick Start

### 1. List Available Datasets

```bash
ingen_fab package synthetic-data list-datasets
```

This will show all predefined dataset configurations including:
- `retail_oltp_small` - Small retail transactional system
- `retail_star_small` - Small retail star schema
- `finance_oltp_small` - Small financial system
- `ecommerce_star_small` - Small e-commerce analytics

### 2. Generate a Small Dataset

```bash
# Generate 10,000 rows for retail OLTP dataset
ingen_fab package synthetic-data generate retail_oltp_small --target-rows 10000

# Generate with specific seed for reproducibility
ingen_fab package synthetic-data generate retail_oltp_small --target-rows 10000 --seed 12345
```

### 3. Generate Large Scale Dataset

```bash
# Generate 1 million rows (automatically uses PySpark)
ingen_fab package synthetic-data generate retail_star_large --target-rows 1000000

# Generate 1 billion rows for testing at scale
ingen_fab package synthetic-data generate retail_star_large --target-rows 1000000000
```

### 4. Target Different Environments

```bash
# Generate for lakehouse (default, uses PySpark)
ingen_fab package synthetic-data generate retail_oltp_small --target-environment lakehouse

# Generate for warehouse (uses Python + SQL)
ingen_fab package synthetic-data generate retail_oltp_small --target-environment warehouse
```

## Dataset Types

### Transactional (OLTP) Datasets

These datasets simulate operational transaction processing systems:

- **Normalized schemas** with proper relationships
- **High write volume** simulation
- **Real-time transaction patterns**
- **Data integrity constraints**

Example tables:
- `customers` - Customer master data
- `products` - Product catalog
- `orders` - Order headers
- `order_items` - Order line items

### Analytical (OLAP/Star Schema) Datasets

These datasets simulate data warehouse and analytics systems:

- **Star schema design** with fact and dimension tables
- **Historical data tracking**
- **Aggregation-friendly structures**
- **Time-based partitioning**

Example tables:
- `fact_sales` - Sales transactions fact table
- `dim_customer` - Customer dimension (SCD Type 2)
- `dim_product` - Product dimension
- `dim_date` - Date dimension
- `dim_store` - Store dimension

## Performance Guidelines

### Small Datasets (< 1M rows)
- Uses Python implementation with pandas/faker
- Suitable for development and testing
- Fast generation and low memory usage
- Recommended for initial development

### Large Datasets (1M+ rows)
- Automatically switches to PySpark implementation
- Distributed generation for scalability
- Chunked processing for memory efficiency
- Optimized for production-scale testing

### Billion-Row Datasets
- Advanced chunking and partitioning
- Delta Lake optimization
- Parallel processing across multiple executors
- Requires sufficient cluster resources

## Configuration Options

### Generation Modes

- **`python`** - Force Python implementation (max ~1M rows)
- **`pyspark`** - Force PySpark implementation (1M+ rows)
- **`auto`** - Automatically select based on target rows (recommended)

### Target Environments

- **`lakehouse`** - Generate Delta tables in Fabric Lakehouse
- **`warehouse`** - Generate tables in Fabric Data Warehouse

### Reproducibility

Use the `--seed` parameter for reproducible datasets:

```bash
# Same seed will always generate identical data
ingen_fab package synthetic-data generate retail_oltp_small --target-rows 10000 --seed 42
```

## Advanced Usage

### Compile Without Execution

```bash
# Just compile the notebook without running it
ingen_fab package synthetic-data compile --dataset-id retail_oltp_small --target-rows 10000
```

### Include DDL Scripts

```bash
# Compile with configuration table DDL scripts
ingen_fab package synthetic-data compile --include-ddl --target-environment warehouse
```

### Custom Dataset Configuration

You can extend the package by:
1. Adding new dataset configurations to the predefined configs
2. Creating custom notebook templates
3. Implementing domain-specific generators

## Sample Data Characteristics

### Data Quality Features
- **Realistic distributions** - Names, addresses, phone numbers
- **Referential integrity** - Proper foreign key relationships
- **Business rules** - Logical constraints and patterns
- **Data variety** - Mixed data types and patterns
- **Temporal consistency** - Proper date/time relationships

### Domain Coverage
- **Retail/E-commerce** - Products, orders, customers, inventory
- **Financial** - Accounts, transactions, compliance patterns
- **Healthcare** - Patients, treatments, appointments (anonymized)
- **Manufacturing** - Parts, assemblies, quality metrics
- **Logistics** - Shipments, routes, tracking data

## Troubleshooting

### Memory Issues
- Reduce `target_rows` or use chunking
- Increase Spark executor memory
- Use appropriate cluster sizing

### Performance Issues
- Ensure proper partitioning for large datasets
- Use Delta Lake optimizations
- Consider data locality and caching

### Data Quality Issues
- Verify seed consistency for reproducible tests
- Check referential integrity constraints
- Validate business rule implementations

## Examples

See the generated notebooks in your workspace for complete examples of:
- Small-scale development datasets
- Production-scale test datasets
- Multi-table relational schemas
- Star schema analytics datasets
- Custom domain-specific patterns
