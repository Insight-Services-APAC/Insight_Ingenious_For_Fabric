# Synthetic Data Generation Package

The Synthetic Data Generation package provides tools for creating realistic synthetic datasets for testing, development, and demonstration purposes. It supports both transactional (OLTP) and analytical (OLAP/star schema) data patterns with configurable scale.

## Overview

The Synthetic Data Generation package enables you to:
- Generate realistic test data at any scale (thousands to billions of rows)
- Create complete relational schemas with referential integrity
- Build star schemas for analytics testing
- Produce industry-specific datasets (retail, finance, healthcare)
- Ensure reproducible data generation with seed values
- Optimize for both small-scale development and large-scale testing

## Installation & Setup

### 1. Compile the Package

```bash
# List available predefined datasets
ingen_fab package synthetic-data list-datasets

# Compile for a specific dataset
ingen_fab package synthetic-data compile --dataset-id retail_oltp_small --size small

# Compile with custom row count
ingen_fab package synthetic-data compile --dataset-id retail_star_large --target-rows 1000000
```

### 2. Deploy DDL Scripts

The package creates configuration tables:

**Lakehouse Tables:**
- `config_synthetic_data_datasets` - Dataset definitions
- `config_synthetic_data_generation_jobs` - Generation job configurations
- `log_synthetic_data_generation` - Execution history

**Warehouse Tables:**
- `config.config_synthetic_data_datasets` - Dataset definitions
- `config.config_synthetic_data_generation_jobs` - Generation job configurations
- `config.log_synthetic_data_generation` - Execution history

## Quick Start

### Generate a Small Dataset

```bash
# Generate 10,000 rows of retail OLTP data
ingen_fab package synthetic-data generate retail_oltp_small --target-rows 10000

# Generate with specific seed for reproducibility
ingen_fab package synthetic-data generate retail_oltp_small --target-rows 10000 --seed 12345
```

### Generate Large Scale Dataset

```bash
# Generate 1 million rows (automatically uses PySpark)
ingen_fab package synthetic-data generate retail_star_large --target-rows 1000000

# Generate 1 billion rows for stress testing
ingen_fab package synthetic-data generate retail_star_large --target-rows 1000000000
```

## Available Datasets

### Retail Datasets

#### retail_oltp_small
Transactional retail system with normalized schema:
- `customers` - Customer demographics and contact info
- `products` - Product catalog with categories
- `stores` - Store locations and details
- `orders` - Order headers with customer links
- `order_items` - Order line items with products
- `inventory` - Stock levels by store and product

#### retail_star_large
Star schema for retail analytics:
- `fact_sales` - Sales transactions fact table
- `dim_customer` - Customer dimension (SCD Type 2)
- `dim_product` - Product hierarchy dimension
- `dim_store` - Store location dimension
- `dim_date` - Date dimension with fiscal calendar
- `dim_time` - Time of day dimension

### Financial Datasets

#### finance_oltp_small
Banking/financial services transactional system:
- `accounts` - Customer accounts
- `customers` - Customer profiles
- `transactions` - Financial transactions
- `merchants` - Merchant information
- `cards` - Payment cards
- `branches` - Bank branches

#### finance_star_large
Financial analytics star schema:
- `fact_transactions` - Transaction fact table
- `dim_account` - Account dimension
- `dim_customer` - Customer dimension (SCD Type 2)
- `dim_merchant` - Merchant categories
- `dim_transaction_type` - Transaction types
- `dim_date` - Date with banking calendar

### E-commerce Datasets

#### ecommerce_oltp_small
Online retail transactional system:
- `users` - User accounts and profiles
- `products` - Product catalog
- `categories` - Product categories
- `carts` - Shopping carts
- `orders` - Completed orders
- `reviews` - Product reviews
- `wishlists` - User wishlists

#### ecommerce_star_large
E-commerce analytics star schema:
- `fact_page_views` - Clickstream fact table
- `fact_orders` - Order fact table
- `dim_user` - User dimension
- `dim_product` - Product dimension
- `dim_session` - Session dimension
- `dim_campaign` - Marketing campaign dimension

### Healthcare Datasets

#### healthcare_oltp_small
Healthcare system (anonymized):
- `patients` - Patient demographics
- `providers` - Healthcare providers
- `appointments` - Scheduled appointments
- `diagnoses` - Diagnosis records
- `prescriptions` - Medication prescriptions
- `procedures` - Medical procedures

## Configuration

### Dataset Configuration

Define custom datasets in `config_synthetic_data_datasets`:

```python
dataset_config = {
    "dataset_id": "custom_dataset",
    "dataset_name": "Custom Business Dataset",
    "dataset_description": "Custom dataset for specific business needs",
    "industry": "CUSTOM",
    "schema_type": "OLTP",
    "tables": [
        {
            "table_name": "customers",
            "columns": [
                {"name": "customer_id", "type": "INT", "is_primary_key": True},
                {"name": "email", "type": "STRING", "generator": "email"},
                {"name": "created_date", "type": "DATE", "generator": "date_between"}
            ],
            "row_count": 10000
        }
    ]
}
```

### Generation Job Configuration

Configure generation jobs in `config_synthetic_data_generation_jobs`:

```python
job_config = {
    "job_id": "DAILY_TEST_DATA",
    "job_name": "Daily Test Data Generation",
    "dataset_id": "retail_oltp_small",
    "target_rows": 50000,
    "generation_mode": "INCREMENTAL",
    "seed_value": 12345,
    "output_format": "DELTA",
    "compression": "SNAPPY"
}
```

## Data Generators

### Built-in Generators

The package includes specialized data generators:

#### Personal Data
- `name` - Full names with cultural diversity
- `first_name` - First names only
- `last_name` - Last names only
- `email` - Valid email addresses
- `phone` - Phone numbers with regional formats
- `ssn` - Masked SSN (last 4 digits only)

#### Address Data
- `address` - Street addresses
- `city` - City names
- `state` - State/province codes
- `postal_code` - ZIP/postal codes
- `country` - Country names/codes

#### Business Data
- `company` - Company names
- `job_title` - Job titles and positions
- `department` - Department names
- `industry` - Industry classifications

#### Financial Data
- `credit_card` - Masked credit card numbers
- `currency_amount` - Monetary values
- `account_number` - Bank account numbers
- `routing_number` - Bank routing numbers

#### Internet Data
- `url` - Website URLs
- `domain` - Domain names
- `ip_address` - IPv4/IPv6 addresses
- `mac_address` - MAC addresses
- `user_agent` - Browser user agents

#### Date/Time Data
- `date_between` - Dates within range
- `datetime_between` - Timestamps within range
- `time_between` - Time values within range
- `date_of_birth` - Age-appropriate DOB

### Custom Generators

Create custom data generators:

```python
def custom_sku_generator(row_num, seed=None):
    """Generate product SKU codes"""
    random.seed(seed + row_num if seed else row_num)
    category = random.choice(['ELEC', 'CLTH', 'HOME', 'FOOD'])
    number = random.randint(1000, 9999)
    variant = random.choice(['A', 'B', 'C'])
    return f"{category}-{number}-{variant}"

# Register custom generator
register_generator('custom_sku', custom_sku_generator)
```

## Generation Modes

### Python Mode

For small datasets (< 1M rows):
- Uses pandas and faker libraries
- Single-threaded generation
- Low memory footprint
- Fast for development

```bash
ingen_fab package synthetic-data generate retail_oltp_small --target-rows 10000 --mode python
```

### PySpark Mode

For large datasets (1M+ rows):
- Distributed generation across Spark cluster
- Parallel processing
- Optimized for billions of rows
- Delta Lake optimizations

```bash
ingen_fab package synthetic-data generate retail_star_large --target-rows 100000000 --mode pyspark
```

### Auto Mode (Default)

Automatically selects based on row count:
- < 1M rows: Python mode
- >= 1M rows: PySpark mode

## Advanced Features

### Referential Integrity

Maintain relationships between tables:

```python
relationships = [
    {
        "parent_table": "customers",
        "parent_column": "customer_id",
        "child_table": "orders",
        "child_column": "customer_id",
        "relationship_type": "one_to_many",
        "null_percentage": 0
    }
]
```

### Data Distributions

Configure realistic data distributions:

```python
distribution_config = {
    "order_status": {
        "type": "weighted",
        "values": {
            "COMPLETED": 0.7,
            "PENDING": 0.15,
            "CANCELLED": 0.1,
            "REFUNDED": 0.05
        }
    },
    "customer_segment": {
        "type": "normal",
        "mean": 50,
        "stddev": 15,
        "min": 0,
        "max": 100
    }
}
```

### Temporal Consistency

Ensure logical time relationships:

```python
temporal_rules = [
    {
        "table": "orders",
        "rules": [
            "order_date >= customer.created_date",
            "ship_date >= order_date",
            "delivery_date >= ship_date"
        ]
    }
]
```

### Data Quality Profiles

Add realistic data quality issues:

```python
quality_profile = {
    "missing_data": {
        "email": 0.05,  # 5% missing emails
        "phone": 0.15   # 15% missing phones
    },
    "duplicates": {
        "customers": 0.02  # 2% duplicate customers
    },
    "outliers": {
        "order_amount": {
            "percentage": 0.01,
            "multiplier": 10
        }
    }
}
```

## Performance Optimization

### Chunking Strategy

For very large datasets:

```python
chunking_config = {
    "chunk_size": 1000000,  # 1M rows per chunk
    "parallel_chunks": 10,   # Process 10 chunks in parallel
    "memory_fraction": 0.8   # Use 80% of available memory
}
```

### Partitioning

Optimize output for querying:

```python
partition_config = {
    "partition_columns": ["year", "month"],
    "bucketing": {
        "bucket_columns": ["customer_id"],
        "num_buckets": 200
    }
}
```

### Delta Optimizations

Enable Delta Lake features:

```python
delta_config = {
    "optimize_write": True,
    "auto_compact": True,
    "z_order_columns": ["customer_id", "order_date"]
}
```

## Monitoring

### Generation Progress

Track generation progress:

```sql
-- View current generation status
SELECT 
    job_id,
    dataset_id,
    start_time,
    tables_completed,
    total_tables,
    rows_generated,
    target_rows,
    DATEDIFF(SECOND, start_time, GETDATE()) as elapsed_seconds
FROM config.log_synthetic_data_generation
WHERE status = 'RUNNING';
```

### Performance Metrics

Analyze generation performance:

```sql
-- Generation speed by dataset
SELECT 
    dataset_id,
    AVG(rows_generated / DATEDIFF(SECOND, start_time, end_time)) as rows_per_second,
    AVG(DATEDIFF(SECOND, start_time, end_time)) as avg_duration_seconds,
    COUNT(*) as total_runs
FROM config.log_synthetic_data_generation
WHERE status = 'COMPLETED'
GROUP BY dataset_id;
```

## Best Practices

### Development

1. **Start small**
   - Test with 1K-10K rows first
   - Validate data quality
   - Check relationships

2. **Use consistent seeds**
   - Same seed = same data
   - Document seed values
   - Version control configs

3. **Profile your data**
   - Analyze distributions
   - Verify constraints
   - Check for anomalies

### Testing

1. **Scale gradually**
   - 10K → 100K → 1M → 10M
   - Monitor resource usage
   - Adjust configurations

2. **Test edge cases**
   - Maximum values
   - Null handling
   - Relationship cardinality

3. **Validate results**
   - Row counts
   - Referential integrity
   - Business rules

### Production

1. **Resource planning**
   - Estimate memory needs
   - Plan cluster sizing
   - Schedule appropriately

2. **Output optimization**
   - Partition strategically
   - Enable compression
   - Use appropriate formats

3. **Documentation**
   - Document schemas
   - Explain generators
   - Provide examples

## Troubleshooting

### Common Issues

#### Out of Memory
```bash
# Reduce chunk size
ingen_fab package synthetic-data generate dataset_id --chunk-size 100000

# Increase cluster memory
# Or use sampling
ingen_fab package synthetic-data generate dataset_id --sample-percent 10
```

#### Slow Generation
- Check generator complexity
- Reduce relationship depth
- Enable parallel processing
- Use PySpark mode

#### Data Quality Issues
- Verify generator logic
- Check seed consistency
- Review distribution configs
- Validate relationships

## Examples

### Example 1: Quick Development Dataset

Generate small dataset for unit testing:

```bash
# 1,000 customers with orders
ingen_fab package synthetic-data generate retail_oltp_small \
    --target-rows 1000 \
    --seed 42 \
    --tables customers,orders,order_items
```

### Example 2: Performance Testing Dataset

Generate large dataset for performance testing:

```bash
# 100M sales transactions
ingen_fab package synthetic-data generate retail_star_large \
    --target-rows 100000000 \
    --partition-by year,month \
    --optimize-delta
```

### Example 3: Custom Industry Dataset

Create custom dataset configuration:

```python
# Insurance claims dataset
custom_config = {
    "dataset_id": "insurance_claims",
    "tables": [
        {
            "table_name": "policies",
            "columns": [
                {"name": "policy_id", "type": "STRING", "generator": "uuid"},
                {"name": "customer_id", "type": "INT", "generator": "sequential"},
                {"name": "policy_type", "type": "STRING", "generator": "choice",
                 "values": ["AUTO", "HOME", "LIFE", "HEALTH"]},
                {"name": "premium", "type": "DECIMAL", "generator": "currency_amount",
                 "min": 100, "max": 5000},
                {"name": "start_date", "type": "DATE", "generator": "date_between",
                 "start": "-2 years", "end": "today"}
            ]
        },
        {
            "table_name": "claims",
            "columns": [
                {"name": "claim_id", "type": "STRING", "generator": "uuid"},
                {"name": "policy_id", "type": "STRING", "generator": "foreign_key",
                 "reference": "policies.policy_id"},
                {"name": "claim_amount", "type": "DECIMAL", "generator": "currency_amount",
                 "min": 500, "max": 50000},
                {"name": "claim_date", "type": "DATE", "generator": "date_between",
                 "start": "-1 year", "end": "today"},
                {"name": "status", "type": "STRING", "generator": "weighted_choice",
                 "weights": {"APPROVED": 0.6, "PENDING": 0.25, "DENIED": 0.15}}
            ]
        }
    ]
}
```

## Integration

### With Testing Frameworks

```python
# pytest fixture
@pytest.fixture
def test_data():
    """Generate test data for each test"""
    from ingen_fab.packages.synthetic_data_generation import generate_dataset
    
    data = generate_dataset(
        dataset_id="retail_oltp_small",
        target_rows=100,
        seed=12345,
        output_format="pandas"
    )
    return data

def test_order_processing(test_data):
    customers = test_data['customers']
    orders = test_data['orders']
    # Run tests with synthetic data
```

### With CI/CD Pipelines

```yaml
# GitHub Actions example
- name: Generate Test Data
  run: |
    ingen_fab package synthetic-data generate retail_oltp_small \
      --target-rows 10000 \
      --seed ${{ github.run_number }}
      
- name: Run Integration Tests
  run: |
    pytest tests/integration --data-path synthetic_data/
```

## Next Steps

- Explore [predefined datasets](https://github.com/your-repo/tree/main/packages/synthetic_data_generation/datasets)
- Learn about [custom generators](../developer_guide/synthetic_data_generators.md)
- See [performance tuning guide](../user_guide/performance.md#synthetic-data)