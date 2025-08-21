# Data Profiling Examples

[Home](../index.md) > [Examples](index.md) > Data Profiling Examples

This section provides practical examples for using the Data Profiling package, from basic statistical analysis to advanced relationship discovery between tables.

## Basic Data Profiling

### Example 1: Simple Table Profiling

Profile a single table with basic statistics:

```sql
-- Add a table to the profiling configuration
INSERT INTO config_data_profiling (table_name, profile_type, output_format)
VALUES ('customers', 'BASIC', 'yaml');
```

This will generate a profile containing:
- Row and column counts
- Null counts and percentages  
- Distinct value statistics
- Completeness and uniqueness metrics
- Top 100 distinct values per column

### Example 2: Statistical Analysis

For deeper statistical insights on numeric columns:

```sql
INSERT INTO config_data_profiling (
    table_name, 
    profile_type, 
    output_format,
    include_samples
) VALUES (
    'sales_transactions', 
    'STATISTICAL', 
    'html',
    true
);
```

This adds:
- Mean, median, standard deviation
- Min/max values and ranges
- Quartile distributions
- String length statistics
- Sample data for validation

## Advanced Profiling Scenarios

### Example 3: Data Quality Assessment

Set up comprehensive quality monitoring:

```sql
INSERT INTO config_data_profiling (
    table_name, 
    profile_type, 
    quality_rules,
    execution_group,
    output_format
) VALUES (
    'customer_master', 
    'DATA_QUALITY',
    '[
        {
            "rule_type": "completeness", 
            "column": "customer_id", 
            "threshold": 0.99,
            "severity": "error"
        },
        {
            "rule_type": "uniqueness", 
            "column": "email", 
            "threshold": 0.95,
            "severity": "warning"
        },
        {
            "rule_type": "pattern", 
            "column": "phone", 
            "pattern": "^[0-9]{10}$",
            "severity": "warning"
        }
    ]',
    'daily_quality_check',
    'yaml'
);
```

### Example 4: Relationship Discovery

Discover relationships between multiple tables:

```sql
-- Profile related tables for relationship analysis
INSERT INTO config_data_profiling (table_name, profile_type, execution_group) VALUES
('customers', 'RELATIONSHIP', 'ecommerce_analysis'),
('orders', 'RELATIONSHIP', 'ecommerce_analysis'), 
('order_items', 'RELATIONSHIP', 'ecommerce_analysis'),
('products', 'RELATIONSHIP', 'ecommerce_analysis'),
('product_categories', 'RELATIONSHIP', 'ecommerce_analysis');
```

This will automatically discover:
- Foreign key relationships (e.g., `orders.customer_id` → `customers.customer_id`)
- Cross-table value overlaps
- Semantic column types (identifiers, measures, dimensions)
- Business rules and constraints

## Performance Optimization Examples

### Example 5: Large Dataset Profiling

For tables with millions of rows, use sampling and column selection:

```sql
INSERT INTO config_data_profiling (
    table_name, 
    profile_type, 
    sample_rate,
    columns_to_profile,
    output_format
) VALUES (
    'transaction_history', 
    'STATISTICAL',
    0.1,  -- Profile 10% sample
    'transaction_id,customer_id,amount,transaction_date,status',
    'yaml'
);
```

### Example 6: Batch Processing

Process multiple tables efficiently using execution groups:

```sql
-- Group related tables for batch processing
INSERT INTO config_data_profiling (table_name, profile_type, execution_group) VALUES
('dim_customer', 'FULL', 'dimensional_tables'),
('dim_product', 'FULL', 'dimensional_tables'),
('dim_geography', 'FULL', 'dimensional_tables'),
('dim_time', 'FULL', 'dimensional_tables');

INSERT INTO config_data_profiling (table_name, profile_type, execution_group, sample_rate) VALUES
('fact_sales', 'STATISTICAL', 'fact_tables', 0.05),
('fact_inventory', 'STATISTICAL', 'fact_tables', 0.05),
('fact_financial', 'STATISTICAL', 'fact_tables', 0.05);
```

## Programmatic Examples

### Example 7: Python API Usage

Use the profiling APIs directly in your code:

```python
from pyspark.sql import SparkSession
from ingen_fab.python_libs.pyspark.data_profiling_pyspark import DataProfilingPySpark
from ingen_fab.python_libs.interfaces.data_profiling_interface import ProfileType

# Initialize Spark and profiler
spark = SparkSession.builder.appName("DataProfiling").getOrCreate()
profiler = DataProfilingPySpark(spark)

# Load your data
df = spark.table("default.customers")

# Generate comprehensive profile
profile = profiler.profile_dataset(df, ProfileType.FULL)

# Generate reports in different formats
yaml_report = profiler.generate_quality_report(profile, "yaml")
html_report = profiler.generate_quality_report(profile, "html")
json_report = profiler.generate_quality_report(profile, "json")

# Save reports
with open("/tmp/customer_profile.yaml", "w") as f:
    f.write(yaml_report)

print(f"Profiled {profile.dataset_name}: {profile.row_count:,} rows, {profile.column_count} columns")
print(f"Data quality score: {profile.data_quality_score:.2f}")
```

### Example 8: Cross-Table Relationship Analysis

Analyze relationships across multiple tables:

```python
from ingen_fab.python_libs.common.cross_profile_analyzer import CrossProfileAnalyzer

# Profile multiple related tables
tables = ['customers', 'orders', 'order_items', 'products']
profiles = {}

for table_name in tables:
    df = spark.table(f"default.{table_name}")
    profile = profiler.profile_dataset(df, ProfileType.RELATIONSHIP)
    profile.dataset_name = table_name
    profiles[table_name] = profile

# Analyze relationships
analyzer = CrossProfileAnalyzer(min_confidence=0.7)
relationships = analyzer.analyze_profiles(profiles)

# Display discovered relationships
print(f"\nDiscovered {len(relationships)} relationships:")
for rel in relationships[:10]:
    print(f"  {rel.source_table}.{rel.source_column.column_name} → "
          f"{rel.target_table}.{rel.target_column.column_name} "
          f"(confidence: {rel.confidence:.2f})")

# Generate relationship report
relationship_report = analyzer.generate_relationship_report(relationships)
with open("/tmp/relationship_analysis.md", "w") as f:
    f.write(relationship_report)
```

### Example 9: Custom Quality Rules

Define and validate custom business rules:

```python
# Define custom quality rules
quality_rules = [
    {
        "rule_type": "completeness",
        "column": "customer_id",
        "threshold": 0.99,
        "description": "Customer ID must be 99% complete"
    },
    {
        "rule_type": "uniqueness", 
        "column": "email",
        "threshold": 0.95,
        "description": "Email addresses should be 95% unique"
    },
    {
        "rule_type": "range",
        "column": "age",
        "min_value": 0,
        "max_value": 120,
        "description": "Age must be between 0 and 120"
    },
    {
        "rule_type": "pattern",
        "column": "phone",
        "pattern": r"^\+?[1-9]\d{1,14}$",
        "description": "Phone must be valid E.164 format"
    }
]

# Validate data against rules
validation_results = profiler.validate_against_rules(df, quality_rules)

print("Quality Validation Results:")
for rule, result in validation_results.items():
    status = "✅ PASS" if result['passed'] else "❌ FAIL"
    print(f"  {status} {rule}: {result['description']}")
    if not result['passed']:
        print(f"    Expected: {result['threshold']}, Actual: {result['actual']}")
```

## Integration Examples

### Example 10: Data Catalog Integration

Export profiling results to a data catalog:

```python
def export_to_catalog(profile, catalog_api):
    """Export profile results to data catalog."""
    
    catalog_entry = {
        'table_name': profile.dataset_name,
        'description': f"Table with {profile.row_count:,} rows and {profile.column_count} columns",
        'row_count': profile.row_count,
        'column_count': profile.column_count,
        'data_quality_score': profile.data_quality_score,
        'last_profiled': profile.profile_timestamp,
        'columns': []
    }
    
    for col in profile.column_profiles:
        col_entry = {
            'name': col.column_name,
            'data_type': col.data_type,
            'description': get_column_description(col),
            'completeness': col.completeness,
            'uniqueness': col.uniqueness,
            'semantic_type': col.semantic_type.value if col.semantic_type else None,
            'business_rules': [rule.rule_description for rule in col.business_rules]
        }
        catalog_entry['columns'].append(col_entry)
    
    # Upload to catalog
    catalog_api.create_or_update_table(catalog_entry)

def get_column_description(col):
    """Generate intelligent column description."""
    desc_parts = []
    
    if col.semantic_type:
        desc_parts.append(f"{col.semantic_type.value.replace('_', ' ').title()}")
    
    if col.naming_pattern and col.naming_pattern.is_id_column:
        desc_parts.append("identifier")
    elif col.naming_pattern and col.naming_pattern.is_foreign_key:
        desc_parts.append("foreign key reference")
    
    if col.value_pattern and col.value_pattern.detected_format.value != 'unknown':
        desc_parts.append(f"format: {col.value_pattern.detected_format.value}")
    
    return " | ".join(desc_parts) if desc_parts else "Data column"
```

### Example 11: Automated Quality Monitoring

Set up automated quality monitoring with alerts:

```python
def setup_quality_monitoring(profiler, tables, alert_config):
    """Set up automated quality monitoring."""
    
    quality_issues = []
    
    for table_name in tables:
        df = spark.table(table_name)
        profile = profiler.profile_dataset(df, ProfileType.DATA_QUALITY)
        
        # Check overall data quality score
        if profile.data_quality_score < alert_config['min_quality_score']:
            quality_issues.append({
                'table': table_name,
                'issue': 'Low data quality score',
                'severity': 'warning',
                'score': profile.data_quality_score,
                'threshold': alert_config['min_quality_score']
            })
        
        # Check for specific column issues
        for col in profile.column_profiles:
            if col.completeness < alert_config['min_completeness']:
                quality_issues.append({
                    'table': table_name,
                    'column': col.column_name,
                    'issue': 'Low completeness',
                    'severity': 'error',
                    'actual': col.completeness,
                    'threshold': alert_config['min_completeness']
                })
            
            if col.uniqueness < alert_config['min_uniqueness'] and col.semantic_type == 'identifier':
                quality_issues.append({
                    'table': table_name,
                    'column': col.column_name,
                    'issue': 'Low uniqueness for identifier',
                    'severity': 'error', 
                    'actual': col.uniqueness,
                    'threshold': alert_config['min_uniqueness']
                })
    
    # Send alerts if issues found
    if quality_issues:
        send_quality_alerts(quality_issues, alert_config['recipients'])
    
    return quality_issues

# Usage
alert_config = {
    'min_quality_score': 0.8,
    'min_completeness': 0.95,
    'min_uniqueness': 0.99,
    'recipients': ['data-team@company.com']
}

quality_issues = setup_quality_monitoring(
    profiler, 
    ['customers', 'orders', 'products'],
    alert_config
)
```

## Real-World Scenarios

### Example 12: E-commerce Data Analysis

Complete profiling setup for an e-commerce platform:

```sql
-- Configure profiling for e-commerce data model
INSERT INTO config_data_profiling (table_name, profile_type, execution_group, quality_rules) VALUES
(
    'dim_customers', 
    'FULL',
    'ecommerce_dimensions',
    '[
        {"rule_type": "completeness", "column": "customer_id", "threshold": 1.0},
        {"rule_type": "uniqueness", "column": "customer_id", "threshold": 1.0},
        {"rule_type": "completeness", "column": "email", "threshold": 0.95},
        {"rule_type": "pattern", "column": "email", "pattern": "^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$"}
    ]'
),
(
    'dim_products',
    'FULL', 
    'ecommerce_dimensions',
    '[
        {"rule_type": "completeness", "column": "product_id", "threshold": 1.0},
        {"rule_type": "completeness", "column": "product_name", "threshold": 1.0},
        {"rule_type": "range", "column": "price", "min_value": 0}
    ]'
),
(
    'fact_orders',
    'RELATIONSHIP',
    'ecommerce_facts', 
    '[
        {"rule_type": "completeness", "column": "order_id", "threshold": 1.0},
        {"rule_type": "completeness", "column": "customer_id", "threshold": 1.0},
        {"rule_type": "range", "column": "order_total", "min_value": 0}
    ]'
);
```

### Example 13: Financial Data Compliance

Set up profiling for financial data with regulatory requirements:

```sql
-- Financial data profiling with compliance rules
INSERT INTO config_data_profiling (
    table_name, 
    profile_type, 
    quality_rules,
    output_format,
    execution_group
) VALUES
(
    'transactions',
    'DATA_QUALITY',
    '[
        {
            "rule_type": "completeness", 
            "column": "transaction_id", 
            "threshold": 1.0,
            "compliance": "SOX_404"
        },
        {
            "rule_type": "completeness", 
            "column": "amount", 
            "threshold": 1.0,
            "compliance": "GAAP"  
        },
        {
            "rule_type": "completeness", 
            "column": "transaction_date", 
            "threshold": 1.0,
            "compliance": "SOX_404"
        },
        {
            "rule_type": "pattern", 
            "column": "account_number", 
            "pattern": "^[0-9]{10,12}$",
            "compliance": "Internal_Policy"
        }
    ]',
    'yaml',
    'financial_compliance'
);
```

## Troubleshooting Examples

### Example 14: Debugging Performance Issues

Optimize profiling for large datasets:

```python
# Start with basic profiling to assess size
basic_profile = profiler.profile_dataset(large_df, ProfileType.BASIC)
print(f"Dataset size: {basic_profile.row_count:,} rows, {basic_profile.column_count} columns")

# If dataset is large, use sampling and column selection
if basic_profile.row_count > 1_000_000:
    # Profile with 5% sample on key columns only
    key_columns = ['id', 'customer_id', 'amount', 'date', 'status']
    sampled_df = large_df.sample(fraction=0.05)
    selected_df = sampled_df.select(*key_columns)
    
    optimized_profile = profiler.profile_dataset(selected_df, ProfileType.STATISTICAL)
    print(f"Optimized profile completed in reduced time")
else:
    # Full profiling for smaller datasets
    full_profile = profiler.profile_dataset(large_df, ProfileType.FULL)
```

### Example 15: Handling Data Quality Issues

Address common data quality problems:

```python
def diagnose_quality_issues(profile):
    """Diagnose and suggest fixes for data quality issues."""
    
    issues_found = []
    suggestions = []
    
    for col in profile.column_profiles:
        # High null percentage
        if col.null_percentage > 20:
            issues_found.append(f"{col.column_name}: {col.null_percentage:.1f}% null values")
            suggestions.append(f"Consider: default values, data source investigation, or nullable design for {col.column_name}")
        
        # Low uniqueness for potential keys
        if col.null_percentage < 5 and col.uniqueness < 0.95 and 'id' in col.column_name.lower():
            issues_found.append(f"{col.column_name}: {col.uniqueness:.1%} uniqueness (potential key column)")
            suggestions.append(f"Investigate: duplicate values in {col.column_name}, consider composite keys")
        
        # Very low cardinality (potential enum/category)
        if col.distinct_count <= 10 and profile.row_count > 1000:
            issues_found.append(f"{col.column_name}: Only {col.distinct_count} distinct values")
            suggestions.append(f"Consider: enumeration constraints, reference tables for {col.column_name}")
    
    return issues_found, suggestions

# Usage
issues, suggestions = diagnose_quality_issues(profile)
if issues:
    print("Data Quality Issues Found:")
    for issue in issues:
        print(f"  ⚠️ {issue}")
    
    print("\nSuggested Actions:")
    for suggestion in suggestions:
        print(f"  💡 {suggestion}")
```

These examples demonstrate the full capabilities of the Data Profiling package, from basic statistical analysis to advanced relationship discovery and quality monitoring. Use them as templates for your own data profiling needs and adapt the configurations to match your specific requirements.