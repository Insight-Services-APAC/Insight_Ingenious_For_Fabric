# Data Profiling Package

[Home](../index.md) > [Packages](index.md) > Data Profiling

## At a Glance

| Item | Summary |
|------|---------|
| Purpose | Automated statistical analysis and relationship discovery for data quality assessment and schema understanding. |
| Inputs | Tables/DataFrames in Lakehouse or Warehouse; configuration via `config_data_profiling` table. |
| Outputs | Profile results in `profile_results` table; quality reports in YAML/HTML; relationship discovery reports. |
| Core commands | `ingen_fab package data_profiling compile`, `ingen_fab ddl compile`, `ingen_fab deploy deploy`. |
| When to use | You need comprehensive data profiling, quality assessment, or automated relationship discovery between tables. |

Note: For CLI flags and global options, see User Guide → CLI Reference and Deploy Guide.

The Data Profiling package provides comprehensive statistical analysis, data quality assessment, and automated relationship discovery for your datasets in Microsoft Fabric. It supports both traditional profiling metrics and advanced relationship discovery using both naming patterns and actual data values.

## Quick Start Guide

### Overview

The Data Profiling package enables you to:

- **Generate comprehensive statistical profiles** for any table or dataset
- **Perform automated relationship discovery** between tables using value-based analysis
- **Assess data quality** with configurable rules and thresholds  
- **Create visual reports** in HTML or space-efficient YAML format
- **Track profiling history** with timestamped results
- **Support multiple profile types** from basic statistics to full relationship analysis
- **Optimize performance** with UltraFast profiling for large datasets
- **Export results** for integration with data catalogs and governance tools

### Step 1: Package Compilation

To add the data profiling package to your workspace, compile it with the following command:

=== "macOS/Linux"

    ```bash
    ingen_fab package data_profiling compile --include-samples --target-datastore both
    ```

=== "Windows"

    ```powershell
    ingen_fab package data_profiling compile --include-samples --target-datastore both
    ```

This generates the notebooks and DDL scripts in your project directory:

```text
fabric_workspace_items/
└── data_profiling/
    ├── data_profiling_processor_lakehouse.Notebook
    ├── data_profiling_processor_warehouse.Notebook
    └── data_profiling_config_lakehouse.Notebook

ddl_scripts/
└── Lakehouses/
    └── Config/
        ├── 001_Initial_Creation_Profiling/
        └── 002_Sample_Data_Profiling/
└── Warehouses/
    └── Config/
        ├── 001_Initial_Creation_Profiling/
        └── 002_Sample_Data_Profiling/
```

### Step 2: Compile Project DDL

Update your project DDL to include the profiling configuration tables:

=== "macOS/Linux"

    ```bash
    ingen_fab ddl compile
    ```

=== "Windows"

    ```powershell
    ingen_fab ddl compile
    ```

This creates the necessary configuration and results tables for data profiling operations.

### Step 3: Deploy to Fabric

Deploy the compiled notebooks and DDL to your Fabric workspace:

```bash
ingen_fab deploy deploy
```

### Step 4: Execute Profiling

1. **Configure profiling targets** by adding records to the `config_data_profiling` table
2. **Run the profiling notebook** in your Fabric workspace
3. **Review results** in the `profile_results` table and generated reports

## Profile Types

The data profiling package supports multiple profile types for different analysis needs:

### BASIC Profile
Fundamental statistics for quick data understanding:

- Row count and column count
- Null counts and percentages
- Distinct value counts and percentages
- Completeness and uniqueness ratios
- Top 100 distinct values per column

**Use Case:** Quick data assessment, initial data exploration

### STATISTICAL Profile
Advanced statistical metrics for numeric analysis:

- Mean, median, standard deviation
- Min/max values and ranges
- Quartiles and percentile distributions
- Value length statistics for strings
- Data type-specific metrics

**Use Case:** Statistical analysis, data distribution understanding

### DATA_QUALITY Profile  
Comprehensive quality assessment:

- Data quality scoring
- Anomaly detection
- Business rule validation
- Data quality issue identification
- Completeness and consistency checks

**Use Case:** Data governance, quality monitoring, validation

### RELATIONSHIP Profile
Advanced relationship discovery between tables:

- Semantic type classification (identifier, measure, dimension, etc.)
- Column naming pattern analysis
- Value format detection (email, phone, UUID, etc.)
- Cross-table relationship discovery
- Foreign key relationship inference
- Business rule detection

**Use Case:** Schema understanding, data modeling, relationship mapping

### FULL Profile
Complete analysis including all above metrics plus:

- Correlation matrices for numeric columns
- Advanced relationship graphs
- Comprehensive data catalog information
- Performance-optimized processing

**Use Case:** Complete data assessment, data catalog population

## Configuration Schema

The data profiling package uses the `config_data_profiling` table for configuration:

```sql
CREATE TABLE config_data_profiling (
    profile_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    profile_type VARCHAR(50) DEFAULT 'BASIC',
    output_format VARCHAR(20) DEFAULT 'yaml',
    include_samples BOOLEAN DEFAULT TRUE,
    sample_rate FLOAT DEFAULT NULL,
    columns_to_profile TEXT DEFAULT NULL,
    quality_rules TEXT DEFAULT NULL,
    execution_group VARCHAR(100) DEFAULT 'default',
    is_enabled BOOLEAN DEFAULT TRUE,
    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    modified_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

### Configuration Parameters

| Parameter | Description | Default | Options |
|-----------|-------------|---------|---------|
| `table_name` | Target table/view name | Required | Any valid table name |
| `profile_type` | Type of profiling to perform | `BASIC` | `BASIC`, `STATISTICAL`, `DATA_QUALITY`, `RELATIONSHIP`, `FULL` |
| `output_format` | Report output format | `yaml` | `yaml`, `html`, `json` |
| `include_samples` | Include sample data in reports | `true` | `true`, `false` |
| `sample_rate` | Fraction of data to sample (null = auto) | `null` | 0.01-1.0 or null |
| `columns_to_profile` | Comma-separated column list | `null` | Column names or null for all |
| `quality_rules` | JSON array of quality rules | `null` | JSON rule definitions |
| `execution_group` | Batch execution grouping | `default` | Any string identifier |

## Advanced Features

### UltraFast Profiling

The package includes an optimized UltraFast profiler that provides 10-40x performance improvement for large datasets:

- **Single-pass aggregation** for multiple statistics
- **Intelligent sampling** with accuracy preservation  
- **Memory-optimized processing** for large tables
- **Exact distinct counting** maintaining 100% accuracy
- **Automatic performance optimization** based on data characteristics

The UltraFast profiler automatically activates for large datasets and maintains identical results to the standard profiler.

### Value-Based Relationship Discovery

Advanced relationship discovery goes beyond naming conventions to analyze actual data values:

#### Value Hash Signatures
- Creates fingerprints of column values for exact match detection
- Enables discovery of relationships even with different column names
- Optimizes performance through efficient hash comparisons

#### Statistical Correlation Analysis  
- Analyzes numeric distributions for similar patterns
- Detects measure-to-measure relationships
- Identifies derived columns and calculations

#### Cardinality-Based Analysis
- Identifies parent-child relationships through cardinality patterns
- Detects potential primary and foreign key columns
- Analyzes selectivity patterns for relationship inference

#### Pattern-Based Discovery
- Matches columns with similar value formats
- Groups columns by semantic patterns (emails, phones, IDs, etc.)
- Discovers relationships across different naming conventions

### Cross-Profile Analysis

The package includes a sophisticated cross-profile analyzer for multi-table relationship discovery:

```python
from ingen_fab.python_libs.common.cross_profile_analyzer import CrossProfileAnalyzer

# Analyze relationships across multiple table profiles
analyzer = CrossProfileAnalyzer(min_confidence=0.5)
relationships = analyzer.analyze_profiles(table_profiles)

# Generate comprehensive relationship report
report = analyzer.generate_relationship_report(relationships)
```

### Business Rules Detection

Automatically discovers and validates business rules:

- **Not-null constraints** based on completeness patterns
- **Range validations** from min/max value analysis  
- **Format validations** from pattern detection
- **Referential integrity** rules from relationship analysis
- **Custom rule definitions** via configuration

## Output Formats and Reports

### YAML Reports (Default)
Space-efficient, human-readable format optimized for:

- Large datasets with minimal storage overhead
- Version control integration
- Automated processing and parsing
- Cross-platform compatibility

Example YAML output structure:
```yaml
dataset_profile:
  dataset_name: customers
  metadata:
    profile_timestamp: '2024-08-20T10:58:37'
    row_count: 10000
    column_count: 15
    data_quality_score: 0.95
  columns:
  - name: customer_id
    data_type: LongType()
    basic_stats:
      null_count: 0
      distinct_count: 10000
      completeness: 1.0
      uniqueness: 1.0
    semantic_type: identifier
    naming_analysis:
      is_identifier: true
      confidence: 0.95
    business_rules:
    - rule_type: not_null_constraint
      description: Column customer_id should not be null
      confidence: 1.0
```

### HTML Reports
Rich, interactive reports with:

- Visual charts and graphs
- Sortable data tables  
- Drill-down capabilities
- Professional formatting for stakeholder presentations

### JSON Reports
Structured data format for:

- API integration
- Data catalog population
- Automated processing pipelines
- System-to-system communication

## Performance Optimization

### Adaptive Sampling
- Automatically adjusts sample size based on data characteristics
- Maintains statistical significance while optimizing performance
- Preserves accuracy for small datasets, scales for large ones

### Intelligent Caching
- Caches intermediate results for repeated analysis
- Optimizes memory usage for large datasets
- Reduces computation time for iterative profiling

### Parallel Processing
- Leverages Spark's distributed computing capabilities
- Processes multiple columns in parallel
- Scales automatically with cluster resources

## Integration Patterns

### Data Catalog Integration
```python
# Export profiles to data catalog
profile_data = profiler.profile_dataset(df, ProfileType.FULL)
catalog_entry = {
    'table_name': profile_data.dataset_name,
    'row_count': profile_data.row_count,
    'column_profiles': profile_data.column_profiles,
    'relationships': profile_data.entity_relationships,
    'quality_score': profile_data.data_quality_score
}
```

### Data Quality Monitoring
```python
# Set up quality monitoring
quality_rules = [
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
    }
]

# Profile with quality assessment
profile = profiler.profile_dataset(
    df, 
    ProfileType.DATA_QUALITY,
    quality_rules=quality_rules
)
```

### Automated Relationship Discovery
```python
# Profile multiple related tables
table_profiles = {}
for table_name, df in tables.items():
    table_profiles[table_name] = profiler.profile_dataset(
        df, ProfileType.RELATIONSHIP
    )

# Discover cross-table relationships
analyzer = CrossProfileAnalyzer(min_confidence=0.7)
relationships = analyzer.analyze_profiles(table_profiles)

# Generate SQL joins from discovered relationships
for rel in relationships:
    if rel.confidence > 0.8:
        print(f"JOIN {rel.target_table} ON {rel.source_table}.{rel.source_column} = {rel.target_table}.{rel.target_column}")
```

## API Reference

### Core Classes

#### DataProfilingPySpark
Main profiling class for PySpark-based analysis:

```python
from ingen_fab.python_libs.pyspark.data_profiling_pyspark import DataProfilingPySpark

profiler = DataProfilingPySpark(spark_session)
profile = profiler.profile_dataset(dataframe, ProfileType.FULL)
```

**Methods:**
- `profile_dataset(dataset, profile_type, columns, sample_size)` - Profile a complete dataset
- `profile_column(dataset, column_name, profile_type)` - Profile a single column  
- `compare_profiles(profile1, profile2)` - Compare two profiles for drift detection
- `generate_quality_report(profile, output_format)` - Generate formatted reports
- `suggest_data_quality_rules(profile)` - Auto-generate quality rules
- `validate_against_rules(dataset, rules)` - Validate data against rules

#### CrossProfileAnalyzer  
Advanced relationship discovery across multiple table profiles:

```python
from ingen_fab.python_libs.common.cross_profile_analyzer import CrossProfileAnalyzer

analyzer = CrossProfileAnalyzer(min_confidence=0.5)
relationships = analyzer.analyze_profiles(profiles_dict)
```

**Methods:**
- `analyze_profiles(profiles)` - Discover relationships across profiles
- `generate_relationship_report(relationships)` - Create relationship documentation

#### Profile Types Enum
```python
from ingen_fab.python_libs.interfaces.data_profiling_interface import ProfileType

ProfileType.BASIC        # Basic statistics
ProfileType.STATISTICAL  # Statistical analysis  
ProfileType.DATA_QUALITY # Quality assessment
ProfileType.RELATIONSHIP # Relationship discovery
ProfileType.FULL         # Complete analysis
```

### Data Structures

#### ColumnProfile
Comprehensive column-level statistics:

```python
@dataclass
class ColumnProfile:
    column_name: str
    data_type: str
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float
    # ... additional statistics
    semantic_type: Optional[SemanticType]
    naming_pattern: Optional[NamingPattern]  
    value_pattern: Optional[ValuePattern]
    business_rules: List[BusinessRule]
    value_statistics: Optional[ValueStatistics]
```

#### DatasetProfile
Complete dataset-level analysis:

```python
@dataclass  
class DatasetProfile:
    dataset_name: str
    row_count: int
    column_count: int
    profile_timestamp: str
    column_profiles: List[ColumnProfile]
    data_quality_score: Optional[float]
    entity_relationships: Optional[EntityRelationshipGraph]
```

## Troubleshooting

### Common Issues

#### Performance Issues with Large Datasets
**Symptoms:** Profiling takes too long or runs out of memory

**Solutions:**
- Enable UltraFast profiling (automatically enabled for large datasets)
- Reduce sample rate: `sample_rate: 0.1` for 10% sampling
- Profile subset of columns: `columns_to_profile: "col1,col2,col3"`
- Increase Spark executor memory

#### Relationship Discovery Not Finding Expected Relationships
**Symptoms:** Expected foreign key relationships not detected

**Solutions:**
- Use `ProfileType.RELATIONSHIP` instead of `BASIC`
- Lower confidence threshold in CrossProfileAnalyzer
- Check data quality - null values affect relationship detection
- Verify column value overlaps exist between tables

#### Memory Errors During Profiling
**Symptoms:** OutOfMemory exceptions during execution

**Solutions:**
- Increase `spark.driver.memory` and `spark.executor.memory`
- Enable sampling: `sample_rate: 0.05`
- Process tables individually rather than in batches
- Use BASIC profile type for initial assessment

#### Incorrect Statistics in Reports
**Symptoms:** Statistics don't match manual queries

**Solutions:**
- Verify UltraFast profiler accuracy (should be identical)
- Check for data changes between profiling runs
- Ensure proper null handling in custom queries
- Review sampling settings if using samples

### Debugging Tips

1. **Enable verbose logging** in Fabric notebooks
2. **Start with BASIC profile** before moving to FULL
3. **Test on small datasets** before scaling up
4. **Compare UltraFast vs standard** profiler results
5. **Check configuration table** for parameter errors

## Example Configurations

### Basic Table Profiling
```sql
INSERT INTO config_data_profiling (table_name, profile_type, output_format)
VALUES ('sales_data', 'BASIC', 'yaml');
```

### Advanced Relationship Discovery
```sql
INSERT INTO config_data_profiling (
    table_name, 
    profile_type, 
    output_format, 
    include_samples
) VALUES 
('customers', 'RELATIONSHIP', 'yaml', true),
('orders', 'RELATIONSHIP', 'yaml', true),
('order_items', 'RELATIONSHIP', 'yaml', true);
```

### Data Quality Monitoring
```sql
INSERT INTO config_data_profiling (
    table_name, 
    profile_type, 
    quality_rules,
    execution_group
) VALUES (
    'customer_master', 
    'DATA_QUALITY',
    '[{"rule_type": "completeness", "column": "customer_id", "threshold": 0.99}]',
    'daily_quality_check'
);
```

### Performance-Optimized Large Dataset
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
    0.1,  -- 10% sample for performance
    'transaction_id,customer_id,amount,transaction_date',  -- Key columns only
    'yaml'
);
```

## Best Practices

### Configuration Management
- **Version control** your profiling configurations
- **Use execution groups** to batch related tables
- **Start with BASIC** profiles before advancing to FULL
- **Test configurations** on development data first

### Performance Optimization
- **Leverage UltraFast profiling** for large datasets
- **Use appropriate sampling** for very large tables
- **Profile incrementally** rather than all tables at once
- **Monitor resource usage** and adjust accordingly

### Relationship Discovery
- **Profile related tables together** for cross-table analysis
- **Use consistent naming conventions** to improve detection accuracy
- **Validate discovered relationships** against known schema
- **Document relationship confidence levels** for governance

### Data Quality
- **Define quality rules upfront** based on business requirements
- **Monitor quality trends** over time
- **Set appropriate thresholds** based on data characteristics
- **Automate quality reporting** for stakeholder visibility

## Support and Resources

- **[CLI Reference](../guides/cli-reference.md#package)** - Complete command documentation
- **[Examples](../examples/index.md)** - Real-world profiling scenarios  
- **[Troubleshooting](../guides/troubleshooting.md#data-profiling-issues)** - Common issues and solutions
- **[Developer Guide](../developer_guide/packages.md)** - Custom profiling development
- **[Python APIs](../api/python_apis.md)** - Programmatic profiling interfaces

---

*Need help with data profiling? This comprehensive package provides everything needed for statistical analysis, quality assessment, and automated relationship discovery in your Microsoft Fabric environment.*