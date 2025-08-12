# Synthetic Data Generator Enhancements

[Home](../index.md) > [Packages](index.md) > Synthetic Data Generator Enhancements

This document outlines the comprehensive enhancements made to the synthetic data generator package to address the requirements for:

1. **Runtime parameter flexibility** - Moving more code to `python_libs` for runtime configuration
2. **Enhanced file naming and folder structures** - Supporting date-based patterns and custom naming
3. **Comprehensive logging with date column tracking** - Detailed correlation information

## Overview of Changes

### 1. Enhanced Configuration System (`python_libs/common/synthetic_data_config.py`)

**Key Features:**
- **Runtime Parameter Support**: Configuration can be modified at notebook execution time
- **Flexible Table Definitions**: Tables can be configured with different types (snapshot/incremental)
- **Dynamic Row Calculation**: Row counts calculated based on seasonal patterns, growth rates, and multipliers
- **Validation System**: Built-in configuration validation with detailed error reporting

**Classes:**
- `TableGenerationConfig`: Individual table configuration with runtime calculation methods
- `DatasetConfiguration`: Complete dataset configuration with runtime override support
- `ConfigurationManager`: Manages predefined configurations and template creation

**Example Usage:**
```python
# Create configuration that can be modified at runtime
config = DatasetConfiguration.from_dict(base_config)

# Apply runtime overrides without recompiling
runtime_overrides = {
    "table_configs": {
        "customers": {"base_rows": 100000, "growth_rate": 0.003},
        "orders": {"base_rows_per_day": 50000, "seasonal_enabled": True}
    },
    "incremental_config": {
        "seasonal_multipliers": {"friday": 1.5, "saturday": 1.3}
    }
}
config.apply_runtime_overrides(runtime_overrides)
```

### 2. Advanced File Path Generation (`python_libs/common/file_path_utils.py`)

**Key Features:**
- **Multiple Path Patterns**: 9+ predefined patterns including nested, flat, and Hive-style
- **Date-Based Naming**: Files can include dates in folders or filenames
- **Custom Pattern Support**: Users can define custom path templates
- **Path Validation**: Validates paths for common issues

**Available Patterns:**
- `nested_daily`: `/2024/01/15/table_name.parquet`
- `flat_with_date`: `/20240115_table_name.parquet`
- `hive_partitioned`: `/year=2024/month=01/day=15/table_name.parquet`
- `quarter_based`: `/2024/Q1/table_name_20240115.parquet`
- And more...

**Example Usage:**
```python
# Configure custom path pattern at runtime
runtime_overrides = {
    "output_settings": {
        "path_format": "hive_partitioned",  # or "custom"
        "custom_path_pattern": "{base_path}/year={yyyy}/month={mm}/{table_name}_{yyyymmdd}",
        "file_extension": "parquet"
    }
}
```

### 3. Enhanced Logging with Date Column Tracking (`python_libs/common/synthetic_data_logger.py`)

**Key Features:**
- **Date Column Analysis**: Automatically identifies and analyzes date columns in generated data
- **Correlation Tracking**: Links file names to actual date ranges in the data
- **Comprehensive Metrics**: Tracks generation rates, durations, and data quality
- **Export Capabilities**: Logs can be exported to JSON or CSV

**Classes:**
- `SyntheticDataLogger`: Main logging interface with enhanced capabilities
- `TableGenerationMetrics`: Comprehensive metrics for each table
- `DatasetGenerationSummary`: Overall dataset generation summary
- `DateColumnInfo`: Detailed information about date columns

**Example Output:**
```
📊 Generated orders:
   📈 Rows: 100,000
   📅 Date columns: order_date (PRIMARY), shipped_date, delivered_date
   🗓️ Data date range: 2024-01-15 to 2024-01-15
   🔗 File 20240115_orders.parquet contains 100,000 records with order_date = 2024-01-15
```

### 4. Enhanced Incremental Data Utils (`python_libs/pyspark/incremental_synthetic_data_utils.py`)

**Key Features:**
- **Configuration-Driven Generation**: Uses enhanced configuration system
- **Advanced File Path Support**: Integrates with new path generation system
- **Enhanced Logging Integration**: Provides detailed metrics and correlation info
- **Backward Compatibility**: Falls back to legacy methods when enhanced features unavailable

**New Methods:**
- `generate_dataset_from_config()`: Main method using enhanced configuration
- `_generate_enhanced_file_path()`: Uses advanced path patterns
- `_save_table_data_enhanced()`: Enhanced saving with path validation

### 5. Configurable Data Generator (`python_libs/pyspark/configurable_synthetic_data_generator.py`)

**Key Features:**
- **High-Level Interface**: Simplified API for complex data generation
- **Runtime Scaling**: Apply scale factors and multipliers at runtime
- **Preview Mode**: Preview generation plans without generating data
- **Template System**: Create configurations from predefined templates

**Classes:**
- `ConfigurableSyntheticDataGenerator`: Main generator with runtime configuration
- `ConfigurableDatasetBuilder`: High-level dataset construction utilities

**Example Usage:**
```python
# Initialize generator
generator = ConfigurableSyntheticDataGenerator(
    lakehouse_utils_instance=lh_utils,
    enhanced_logging=True
)

# Generate with runtime scaling
results = generator.generate_dataset_with_runtime_scaling(
    base_config_id="retail_oltp_enhanced",
    generation_date="2024-01-15",
    scale_factor=1.5,  # 50% more data
    table_multipliers={"orders": 2.0}  # Double orders specifically
)

# Preview before generating
plan = generator.preview_generation_plan(
    dataset_config_id="retail_oltp_enhanced",
    generation_date="2024-01-15"
)
```

### 6. Enhanced Notebook Template (`templates/enhanced_synthetic_data_lakehouse_notebook.py.jinja`)

**Key Features:**
- **Runtime Parameter Cells**: Configurable parameters at the top of notebook
- **Generation Preview**: Shows what will be generated before execution
- **Enhanced Validation**: Comprehensive data quality checks
- **Detailed Reporting**: Rich output with correlation information

**Runtime Parameters:**
```python
# Runtime Configuration Overrides (Configurable at Runtime!)
runtime_overrides = {
    "global_scale_factor": 1.0,
    "table_configs": {
        "customers": {"base_rows": 50000, "growth_enabled": True},
        "orders": {"base_rows_per_day": 100000, "seasonal_enabled": True}
    },
    "incremental_config": {
        "seasonal_multipliers": {
            "friday": 1.3, "saturday": 1.2, "sunday": 0.7
        }
    },
    "output_settings": {
        "path_format": "nested_daily",
        "custom_path_pattern": "{base_path}/year={yyyy}/month={mm}/{table_name}_{yyyymmdd}"
    }
}
```

### 7. Enhanced Package Compiler (`synthetic_data_generation.py`)

**Key Features:**
- **Enhanced Compilation Methods**: New methods for advanced configuration
- **Template Selection**: Automatically chooses appropriate templates
- **Configuration Templates**: Support for predefined configuration templates
- **Backward Compatibility**: Falls back to legacy methods when needed

**New Methods:**
- `compile_enhanced_synthetic_data_notebook()`: Uses enhanced features
- `compile_configurable_dataset_notebook()`: Uses configuration templates
- `compile_all_enhanced_synthetic_data_notebooks()`: Batch compilation with enhancements

## Benefits Achieved

### A) Runtime Parameter Flexibility ✅

**Before**: Parameters hardcoded at compilation time
```python
# Fixed at compile time
target_rows = 10000
chunk_size = 1000000
```

**After**: Parameters configurable at runtime
```python
# Configurable at runtime
runtime_overrides = {
    "table_configs": {
        "orders": {"base_rows_per_day": 50000}  # Can be changed without recompiling
    },
    "global_scale_factor": 1.5  # Apply 50% scaling to all tables
}
```

### B) Enhanced Date-Based File Naming ✅

**Before**: Limited to basic patterns
- `/YYYY/MM/DD/table_name.parquet`
- `YYYYMMDD_table_name.parquet`

**After**: Multiple flexible patterns
- Hive partitioning: `/year=2024/month=01/day=15/table_name.parquet`
- Quarter-based: `/2024/Q1/table_name_20240115.parquet`
- Custom patterns: User-defined templates

### C) Comprehensive Logging with Date Correlation ✅

**Before**: Basic row count logging
```
✅ Generated orders: 100,000 rows
```

**After**: Detailed correlation information
```
✅ Generated orders:
   📈 Rows: 100,000
   📅 Date columns: order_date (PRIMARY), shipped_date, delivered_date
   🗓️ Data date range: 2024-01-15 to 2024-01-15
   🔗 File 20240115_orders.parquet contains 100,000 records with order_date = 2024-01-15
```

## Usage Examples

### 1. Basic Enhanced Generation
```python
from configurable_synthetic_data_generator import ConfigurableSyntheticDataGenerator

generator = ConfigurableSyntheticDataGenerator(lakehouse_utils_instance=lh_utils)

# Generate with runtime overrides
results = generator.generate_dataset_from_config(
    dataset_config_id="retail_oltp_enhanced",
    generation_date="2024-01-15",
    runtime_overrides={
        "table_configs": {"orders": {"base_rows_per_day": 50000}},
        "output_settings": {"path_format": "hive_partitioned"}
    }
)
```

### 2. Custom Dataset Creation
```python
# Define custom tables at runtime
custom_tables = [
    {
        "table_name": "events",
        "table_type": "incremental",
        "base_rows_per_day": 10000,
        "date_columns": ["event_timestamp"],
        "primary_date_column": "event_timestamp"
    }
]

results = generator.generate_custom_dataset(
    dataset_id="custom_events",
    table_definitions=custom_tables,
    generation_date="2024-01-15"
)
```

### 3. Preview Generation Plan
```python
# Preview what will be generated
plan = generator.preview_generation_plan(
    dataset_config_id="retail_oltp_enhanced",
    generation_date="2024-01-15",
    runtime_overrides={"global_scale_factor": 2.0}
)

print(f"Will generate {plan['total_estimated_rows']:,} rows across {len(plan['tables'])} tables")
```

## Migration Guide

### For Existing Users
1. **No Breaking Changes**: All existing notebooks continue to work
2. **Gradual Migration**: Can adopt enhanced features incrementally
3. **Fallback Support**: Enhanced features gracefully fall back to legacy methods

### For New Users
1. **Use Enhanced Templates**: Start with `enhanced_synthetic_data_lakehouse_notebook.py.jinja`
2. **Configure at Runtime**: Modify parameters in the notebook rather than recompiling
3. **Leverage Advanced Features**: Use custom path patterns and enhanced logging

## Technical Architecture

```
Enhanced Synthetic Data Generator Architecture

┌─────────────────────────────────────────────────────────────────┐
│                        Notebook Layer                          │
├─────────────────────────────────────────────────────────────────┤
│  Enhanced Template with Runtime Parameters                     │
│  • Configurable parameters at notebook execution               │
│  • Preview generation plans                                    │
│  • Enhanced validation and reporting                           │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Configuration Layer                         │
├─────────────────────────────────────────────────────────────────┤
│  ConfigurableSyntheticDataGenerator                           │
│  • Runtime parameter application                               │
│  • Template-based configuration                                │
│  • High-level dataset building                                 │
└─────────────────────────────────────────────────────────────────┘
                                │
                    ┌───────────┼───────────┐
                    ▼           ▼           ▼
┌─────────────────┐ ┌──────────────┐ ┌──────────────┐
│  Configuration  │ │  File Path   │ │   Enhanced   │
│    Management   │ │  Generation  │ │   Logging    │
├─────────────────┤ ├──────────────┤ ├──────────────┤
│ • Runtime       │ │ • 9+ patterns│ │ • Date column│
│   overrides     │ │ • Custom     │ │   analysis   │
│ • Validation    │ │   templates  │ │ • Correlation│
│ • Templates     │ │ • Path       │ │   tracking   │
└─────────────────┘ │   validation │ │ • Metrics    │
                    └──────────────┘ └──────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Generation Layer                            │
├─────────────────────────────────────────────────────────────────┤
│  Enhanced Incremental Generator + Base Generators              │
│  • Configuration-driven generation                             │
│  • Advanced file path support                                  │
│  • Comprehensive logging integration                           │
└─────────────────────────────────────────────────────────────────┘
```

## Conclusion

These enhancements successfully address all three requirements:

1. ✅ **Runtime Flexibility**: Parameters can now be modified at notebook execution time without recompilation
2. ✅ **Advanced File Naming**: Multiple date-based patterns support various organizational needs
3. ✅ **Enhanced Logging**: Comprehensive date column tracking provides clear correlation between files and data

The implementation maintains backward compatibility while providing powerful new capabilities for users who need more flexibility and control over their synthetic data generation workflows.