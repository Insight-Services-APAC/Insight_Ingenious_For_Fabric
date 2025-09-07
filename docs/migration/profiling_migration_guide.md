# Data Profiling Migration Guide

## Overview
The data profiling module has been refactored to consolidate multiple profiling approaches into a single, powerful **TieredProfiler** system. This guide will help you migrate from the previous profiling implementations to the new unified approach.

## What's Changed

### Removed Components
- **UltraFastProfiler** - Functionality merged into TieredProfiler Level 3
- **Ultra-fast single-pass profiling** - Now part of TieredProfiler's efficient scanning

### New Primary Component
- **TieredProfiler** - A progressive 4-level profiling system that combines the best of all previous approaches

## Key Benefits of TieredProfiler

1. **Progressive Enhancement**: Start with fast metadata scans and progressively add detail
2. **Resumable Operations**: Can restart from any level if interrupted
3. **Persistent State**: Saves progress between runs using Delta tables
4. **Memory Efficient**: Optimized for datasets from thousands to billions of rows
5. **Flexible**: Works with DataFrames, table names, and lakehouse tables

## The Four Scan Levels

### Level 1: Discovery (Metadata Only)
- Fast table discovery and basic metadata
- Row counts, file counts, partition information
- No data scanning required

### Level 2: Schema (Column Metadata)
- Column names, types, and nullability
- Identifies potential keys based on naming patterns
- Still no data scanning

### Level 3: Profile (Single-Pass Statistics)
- Efficient single-pass profiling
- Basic statistics, null counts, distinct counts
- Uses approximate algorithms for large datasets
- Replaces the old UltraFastProfiler functionality

### Level 4: Advanced (Multi-Pass Analysis)
- Cross-column correlations
- Relationship discovery
- Pattern detection
- Business rule inference
- Percentiles and distributions

## Migration Examples

### Using ProfilerRegistry (Recommended)

#### Before (using UltraFastProfiler):
```python
from ingen_fab.python_libs.interfaces.profiler_registry import get_profiler

# Old way - ultra_fast no longer exists
profiler = get_profiler("ultra_fast", spark=spark)
profile = profiler.profile_dataset("my_table", ProfileType.BASIC)
```

#### After (using TieredProfiler):
```python
from ingen_fab.python_libs.interfaces.profiler_registry import get_profiler

# New way - tiered is now the default
profiler = get_profiler("tiered", spark=spark, lakehouse=lakehouse)
# Or simply use "default"
profiler = get_profiler("default", spark=spark, lakehouse=lakehouse)

profile = profiler.profile_dataset("my_table", ProfileType.BASIC)
```

### Direct Usage

#### Before:
```python
from ingen_fab.python_libs.pyspark.ultra_fast_profiler import UltraFastProfiler

profiler = UltraFastProfiler(spark=spark)
profile = profiler.profile_dataset(df, ProfileType.BASIC)
```

#### After:
```python
from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler

profiler = TieredProfiler(spark=spark, lakehouse=lakehouse)
profile = profiler.profile_dataset(df, ProfileType.BASIC)
```

### Progressive Profiling (New Capability)

The TieredProfiler allows you to run scans progressively:

```python
from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler

profiler = TieredProfiler(lakehouse=lakehouse, table_prefix="my_profiles")

# Run Level 1 - Fast discovery
tables = profiler.scan_level_1_discovery()

# Run Level 2 - Schema analysis
schemas = profiler.scan_level_2_schema()

# Run Level 3 - Efficient profiling (equivalent to old UltraFastProfiler)
profiles = profiler.scan_level_3_profile()

# Run Level 4 - Advanced analytics (when needed)
advanced = profiler.scan_level_4_advanced()

# Or run all at once via profile_dataset()
full_profile = profiler.profile_dataset("my_table", ProfileType.FULL)
```

### Resumable Operations

If a scan is interrupted, TieredProfiler can resume from where it left off:

```python
# Initial run - might be interrupted
profiler.scan_level_3_profile(resume=True)

# Later - will skip already processed tables
profiler.scan_level_3_profile(resume=True)
```

### Exploring Results

Use the ProfileExplorer for interactive analysis:

```python
explorer = profiler.get_explorer()
explorer.show_summary()
explorer.show_table_details("my_table")
```

## ProfileType Mapping

The ProfileType enum now maps to TieredProfiler scan levels:

| ProfileType | Scan Levels Used | Description |
|------------|------------------|-------------|
| BASIC | Level 1 & 2 | Metadata and schema only |
| STATISTICAL | Level 1, 2 & 3 | Includes efficient statistics |
| DATA_QUALITY | Level 1, 2 & 3 | Quality metrics and completeness |
| RELATIONSHIP | All 4 levels | Full analysis with relationships |
| FULL | All 4 levels | Complete profiling |

## Configuration Changes

### DataProfilingPySpark Auto-Selection

The standard profiler now automatically uses TieredProfiler for large datasets:

```python
from ingen_fab.python_libs.pyspark.data_profiling_pyspark import DataProfilingPySpark

# Automatically switches to TieredProfiler for datasets > 10M rows
profiler = DataProfilingPySpark(
    spark=spark,
    lakehouse=lakehouse,
    use_tiered_for_large_datasets=True,  # Default
    large_dataset_threshold=10_000_000    # Default
)
```

## Performance Considerations

### When to Use Each Profiler

| Dataset Size | Recommended Profiler | Reason |
|-------------|---------------------|---------|
| < 1M rows | Standard | Full features, reasonable performance |
| 1M - 10M rows | Optimized or Tiered | Better memory management |
| > 10M rows | Tiered (auto-selected) | Progressive scanning, persistent state |
| > 100M rows | Tiered (required) | Only viable option for very large datasets |

### Memory Usage

TieredProfiler uses memory-efficient algorithms:
- Approximate distinct counts (HyperLogLog)
- Staged aggregation for very large datasets
- Persistent intermediate results

## Troubleshooting

### Issue: ImportError for UltraFastProfiler
```python
# Old import - no longer works
from ingen_fab.python_libs.pyspark.ultra_fast_profiler import UltraFastProfiler
```

**Solution**: Use TieredProfiler instead:
```python
from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler
```

### Issue: Missing lakehouse parameter
TieredProfiler works best with lakehouse_utils for persistence:

```python
# Minimal setup
from ingen_fab.python_libs.pyspark.tiered_profiler import TieredProfiler
profiler = TieredProfiler(spark=spark)  # Works but limited

# Recommended setup
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
lakehouse = lakehouse_utils(...)
profiler = TieredProfiler(lakehouse=lakehouse)  # Full functionality
```

### Issue: Profile results not persisting
Ensure Delta tables can be created in your lakehouse:

```python
# Check persistence tables
profiler.get_scan_summary()  # Shows all scan progress
```

## Best Practices

1. **Use the Registry**: Always get profilers through ProfilerRegistry for flexibility
2. **Start with Level 3**: For most use cases, Level 3 provides excellent statistics efficiently
3. **Use Resume**: For large batch jobs, enable resume to handle interruptions
4. **Monitor Progress**: Use get_scan_summary() to track profiling progress
5. **Leverage Persistence**: Results are saved automatically - query them later

## Support

For issues or questions about the migration:
1. Check the [ProfileExplorer documentation](../packages/data_profiling.md)
2. Review example notebooks in `sample_project/`
3. File issues on the project repository