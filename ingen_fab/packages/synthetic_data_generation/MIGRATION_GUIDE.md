# Synthetic Data Generation Migration Guide

## Overview

The synthetic data generation package has been unified and modernized. This guide helps you migrate from the old commands to the new unified interface.

## New Unified Command Structure

### Before (Old Commands)
```bash
# Multiple scattered commands
python -m ingen_fab.cli package synthetic-data compile --dataset-id retail_oltp_small
python -m ingen_fab.cli package synthetic-data generate retail_oltp_small --target-rows 10000
python -m ingen_fab.cli package synthetic-data generate-incremental retail_oltp_small_incremental
python -m ingen_fab.cli package synthetic-data generate-series retail_oltp_small_incremental --start-date 2024-01-01
python -m ingen_fab.cli package synthetic-data list-datasets
python -m ingen_fab.cli package synthetic-data list-enhanced
python -m ingen_fab.cli package synthetic-data compile-generic-templates
```

### After (New Unified Commands)
```bash
# Three simple, powerful commands
python -m ingen_fab.cli package synthetic-data generate <dataset-id> [OPTIONS]
python -m ingen_fab.cli package synthetic-data list [OPTIONS]
python -m ingen_fab.cli package synthetic-data compile-unified <template> [OPTIONS]
```

## Command Migration Examples

### 1. Single Dataset Generation
**Old:**
```bash
python -m ingen_fab.cli package synthetic-data generate retail_oltp_small --target-rows 50000 --seed 123
```

**New:**
```bash
python -m ingen_fab.cli package synthetic-data generate retail_oltp_small \
  --mode single \
  --parameters '{"target_rows": 50000, "seed_value": 123}'
```

### 2. Incremental Data Generation
**Old:**
```bash
python -m ingen_fab.cli package synthetic-data generate-incremental retail_oltp_small_incremental \
  --date 2024-01-15 --path-format nested
```

**New:**
```bash
python -m ingen_fab.cli package synthetic-data generate retail_oltp_small_incremental \
  --mode incremental \
  --parameters '{"generation_date": "2024-01-15", "path_format": "nested"}'
```

### 3. Series Generation
**Old:**
```bash
python -m ingen_fab.cli package synthetic-data generate-series retail_oltp_small_incremental \
  --start-date 2024-01-01 --end-date 2024-01-31 --batch-size 5
```

**New:**
```bash
python -m ingen_fab.cli package synthetic-data generate retail_oltp_small_incremental \
  --mode series \
  --parameters '{"start_date": "2024-01-01", "end_date": "2024-01-31", "batch_size": 5}'
```

### 4. Listing Resources
**Old:**
```bash
python -m ingen_fab.cli package synthetic-data list-datasets
python -m ingen_fab.cli package synthetic-data list-enhanced
python -m ingen_fab.cli package synthetic-data list-generic-templates
```

**New:**
```bash
python -m ingen_fab.cli package synthetic-data list --type datasets
python -m ingen_fab.cli package synthetic-data list --type templates
python -m ingen_fab.cli package synthetic-data list --type all
```

### 5. Compilation
**Old:**
```bash
python -m ingen_fab.cli package synthetic-data compile --dataset-id retail_oltp_small --enhanced
python -m ingen_fab.cli package synthetic-data compile-generic-templates --target-environment lakehouse
```

**New:**
```bash
python -m ingen_fab.cli package synthetic-data compile-unified retail_oltp_small \
  --runtime-config '{"enhanced": true}'
python -m ingen_fab.cli package synthetic-data compile-unified generic_single_dataset_lakehouse
```

## Python API Migration

### Old Python API
```python
from ingen_fab.packages.synthetic_data_generation.synthetic_data_generation import SyntheticDataGenerationCompiler
from ingen_fab.packages.synthetic_data_generation.incremental_data_generation import IncrementalSyntheticDataGenerationCompiler

# Old way - multiple classes
compiler = SyntheticDataGenerationCompiler()
incremental_compiler = IncrementalSyntheticDataGenerationCompiler()

# Generate single dataset
notebook_path = compiler.compile_predefined_dataset_notebook(
    dataset_id="retail_oltp_small",
    target_rows=10000
)

# Generate incremental
notebook_path = incremental_compiler.compile_incremental_dataset_notebook(
    dataset_config=config,
    generation_date="2024-01-01"
)
```

### New Python API
```python
from ingen_fab.packages.synthetic_data_generation.unified_commands import UnifiedSyntheticDataGenerator, GenerationMode

# New way - single unified class
generator = UnifiedSyntheticDataGenerator(
    fabric_workspace_repo_dir="./project",
    fabric_environment="development"
)

# Generate single dataset
result = generator.generate(
    config="retail_oltp_small",
    mode=GenerationMode.SINGLE,
    parameters={"target_rows": 10000}
)

# Generate incremental
result = generator.generate(
    config="retail_oltp_small_incremental", 
    mode=GenerationMode.INCREMENTAL,
    parameters={"generation_date": "2024-01-01"}
)

# Generate series
result = generator.generate(
    config="retail_oltp_small_incremental",
    mode=GenerationMode.SERIES,
    parameters={
        "start_date": "2024-01-01",
        "end_date": "2024-01-31"
    }
)

# List available resources
datasets = generator.list_items(ListType.DATASETS)
templates = generator.list_items(ListType.TEMPLATES)
```

## Backward Compatibility

For existing code, backward compatibility is maintained through migration adapters:

```python
# This still works but shows deprecation warnings
from ingen_fab.packages.synthetic_data_generation.migration_adapter import create_legacy_compiler

compiler = create_legacy_compiler()
result = compiler.compile_predefined_dataset_notebook("retail_oltp_small")
```

## Benefits of the New System

1. **Simplified Interface**: 3 commands instead of 10+
2. **Consistent Parameters**: JSON-based parameter system
3. **Better Validation**: Built-in parameter validation
4. **Dry Run Support**: Test configurations without generation
5. **Unified Backend**: Single code path for all modes
6. **Better Error Handling**: Structured error responses
7. **Future-Proof**: Extensible architecture

## Migration Timeline

- **Phase 1** (Current): Old commands show deprecation warnings
- **Phase 2** (3 months): Old commands marked as legacy
- **Phase 3** (6 months): Old commands removed in major version

## Getting Help

If you encounter issues during migration:

1. Use `--dry-run` flag to test new commands
2. Check parameter validation with new system
3. Use `--help` on new commands for detailed usage
4. Consult this guide for command mappings

## Common Parameter Mappings

| Old Parameter | New Parameter (in JSON) | Notes |
|---------------|------------------------|-------|
| `--target-rows` | `target_rows` | Direct mapping |
| `--seed` | `seed_value` | Renamed for consistency |
| `--date` | `generation_date` | For incremental mode |
| `--start-date` | `start_date` | For series mode |
| `--end-date` | `end_date` | For series mode |
| `--batch-size` | `batch_size` | For series mode |
| `--path-format` | `path_format` | Direct mapping |
| `--output-mode` | `output_mode` | Direct mapping |

Start migrating today to take advantage of the improved synthetic data generation system!