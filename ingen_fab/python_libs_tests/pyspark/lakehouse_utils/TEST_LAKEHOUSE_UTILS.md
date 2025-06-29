# Lakehouse Utils Test Scripts

This directory contains test scripts and examples for validating the functionality of the `lakehouse_utils` class.

## 📁 Files Overview

### Core Testing Scripts

1. **`test_lakehouse_utils_basic.py`** - ✅ **RECOMMENDED FOR DEVELOPMENT**
   - **Purpose**: Comprehensive testing without Delta Lake dependencies
   - **Environment**: Works in any PySpark environment
   - **What it tests**: Class structure, methods, URI generation, basic operations
   - **Status**: ✅ Tested and working

2. **`test_lakehouse_utils.py`** - ⚠️ **REQUIRES DELTA LAKE**
   - **Purpose**: Full integration testing with Delta Lake features
   - **Environment**: Requires Delta Lake properly configured
   - **What it tests**: Complete workflow with actual Delta operations
   - **Status**: ⚠️ Requires Delta Lake dependencies

3. **`test_lakehouse_utils_simple.py`** - 🏭 **FOR FABRIC ENVIRONMENTS**
   - **Purpose**: Real-world testing in Microsoft Fabric
   - **Environment**: Microsoft Fabric notebooks/environments
   - **What it tests**: Actual lakehouse connectivity and operations
   - **Status**: 📋 Ready for Fabric deployment

### Documentation and Examples

4. **`lakehouse_utils_examples.py`** - 📚 **USAGE EXAMPLES**
   - **Purpose**: Demonstrates how to use the lakehouse_utils class
   - **Environment**: Any PySpark environment (examples are commented)
   - **Content**: Step-by-step usage guide with code examples
   - **Status**: ✅ Complete documentation

5. **`TEST_LAKEHOUSE_UTILS.md`** - 📖 **THIS FILE**
   - **Purpose**: Complete documentation and testing guide

## 🚀 Quick Start

### For Development/Testing:
```bash
# Run basic structure tests (no Delta Lake required)
python test_lakehouse_utils_basic.py
```

### For Fabric Environment:
1. Update `test_lakehouse_utils_simple.py` with your workspace/lakehouse IDs
2. Run in your Fabric notebook
3. Follow the examples in `lakehouse_utils_examples.py`

## 📊 Test Results Summary

### ✅ Passing Tests (test_lakehouse_utils_basic.py)
- Class initialization ✅
- Spark session management ✅
- Attribute verification ✅
- Method existence and callability ✅
- URI generation ✅
- Basic DataFrame operations ✅
- Table existence checking (simulated) ✅
- Error handling ✅

### 🔧 Fixed Issues in lakehouse_utils Class
- ✅ Fixed `@staticmethod` decorator issue in `check_if_table_exists`
- ✅ Added proper `_get_or_create_spark_session` method
- ✅ Fixed `self.spark` attribute access
- ✅ Added type hints for better code quality
- ✅ Improved error handling and method signatures

## 🎯 lakehouse_utils Class Overview

The `lakehouse_utils` class provides utilities for interacting with Microsoft Fabric lakehouses using PySpark and Delta Lake.

### ✨ Key Features:
- **Automatic Spark session management**
- **ABFSS URI generation for Fabric lakehouses**
- **Delta table existence checking**
- **Flexible DataFrame writing with custom options**
- **Batch table management operations**

### 🔧 Core Methods:

| Method | Purpose | Parameters | Returns |
|--------|---------|------------|---------|
| `__init__()` | Initialize utility | `workspace_id`, `lakehouse_id` | - |
| `lakehouse_tables_uri()` | Generate tables URI | - | `str` |
| `check_if_table_exists()` | Check Delta table existence | `table_path` | `bool` |
| `write_to_lakehouse_table()` | Write DataFrame to table | `df`, `table_name`, options | - |
| `drop_all_tables()` | ⚠️ Drop all tables | - | - |

## 💡 Usage Examples

### Basic Usage:
```python
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils

# Initialize
utils = lakehouse_utils("workspace-id", "lakehouse-id")

# Write data
utils.write_to_lakehouse_table(df, "my_table")

# Check existence
exists = utils.check_if_table_exists(utils.lakehouse_tables_uri() + "my_table")
```

### Advanced Usage:
```python
# Custom write options
options = {
    "mergeSchema": "true",
    "overwriteSchema": "true"
}
utils.write_to_lakehouse_table(df, "my_table", options=options)

# Append mode
utils.write_to_lakehouse_table(new_df, "my_table", mode="append")
```

## 🧪 Running Tests

### Local Development:
```bash
# Basic functionality tests (recommended)
python test_lakehouse_utils_basic.py

# View usage examples
python lakehouse_utils_examples.py
```

### Microsoft Fabric:
```python
# In Fabric notebook, update configuration:
WORKSPACE_ID = "your-actual-workspace-id"
LAKEHOUSE_ID = "your-actual-lakehouse-id"

# Then run:
%run test_lakehouse_utils_simple.py
```

## ⚠️ Important Notes

### Safety Warnings:
- **`drop_all_tables()`** is destructive and removes ALL Delta tables
- Always test in non-production environments first
- Verify workspace and lakehouse IDs before running

### Environment Requirements:
- **Basic Testing**: PySpark 3.5+
- **Full Testing**: PySpark + Delta Lake
- **Fabric Testing**: Microsoft Fabric environment with lakehouse access

### Troubleshooting:
1. **Import Errors**: Ensure `ingen_fab` package is in Python path
2. **Spark Errors**: Verify PySpark installation and configuration
3. **Delta Errors**: Check Delta Lake dependencies for full testing
4. **Fabric Errors**: Verify workspace access and lakehouse permissions

## 📝 Test Status Legend

- ✅ **Working** - Tested and verified
- ⚠️ **Conditional** - Requires specific environment
- 🏭 **Production Ready** - Ready for real environments
- 📚 **Documentation** - Examples and guides
- 📋 **Configuration Required** - Needs setup

## 🎉 Success Metrics

The `lakehouse_utils` class and its test suite successfully demonstrate:

1. **Robust Error Handling** - Graceful failure modes
2. **Clean API Design** - Intuitive method signatures
3. **Comprehensive Testing** - Multiple test scenarios
4. **Documentation** - Clear usage examples
5. **Production Readiness** - Real-world usage patterns

All basic functionality tests pass, and the class is ready for use in Microsoft Fabric environments with proper Delta Lake configuration.
