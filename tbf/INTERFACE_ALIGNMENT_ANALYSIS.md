# Data Store Interface Alignment Analysis

## Overview
This document compares the `lakehouse_utils` and `warehouse_utils` classes and presents a standardized interface that both can implement for consistent behavior across different storage systems.

## Standard Interface: `DataStoreInterface`

Located at: `ingen_fab/python_libs/interfaces/data_store_interface.py`

The interface defines the following common methods and properties:

### Properties
- `target_workspace_id: str` - The workspace ID
- `target_store_id: str` - The store ID (lakehouse_id or warehouse_id)

### Methods
- `get_connection() -> Any` - Get connection object
- `check_if_table_exists(table_name: str, schema_name: str | None = None) -> bool`
- `write_to_table(df, table_name: str, schema_name: str | None = None, mode: str = "overwrite", options: dict | None = None) -> None`
- `drop_all_tables(schema_name: str | None = None, table_prefix: str | None = None) -> None`

## Class Comparison

### lakehouse_utils (Spark/Delta Lake)
**Location**: `ingen_fab/python_libs/pyspark/lakehouse_utils.py`

**Key Characteristics**:
- Uses Spark and Delta Lake for big data processing
- Stores data in Delta format in lakehouse Tables directory
- No concept of schemas (tables are in flat Tables/ directory)
- Uses ABFSS URIs for Fabric or local file paths for testing

**Updated Methods**:
- ✅ `target_workspace_id` (property)
- ✅ `target_store_id` (property) 
- ✅ `get_connection()` - Returns SparkSession
- ✅ `check_if_table_exists(table_name, schema_name=None)` - schema_name ignored
- ✅ `write_to_table(df, table_name, schema_name=None, mode, options)` - schema_name ignored
- ✅ `drop_all_tables(schema_name=None, table_prefix=None)` - schema_name ignored
- 🔄 `write_to_lakehouse_table()` - Legacy method maintained for backward compatibility

### warehouse_utils (SQL Server/Fabric Warehouse)
**Location**: `ingen_fab/python_libs/python/warehouse_utils.py`

**Key Characteristics**:
- Uses SQL Server/Fabric Warehouse with SQL queries
- Supports schemas (default: "dbo")
- Uses pyodbc or Fabric notebook connections
- Works with pandas DataFrames

**Updated Methods**:
- ✅ `target_workspace_id` (property)
- ✅ `target_store_id` (property)
- ✅ `get_connection()` - Returns pyodbc connection or Fabric connection
- ✅ `check_if_table_exists(table_name, schema_name="dbo")`
- ✅ `write_to_table(df, table_name, schema_name="dbo", mode, options)` - New interface method
- ✅ `drop_all_tables(schema_name=None, table_prefix=None)` - Updated signature
- 🔄 `write_to_warehouse_table()` - Legacy method maintained for backward compatibility
- 🔄 `create_schema_if_not_exists()` - Warehouse-specific method

## Key Differences Addressed

### 1. Method Naming Standardization
- **Before**: `write_to_lakehouse_table()` vs `write_to_warehouse_table()`
- **After**: Both implement `write_to_table()` with legacy methods for backward compatibility

### 2. Parameter Alignment
- **Before**: Different parameter orders and optional parameters
- **After**: Consistent signature: `(df, table_name, schema_name=None, mode="overwrite", options=None)`

### 3. Schema Handling
- **lakehouse_utils**: Ignores `schema_name` parameter (tables in flat Tables/ directory)
- **warehouse_utils**: Uses `schema_name` with default "dbo"

### 4. Connection Management
- **lakehouse_utils**: Returns `SparkSession`
- **warehouse_utils**: Returns pyodbc connection or Fabric connection object

### 5. Error Handling
- Both classes maintain their specific error handling while implementing the common interface

## Benefits of Standardization

1. **Consistent API**: Users can switch between storage systems with minimal code changes
2. **Polymorphism**: Code can be written against the interface without knowing the implementation
3. **Testing**: Easier to create mock implementations for testing
4. **Documentation**: Clear contract for what each storage system supports
5. **Future Extensions**: New storage systems can implement the same interface

## Usage Examples

### Before Standardization
```python
# Different methods for different stores
lakehouse = lakehouse_utils(workspace_id, lakehouse_id)
lakehouse.write_to_lakehouse_table(df, "my_table")

warehouse = warehouse_utils(workspace_id, warehouse_id)
warehouse.write_to_warehouse_table(df, "my_table", "dbo")
```

### After Standardization
```python
# Same interface for both stores
store1 = lakehouse_utils(workspace_id, lakehouse_id)
store2 = warehouse_utils(workspace_id, warehouse_id)

# Polymorphic usage
for store in [store1, store2]:
    store.write_to_table(df, "my_table")
    if store.check_if_table_exists("my_table"):
        print(f"Table exists in {store.target_store_id}")
```

## Implementation Notes

1. **Backward Compatibility**: Original methods are preserved to avoid breaking existing code
2. **Type Hints**: Full type hinting added for better IDE support and documentation
3. **Error Handling**: Each implementation maintains its specific error handling patterns
4. **Properties**: Consistent property access for workspace and store IDs
5. **Optional Parameters**: Schema parameter is optional to support both storage paradigms

## Future Improvements

1. **Full Interface Implementation**: Both classes could formally implement the `DataStoreInterface` abstract class
2. **Configuration Validation**: Add validation for required parameters in constructors
3. **Logging Standardization**: Align logging patterns between both classes
4. **Connection Pooling**: Consider connection pooling strategies for warehouse_utils
5. **Async Support**: Add async method variants for better performance in concurrent scenarios
