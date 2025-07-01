# warehouse_utils Testing Framework

This document describes the comprehensive testing framework created for the `warehouse_utils` class against a local SQL Server instance.

## Files Updated

### 1. `local_sql_test.py` - Main Test File
**Purpose**: Comprehensive testing of `warehouse_utils` class with both real SQL Server and mocked connections.

**Key Features**:
- **Automatic SQL Server Detection**: Checks if SQL Server is available and falls back to mocked tests if not
- **Comprehensive Test Suite**: Tests all major functionality of `warehouse_utils`
- **Error Handling**: Tests error scenarios and edge cases
- **Cleanup**: Automatically cleans up test data and schemas

**Test Categories**:
1. **Initialization Tests**: Verify proper setup of `warehouse_utils` with SQL Server dialect
2. **Connection Tests**: Test database connectivity and basic queries
3. **Schema Operations**: Test schema creation and management
4. **Table Existence Checks**: Verify table existence detection
5. **Write Operations**: Test all write modes (overwrite, append, error, ignore)
6. **Query Execution**: Test direct SQL query execution
7. **Drop Operations**: Test table dropping with prefix filtering
8. **Error Handling**: Test invalid queries and data

### 2. `local_sql_test_demo.py` - Demo with Mocks
**Purpose**: Demonstration version that runs entirely with mocked connections.

**Key Features**:
- **Complete Mock Environment**: Simulates SQL Server without requiring actual database
- **All Test Scenarios**: Covers same test cases as main file
- **Educational**: Shows expected behavior and output format

## Usage

### Running Tests with Real SQL Server

1. **Start SQL Server**: Ensure SQL Server is running on `localhost:1433`
2. **Set Password**: Set the `SQL_SERVER_PASSWORD` environment variable
3. **Run Tests**: Execute `uv run python local_sql_test.py`

```bash
# Example
export SQL_SERVER_PASSWORD="YourPassword123!"
uv run python local_sql_test.py
```

### Running Demo Tests (No SQL Server Required)

```bash
# This will automatically run if SQL Server is not available
uv run python local_sql_test.py

# Or run the demo directly
uv run python local_sql_test_demo.py
```

## Test Implementation Details

### Connection String Format
```python
connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "UID=sa;"
    f"PWD={password};"
    "TrustServerCertificate=yes;"
)
```

### Mock Integration
The test framework includes intelligent mocking:
- **notebookutils**: Mocked for non-Fabric environments
- **pyodbc connections**: Mocked when SQL Server unavailable
- **Query results**: Simulated realistic responses

### Test Data Structure
```python
test_data = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'department': ['Engineering', 'Sales', 'Marketing']
})
```

## warehouse_utils Methods Tested

### Core Methods
- `__init__()`: Constructor with SQL Server dialect
- `get_connection()`: Database connection establishment
- `execute_query()`: SQL query execution
- `create_schema_if_not_exists()`: Schema management

### Table Operations
- `check_if_table_exists()`: Table existence verification
- `write_to_table()`: DataFrame writing with multiple modes
- `write_to_warehouse_table()`: Legacy method support
- `drop_all_tables()`: Bulk table dropping with prefix filtering

### Properties
- `target_workspace_id`: Workspace identifier
- `target_store_id`: Warehouse identifier

## Error Scenarios Tested

1. **Invalid SQL Syntax**: Malformed queries
2. **Missing Tables**: Operations on non-existent tables
3. **Invalid Data Types**: Non-serializable objects
4. **Connection Failures**: Network/authentication issues
5. **Schema Conflicts**: Duplicate schema creation

## Output Format

The test framework provides comprehensive output:

```
🚀 Starting comprehensive warehouse_utils tests against local SQL Server
================================================================================
✅ SQL Server detected! Running tests against real database.
================================================================================

==================== Connection Test ====================
🧪 Testing warehouse_utils connection...
✅ Connection established successfully
   - SQL Server Version: Microsoft SQL Server 2022...
✅ Connection Test PASSED

[... additional test output ...]

================================================================================
🎯 TEST SUMMARY
================================================================================
✅ Passed: 7/7
❌ Failed: 0/7
🎉 ALL TESTS PASSED! warehouse_utils is working correctly with local SQL Server.
```

## Dependencies Added

The following packages were added to support testing:
- `pyodbc`: SQL Server connectivity
- `pandas`: DataFrame operations

```bash
uv add pyodbc pandas
```

## Benefits of This Testing Framework

1. **Comprehensive Coverage**: Tests all major `warehouse_utils` functionality
2. **Flexible Execution**: Works with or without SQL Server
3. **Clear Output**: Easy-to-read test results with emoji indicators
4. **Realistic Scenarios**: Tests actual use cases and edge conditions
5. **Automated Cleanup**: Prevents test data pollution
6. **Educational Value**: Demonstrates proper usage patterns

## Future Enhancements

1. **Parameterized Tests**: Support for different SQL Server versions
2. **Performance Tests**: Measure execution times for large datasets
3. **Concurrent Testing**: Multi-threaded operation validation
4. **Integration Tests**: End-to-end workflow testing
5. **Security Tests**: Authentication and authorization scenarios

## Integration with Existing Test Suite

This testing framework complements the existing unit tests in `/tests/test_warehouse_utils.py` by providing:
- **Integration-level testing** vs unit-level mocking
- **Real database interactions** vs isolated component testing
- **End-to-end workflows** vs individual method testing
- **Performance validation** vs functional correctness

The combination provides comprehensive coverage across all testing levels.
