# DDL Utils Interface

This document describes the interface for DDL utilities that provide execution tracking and script management in a lakehouse environment.

## Overview

The `DDLUtilsInterface` defines a contract for utilities that handle:
- DDL script execution tracking
- Prevention of duplicate script runs
- Execution logging and status monitoring
- Automatic GUID generation for scripts

## Interface Definition

The interface is defined as a Python Protocol in `ddl_utils_interface.py`, which provides structural typing without requiring explicit inheritance.

### Key Methods

#### `__init__(target_workspace_id: str, target_lakehouse_id: str) -> None`
Initialize the DDL utilities with workspace and lakehouse identifiers.

#### `execution_log_schema() -> StructType`
Static method that returns the schema for the execution log table.

#### `print_log() -> None`
Display the execution log table showing all script executions.

#### `check_if_script_has_run(script_id: str) -> bool`
Check if a script has already been successfully executed.

#### `run_once(work_fn: Callable[[], None], object_name: str, guid: str | None = None) -> None`
Execute a function exactly once, tracked by GUID. If no GUID is provided, one will be auto-generated from the function's source code.

#### `write_to_execution_log(object_guid: str, object_name: str, script_status: str) -> None`
Write an execution entry to the log table.

#### `initialise_ddl_script_executions_table() -> None`
Initialize the execution log table if it doesn't exist.

## Implementation

The `ddl_utils` class implements this interface and provides:

- **Execution tracking**: Maintains a Delta table of script executions
- **Idempotency**: Ensures scripts run exactly once using GUID-based tracking
- **Error handling**: Logs both successful and failed executions
- **Auto-GUID generation**: Generates unique identifiers from function source code when not provided

## Usage Example

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .ddl_utils_interface import DDLUtilsInterface

def example_ddl_operations(ddl_util: DDLUtilsInterface) -> None:
    # Define a DDL operation
    def create_sales_table() -> None:
        # SQL DDL logic here
        spark.sql('''
            CREATE TABLE IF NOT EXISTS sales (
                id BIGINT,
                amount DECIMAL(10,2),
                date DATE
            ) USING DELTA
        ''')
    
    # Run the operation exactly once
    ddl_util.run_once(
        work_fn=create_sales_table,
        object_name="sales_table",
        guid=None  # Will auto-generate from function source
    )
    
    # Check execution status
    if ddl_util.check_if_script_has_run("some-script-id"):
        print("Script already executed")
    
    # View execution log
    ddl_util.print_log()

# Create an instance
from .ddl_utils import ddl_utils

ddl_util = ddl_utils("workspace-123", "lakehouse-456")
example_ddl_operations(ddl_util)
```

## Benefits

1. **Type Safety**: The interface provides compile-time type checking
2. **Consistency**: Ensures all implementations follow the same contract
3. **Testability**: Easy to mock implementations for testing
4. **Documentation**: Clear method signatures and documentation
5. **Maintainability**: Changes to the interface propagate to all implementations

## Schema

The execution log table uses the following schema:

```python
StructType([
    StructField("script_id", StringType(), nullable=False),
    StructField("script_name", StringType(), nullable=False), 
    StructField("execution_status", StringType(), nullable=False),
    StructField("update_date", TimestampType(), nullable=False),
])
```

## Error Handling

The implementation handles errors by:
- Logging failed executions with "Failure" status
- Exiting notebooks on failure to prevent downstream issues
- Providing detailed error messages with GUID and object name context

## Best Practices

1. **Use meaningful object names** for better logging and debugging
2. **Provide explicit GUIDs** for critical operations to avoid hash collisions
3. **Handle exceptions** in your work functions appropriately
4. **Test your DDL operations** in isolation before using with `run_once`
5. **Monitor execution logs** regularly to ensure scripts are running as expected
