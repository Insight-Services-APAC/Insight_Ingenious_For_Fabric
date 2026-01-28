# Lakehouse SQL Endpoint DDL Execution

## Overview

This implementation adds support for executing `.sql` files on Fabric Lakehouse SQL endpoints using PyODBC connections, similar to how warehouse SQL files are executed. This feature enables DDL operations (CREATE VIEW, CREATE TABLE, etc.) on lakehouses using T-SQL syntax through the SQL endpoint.

## Architecture

### Components

1. **FabricLakehouseConnection** (`ingen_fab/python_libs/python/lakehouse_sql_connection.py`)
   - Manages PyODBC connections to Lakehouse SQL endpoints
   - Handles authentication using Azure AD tokens
   - Provides methods for executing queries and DDL statements
   - Includes retry logic and error handling

2. **LakehouseDDLUtils** (`ingen_fab/python_libs/python/lakehouse_ddl_utils.py`)
   - Provides run-once tracking for DDL scripts
   - Maintains execution log in `dbo.ddl_script_executions` table
   - Prevents duplicate execution of scripts using GUID-based tracking
   - Compatible with both Spark-based and SQL endpoint-based execution

3. **Lakehouse SQL Template** (`ingen_fab/templates/ddl/execution_cells/lakehouse_sql.py.jinja`)
   - Jinja2 template for generating notebook cells that execute SQL files
   - Wraps SQL execution with run-once tracking
   - Provides clear logging and error handling

4. **Updated Notebook Generator** (`ingen_fab/ddl_scripts/notebook_generator.py`)
   - Enhanced to detect `.sql` files in lakehouse mode
   - Routes SQL files to the lakehouse SQL template
   - Passes necessary connection variables to templates

5. **Updated Lakehouse Notebook Template** (`ingen_fab/templates/ddl/lakehouse/notebook_content.py.jinja`)
   - Includes SQL endpoint connection setup
   - Initializes both Spark-based DDL utils (`du`) and SQL endpoint DDL utils (`ldu`)
   - Installs PyODBC package
   - Displays both execution logs

## How It Works

### Execution Flow

1. **DDL Compilation** (`ingen_fab ddl compile --generation-mode Lakehouse`)
   - Scans `ddl_scripts/Lakehouses/{lakehouse_name}` directories
   - Identifies `.py` files (executed via Spark) and `.sql` files (executed via SQL endpoint)
   - Generates notebooks with appropriate execution cells

2. **Notebook Initialization**
   - Loads custom Python libraries including connection and DDL utils classes
   - Retrieves lakehouse configuration (lakehouse ID, workspace ID)
   - Establishes SQL endpoint connection using Fabric API
   - Creates PyODBC connection with Azure AD token authentication

3. **SQL Execution**
   - For each `.sql` file:
     - Generates unique GUID based on file path
     - Checks if script has already executed (via `ddl_script_executions` table)
     - If not executed, runs the SQL via PyODBC connection
     - Logs execution status to tracking table
     - Prevents re-execution on subsequent runs

4. **Dual Execution Tracking**
   - `.py` files tracked in Spark Delta table (PySpark DDL utils)
   - `.sql` files tracked in SQL endpoint table (SQL DDL utils)
   - Both logs displayed at notebook completion

## Configuration

### Prerequisites

- Fabric Lakehouse with SQL endpoint enabled
- PyODBC installed (automatically installed by notebook)
- ODBC Driver 18 for SQL Server (available in Fabric Spark environment)
- Appropriate permissions to access lakehouse SQL endpoint

### File Organization

```
ddl_scripts/
└── Lakehouses/
    └── {lakehouse_name}/
        └── {execution_order}/
            ├── 001_create_tables.py      # Executed via Spark
            ├── 002_create_view.sql       # Executed via SQL endpoint
            └── 003_more_ddl.sql          # Executed via SQL endpoint
```

## SQL File Requirements

### Supported SQL Dialects

Lakehouse SQL endpoints support T-SQL syntax compatible with SQL Server. Common operations:

- `CREATE TABLE`
- `CREATE VIEW` / `CREATE OR ALTER VIEW`
- `ALTER TABLE`
- `CREATE INDEX`
- `INSERT`, `UPDATE`, `DELETE` statements

### Example SQL File

```sql
-- Create a view for active customers
-- File: ddl_scripts/Lakehouses/lh_gold/001_Initial_Creation/002_customer_view.sql

CREATE OR ALTER VIEW dbo.vw_active_customers AS
SELECT
    customer_id,
    customer_name,
    email,
    created_date
FROM dbo.customers
WHERE is_active = 1;
```

### Naming Conventions

- Prefix files with numeric order: `001_`, `002_`, etc.
- Use descriptive names: `create_view.sql`, `add_indexes.sql`
- File name becomes the tracking object name

## Run-Once Tracking

### Execution Log Table

Both warehouse and lakehouse SQL executions maintain a tracking table:

```sql
CREATE TABLE dbo.ddl_script_executions (
    object_guid VARCHAR(128) NOT NULL,
    object_name VARCHAR(255) NOT NULL,
    script_status VARCHAR(50) NOT NULL,
    execution_timestamp DATETIME2(3) NOT NULL,
    target_lakehouse_id VARCHAR(128) NOT NULL,
    target_workspace_id VARCHAR(128) NOT NULL
);
```

### GUID Generation

Each SQL file gets a unique GUID based on:
- Relative path from lakehouse root
- File name
- Lakehouse and workspace IDs

This ensures:
- Scripts run once per lakehouse
- Moving files to different directories triggers re-execution
- Same file in different lakehouses executes independently

## Comparison: Warehouse vs. Lakehouse SQL

| Feature | Warehouse SQL | Lakehouse SQL Endpoint |
|---------|---------------|------------------------|
| **Connection** | PyODBC to Warehouse | PyODBC to Lakehouse SQL Endpoint |
| **File Extension** | `.sql` | `.sql` |
| **Template** | `warehouse_sql.py.jinja` | `lakehouse_sql.py.jinja` |
| **DDL Utils Class** | `WarehouseDDLUtils` | `LakehouseDDLUtils` |
| **Connection Class** | `FabricWarehouseConnection` | `FabricLakehouseConnection` |
| **Tracking Table** | `dbo.ddl_script_executions` | `dbo.ddl_script_executions` |
| **Variable Name** | `du` | `ldu` |

## Usage Examples

### 1. Create a Simple View

**File**: `ddl_scripts/Lakehouses/lh_gold/001_Setup/001_create_view.sql`

```sql
CREATE OR ALTER VIEW dbo.vw_sales_summary AS
SELECT
    product_id,
    SUM(quantity) as total_quantity,
    SUM(amount) as total_amount
FROM dbo.sales
GROUP BY product_id;
```

### 2. Create Multiple Related Objects

**File**: `ddl_scripts/Lakehouses/lh_silver/002_Schema/001_create_tables.sql`

```sql
-- Create customer dimension table
CREATE TABLE dbo.dim_customer (
    customer_key INT NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    effective_date DATE NOT NULL,
    end_date DATE NULL,
    is_current BIT NOT NULL
);

-- Create index
CREATE INDEX idx_customer_id ON dbo.dim_customer(customer_id);
```

### 3. Mix Spark and SQL Executions

**Directory Structure**:
```
ddl_scripts/Lakehouses/lh_bronze/001_Initial_Setup/
├── 001_create_delta_table.py      # Use Spark for Delta table features
├── 002_create_view.sql            # Use SQL endpoint for view
└── 003_add_constraints.sql        # Use SQL endpoint for constraints
```

## Commands

### Compile DDL Scripts for Lakehouse

```bash
# Generate notebooks from DDL scripts
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# Output location
fabric_workspace_items/ddl_scripts/Lakehouses/{lakehouse_name}/
```

### Execute in Fabric

1. Open the generated orchestrator notebook
2. Run the notebook in Fabric workspace
3. Monitor execution logs for both Spark and SQL endpoint operations

## Troubleshooting

### Connection Issues

**Problem**: "Failed to connect to lakehouse SQL endpoint"

**Solutions**:
- Verify lakehouse SQL endpoint is enabled
- Check workspace and lakehouse IDs are correct
- Ensure proper permissions to access SQL endpoint
- Confirm ODBC driver is available

### SQL Execution Errors

**Problem**: SQL syntax errors

**Solutions**:
- Ensure T-SQL syntax compatible with SQL Server
- Check table/view names exist
- Verify schema names are correct (default: `dbo`)
- Test SQL in Fabric SQL query editor first

### Run-Once Tracking Issues

**Problem**: Script executes every time despite run-once tracking

**Solutions**:
- Check `ddl_script_executions` table exists
- Verify GUID generation is consistent
- Ensure workspace/lakehouse IDs match configuration
- Check for errors in previous execution (status = 'Failure')

### Mixed Execution Logs

**Problem**: Can't see SQL endpoint execution log

**Solutions**:
- Ensure `ldu` variable initialized successfully
- Check SQL endpoint connection was established
- Verify tracking table was created
- Look for initialization errors in notebook output

## Best Practices

### When to Use SQL Endpoint vs. Spark

**Use SQL Endpoint (`.sql` files) for:**
- Creating views
- T-SQL specific operations
- Operations requiring SQL Server compatibility
- Simple DDL statements

**Use Spark (`.py` files) for:**
- Creating Delta tables with advanced features
- Data transformations using PySpark
- Operations requiring Spark SQL functions
- Complex Python-based logic

### File Organization

1. **Order files logically**: Use numeric prefixes (001, 002, etc.)
2. **Group related operations**: Keep related SQL in same subdirectory
3. **Separate concerns**: Use different subdirectories for different phases
4. **Document scripts**: Add comments explaining purpose

### Error Handling

1. **Test SQL separately**: Validate SQL in Fabric SQL editor before adding to DDL scripts
2. **Use idempotent operations**: Prefer `CREATE OR ALTER VIEW` over `CREATE VIEW`
3. **Handle dependencies**: Ensure prerequisite objects exist before referencing them
4. **Check execution logs**: Review both Spark and SQL endpoint logs after execution

## Limitations

1. **SQL Endpoint Availability**: Lakehouse SQL endpoints must be enabled and accessible
2. **T-SQL Compatibility**: Only T-SQL syntax supported (no Spark SQL extensions)
3. **Performance**: SQL endpoint operations may be slower than Spark for large data operations
4. **Schema Support**: Limited schema creation capabilities compared to warehouses
5. **Concurrent Execution**: Multiple notebooks shouldn't execute same SQL file simultaneously

## Future Enhancements

Potential improvements for future versions:

1. **Retry Logic**: Enhanced retry mechanisms for transient failures
2. **Parallel Execution**: Execute independent SQL files in parallel
3. **Dependency Management**: Automatic dependency resolution between scripts
4. **Validation**: Pre-execution validation of SQL syntax
5. **Rollback Support**: Transaction-based rollback for failed operations
6. **Performance Monitoring**: Execution time tracking and reporting

## Related Documentation

- [DDL Organization Guide](./ddl-organization.md)
- [Warehouse Utils Documentation](../api/warehouse_utils.md)
- [Lakehouse Utils Documentation](../api/lakehouse_utils.md)
- [Fabric SQL Endpoint Documentation](https://learn.microsoft.com/en-us/fabric/data-warehouse/sql-endpoint-overview)
