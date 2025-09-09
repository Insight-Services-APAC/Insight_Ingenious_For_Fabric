# Python Code Injection Feature - Final Implementation

## Overview
The `create-python-classes` command has been enhanced to inject custom Python code from a `models_python` directory into the generated classes. This allows users to add preprocessing logic that executes before SQL statements.

## Key Features

### 1. **Automatic Spark Reference Replacement**
- All references to `spark` in injected code are automatically replaced with `self.spark`
- Handles both standalone `spark` and `spark.method()` patterns
- Prevents double replacement if code already uses `self.spark`

### 2. **File Discovery Logic**
The command searches for Python files in `{workspace}/{dbt_project}/models_python/` using these patterns:
- `{node_id}.py` - Exact node ID match
- `{node_id.replace('.', '_')}.py` - Node ID with dots replaced by underscores  
- `{raw_model_name}.py` - Normalized model name

### 3. **Execution Order**
In the generated class's `execute_all()` method:
1. **First**: Execute injected Python code via `execute_injected_python()`
2. **Then**: Execute SQL statements from JSON files

## Directory Structure
```
sample_project/
└── dbt_project/
    ├── target/sql/                         # SQL JSON files
    │   └── model.pmo_reports.dim_users.json
    └── models_python/                      # NEW: Python injection files
        └── model.pmo_reports.dim_users.py
```

## Example Injected Python File
```python
# models_python/model.pmo_reports.dim_users.py
# Note: spark references will be auto-replaced with self.spark

# Validate data exists
if not spark:  # Becomes: if not self.spark:
    raise RuntimeError("Spark session required")

source_df = spark.table("lakehouse.stg_users")  # Becomes: self.spark.table()
if source_df.count() == 0:
    raise RuntimeError("No source data found")

# Custom transformations
transformed_df = spark.sql("""  # Becomes: self.spark.sql()
    SELECT * FROM stg_users 
    WHERE is_active = true
""")

return transformed_df
```

## Generated Class Structure
```python
class DimUsers:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        # ...
    
    def execute_injected_python(self) -> Optional[Any]:
        """Execute injected Python code from models_python directory."""
        try:
            # Injected code with spark replaced by self.spark
            if not self.spark:
                raise RuntimeError("Spark session required")
            
            source_df = self.spark.table("lakehouse.stg_users")
            # ... rest of injected code
            
            return transformed_df
        except Exception as e:
            raise RuntimeError(f"Failed to execute injected Python: {e}")
    
    def finalize_data(self) -> Optional[DataFrame]:
        """Execute SQL from JSON file."""
        sql = "CREATE OR REPLACE TABLE dim_users AS ..."
        return self.spark.sql(sql)
    
    def execute_all(self) -> List[Optional[DataFrame]]:
        """Execute all operations in sequence."""
        results = []
        
        # Step 1: Execute injected Python (if exists)
        injected_result = self.execute_injected_python()
        results.append(injected_result)
        
        # Step 2: Execute SQL statements
        sql_result = self.finalize_data()
        results.append(sql_result)
        
        return results
```

## Command Usage
```bash
# Create Python classes with automatic injection
ingen_fab dbt create-python-classes --dbt-project dbt_project

# Output will indicate when Python files are found:
# [blue]Found Python code to inject from model.pmo_reports.dim_users.py[/blue]
```

## Benefits
1. **Custom Preprocessing**: Add validation, data quality checks, or transformations
2. **Seamless Integration**: Injected code uses the class's Spark session automatically
3. **Flexible Naming**: Multiple file naming patterns supported
4. **Error Handling**: Comprehensive error messages for debugging
5. **Backward Compatible**: Works with or without models_python directory

## Implementation Files Changed
- `/workspaces/i4f/ingen_fab/cli_utils/dbt/commands.py` - Added injection logic
- `/workspaces/i4f/ingen_fab/cli_utils/dbt/templates/python_class.py.j2` - Added execute_injected_python method

## Notes
- The `spark` to `self.spark` replacement is automatic and mandatory
- Injected code has full access to the class instance and its Spark session
- Any return value from injected code is captured in the results list
- Exceptions in injected code are wrapped with descriptive error messages