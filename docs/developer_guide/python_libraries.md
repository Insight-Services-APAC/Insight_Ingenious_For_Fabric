# Python Libraries

The Python libraries are the heart of the Ingenious Fabric Accelerator, providing reusable components that work seamlessly across both local development and Fabric runtime environments. This allows developers to build high quality, reusable code libraries that can be both unit tested and functionally tested locally before being deployed to Fabric.

## Architecture

The library architecture follows a layered approach:


### Library Abstraction
```mermaid
graph TB
    A[Fabric Notebooks] --> B[Python Libraries]
    B --> C[Local Development]
    B --> D[Fabric Workspace]

    classDef grey fill:#a9a9a9,stroke:#000,stroke-width:1px,color:#fff;
    classDef green fill:#228B22,stroke:#000,stroke-width:1px,color:#fff;

    class C grey;
    class D green;   

```

### Table Operation Abstractions

```mermaid
graph TB
    K[Datastore Interface] --> L[Lakehouse Implementation]
    K --> M[Warehouse Implementation]
    L --> N[Local Spark Instance]
    L --> O[Fabric Lakehouse]
    M --> P[Local SQL Server]
    M --> Q[Fabric Warehouse]

    classDef grey fill:#a9a9a9,stroke:#000,stroke-width:1px,color:#fff;
    classDef green fill:#228B22,stroke:#000,stroke-width:1px,color:#fff;

    class N,P grey;
    class O,Q green;    
    

```

## Directory Structure

```
python_libs/
├── common/                    # Shared utilities
│   ├── config_utils.py       # Configuration management
│   ├── data_utils.py         # Data processing utilities
│   └── workflow_utils.py     # Workflow orchestration
├── interfaces/               # Abstract interfaces
│   ├── data_store_interface.py
│   └── ddl_utils_interface.py
├── python/                   # CPython implementations
│   ├── ddl_utils.py          # DDL execution utilities
│   ├── lakehouse_utils.py    # Lakehouse operations
│   ├── notebook_utils_abstraction.py
│   ├── pipeline_utils.py     # Pipeline utilities
│   ├── sql_template_factory/ # SQL template system
│   └── warehouse_utils.py    # Warehouse operations
├── pyspark/                  # PySpark implementations
│   ├── ddl_utils.py          # Spark DDL utilities
│   ├── lakehouse_utils.py    # Spark lakehouse operations
│   ├── notebook_utils_abstraction.py
│   └── parquet_load_utils.py # Parquet processing
└── gather_python_libs.py    # Library collection script
```

## Core Components

### Common Utilities

#### `config_utils.py`
Configuration management with environment-specific settings:

```python
from ingen_fab.python_libs.common.config_utils import ConfigUtils

# Load configuration from variable library
config_utils = ConfigUtils()

# Access configuration values
fabric_environment = config_utils.get_fabric_environment()
workspace_id = config_utils.get_variable('config_workspace_id')
lakehouse_id = config_utils.get_variable('config_lakehouse_id')
```

#### `data_utils.py`
Data processing and validation utilities:

```python
from ingen_fab.python_libs.common.data_utils import DataUtils

# Data processing utilities
data_utils = DataUtils()

# Common data operations would be implemented here
# Note: The actual implementation may vary based on specific use cases
```

#### `workflow_utils.py`
Workflow orchestration and dependency management:

```python
from ingen_fab.python_libs.common.workflow_utils import WorkflowUtils

# Workflow utilities for orchestration
workflow_utils = WorkflowUtils()

# Common workflow operations would be implemented here
# Note: The actual implementation may vary based on specific use cases
```

### Interfaces

#### `data_store_interface.py`
Abstract interface for data store operations:

```python
from abc import ABC, abstractmethod

class DataStoreInterface(ABC):
    @abstractmethod
    def read_table(self, table_name: str) -> Any:
        pass
    
    @abstractmethod
    def write_table(self, table_name: str, data: Any) -> None:
        pass
```

#### `ddl_utils_interface.py`
Interface for DDL execution:

```python
class DDLUtilsInterface(ABC):
    @abstractmethod
    def execute_ddl(self, sql: str, description: str) -> None:
        pass
    
    @abstractmethod
    def log_execution(self, script_name: str, description: str) -> None:
        pass
```

### Python Implementation

#### `ddl_utils.py`
DDL execution with logging and error handling:

```python
from ingen_fab.python_libs.python.ddl_utils import DDLUtils

ddl_utils = DDLUtils(
    target_warehouse_id="warehouse-guid",
    target_workspace_id="workspace-guid",
    config_workspace_id="config-workspace-guid",
    config_lakehouse_id="config-lakehouse-guid"
)

# Execute DDL with logging
ddl_utils.execute_ddl(
    sql="CREATE TABLE test (id INT, name STRING)",
    description="Create test table"
)
```

#### `lakehouse_utils.py`
Lakehouse operations for file and table management:

```python
from ingen_fab.python_libs.python.lakehouse_utils import LakehouseUtils

lakehouse_utils = LakehouseUtils(
    target_lakehouse_id="lakehouse-guid",
    target_workspace_id="workspace-guid"
)

# Table operations
tables = lakehouse_utils.list_tables()
df = lakehouse_utils.read_table("config.metadata")
lakehouse_utils.write_table(df, "output_table")
```

#### `warehouse_utils.py`
Warehouse connectivity and query execution:

```python
from ingen_fab.python_libs.python.warehouse_utils import WarehouseUtils

warehouse_utils = WarehouseUtils(
    target_warehouse_id="warehouse-guid",
    target_workspace_id="workspace-guid",
    dialect="fabric"  # or "sqlserver"
)

# Execute queries
result = warehouse_utils.execute_query("SELECT * FROM config.metadata")
warehouse_utils.execute_non_query("INSERT INTO logs VALUES (...)")
```

#### `notebook_utils_abstraction.py`
Environment-agnostic notebook utilities:

```python
from ingen_fab.python_libs.python.notebook_utils_abstraction import get_notebook_utils

# Automatically detects environment
utils = get_notebook_utils()

# Works in both local and Fabric environments
utils.display(dataframe)
connection = utils.connect_to_warehouse(warehouse_id, workspace_id)
# Note: Secret management functionality depends on environment setup
```

### PySpark Implementation

#### `ddl_utils.py`
Spark-compatible DDL execution:

```python
from ingen_fab.python_libs.pyspark.ddl_utils import DDLUtils

ddl_utils = DDLUtils(
    target_lakehouse_id="lakehouse-guid",
    target_workspace_id="workspace-guid",
    spark_session=spark
)

# Execute DDL in Spark context
ddl_utils.execute_ddl(
    sql="CREATE TABLE delta_table USING DELTA AS SELECT * FROM source",
    description="Create Delta table"
)
```

#### `lakehouse_utils.py`
Spark-based lakehouse operations:

```python
from ingen_fab.python_libs.pyspark.lakehouse_utils import LakehouseUtils

lakehouse_utils = LakehouseUtils(
    target_lakehouse_id="lakehouse-guid",
    target_workspace_id="workspace-guid",
    spark_session=spark
)

# Read/write tables using Spark
df = lakehouse_utils.read_table("config.metadata")
lakehouse_utils.write_table(df, "output_results")
```

#### `parquet_load_utils.py`
Parquet file processing utilities:

```python
from ingen_fab.python_libs.pyspark.parquet_load_utils import ParquetLoadUtils

parquet_utils = ParquetLoadUtils(spark_session=spark)

# The parquet load utilities would provide methods for loading and processing parquet files
# Note: Specific implementation details may vary
```

## SQL Template Factory

The SQL template system provides database-agnostic SQL generation:

```python
from ingen_fab.python_libs.python.sql_templates import SQLTemplateFactory

# Create templates instance
template_factory = SQLTemplateFactory(dialect="fabric")  # or "sql_server"

# Generate SQL
sql = template_factory.get_sql("check_table_exists", 
                               schema_name="config", 
                               table_name="metadata")
```

Available templates:
- `check_table_exists` - Check if table exists
- `create_table` - Create table with schema
- `drop_table` - Drop table if exists
- `insert_row` - Insert single row
- `list_tables` - List all tables
- `get_table_schema` - Get table schema information

## Testing

### Unit Tests

Each library has comprehensive unit tests:

```bash
# Run all library tests
pytest ./ingen_fab/python_libs_tests/ -v

# Run specific library tests
pytest ./ingen_fab/python_libs_tests/python/test_warehouse_utils_pytest.py -v
pytest ./ingen_fab/python_libs_tests/pyspark/test_lakehouse_utils_pytest.py -v

# Run with coverage
pytest ./ingen_fab/python_libs_tests/ --cov=ingen_fab.python_libs --cov-report=html
```

### Platform Tests

Test with actual Fabric workspaces:

```bash
# Generate platform test notebooks
ingen_fab test platform generate

# The generated notebooks can then be run in Fabric to test the libraries
```

## Development Guidelines

### Adding New Libraries

1. **Create the library module**:
   ```python
   # python_libs/python/my_new_utils.py
   from .notebook_utils_abstraction import get_notebook_utils
   
   class MyNewUtils:
       def __init__(self):
           self.notebook_utils = get_notebook_utils()
       
       def my_method(self):
           return "result"
   ```

2. **Add corresponding tests**:
   ```python
   # python_libs_tests/python/test_my_new_utils_pytest.py
   import pytest
   from ingen_fab.python_libs.python.my_new_utils import MyNewUtils
   
   def test_my_method():
       utils = MyNewUtils()
       assert utils.my_method() == "result"
   ```

3. **Update library collection**:
   ```python
   # python_libs/gather_python_libs.py
   # Add your library to the collection process
   ```

### Interface Implementation

When creating new implementations:

1. **Define interface first**:
   ```python
   # interfaces/my_interface.py
   from abc import ABC, abstractmethod
   
   class MyInterface(ABC):
       @abstractmethod
       def my_method(self) -> str:
           pass
   ```

2. **Implement for both runtimes**:
   ```python
   # python/my_implementation.py
   from ..interfaces.my_interface import MyInterface
   
   class MyImplementation(MyInterface):
       def my_method(self) -> str:
           return "python implementation"
   ```

3. **Create PySpark version**:
   ```python
   # pyspark/my_implementation.py
   from ..interfaces.my_interface import MyInterface
   
   class MyImplementation(MyInterface):
       def my_method(self) -> str:
           return "pyspark implementation"
   ```

### Best Practices

1. **Environment Agnostic**: Use abstractions to work in both local and Fabric environments
2. **Error Handling**: Always include proper error handling and logging
3. **Type Hints**: Use type hints for better code documentation
4. **Testing**: Write comprehensive tests for all functionality
5. **Documentation**: Include docstrings and usage examples

## Library Injection

Libraries are automatically injected into generated notebooks:

```python
# In generated notebook (libraries are injected automatically)
from lakehouse_utils import LakehouseUtils
from warehouse_utils import WarehouseUtils  
from ddl_utils import DDLUtils

# Libraries are available for use with proper initialization
lakehouse_utils = LakehouseUtils(
    target_lakehouse_id=target_lakehouse_id,
    target_workspace_id=target_workspace_id
)
warehouse_utils = WarehouseUtils(
    target_warehouse_id=target_warehouse_id,
    target_workspace_id=target_workspace_id
)
ddl_utils = DDLUtils(
    config_workspace_id=config_workspace_id,
    config_lakehouse_id=config_lakehouse_id
)
```

## Performance Considerations

- **Lazy Loading**: Libraries use lazy loading where possible
- **Connection Pooling**: Database connections are reused
- **Caching**: Results are cached when appropriate
- **Memory Management**: Large datasets are processed in chunks

## Troubleshooting

### Common Issues

1. **Import Errors**: Check that libraries are properly injected
2. **Connection Failures**: Verify workspace and lakehouse IDs
3. **Permission Errors**: Ensure proper authentication
4. **Type Errors**: Use type hints and validation

### Debugging

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Test library functionality
from python.warehouse_utils import WarehouseUtils
utils = WarehouseUtils()
utils.test_connection()
```

The Python libraries provide a robust foundation for building Fabric applications with consistent, testable, and maintainable code.