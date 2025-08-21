# Python APIs

[Home](../index.md) > [API Reference](index.md) > Python APIs

## Overview

The Ingenious Fabric Accelerator provides comprehensive Python APIs for programmatic access to all functionality. These APIs enable you to build custom solutions, integrate with existing workflows, and automate complex operations.

## Core Modules

### `ingen_fab.python_libs`

The core Python libraries module provides environment-agnostic utilities.

#### `get_notebook_utils()`

Returns the appropriate notebook utilities for the current environment.

```python
from ingen_fab.python_libs import get_notebook_utils

# Get environment-appropriate utilities
utils = get_notebook_utils()

# Execute SQL query
result = utils.execute_query("SELECT * FROM my_table")

# Read data
data = utils.read_table("schema.table")
```

#### `NotebookUtils` Interface

Base interface for notebook utilities.

```python
class NotebookUtils:
    def execute_query(self, query: str) -> Any:
        """Execute SQL query"""
        pass
    
    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read table data"""
        pass
    
    def write_table(self, table_name: str, data: pd.DataFrame) -> None:
        """Write table data"""
        pass
    
    def get_connection(self) -> Any:
        """Get database connection"""
        pass
```

### `ingen_fab.python_libs.python`

Python-specific implementations for local development.

#### `DDLUtils`

Utilities for Data Definition Language operations.

```python
from ingen_fab.python_libs.python.ddl_utils import DDLUtils

ddl = DDLUtils()

# Create table
ddl.create_table(
    schema_name="analytics",
    table_name="user_metrics",
    columns=[
        {"name": "user_id", "type": "BIGINT", "nullable": False},
        {"name": "metric_date", "type": "DATE", "nullable": False}
    ]
)

# Check if table exists
exists = ddl.table_exists("analytics", "user_metrics")

# Drop table
ddl.drop_table("analytics", "user_metrics")
```

#### `LakehouseUtils`

Utilities for Lakehouse operations.

```python
from ingen_fab.python_libs.python.lakehouse_utils import LakehouseUtils

lakehouse = LakehouseUtils()

# Read parquet file
df = lakehouse.read_parquet("path/to/file.parquet")

# Write parquet file
lakehouse.write_parquet(df, "path/to/output.parquet")

# List files
files = lakehouse.list_files("path/to/directory")
```

#### `WarehouseUtils`

Utilities for Warehouse operations.

```python
from ingen_fab.python_libs.python.warehouse_utils import WarehouseUtils

warehouse = WarehouseUtils()

# Execute SQL
result = warehouse.execute_sql("SELECT COUNT(*) FROM users")

# Bulk insert
warehouse.bulk_insert("target_table", data)

# Create view
warehouse.create_view("user_summary", "SELECT * FROM users WHERE active = 1")
```

### `ingen_fab.python_libs.pyspark`

PySpark-specific implementations for Fabric runtime.

#### `DDLUtils` (PySpark)

PySpark implementation of DDL utilities.

```python
from ingen_fab.python_libs.pyspark.ddl_utils import DDLUtils

ddl = DDLUtils()

# Create Delta table
ddl.create_delta_table(
    schema_name="analytics",
    table_name="events",
    columns=[
        {"name": "event_id", "type": "STRING", "nullable": False},
        {"name": "timestamp", "type": "TIMESTAMP", "nullable": False}
    ]
)

# Optimize table
ddl.optimize_table("analytics.events")
```

#### `LakehouseUtils` (PySpark)

PySpark implementation for Lakehouse operations.

```python
from ingen_fab.python_libs.pyspark.lakehouse_utils import LakehouseUtils

lakehouse = LakehouseUtils()

# Read Delta table
df = lakehouse.read_delta_table("analytics.events")

# Write Delta table
lakehouse.write_delta_table(df, "analytics.processed_events")

# Merge data
lakehouse.merge_delta_table(
    target_table="analytics.users",
    source_df=new_users_df,
    merge_condition="target.user_id = source.user_id"
)
```

### `ingen_fab.ddl_scripts`

DDL script generation and management.

#### `DDLScriptGenerator`

Generates DDL scripts from templates.

```python
from ingen_fab.ddl_scripts import DDLScriptGenerator

generator = DDLScriptGenerator()

# Set template directory
generator.set_template_directory("templates/")

# Generate scripts for environment
scripts = generator.generate_scripts("production")

# Generate specific script
script = generator.generate_script("create_tables.sql.jinja", variables={
    "schema_name": "analytics",
    "environment": "production"
})
```

#### `NotebookGenerator`

Generates notebooks from templates.

```python
from ingen_fab.ddl_scripts.notebook_generator import NotebookGenerator

generator = NotebookGenerator()

# Generate notebook
notebook = generator.generate_notebook(
    template_path="templates/data_processing.py.jinja",
    variables={
        "source_table": "raw_data",
        "target_table": "processed_data"
    }
)

# Save notebook
generator.save_notebook(notebook, "output/data_processing.ipynb")
```

### `ingen_fab.fabric_api`

Microsoft Fabric API integration.

#### `FabricClient`

Main client for Fabric operations.

```python
from ingen_fab.fabric_api import FabricClient
from azure.identity import DefaultAzureCredential

# Initialize client
credential = DefaultAzureCredential()
client = FabricClient(credential)

# List workspaces
workspaces = client.list_workspaces()

# Get workspace
workspace = client.get_workspace("workspace-id")

# List items in workspace
items = client.list_workspace_items("workspace-id")
```

#### `FabricWorkspace`

Workspace-specific operations.

```python
from ingen_fab.fabric_api import FabricWorkspace

workspace = FabricWorkspace("workspace-id")

# Create notebook
notebook = workspace.create_notebook("My Notebook", content)

# Execute notebook
result = workspace.execute_notebook(notebook.id)

# Create lakehouse
lakehouse = workspace.create_lakehouse("My Lakehouse")
```

### `ingen_fab.notebook_utils`

Notebook utilities and abstractions.

#### `SimpleNotebook`

Basic notebook operations.

```python
from ingen_fab.notebook_utils import SimpleNotebook

notebook = SimpleNotebook()

# Add code cell
notebook.add_code_cell("print('Hello, World!')")

# Add markdown cell
notebook.add_markdown_cell("# My Analysis")

# Execute notebook
results = notebook.execute()

# Save notebook
notebook.save("output.ipynb")
```

#### `FabricCliNotebook`

Fabric-specific notebook operations.

```python
from ingen_fab.notebook_utils import FabricCliNotebook

notebook = FabricCliNotebook()

# Set Fabric context
notebook.set_workspace("workspace-id")

# Execute with Fabric APIs
result = notebook.execute_with_fabric_apis(code)
```

### `ingen_fab.config_utils`

Configuration management utilities.

#### `VariableLib`

Variable library management.

```python
from ingen_fab.config_utils import VariableLib

var_lib = VariableLib()

# Get variable
value = var_lib.get_variable("database_name")

# Set variable
var_lib.set_variable("connection_string", "server=localhost")

# Load variables from file
var_lib.load_variables("config/variables.json")
```

#### `ConfigManager`

Configuration management.

```python
from ingen_fab.config_utils import ConfigManager

config = ConfigManager()

# Load configuration
config.load_config("ingen_fab.yaml")

# Get configuration value
workspace_id = config.get("workspace_id")

# Set configuration value
config.set("environment", "production")
```

## Data Profiling APIs

### `ingen_fab.python_libs.pyspark.data_profiling_pyspark`

Core data profiling implementation using PySpark for distributed processing.

#### `DataProfilingPySpark`

Main class for comprehensive data profiling with relationship discovery.

```python
from ingen_fab.python_libs.pyspark.data_profiling_pyspark import DataProfilingPySpark
from ingen_fab.python_libs.interfaces.data_profiling_interface import ProfileType
from pyspark.sql import SparkSession

# Initialize
spark = SparkSession.builder.appName("DataProfiling").getOrCreate()
profiler = DataProfilingPySpark(spark)

# Profile a dataset with different levels of analysis
basic_profile = profiler.profile_dataset(df, ProfileType.BASIC)
statistical_profile = profiler.profile_dataset(df, ProfileType.STATISTICAL)  
quality_profile = profiler.profile_dataset(df, ProfileType.DATA_QUALITY)
relationship_profile = profiler.profile_dataset(df, ProfileType.RELATIONSHIP)
full_profile = profiler.profile_dataset(df, ProfileType.FULL)
```

##### Methods

**`profile_dataset(dataset, profile_type=ProfileType.BASIC, columns=None, sample_size=None)`**

Profile a complete dataset with comprehensive statistics.

- `dataset`: DataFrame or table name to profile
- `profile_type`: Type of profiling (BASIC, STATISTICAL, DATA_QUALITY, RELATIONSHIP, FULL)
- `columns`: List of columns to profile (None for all columns)
- `sample_size`: Fraction of data to sample (None for auto-sampling)

Returns `DatasetProfile` with complete analysis results.

**`profile_column(dataset, column_name, profile_type=ProfileType.BASIC)`**

Profile a single column in detail.

- `dataset`: DataFrame containing the column
- `column_name`: Name of column to profile
- `profile_type`: Level of analysis to perform

Returns `ColumnProfile` with column-specific statistics.

**`generate_quality_report(profile, output_format="yaml")`**

Generate formatted reports from profile results.

- `profile`: DatasetProfile to report on
- `output_format`: "yaml", "html", or "json"

Returns formatted report string.

**`compare_profiles(profile1, profile2)`**

Compare two profiles to identify data drift and changes.

- `profile1`: First dataset profile (baseline)
- `profile2`: Second dataset profile (comparison)

Returns dictionary with comparison metrics and drift analysis.

**`suggest_data_quality_rules(profile)`**

Auto-generate quality rules based on profile characteristics.

- `profile`: DatasetProfile to analyze

Returns list of suggested quality rule definitions.

**`validate_against_rules(dataset, rules)`**

Validate dataset against quality rules.

- `dataset`: DataFrame to validate
- `rules`: List of quality rule definitions

Returns validation results with pass/fail status.

### `ingen_fab.python_libs.common.cross_profile_analyzer`

Advanced relationship discovery across multiple dataset profiles.

#### `CrossProfileAnalyzer`

Analyzes relationships between tables using profile data and value analysis.

```python
from ingen_fab.python_libs.common.cross_profile_analyzer import CrossProfileAnalyzer

# Initialize with confidence threshold
analyzer = CrossProfileAnalyzer(min_confidence=0.7)

# Profile multiple tables
profiles = {}
for table_name in ['customers', 'orders', 'products']:
    df = spark.table(table_name)
    profile = profiler.profile_dataset(df, ProfileType.RELATIONSHIP)
    profiles[table_name] = profile

# Discover relationships
relationships = analyzer.analyze_profiles(profiles)

# Generate relationship documentation
report = analyzer.generate_relationship_report(relationships)
```

##### Methods

**`analyze_profiles(profiles)`**

Discover relationships across multiple table profiles.

- `profiles`: Dictionary of {table_name: DatasetProfile}

Returns list of `RelationshipCandidate` objects with discovered relationships.

**`generate_relationship_report(relationships)`**

Create comprehensive relationship documentation.

- `relationships`: List of discovered relationships

Returns Markdown report with relationship analysis.

### Data Structures

#### `ProfileType` Enum

Defines the level of profiling analysis:

```python
ProfileType.BASIC        # Row/column counts, nulls, distinct values
ProfileType.STATISTICAL  # Statistical measures, distributions  
ProfileType.DATA_QUALITY # Quality assessment, anomaly detection
ProfileType.RELATIONSHIP # Relationship discovery, semantic analysis
ProfileType.FULL         # Complete analysis with all features
```

#### `DatasetProfile`

Complete dataset analysis results:

```python
@dataclass
class DatasetProfile:
    dataset_name: str
    row_count: int
    column_count: int
    profile_timestamp: str
    column_profiles: List[ColumnProfile]
    data_quality_score: Optional[float]
    correlations: Optional[Dict[str, Dict[str, float]]]
    entity_relationships: Optional[EntityRelationshipGraph]
    statistics: Optional[Dict[str, Any]]
```

#### `ColumnProfile`  

Individual column analysis with enhanced relationship data:

```python
@dataclass
class ColumnProfile:
    column_name: str
    data_type: str
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float
    completeness: float
    uniqueness: float
    # Enhanced relationship discovery fields
    semantic_type: Optional[SemanticType]
    naming_pattern: Optional[NamingPattern]
    value_pattern: Optional[ValuePattern]
    business_rules: List[BusinessRule]
    value_statistics: Optional[ValueStatistics]
```

#### `RelationshipCandidate`

Discovered relationship between columns:

```python
@dataclass  
class RelationshipCandidate:
    source_table: str
    source_column: ColumnProfile
    target_table: str
    target_column: ColumnProfile
    confidence: float
    relationship_type: RelationshipType
    evidence: List[str]  # Evidence supporting the relationship
    value_overlap_score: float
    name_similarity_score: float
```

#### `SemanticType` Enum

Column semantic classifications:

```python
SemanticType.IDENTIFIER   # Primary keys, unique identifiers
SemanticType.FOREIGN_KEY  # References to other tables
SemanticType.MEASURE     # Numeric values for aggregation
SemanticType.DIMENSION   # Categorical values for grouping
SemanticType.TIMESTAMP   # Date/time values
SemanticType.STATUS      # State indicators
SemanticType.DESCRIPTION # Text descriptions
```

#### `ValueStatistics`

Enhanced value analysis for relationship discovery:

```python
@dataclass
class ValueStatistics:
    value_hash_signature: Optional[str]  # Unique fingerprint of values
    selectivity: float                   # Distinct ratio
    is_unique_key: bool                  # Perfect uniqueness indicator
    dominant_value_ratio: float          # Most common value frequency
    sample_values: List[Any]             # Random sample for overlap testing
    value_count_distribution: Dict[int, int]  # Cardinality patterns
```

### Usage Examples

#### Basic Profiling Workflow

```python
# Initialize profiling
spark = SparkSession.builder.appName("Profiling").getOrCreate()
profiler = DataProfilingPySpark(spark)

# Load data
df = spark.table("analytics.customer_data")

# Profile with relationship discovery
profile = profiler.profile_dataset(df, ProfileType.RELATIONSHIP)

# Generate YAML report
report = profiler.generate_quality_report(profile, "yaml")
print(report)

# Check data quality
if profile.data_quality_score < 0.8:
    print(f"Warning: Data quality score is {profile.data_quality_score:.2f}")
    
    # Get quality improvement suggestions
    suggestions = profiler.suggest_data_quality_rules(profile)
    for suggestion in suggestions:
        print(f"Suggestion: {suggestion['description']}")
```

#### Advanced Relationship Discovery

```python
# Profile multiple related tables
tables = ['customers', 'orders', 'order_items', 'products']
profiles = {}

for table in tables:
    df = spark.table(f"ecommerce.{table}")
    profile = profiler.profile_dataset(df, ProfileType.RELATIONSHIP)
    profiles[table] = profile

# Analyze cross-table relationships
analyzer = CrossProfileAnalyzer(min_confidence=0.6)
relationships = analyzer.analyze_profiles(profiles)

# Display high-confidence relationships
high_confidence = [r for r in relationships if r.confidence >= 0.8]
print(f"Found {len(high_confidence)} high-confidence relationships:")

for rel in high_confidence:
    print(f"  {rel.source_table}.{rel.source_column.column_name} → "
          f"{rel.target_table}.{rel.target_column.column_name} "
          f"(confidence: {rel.confidence:.2f})")
    print(f"    Evidence: {', '.join(rel.evidence)}")
    print(f"    Type: {rel.relationship_type.value}")
```

#### Custom Quality Validation

```python
# Define business-specific quality rules
quality_rules = [
    {
        "rule_type": "completeness",
        "column": "customer_id",
        "threshold": 0.99,
        "severity": "error"
    },
    {
        "rule_type": "uniqueness", 
        "column": "email",
        "threshold": 0.95,
        "severity": "warning"
    },
    {
        "rule_type": "range",
        "column": "age",
        "min_value": 0,
        "max_value": 120,
        "severity": "error"
    }
]

# Validate data
validation_results = profiler.validate_against_rules(df, quality_rules)

# Report validation issues
for rule_name, result in validation_results.items():
    if not result['passed']:
        print(f"❌ {rule_name}: {result['description']}")
        print(f"   Expected: {result['threshold']}, Actual: {result['actual']}")
    else:
        print(f"✅ {rule_name}: Passed")
```

## Error Handling

### Exception Classes

```python
from ingen_fab.exceptions import (
    IngenFabError,
    NotebookUtilsError,
    DDLScriptError,
    FabricApiError
)

# Base exception
class IngenFabError(Exception):
    """Base exception for all Ingenious Fabric errors"""
    pass

# Specific exceptions
class NotebookUtilsError(IngenFabError):
    """Notebook utilities error"""
    pass

class DDLScriptError(IngenFabError):
    """DDL script generation error"""
    pass

class FabricApiError(IngenFabError):
    """Fabric API error"""
    pass
```

### Error Handling Patterns

```python
try:
    utils = get_notebook_utils()
    result = utils.execute_query(sql)
except NotebookUtilsError as e:
    logger.error(f"Notebook operation failed: {e}")
    # Handle error appropriately
except FabricApiError as e:
    logger.error(f"Fabric API error: {e}")
    # Handle Fabric-specific errors
except IngenFabError as e:
    logger.error(f"General error: {e}")
    # Handle other errors
```

## Async Support

### Async Operations

```python
import asyncio
from ingen_fab.fabric_api import AsyncFabricClient

async def main():
    client = AsyncFabricClient()
    
    # Async operations
    workspaces = await client.list_workspaces()
    
    # Concurrent operations
    tasks = [
        client.get_workspace(ws_id) 
        for ws_id in workspace_ids
    ]
    results = await asyncio.gather(*tasks)

# Run async code
asyncio.run(main())
```

## Type Hints

### Comprehensive Type Support

```python
from typing import List, Dict, Any, Optional, Union
from pandas import DataFrame
from pyspark.sql import DataFrame as SparkDataFrame

def process_data(
    data: Union[DataFrame, SparkDataFrame],
    config: Dict[str, Any],
    output_path: Optional[str] = None
) -> List[Dict[str, Any]]:
    """Process data with full type hints"""
    pass
```

## Testing

### Unit Testing

```python
import unittest
from unittest.mock import Mock, patch
from ingen_fab.python_libs import get_notebook_utils

class TestNotebookUtils(unittest.TestCase):
    def setUp(self):
        self.utils = get_notebook_utils()
    
    @patch('ingen_fab.python_libs.get_connection')
    def test_execute_query(self, mock_connection):
        mock_connection.return_value = Mock()
        result = self.utils.execute_query("SELECT 1")
        self.assertIsNotNone(result)
    
    def test_table_operations(self):
        # Test table creation
        self.utils.create_table("test_schema", "test_table", columns)
        
        # Test table existence
        exists = self.utils.table_exists("test_schema", "test_table")
        self.assertTrue(exists)
```

### Integration Testing

```python
import pytest
from ingen_fab.fabric_api import FabricClient

@pytest.mark.integration
async def test_fabric_integration():
    client = FabricClient()
    workspaces = await client.list_workspaces()
    assert len(workspaces) > 0
    
    # Test workspace operations
    workspace = await client.get_workspace(workspaces[0].id)
    assert workspace is not None
```

### Mocking

```python
from unittest.mock import Mock, patch
import pandas as pd

# Mock external dependencies
with patch('ingen_fab.python_libs.get_connection') as mock_conn:
    mock_conn.return_value = Mock()
    mock_conn.return_value.execute.return_value = pd.DataFrame({'col1': [1, 2, 3]})
    
    # Test code with mocked dependencies
    result = utils.execute_query("SELECT * FROM table")
    assert isinstance(result, pd.DataFrame)
```

## Performance Optimization

### Connection Pooling

```python
from ingen_fab.python_libs import ConnectionPool

# Configure connection pool
pool = ConnectionPool(
    max_connections=10,
    timeout=30,
    retry_count=3
)

# Use pooled connections
with pool.get_connection() as conn:
    result = conn.execute(query)
```

### Caching

```python
from functools import lru_cache
from ingen_fab.python_libs import cache

# Method-level caching
@lru_cache(maxsize=128)
def expensive_operation(param):
    return compute_result(param)

# Class-level caching
class CachedUtils:
    @cache(ttl=300)  # Cache for 5 minutes
    def get_table_schema(self, table_name):
        return self.fetch_schema(table_name)
```

### Batch Operations

```python
from ingen_fab.python_libs import BatchProcessor

processor = BatchProcessor(batch_size=1000)

# Process large datasets in batches
for batch in processor.process(large_dataset):
    results = utils.process_batch(batch)
```

## Advanced Features

### Custom Implementations

```python
from ingen_fab.python_libs import NotebookUtils

class CustomNotebookUtils(NotebookUtils):
    def __init__(self, custom_config):
        self.config = custom_config
    
    def execute_query(self, query: str) -> Any:
        # Custom implementation
        return self.custom_execute(query)
    
    def custom_operation(self, data):
        """Custom business logic"""
        return self.process_custom_data(data)
```

### Plugin System

```python
from ingen_fab.plugins import register_plugin

@register_plugin('my_plugin')
class MyPlugin:
    def initialize(self):
        pass
    
    def process(self, data):
        return transformed_data
```

### Event Hooks

```python
from ingen_fab.events import on_event

@on_event('before_query_execute')
def log_query(query):
    logger.info(f"Executing query: {query}")

@on_event('after_query_execute')
def log_result(result):
    logger.info(f"Query returned {len(result)} rows")
```

## Configuration

### Environment-Specific Settings

```python
from ingen_fab.config_utils import get_config

config = get_config()

# Environment-specific settings
if config.environment == 'production':
    connection_string = config.prod_connection_string
else:
    connection_string = config.dev_connection_string
```

### Dynamic Configuration

```python
from ingen_fab.config_utils import ConfigManager

config = ConfigManager()

# Load configuration from multiple sources
config.load_from_file('config.yaml')
config.load_from_env()
config.load_from_args(sys.argv)

# Get merged configuration
final_config = config.get_merged_config()
```

## Examples

### Complete Data Processing Pipeline

```python
from ingen_fab.python_libs import get_notebook_utils
from ingen_fab.python_libs.python import DDLUtils, LakehouseUtils
from ingen_fab.fabric_api import FabricClient

# Initialize components
utils = get_notebook_utils()
ddl = DDLUtils()
lakehouse = LakehouseUtils()
fabric_client = FabricClient()

# Create target table
ddl.create_table(
    schema_name="analytics",
    table_name="processed_data",
    columns=[
        {"name": "id", "type": "BIGINT", "nullable": False},
        {"name": "processed_date", "type": "TIMESTAMP", "nullable": False},
        {"name": "value", "type": "DECIMAL(10,2)", "nullable": True}
    ]
)

# Read source data
source_data = lakehouse.read_parquet("input/raw_data.parquet")

# Process data
processed_data = utils.execute_query("""
    SELECT 
        id,
        CURRENT_TIMESTAMP() as processed_date,
        value * 1.1 as value
    FROM source_data
    WHERE value > 0
""")

# Write results
lakehouse.write_parquet(processed_data, "output/processed_data.parquet")

# Deploy to Fabric
notebook = fabric_client.create_notebook("Data Processing", notebook_content)
fabric_client.execute_notebook(notebook.id)

print("Pipeline completed successfully!")
```

### Custom Utility Class

```python
from ingen_fab.python_libs import get_notebook_utils
from typing import Dict, Any, List

class DataAnalyzer:
    def __init__(self):
        self.utils = get_notebook_utils()
    
    def analyze_table(self, table_name: str) -> Dict[str, Any]:
        """Comprehensive table analysis"""
        
        # Get basic stats
        stats = self.utils.execute_query(f"""
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT *) as distinct_count
            FROM {table_name}
        """)
        
        # Get column info
        columns = self.utils.execute_query(f"""
            DESCRIBE {table_name}
        """)
        
        # Get data quality metrics
        quality_metrics = self._calculate_quality_metrics(table_name)
        
        return {
            'table_name': table_name,
            'statistics': stats,
            'columns': columns,
            'quality_metrics': quality_metrics
        }
    
    def _calculate_quality_metrics(self, table_name: str) -> Dict[str, float]:
        """Calculate data quality metrics"""
        # Custom data quality logic
        pass
    
    def generate_report(self, tables: List[str]) -> str:
        """Generate analysis report for multiple tables"""
        analyses = [self.analyze_table(table) for table in tables]
        return self._format_report(analyses)
```

## Best Practices

### Error Handling

```python
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@contextmanager
def error_handler(operation_name: str):
    """Context manager for consistent error handling"""
    try:
        logger.info(f"Starting {operation_name}")
        yield
        logger.info(f"Completed {operation_name}")
    except Exception as e:
        logger.error(f"Failed {operation_name}: {e}")
        raise
```

### Resource Management

```python
from contextlib import contextmanager

@contextmanager
def managed_connection():
    """Manage database connections properly"""
    conn = None
    try:
        conn = get_connection()
        yield conn
    finally:
        if conn:
            conn.close()
```

### Logging

```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

# Use throughout your code
logger.info("Processing started")
logger.debug("Debug information")
logger.error("Error occurred", exc_info=True)
```

## API Reference Summary

### Core Classes

- `NotebookUtils`: Base utilities interface
- `DDLUtils`: Data Definition Language operations
- `LakehouseUtils`: Lakehouse operations
- `WarehouseUtils`: Warehouse operations
- `FabricClient`: Fabric API client
- `DDLScriptGenerator`: DDL script generation
- `VariableLib`: Variable management

### Key Functions

- `get_notebook_utils()`: Get environment-appropriate utilities
- `get_config()`: Get configuration settings
- `register_plugin()`: Register custom plugins

### Exception Classes

- `IngenFabError`: Base exception
- `NotebookUtilsError`: Notebook utilities errors
- `DDLScriptError`: DDL script errors
- `FabricApiError`: Fabric API errors

For complete API documentation with all methods and parameters, refer to the docstrings in the source code or generate API documentation using tools like Sphinx.