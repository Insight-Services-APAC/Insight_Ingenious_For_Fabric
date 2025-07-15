# Examples

This section provides practical examples and real-world scenarios for using the Ingenious Fabric Accelerator. From simple getting-started examples to complex multi-environment deployments, these examples will help you understand how to use the tool effectively.

## Available Examples

### [Sample Project](sample_project.md)
A complete walkthrough of the included sample project, demonstrating:
- Project structure and organization
- Environment configuration
- DDL script development
- Deployment workflow
- Testing strategies

### [Project Templates](templates.md)
Guide to using and customizing project templates:
- Understanding template structure
- Creating custom templates
- Template variables and substitution
- Best practices for template development

## Quick Examples

### Basic Project Setup

```bash
# Create a new project
ingen_fab init init-solution --project-name "Analytics Platform"

# Navigate to project directory
cd analytics-platform

# Configure your environment
vim fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
```

### Simple DDL Script

```python
# ddl_scripts/Lakehouses/Config/001_Initial_Setup/001_create_config_table.py
from lakehouse_utils import LakehouseUtils
from ddl_utils import DDLUtils

# Initialize utilities
lakehouse_utils = LakehouseUtils()
ddl_utils = DDLUtils()

# Create configuration table
sql = """
CREATE TABLE IF NOT EXISTS config.application_settings (
    setting_name STRING,
    setting_value STRING,
    environment STRING,
    created_date TIMESTAMP
) USING DELTA
LOCATION 'Tables/config/application_settings'
"""

ddl_utils.execute_ddl(sql, "Create application settings table")
print("✅ Configuration table created successfully!")
```

### Deployment Workflow

```bash
# Generate notebooks
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# Test locally (requires FABRIC_ENVIRONMENT=local)
export FABRIC_ENVIRONMENT=local
ingen_fab test local python
ingen_fab test local pyspark

# Deploy to development
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development

# Generate platform tests
export FABRIC_ENVIRONMENT=development
ingen_fab test platform generate
```

## Common Patterns

### Configuration Management

```python
# Using environment-specific configuration
from common.config_utils import FabricConfig

config = FabricConfig.from_environment()
workspace_id = config.workspace_id
lakehouse_id = config.lakehouse_id
```

### Data Processing

```python
# Processing data with utilities
from lakehouse_utils import LakehouseUtils
from warehouse_utils import WarehouseUtils

# Read from lakehouse
lakehouse_utils = LakehouseUtils()
source_data = lakehouse_utils.read_table("raw.source_data")

# Process and write to warehouse
warehouse_utils = WarehouseUtils()
processed_data = process_data(source_data)
warehouse_utils.write_table("processed.clean_data", processed_data)
```

### Testing Approach

```python
# Testing utilities locally
from python.warehouse_utils import WarehouseUtils

def test_warehouse_connection():
    utils = WarehouseUtils(dialect="sqlserver")  # Use local SQL Server
    assert utils.test_connection()

def test_data_processing():
    utils = WarehouseUtils()
    result = utils.execute_query("SELECT COUNT(*) FROM test_table")
    assert result > 0
```

## Advanced Examples

### Multi-Environment Deployment

```bash
# Deploy to multiple environments
for env in development test production; do
    echo "Deploying to $env..."
    ingen_fab deploy deploy \
        --fabric-workspace-repo-dir . \
        --fabric-environment $env
    
    echo "Generating platform tests for $env..."
    ingen_fab test platform generate \
        --fabric-workspace-repo-dir . \
        --fabric-environment $env
done
```

### Custom Template Usage

```python
# Custom DDL script with advanced features
from lakehouse_utils import LakehouseUtils
from warehouse_utils import WarehouseUtils
from notebook_utils_abstraction import get_notebook_utils

# Get environment-appropriate utilities
notebook_utils = get_notebook_utils()
lakehouse_utils = LakehouseUtils()

# Create complex data structure
sql = """
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    sale_id BIGINT,
    product_id BIGINT,
    customer_id BIGINT,
    sale_date DATE,
    amount DECIMAL(10,2),
    region STRING,
    partition_date DATE
) USING DELTA
PARTITIONED BY (partition_date)
LOCATION 'Tables/analytics/fact_sales'
"""

try:
    ddl_utils.execute_ddl(sql, "Create fact_sales table")
    notebook_utils.display("✅ Fact sales table created successfully!")
except Exception as e:
    notebook_utils.display(f"❌ Error creating table: {e}")
    raise
```

## Best Practices Examples

### Error Handling

```python
# Robust error handling in DDL scripts
from ddl_utils import DDLUtils
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ddl_utils = DDLUtils()

try:
    # Attempt DDL execution
    ddl_utils.execute_ddl(complex_sql, "Create complex table")
    logger.info("DDL executed successfully")
except Exception as e:
    logger.error(f"DDL execution failed: {e}")
    # Cleanup or rollback logic
    ddl_utils.execute_ddl(rollback_sql, "Rollback changes")
    raise
```

### Performance Optimization

```python
# Optimized data processing
from pyspark.lakehouse_utils import LakehouseUtils
from pyspark.parquet_load_utils import ParquetLoadUtils

# Use PySpark for large data processing
lakehouse_utils = LakehouseUtils(spark_session=spark)
parquet_utils = ParquetLoadUtils(spark_session=spark)

# Process large datasets efficiently
large_df = lakehouse_utils.read_delta_table("raw.large_dataset")
processed_df = parquet_utils.process_parquet_data(large_df, transformations)
lakehouse_utils.write_delta_table(processed_df, "processed.large_dataset")
```

### Configuration-Driven Development

```python
# Configuration-driven DDL script
from common.config_utils import FabricConfig
from ddl_utils import DDLUtils

config = FabricConfig.from_environment()
ddl_utils = DDLUtils()

# Use configuration to drive behavior
table_name = config.get_setting("target_table_name")
retention_days = config.get_setting("data_retention_days", default=30)

sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGINT,
    data STRING,
    created_date TIMESTAMP
) USING DELTA
TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval {retention_days} days'
)
"""

ddl_utils.execute_ddl(sql, f"Create {table_name} with {retention_days} day retention")
```

## Troubleshooting Examples

### Common Issues and Solutions

#### Authentication Problems

```bash
# Check authentication
az account show

# Set environment variables
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"

# Test deployment
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development --dry-run
```

#### DDL Script Issues

```python
# Debug DDL scripts
from ddl_utils import DDLUtils

ddl_utils = DDLUtils()

# Test SQL syntax
test_sql = "SELECT 1 as test"
try:
    ddl_utils.execute_ddl(test_sql, "Test connection")
    print("✅ Connection successful")
except Exception as e:
    print(f"❌ Connection failed: {e}")
```

#### Notebook Generation Issues

```bash
# Check template structure
find ddl_scripts -name "*.py" -o -name "*.sql" | sort

# Verify file naming
ls -la ddl_scripts/Lakehouses/Config/001_Initial_Setup/

# Test generation
ingen_fab ddl compile --output-mode local --generation-mode Lakehouse
```

## Integration Examples

### CI/CD Pipeline

```yaml
# .github/workflows/fabric-deploy.yml
name: Deploy to Fabric

on:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.12'
    
    - name: Install dependencies
      run: |
        pip install uv
        uv sync
    
    - name: Generate notebooks
      run: |
        uv run ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
        uv run ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse
    
    - name: Deploy to staging
      run: |
        uv run ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment staging
      env:
        AZURE_TENANT_ID: ${{ "{{" }} secrets.AZURE_TENANT_ID {{ "}}" }}
        AZURE_CLIENT_ID: ${{ "{{" }} secrets.AZURE_CLIENT_ID {{ "}}" }}
        AZURE_CLIENT_SECRET: ${{ "{{" }} secrets.AZURE_CLIENT_SECRET {{ "}}" }}
```

### Docker Integration

```dockerfile
# Dockerfile for containerized deployment
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

# Copy project files
COPY . .

# Install project dependencies
RUN uv sync

# Set entrypoint
ENTRYPOINT ["uv", "run", "ingen_fab"]
```

## Next Steps

Ready to try these examples? Start with:

1. **[Sample Project](sample_project.md)** - Complete walkthrough
2. **[Project Templates](templates.md)** - Customization guide
3. **[User Guide](../user_guide/index.md)** - Comprehensive usage documentation
4. **[Developer Guide](../developer_guide/index.md)** - Architecture and development

## Contributing Examples

Have a great example to share? We welcome contributions! Please:

1. Fork the repository
2. Add your example to the appropriate section
3. Include complete, tested code
4. Add clear documentation
5. Submit a pull request

Your examples help the entire community learn and succeed with the Ingenious Fabric Accelerator!