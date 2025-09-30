# Project Templates

[Home](../index.md) > [Examples](index.md) > Project Templates

## Overview

The Ingenious Fabric Accelerator provides a comprehensive set of project templates to help you quickly bootstrap new Microsoft Fabric projects. These templates include pre-configured workspace items, DDL scripts, and common patterns for data processing workflows.

## Available Templates

### Basic Project Template

A minimal project structure with essential components:

```
project_templates/
├── ddl_scripts/           # DDL generation scripts
├── fabric_workspace_items/ # Fabric workspace definitions
├── README.md             # Project documentation
└── .gitignore           # Git ignore patterns
```

### Data Warehouse Template

Complete template for building data warehouses:

```
project_templates/
├── ddl_scripts/
│   ├── Warehouses/
│   │   └── EDW/
│   │       └── 001_Initial_Creation/
│   └── Lakehouses/
│       └── Config/
│           └── 001_Initial_Creation/
├── fabric_workspace_items/
│   ├── warehouses/
│   │   └── EDW.Warehouse/
│   └── lakehouses/
│       └── config.Lakehouse/
└── config/
    └── variables.json
```

### ETL Pipeline Template

Template for data extraction, transformation, and loading:

```
project_templates/
├── extract/
│   ├── extract_from_synapse.Notebook/
│   └── extract_pipeline.DataPipeline/
├── transform/
│   ├── data_transformation.Notebook/
│   └── validation_rules.Notebook/
├── load/
│   └── load_to_warehouse.Notebook/
└── config/
    └── pipeline_config.json
```

## Using Templates

### Initialize New Project

```bash
# Create new project from template
ingen_fab init --template basic --name my_project

# Create with specific template
ingen_fab init --template data-warehouse --name analytics_project
```

### Custom Template Creation

```bash
# Create custom template
mkdir my_custom_template
cd my_custom_template

# Set up template structure
mkdir -p ddl_scripts/Warehouses/MyWarehouse/001_Initial_Creation
mkdir -p fabric_workspace_items/warehouses
mkdir -p config
```

## Template Structure

### DDL Scripts

Templates include pre-configured DDL scripts:

```python
# ddl_scripts/Warehouses/Config/001_Initial_Creation/001_config_tables.py
from ddl_utils import create_table

# Configuration tables
create_table(
    schema_name='config',
    table_name='extraction_log',
    columns=[
        {'name': 'id', 'type': 'BIGINT', 'nullable': False},
        {'name': 'source_system', 'type': 'STRING', 'nullable': False},
        {'name': 'extraction_date', 'type': 'TIMESTAMP', 'nullable': False},
        {'name': 'status', 'type': 'STRING', 'nullable': False}
    ]
)
```

### Fabric Workspace Items

Templates provide workspace item definitions:

```json
{
  "type": "Warehouse",
  "displayName": "EDW",
  "description": "Enterprise Data Warehouse"
}
```

### Configuration Files

Templates include configuration for different environments:

```json
{
  "environments": {
    "development": {
      "workspace_id": "dev-workspace-id",
      "database_name": "dev_edw"
    },
    "production": {
      "workspace_id": "prod-workspace-id",
      "database_name": "prod_edw"
    }
  }
}
```

## Template Customization

### Variables and Substitution

Templates support variable substitution:

```python
# Template with variables
DATABASE_NAME = "{{ database_name }}"
SCHEMA_NAME = "{{ schema_name }}"

# Configuration
{
  "database_name": "my_warehouse",
  "schema_name": "analytics"
}
```

### Conditional Logic

Templates can include conditional content:

```python
{% if include_logging %}
# Logging configuration
LOGGING_ENABLED = True
LOG_LEVEL = "INFO"
{% endif %}

{% if environment == "production" %}
# Production-specific settings
ENABLE_MONITORING = True
{% endif %}
```

### Template Inheritance

Templates can extend base templates:

```python
# base_warehouse.py
class BaseWarehouse:
    def __init__(self):
        self.connection = get_connection()

    def create_schema(self, schema_name):
        """Base schema creation"""
        pass

# custom_warehouse.py
class CustomWarehouse(BaseWarehouse):
    def create_analytics_schema(self):
        """Custom analytics schema"""
        self.create_schema('analytics')
```

## Template Development

### Creating Custom Templates

1. **Define structure**: Create directory structure
2. **Add templates**: Create Jinja2 templates
3. **Configure variables**: Define template variables
4. **Add documentation**: Include README and examples
5. **Test templates**: Validate template generation

### Template Validation

```python
# validate_template.py
from jinja2 import Template
import json

def validate_template(template_path, variables):
    """Validate template can be rendered"""
    with open(template_path, 'r') as f:
        template = Template(f.read())

    try:
        result = template.render(**variables)
        return True, result
    except Exception as e:
        return False, str(e)
```

### Best Practices

1. **Use meaningful names**: Template and variable names should be descriptive
2. **Include documentation**: Add comments and README files
3. **Provide examples**: Include sample configurations
4. **Test thoroughly**: Validate templates in different environments
5. **Version control**: Track template changes

## Template Categories

### Data Engineering

Templates for data processing workflows:

- **Bronze/Silver/Gold**: Medallion architecture patterns
- **Batch Processing**: Scheduled data processing
- **Stream Processing**: Real-time data ingestion
- **Data Quality**: Validation and monitoring

### Analytics

Templates for analytics workloads:

- **Reporting**: Standard report templates
- **Dashboards**: Interactive dashboard patterns
- **Data Science**: ML and AI workflows
- **Business Intelligence**: BI solution templates

### Integration

Templates for system integration:

- **API Integration**: REST API connectors
- **Database Sync**: Database synchronization
- **File Processing**: File ingestion patterns
- **Event Processing**: Event-driven architectures

## Configuration Management

### Environment-Specific Configuration

```json
{
  "environments": {
    "local": {
      "database_server": "localhost",
      "authentication": "integrated"
    },
    "development": {
      "database_server": "dev-sql-server",
      "authentication": "service_principal"
    },
    "production": {
      "database_server": "prod-sql-server",
      "authentication": "service_principal"
    }
  }
}
```

### Variable Libraries

Templates integrate with variable libraries:

```python
from variable_lib import get_variable

# Get environment-specific variables
database_name = get_variable('database_name')
connection_string = get_variable('connection_string')
```

## Deployment

### Template Deployment

```bash
# Deploy template to environment
ingen_fab deploy --template my_template --environment production

# Deploy with variable overrides
ingen_fab deploy --template my_template --environment production --vars database_name=prod_db
```

### CI/CD Integration

Templates can be deployed through CI/CD pipelines:

```yaml
# azure-pipelines.yml
- task: PythonScript@0
  displayName: 'Deploy Template'
  inputs:
    scriptSource: 'inline'
    script: |
      import subprocess
      subprocess.run(['ingen_fab', 'deploy', '--template', 'analytics', '--environment', 'production'])
```

## Examples

### Sample Project Structure

```
my_analytics_project/
├── ddl_scripts/
│   ├── Warehouses/
│   │   └── Analytics/
│   │       └── 001_Initial_Creation/
│   │           ├── 001_fact_tables.sql
│   │           ├── 002_dimension_tables.sql
│   │           └── 003_views.sql
│   └── Lakehouses/
│       └── RawData/
│           └── 001_Initial_Creation/
│               ├── 001_source_tables.py
│               └── 002_staging_tables.py
├── fabric_workspace_items/
│   ├── warehouses/
│   │   └── Analytics.Warehouse/
│   │       └── .platform
│   ├── lakehouses/
│   │   └── RawData.Lakehouse/
│   │       └── .platform
│   └── notebooks/
│       ├── data_extraction.Notebook/
│       └── data_transformation.Notebook/
├── config/
│   ├── variables.json
│   └── environment_config.json
└── README.md
```

### Template Usage Examples

```python
# Using template in notebook
from template_utils import load_template

# Load and render template
template = load_template('data_extraction')
notebook_content = template.render(
    source_system='salesforce',
    target_table='raw_contacts',
    environment='production'
)

# Execute generated content
exec(notebook_content)
```

## Integration with Fabric

### Workspace Integration

Templates integrate with Fabric workspaces:

```python
from fabric_api import FabricWorkspace

workspace = FabricWorkspace(workspace_id)

# Deploy template items to workspace
for item in template.get_workspace_items():
    workspace.create_item(item)
```

### Pipeline Integration

Templates can be used in data pipelines:

```python
# Pipeline activity using template
{
    "name": "ExecuteTemplate",
    "type": "ExecuteNotebook",
    "typeProperties": {
        "notebook": {
            "referenceName": "{{ template_name }}",
            "type": "NotebookReference"
        },
        "parameters": {
            "source_table": "{{ source_table }}",
            "target_table": "{{ target_table }}"
        }
    }
}
```

## Troubleshooting

### Common Issues

1. **Template not found**: Check template path and naming
2. **Variable errors**: Verify all required variables are provided
3. **Rendering failures**: Check Jinja2 template syntax
4. **Deployment errors**: Verify workspace permissions

### Debugging

```python
# Debug template rendering
from template_utils import debug_template

debug_info = debug_template(
    template_name='my_template',
    variables={'key': 'value'}
)
print(debug_info)
```

## Resources

- [DDL Scripts](../developer_guide/ddl_scripts.md) - Template system details
- [Notebook Utils](../developer_guide/notebook_utils.md) - Notebook integration
- [Sample Project](sample_project.md) - Complete project example
- [CLI Reference](../user_guide/cli_reference.md) - Command-line usage
