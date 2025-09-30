# SQL Templates

[Home](../index.md) > [Developer Guide](index.md) > SQL Templates

## Overview

The SQL Templates system provides a flexible, Jinja2-based templating engine for generating SQL queries across different database dialects. This system supports Microsoft Fabric (Spark SQL), SQL Server, and other database platforms with consistent interfaces and reusable templates.

## Architecture

The SQL template system is organized by database dialect:

```
sql_template_factory/
├── fabric/           # Microsoft Fabric templates
├── sql_server/       # SQL Server templates
├── common/           # Shared templates
└── generate_templates.py  # Template generator
```

## Supported Dialects

### Microsoft Fabric (Spark SQL)
- Delta Lake operations
- Lakehouse queries
- Warehouse operations
- PySpark integration

### SQL Server
- Traditional SQL Server syntax
- Stored procedures
- Views and functions
- Performance optimizations

## Core Templates

### Table Operations

#### Create Table
```sql
{%- raw -%}
-- fabric/create_table.sql.jinja
CREATE TABLE {{ schema_name }}.{{ table_name }} (
    {% for column in columns %}
    {{ column.name }} {{ column.type }}{% if column.nullable %} NULL{% else %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}
    {% endfor %}
)
{% if partition_by %}
PARTITIONED BY ({{ partition_by | join(', ') }})
{% endif %}
{%- endraw -%}
```

#### Read Table
```sql
{%- raw -%}
-- fabric/read_table.sql.jinja
SELECT
    {% for column in columns %}
    {{ column }}{% if not loop.last %},{% endif %}
    {% endfor %}
FROM {{ schema_name }}.{{ table_name }}
{% if where_clause %}
WHERE {{ where_clause }}
{% endif %}
{% if limit %}
LIMIT {{ limit }}
{% endif %}
{%- endraw -%}
```

### Schema Operations

#### List Schemas
```sql
{%- raw -%}
-- fabric/list_schemas.sql.jinja
SHOW SCHEMAS
{% if like_pattern %}
LIKE '{{ like_pattern }}'
{% endif %}
{%- endraw -%}
```

#### Check Schema Exists
```sql
{%- raw -%}
-- fabric/check_schema_exists.sql.jinja
SELECT COUNT(*) as schema_count
FROM information_schema.schemata
WHERE schema_name = '{{ schema_name }}'
{%- endraw -%}
```

### Data Operations

#### Insert Data
```sql
{%- raw -%}
-- fabric/insert_row.sql.jinja
INSERT INTO {{ schema_name }}.{{ table_name }}
({{ columns | join(', ') }})
VALUES
{% for row in rows %}
({{ row | join(', ') }}){% if not loop.last %},{% endif %}
{% endfor %}
{%- endraw -%}
```

#### Delete Data
```sql
{%- raw -%}
-- fabric/delete_from_table.sql.jinja
DELETE FROM {{ schema_name }}.{{ table_name }}
{% if where_clause %}
WHERE {{ where_clause }}
{% endif %}
{%- endraw -%}
```

## Template Generation

### Python Interface

```python
from sql_template_factory import get_template

# Get template for specific dialect
template = get_template('fabric', 'create_table')

# Render template with parameters
sql = template.render(
    schema_name='my_schema',
    table_name='my_table',
    columns=[
        {'name': 'id', 'type': 'BIGINT', 'nullable': False},
        {'name': 'name', 'type': 'STRING', 'nullable': True}
    ]
)
```

### Template Factory

```python
class SqlTemplateFactory:
    def __init__(self, dialect='fabric'):
        self.dialect = dialect

    def create_table(self, schema_name, table_name, columns):
        """Generate CREATE TABLE statement"""
        template = self.get_template('create_table')
        return template.render(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns
        )

    def read_table(self, schema_name, table_name, columns=None, where_clause=None):
        """Generate SELECT statement"""
        template = self.get_template('read_table')
        return template.render(
            schema_name=schema_name,
            table_name=table_name,
            columns=columns or ['*'],
            where_clause=where_clause
        )
```

## Usage Examples

### Basic Table Creation

```python
from sql_template_factory import SqlTemplateFactory

factory = SqlTemplateFactory('fabric')

# Create table
create_sql = factory.create_table(
    schema_name='analytics',
    table_name='user_metrics',
    columns=[
        {'name': 'user_id', 'type': 'BIGINT', 'nullable': False},
        {'name': 'metric_date', 'type': 'DATE', 'nullable': False},
        {'name': 'page_views', 'type': 'INT', 'nullable': True}
    ]
)

print(create_sql)
```

### Dynamic Query Building

```python
# Build query with conditions
query = factory.read_table(
    schema_name='analytics',
    table_name='user_metrics',
    columns=['user_id', 'SUM(page_views) as total_views'],
    where_clause='metric_date >= "2024-01-01"'
)
```

### Cross-Dialect Support

```python
# Create templates for different dialects
fabric_factory = SqlTemplateFactory('fabric')
sqlserver_factory = SqlTemplateFactory('sql_server')

# Same interface, different SQL output
fabric_sql = fabric_factory.create_table(schema, table, columns)
sqlserver_sql = sqlserver_factory.create_table(schema, table, columns)
```

## Configuration

### Template Paths

```python
# Configure custom template paths
factory = SqlTemplateFactory(
    dialect='fabric',
    template_path='/custom/templates'
)
```

### Environment Variables

```bash
# Set default dialect
export SQL_TEMPLATE_DIALECT=fabric

# Set custom template directory
export SQL_TEMPLATE_PATH=/path/to/templates
```

## Advanced Features

### Custom Filters

```python
# Add custom Jinja2 filters
def quote_identifier(value):
    return f'`{value}`'

factory.add_filter('quote', quote_identifier)
```

### Template Inheritance

```jinja2
{%- raw -%}
<!-- base_query.sql.jinja -->
SELECT
    {% block columns %}*{% endblock %}
FROM {{ schema_name }}.{{ table_name }}
{% block where_clause %}{% endblock %}

<!-- user_query.sql.jinja -->
{% extends "base_query.sql.jinja" %}
{% block content %}
    user_id,
    user_name,
    created_date
{% endblock %}
{% block where_clause %}
WHERE active = 1
{% endblock %}
{%- endraw -%}
```

### Macros

```jinja2
{%- raw -%}
{% macro render_column(column) %}
    {{ column.name }} {{ column.type }}
    {%- if not column.nullable %} NOT NULL{% endif %}
    {%- if column.default %} DEFAULT {{ column.default }}{% endif %}
{% endmacro %}
{%- endraw -%}
```

## Integration

### With DDL Scripts

```python
from ddl_scripts import DDLScriptGenerator
from sql_template_factory import SqlTemplateFactory

class FabricDDLGenerator(DDLScriptGenerator):
    def __init__(self):
        self.sql_factory = SqlTemplateFactory('fabric')

    def generate_create_table(self, schema, table, columns):
        return self.sql_factory.create_table(schema, table, columns)
```

### With Notebook Utils

```python
from notebook_utils import get_notebook_utils
from sql_template_factory import SqlTemplateFactory

utils = get_notebook_utils()
factory = SqlTemplateFactory('fabric')

# Generate and execute query
sql = factory.read_table('analytics', 'metrics')
result = utils.execute_query(sql)
```

## Testing

### Template Tests

```python
import pytest
from sql_template_factory import SqlTemplateFactory

def test_create_table_template():
    factory = SqlTemplateFactory('fabric')
    sql = factory.create_table(
        'test_schema',
        'test_table',
        [{'name': 'id', 'type': 'BIGINT', 'nullable': False}]
    )
    assert 'CREATE TABLE test_schema.test_table' in sql
    assert 'id BIGINT NOT NULL' in sql
```

### Integration Tests

```python
def test_fabric_integration():
    """Test template generation with Fabric"""
    factory = SqlTemplateFactory('fabric')
    # Test with actual Fabric environment
    pass
```

## Best Practices

### Template Organization

1. **Separate by dialect**: Keep templates organized by database type
2. **Use meaningful names**: Template names should reflect their purpose
3. **Include documentation**: Add comments explaining template usage
4. **Version control**: Track template changes over time

### Performance Considerations

1. **Template caching**: Cache compiled templates for reuse
2. **Lazy loading**: Load templates only when needed
3. **Validation**: Validate template parameters before rendering

### Security

1. **Parameter validation**: Validate all input parameters
2. **SQL injection prevention**: Use parameterized queries where possible
3. **Access control**: Limit template access based on user permissions

## Troubleshooting

### Common Issues

1. **Template not found**: Check template path and naming
2. **Rendering errors**: Verify all required parameters are provided
3. **Syntax errors**: Check Jinja2 template syntax
4. **Dialect mismatches**: Ensure template matches target database

### Debugging

```python
# Enable template debugging
factory = SqlTemplateFactory('fabric', debug=True)

# View rendered template
print(factory.debug_render('create_table', **params))
```

## API Reference

For complete API documentation, see the [Python APIs](../api/python_apis.md) reference.

## Examples

Complete examples are available in the `examples/` directory, including:

- Basic template usage
- Cross-dialect queries
- Custom template development
- Integration patterns
