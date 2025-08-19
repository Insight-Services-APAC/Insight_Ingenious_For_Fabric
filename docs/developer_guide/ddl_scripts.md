# DDL Scripts

[Home](../index.md) > [Developer Guide](index.md) > DDL Scripts

## Overview

The DDL Scripts system provides a template-based approach for generating Data Definition Language (DDL) notebooks for Microsoft Fabric. This system allows you to create reusable templates that can be customized for different environments and use cases.

## Architecture

The DDL Scripts system consists of:

- **Templates**: Jinja2 templates that define the structure and content of DDL scripts
- **Generators**: Python modules that process templates and generate notebooks
- **Configuration**: YAML and JSON files that define variables and settings

## Key Components

### Template Engine

The template engine uses Jinja2 to process templates and generate executable notebooks. Templates can include:

- SQL statements
- Python code
- Environment-specific variables
- Conditional logic

### Notebook Generator

The notebook generator (`notebook_generator.py`) processes templates and creates Jupyter notebooks that can be executed in Microsoft Fabric.

## Usage

### Basic Template Structure

```jinja2
# Generated DDL script for {{ entity_name }}
{% if create_schema %}
CREATE SCHEMA {{ schema_name }}
{% endif %}

CREATE TABLE {{ table_name }} (
    {% for column in columns %}
    {{ column.name }} {{ column.type }}{% if not loop.last %},{% endif %}
    {% endfor %}
)
```

### Generating Notebooks

```bash
# Generate DDL notebooks from templates
ingen_fab ddl compile --environment production
```

## Template Development

### Creating New Templates

1. Create template files in `_templates/` directory
2. Use Jinja2 syntax for dynamic content
3. Include appropriate metadata and configuration
4. Test template generation locally

### Best Practices

- Use meaningful variable names
- Include error handling
- Add documentation comments
- Test with different environments
- Follow consistent naming conventions

## Configuration

Templates can be configured using:

- Environment variables
- Configuration files
- Command-line parameters
- Template-specific settings

## Advanced Features

### Conditional Generation

Templates support conditional logic for environment-specific customization:

```jinja2
{% if environment == 'production' %}
-- Production-specific settings
{% else %}
-- Development settings
{% endif %}
```

### Template Inheritance

Templates can extend base templates for consistency:

```jinja2
{%- raw -%}
{% extends "base_template.sql.jinja" %}
{% block content %}
-- Custom content here
{% endblock %}
{%- endraw -%}
```

## Integration

DDL Scripts integrate with:

- Microsoft Fabric workspaces
- Azure DevOps pipelines
- Local development environments
- CI/CD workflows

## Troubleshooting

### Common Issues

1. **Template Syntax Errors**: Check Jinja2 syntax
2. **Missing Variables**: Ensure all required variables are provided
3. **Generation Failures**: Check file permissions and paths
4. **Environment Issues**: Verify configuration settings

### Debugging

Use verbose mode for detailed output:

```bash
ingen_fab ddl compile --verbose
```

## Examples

See the `examples/` directory for sample templates and usage patterns.

## API Reference

For detailed API documentation, see the [Python APIs](../api/python_apis.md) reference.