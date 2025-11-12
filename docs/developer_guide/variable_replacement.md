# Variable Replacement System

[Home](../index.md) > [Developer Guide](index.md) > Variable Replacement System

The Ingenious Fabric Accelerator includes a sophisticated variable replacement system that enables environment-specific configuration management across templates, notebooks, and deployments. This system allows the same codebase to work seamlessly across development, testing, and production environments.

## Overview

The variable replacement system operates through two primary mechanisms:

1. **Placeholder Replacement**: Substituting `{{varlib:variable_name}}` markers with actual values
2. **Code Injection**: Inserting variable dictionaries and objects between injection markers

## Architecture

```mermaid
graph TD
    A[Variable Library JSON] --> B[VariableLibraryUtils]
    B --> C[Template Processing]
    B --> D[Code Injection]
    C --> E[Placeholder Replacement]
    D --> F[Variable Objects]
    E --> G[Generated Notebooks]
    F --> G
    G --> H[Deployment]
```

## Core Components

### VariableLibraryUtils

The central class that handles all variable operations:

```python
from ingen_fab.config_utils.variable_lib import VariableLibraryUtils

# Basic instantiation
vlu = VariableLibraryUtils(
    project_path=Path("sample_project"),
    environment="development"
)

# Get workspace ID
workspace_id = vlu.get_workspace_id()

# Get specific variable
lakehouse_id = vlu.get_variable_value("config_lakehouse_id")
```

### Factory Functions (Recommended)

For common patterns, use the factory functions:

```python
from ingen_fab.config_utils.variable_lib_factory import VariableLibraryFactory

# From CLI context
vlu = VariableLibraryFactory.from_cli_context(ctx)

# For specific workflow
dev_injector = VariableLibraryFactory.for_development(environment, project_path)
deploy_injector = VariableLibraryFactory.for_deployment(environment, project_path)
readonly_lib = VariableLibraryFactory.for_readonly(environment, project_path)
```

## Variable Configuration

### JSON Structure

Variables are defined in environment-specific JSON files:

```
sample_project/
└── fabric_workspace_items/
    └── config/
        └── var_lib.VariableLibrary/
            └── valueSets/
                ├── development.json
                ├── test.json
                └── production.json
```

### Example Configuration

```json
{
  "variableOverrides": [
    {
      "name": "fabric_deployment_workspace_id",
      "value": "544530ea-a8c9-4464-8878-f666d2a8f418"
    },
    {
      "name": "config_lakehouse_id", 
      "value": "514ebe8f-2bf9-4a31-88f7-13d84706431c"
    },
    {
      "name": "config_workspace_name",
      "value": "metcash_demo"
    }
  ]
}
```

## Replacement Mechanisms

### 1. Placeholder Replacement

**When**: Template compilation and deployment  
**Format**: `{{varlib:variable_name}}`  
**Usage**: Connection strings, configuration values

#### Example Template

```python
# Template (library_loader.py.jinja)
notebookutils.fs.mount(
    "abfss://{{varlib:config_workspace_name}}@onelake.dfs.fabric.microsoft.com/{{varlib:config_lakehouse_name}}.Lakehouse/Files/", 
    "/config_files"
)
```

#### Generated Result

```python
# Generated notebook
notebookutils.fs.mount(
    "abfss://metcash_demo@onelake.dfs.fabric.microsoft.com/config.Lakehouse/Files/", 
    "/config_files"
)
```

### 2. Code Injection

**When**: Development and deployment workflows  
**Format**: Injection markers  
**Usage**: Variable dictionaries and objects

#### Injection Markers

```python
# variableLibraryInjectionStart: var_lib
# This content will be replaced
# variableLibraryInjectionEnd: var_lib
```

#### Generated Content

```python
# variableLibraryInjectionStart: var_lib

# All variables as a dictionary
configs_dict = {
    'fabric_environment': 'development',
    'fabric_deployment_workspace_id': '544530ea-a8c9-4464-8878-f666d2a8f418',
    'config_workspace_name': 'metcash_demo',
    'config_lakehouse_id': '514ebe8f-2bf9-4a31-88f7-13d84706431c'
}

# All variables as an object
from dataclasses import dataclass
@dataclass
class ConfigsObject:
    fabric_environment: str 
    fabric_deployment_workspace_id: str 
    config_workspace_name: str 
    config_lakehouse_id: str 

configs_object: ConfigsObject = ConfigsObject(**configs_dict)
# variableLibraryInjectionEnd: var_lib
```

## When Replacements Occur

### Supported Artifact Types

Variable substitution is applied to the following artifact types during deployment:

| Artifact Type | File Pattern | Description |
|---------------|--------------|-------------|
| **Notebooks** | `notebook-content.py` | Python notebook source files |
| **Semantic Models** | `*.tmdl` | Tabular Model Definition Language files |
| **GraphQL APIs** | `graphql-definition.json` | GraphQL API definition files |

These artifact types support both placeholder replacement (`{{varlib:variable_name}}`) and code injection between markers during the deployment process.

### Development Workflow

```bash
# Code injection only (preserves placeholders)
ingen_fab deploy perform-code-replacements
```

- **Placeholder replacement**: ❌ No (preserved for deployment)
- **Code injection**: ✅ Yes (enables local development)
- **Location**: In-place file modification
- **Artifacts**: Notebooks only (for local development)

### DDL Compilation

```bash
# Template processing (preserves placeholders)
ingen_fab ddl compile
```

- **Placeholder replacement**: ❌ No (preserved in templates)
- **Code injection**: ❌ No (happens later during deployment)
- **Location**: Generated notebook files

### Deployment

```bash
# Full variable substitution
ingen_fab deploy deploy
```

- **Placeholder replacement**: ✅ Yes (environment-specific values)
- **Code injection**: ✅ Yes (complete configuration)
- **Location**: Output directory for deployment
- **Artifacts**: Notebooks, Semantic Models, and GraphQL APIs

### OneLake Upload

```bash
# Variable processing during upload
ingen_fab deploy upload-python-libs
```

- **Placeholder replacement**: ✅ Yes (if placeholders exist)
- **Code injection**: ✅ Yes (if markers exist)
- **Location**: Files processed during upload to Fabric

## Workflow-Specific Classes

### DevelopmentVariableInjector

Pre-configured for development workflows:

```python
from ingen_fab.config_utils.variable_lib_workflows import DevelopmentVariableInjector

# Optimized for development
injector = DevelopmentVariableInjector(project_path, environment)
injector.inject_variables()  # Code injection only, preserves placeholders
```

### DeploymentVariableInjector  

Pre-configured for deployment workflows:

```python
from ingen_fab.config_utils.variable_lib_workflows import DeploymentVariableInjector

# Optimized for deployment
injector = DeploymentVariableInjector(project_path, environment)
injector.inject_variables(output_dir=Path("dist"))  # Full substitution
```

### ReadOnlyVariableLibrary

For variable lookup without injection:

```python
from ingen_fab.config_utils.variable_lib_workflows import ReadOnlyVariableLibrary

# Read-only access
reader = ReadOnlyVariableLibrary(project_path, environment)
workspace_id = reader.workspace_id
config_value = reader.get_variable("config_lakehouse_id")
```

## Advanced Usage

### Artifact-Specific Examples

#### Notebooks (notebook-content.py)

Notebooks support both placeholder replacement and code injection:

```python
# Template with placeholders
workspace_id = "{{varlib:fabric_deployment_workspace_id}}"
lakehouse_id = "{{varlib:config_lakehouse_id}}"

# variableLibraryInjectionStart: var_lib
# Config variables will be injected here during deployment
# variableLibraryInjectionEnd: var_lib

# Use injected configs_dict
spark.conf.set(f"spark.sql.lakehouse.id", configs_dict['config_lakehouse_id'])
```

After deployment:
```python
# Placeholders replaced
workspace_id = "544530ea-a8c9-4464-8878-f666d2a8f418"
lakehouse_id = "514ebe8f-2bf9-4a31-88f7-13d84706431c"

# variableLibraryInjectionStart: var_lib
configs_dict = {
    'fabric_deployment_workspace_id': '544530ea-a8c9-4464-8878-f666d2a8f418',
    'config_lakehouse_id': '514ebe8f-2bf9-4a31-88f7-13d84706431c'
}
# variableLibraryInjectionEnd: var_lib

# Use injected configs_dict
spark.conf.set(f"spark.sql.lakehouse.id", configs_dict['config_lakehouse_id'])
```

#### Semantic Models (*.tmdl)

Semantic model TMDL files can use placeholders for data sources:

```tmdl
table Customer
    lineageTag: abc123
    sourceExpression:
        kind: m
        expression: =
            let
                Source = Lakehouse.Contents("{{varlib:config_lakehouse_id}}"),
                Navigation = Source{[workspaceId="{{varlib:fabric_deployment_workspace_id}}"]}[Data]
            in
                Navigation
```

After deployment:
```tmdl
table Customer
    lineageTag: abc123
    sourceExpression:
        kind: m
        expression: =
            let
                Source = Lakehouse.Contents("514ebe8f-2bf9-4a31-88f7-13d84706431c"),
                Navigation = Source{[workspaceId="544530ea-a8c9-4464-8878-f666d2a8f418"]}[Data]
            in
                Navigation
```

#### GraphQL APIs (graphql-definition.json)

GraphQL API definitions can use placeholders for connection information:

```json
{
  "dataSource": {
    "type": "Lakehouse",
    "workspaceId": "{{varlib:fabric_deployment_workspace_id}}",
    "lakehouseId": "{{varlib:config_lakehouse_id}}"
  },
  "schema": {
    "types": [...],
    "queries": [...]
  }
}
```

After deployment:
```json
{
  "dataSource": {
    "type": "Lakehouse",
    "workspaceId": "544530ea-a8c9-4464-8878-f666d2a8f418",
    "lakehouseId": "514ebe8f-2bf9-4a31-88f7-13d84706431c"
  },
  "schema": {
    "types": [...],
    "queries": [...]
  }
}
```

### Custom Variable Processing

```python
# Fine-grained control
vlu = VariableLibraryUtils(project_path, environment)

# Only placeholder replacement
content = vlu.perform_code_replacements(
    content,
    replace_placeholders=True,
    inject_code=False
)

# Only code injection
content = vlu.perform_code_replacements(
    content,
    replace_placeholders=False,
    inject_code=True
)
```

### Caching for Performance

```python
from ingen_fab.config_utils.variable_lib_factory import VariableLibraryCache

# Cache management
cache_size = VariableLibraryCache.cache_size()
VariableLibraryCache.clear_cache()  # Clear when switching environments
```

## Best Practices

### 1. Environment Separation

- ✅ **Do**: Use separate JSON files for each environment
- ✅ **Do**: Keep sensitive values in production-only configs  
- ❌ **Don't**: Hardcode environment-specific values in templates

### 2. Template Design

- ✅ **Do**: Use placeholders for connection strings and IDs
- ✅ **Do**: Use injection markers for variable objects
- ❌ **Don't**: Mix placeholder and hardcoded values

### 3. Development Workflow

- ✅ **Do**: Use code injection during development for testing
- ✅ **Do**: Preserve placeholders until deployment
- ❌ **Don't**: Inject variables multiple times (causes redundancy)

### 4. Performance

- ✅ **Do**: Use factory functions for common patterns
- ✅ **Do**: Enable caching for repeated operations
- ❌ **Don't**: Create multiple instances unnecessarily

## Common Patterns

### CLI Integration

```python
def my_command(ctx: typer.Context):
    # Use factory for CLI context
    vlu = VariableLibraryFactory.from_cli_context(ctx)
    workspace_id = vlu.get_workspace_id()
```

### Template Processing

```python
def process_template(template_content: str, environment: str):
    vlu = VariableLibraryFactory.from_environment_and_path(environment, project_path)
    return vlu.perform_code_replacements(template_content)
```

### Deployment Pipeline

```python
def deploy_notebooks(environment: str, project_path: Path):
    # Use deployment-specific injector
    injector = VariableLibraryFactory.for_deployment(environment, project_path)
    injector.inject_variables(output_dir=Path("dist"))
```

## Troubleshooting

### Common Issues

1. **Variables not found**
   ```
   ValueError: variable_name not found in variable library
   ```
   - Check JSON file exists for environment
   - Verify variable name spelling
   - Ensure `variableOverrides` array structure

2. **Injection markers not processed**
   ```python
   # Check markers are exactly formatted:
   # variableLibraryInjectionStart: var_lib
   # variableLibraryInjectionEnd: var_lib
   ```

3. **Placeholders not replaced**
   - Ensure `replace_placeholders=True` is set
   - Check placeholder format: `{{varlib:variable_name}}`
   - Verify deployment workflow is used

### Debugging

```python
# Enable verbose output
import logging
logging.basicConfig(level=logging.DEBUG)

# Check variable loading
vlu = VariableLibraryUtils(project_path, environment)
print(f"Loaded variables: {vlu.variables}")

# Test specific operations
result = vlu.replace_variable_placeholders("{{varlib:config_workspace_name}}")
print(f"Replacement result: {result}")
```

## Related Documentation

- [Python Libraries](python_libraries.md) - Core library architecture
- [DDL Scripts](ddl_scripts.md) - Template system integration  
- [CLI Reference](../user_guide/cli_reference.md) - Command usage
- [Workflows](../user_guide/workflows.md) - End-to-end processes

The variable replacement system is fundamental to enabling environment-agnostic development and deployment in the Ingenious Fabric Accelerator. Understanding when and how replacements occur is crucial for effective use of the platform.