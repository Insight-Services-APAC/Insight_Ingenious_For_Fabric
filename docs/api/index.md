# API Reference

## Overview

The Ingenious Fabric Accelerator provides comprehensive APIs for building, deploying, and managing Microsoft Fabric applications. This reference covers all available APIs, including CLI commands and Python interfaces.

## API Categories

### [CLI Commands](cli_commands.md)
Command-line interface for interacting with the accelerator tools.

### [Python APIs](python_apis.md)
Python libraries and modules for programmatic access.

## Quick Start

### Command Line Interface

```bash
# Install the CLI
pip install insight-ingenious-for-fabric

# Get help
ingen_fab --help

# Initialize new project
ingen_fab init --template basic --name my_project

# Compile DDL scripts
ingen_fab ddl compile --environment production
```

### Python API

```python
from ingen_fab.python_libs import get_notebook_utils
from ingen_fab.ddl_scripts import DDLScriptGenerator

# Get utilities
utils = get_notebook_utils()

# Generate DDL scripts
generator = DDLScriptGenerator()
scripts = generator.generate_all()
```

## Authentication

### CLI Authentication

```bash
# Login to Azure
az login

# Set Fabric workspace
export FABRIC_WORKSPACE_ID=your-workspace-id
```

### Python Authentication

```python
from azure.identity import DefaultAzureCredential
from ingen_fab.fabric_api import FabricClient

# Authenticate
credential = DefaultAzureCredential()
client = FabricClient(credential)
```

## Error Handling

### CLI Errors

```bash
# Verbose output for debugging
ingen_fab --verbose ddl compile

# Check logs
tail -f ~/.ingen_fab/logs/cli.log
```

### Python Errors

```python
from ingen_fab.exceptions import IngenFabError

try:
    result = utils.execute_query(sql)
except IngenFabError as e:
    print(f"Operation failed: {e}")
```

## Configuration

### Global Configuration

```json
{
  "default_environment": "development",
  "logging": {
    "level": "INFO",
    "file": "~/.ingen_fab/logs/api.log"
  },
  "fabric": {
    "workspace_id": "default-workspace-id",
    "timeout": 30
  }
}
```

### Environment Variables

```bash
# Core settings
export INGEN_FAB_ENVIRONMENT=production
export FABRIC_WORKSPACE_ID=your-workspace-id

# Logging
export INGEN_FAB_LOG_LEVEL=DEBUG
export INGEN_FAB_LOG_FILE=/path/to/log/file

# Database connections
export DATABASE_CONNECTION_STRING=your-connection-string
```

## API Versioning

The API follows semantic versioning:

- **Major version**: Breaking changes
- **Minor version**: New features, backward compatible
- **Patch version**: Bug fixes

Current version: `0.1.0`

### Version Compatibility

```python
import ingen_fab

# Check version
print(ingen_fab.__version__)

# Check compatibility
if ingen_fab.version_info >= (0, 1, 0):
    # Use new features
    pass
```

## Rate Limiting

### Fabric API Limits

- **Requests per minute**: 1000
- **Concurrent requests**: 10
- **Data transfer**: 100MB per request

### Best Practices

```python
import time
from ingen_fab.fabric_api import FabricClient

client = FabricClient()

# Rate limiting
for item in large_dataset:
    client.process_item(item)
    time.sleep(0.1)  # Throttle requests
```

## Common Patterns

### Batch Operations

```python
from ingen_fab.python_libs import BatchProcessor

processor = BatchProcessor(batch_size=100)

# Process items in batches
for batch in processor.process(items):
    results = client.process_batch(batch)
```

### Error Recovery

```python
from ingen_fab.utils import RetryHandler

retry_handler = RetryHandler(max_retries=3, backoff_factor=2)

@retry_handler.retry
def unreliable_operation():
    return client.execute_query(sql)
```

### Context Management

```python
from ingen_fab.notebook_utils import get_notebook_utils

with get_notebook_utils() as utils:
    # Resources are automatically managed
    result = utils.execute_query(sql)
```

## Testing

### Unit Testing

```python
import unittest
from unittest.mock import patch
from ingen_fab.python_libs import NotebookUtils

class TestNotebookUtils(unittest.TestCase):
    @patch('ingen_fab.python_libs.get_connection')
    def test_execute_query(self, mock_connection):
        utils = NotebookUtils()
        result = utils.execute_query("SELECT 1")
        self.assertIsNotNone(result)
```

### Integration Testing

```python
import pytest
from ingen_fab.fabric_api import FabricClient

@pytest.mark.integration
def test_fabric_integration():
    client = FabricClient()
    workspaces = client.list_workspaces()
    assert len(workspaces) > 0
```

## Performance

### Optimization Tips

1. **Use batch operations** for multiple items
2. **Enable connection pooling** for database operations
3. **Cache frequently accessed data**
4. **Use async operations** where available

### Monitoring

```python
from ingen_fab.monitoring import MetricsCollector

collector = MetricsCollector()

# Track operation performance
with collector.timer('query_execution'):
    result = utils.execute_query(sql)

# View metrics
print(collector.get_metrics())
```

## Examples

### Complete Workflow

```python
from ingen_fab.python_libs import get_notebook_utils
from ingen_fab.ddl_scripts import DDLScriptGenerator
from ingen_fab.fabric_api import FabricClient

# Initialize components
utils = get_notebook_utils()
generator = DDLScriptGenerator()
client = FabricClient()

# Generate DDL scripts
scripts = generator.generate_all()

# Deploy to Fabric
for script in scripts:
    notebook = client.create_notebook(script)
    client.execute_notebook(notebook.id)

# Verify deployment
tables = utils.list_tables()
print(f"Created {len(tables)} tables")
```

### Custom Extensions

```python
from ingen_fab.python_libs import NotebookUtils

class CustomNotebookUtils(NotebookUtils):
    def custom_operation(self, data):
        """Custom business logic"""
        processed_data = self.process_data(data)
        return self.save_results(processed_data)

# Register custom implementation
utils = CustomNotebookUtils()
```

## Support

### Documentation

- **User Guide**: Step-by-step instructions
- **Developer Guide**: Architecture and development
- **Examples**: Real-world usage patterns

### Community

- **GitHub Issues**: Bug reports and feature requests
- **Discussions**: Community questions and answers
- **Contributions**: How to contribute to the project

### Getting Help

```bash
# CLI help
ingen_fab --help
ingen_fab <command> --help

# Python help
python -c "import ingen_fab; help(ingen_fab)"
```

## Changelog

### Version 0.1.0

- Initial release
- Basic CLI commands
- Core Python libraries
- Fabric integration
- Template system

### Upcoming Features

- Advanced error handling
- Performance optimizations
- Extended template library
- Enhanced monitoring

## Security

### Authentication

- Azure AD integration
- Service principal support
- Token-based authentication
- Role-based access control

### Data Protection

- Encryption at rest
- Secure connections
- Audit logging
- Privacy compliance

## License

This project is licensed under the MIT License. See LICENSE file for details.

## Contributing

We welcome contributions! Please see our contributing guidelines for details on how to submit pull requests, report issues, and suggest improvements.