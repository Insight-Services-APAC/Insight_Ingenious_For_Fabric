[← Back to Python Libraries](../README.md) | [← Developer Guide](../../../docs/developer_guide/python_libraries.md)

# Notebook Utils Abstraction

This module provides an abstraction layer for `notebookutils` that allows code to work seamlessly in both local development environments and Fabric notebook execution environments.

## Overview

The abstraction automatically detects whether it's running in a Fabric notebook environment or a local development environment and provides the appropriate implementation:

- **FabricNotebookUtils**: Uses the actual `notebookutils` when available (Fabric environment)
- **LocalNotebookUtils**: Provides local alternatives using pyodbc, environment variables, etc.

## Usage

### Basic Usage

```python
from ingen_fab.python_libs.python.notebook_utils_abstraction import get_notebook_utils

# Get notebook utils - automatically detects environment
utils = get_notebook_utils()

# Display data (works in both environments)
utils.display(dataframe)

# Connect to artifact (Fabric warehouse or local SQL Server)
conn = utils.connect_to_artifact(warehouse_id, workspace_id)

# Get secrets (Azure Key Vault or environment variables)
api_key = utils.get_secret("API_KEY", "key-vault-name")

# Exit notebook (actual exit in Fabric, logged in local)
utils.exit_notebook("Success!")
```

### Factory Pattern

```python
from ingen_fab.python_libs.python.notebook_utils_abstraction import NotebookUtilsFactory

# Auto-detect environment
utils = NotebookUtilsFactory.get_instance()

# Force local environment (useful for testing)
local_utils = NotebookUtilsFactory.create_instance(force_local=True)

# Reset singleton (useful for testing)
NotebookUtilsFactory.reset_instance()
```

## Environment Configuration

### Local Development

For local development, configure the following environment variables:

```bash
# SQL Server connection
export SQL_SERVER_PASSWORD="YourStrong!Passw0rd"

# Secrets (prefix with SECRET_)
export SECRET_API_KEY="your-api-key"
export SECRET_CONNECTION_STRING="your-connection-string"
```

### Fabric Environment

In Fabric notebooks, the abstraction automatically uses `notebookutils` when available.

## API Reference

### NotebookUtilsInterface

Abstract interface defining the contract for notebook utilities.

#### Methods

- `connect_to_artifact(artifact_id: str, workspace_id: str) -> Any`: Connect to a Fabric artifact
- `display(obj: Any) -> None`: Display an object in the notebook
- `exit_notebook(value: Any = None) -> None`: Exit the notebook with optional return value
- `get_secret(secret_name: str, key_vault_name: str) -> str`: Get a secret from Key Vault
- `is_available() -> bool`: Check if the implementation is available

### FabricNotebookUtils

Fabric-specific implementation using `notebookutils`.

### LocalNotebookUtils

Local development implementation with fallbacks:

- Uses `pyodbc` for database connections
- Uses environment variables for secrets
- Uses `print()` for display
- Logs notebook exit instead of actual exit

### NotebookUtilsFactory

Factory class for creating notebook utils instances.

#### Methods

- `get_instance(force_local: bool = False) -> NotebookUtilsInterface`: Get singleton instance
- `create_instance(force_local: bool = False) -> NotebookUtilsInterface`: Create new instance
- `reset_instance() -> None`: Reset singleton instance

## Integration with Existing Code

The abstraction is integrated with existing utilities:

### warehouse_utils.py

```python
# Updated to use abstraction
from .notebook_utils_abstraction import NotebookUtilsFactory

class warehouse_utils:
    def __init__(self, ...):
        self.notebook_utils = NotebookUtilsFactory.get_instance()
        
    def get_connection(self):
        if self.dialect == "fabric":
            return self.notebook_utils.connect_to_artifact(
                self._target_warehouse_id, self._target_workspace_id
            )
        # ... rest of implementation
```

### ddl_utils.py

```python
# Updated to use abstraction
from .notebook_utils_abstraction import get_notebook_utils

class ddl_utils:
    def __init__(self, ...):
        self.notebook_utils = get_notebook_utils()
        
    def print_log(self):
        # ... query execution
        self.notebook_utils.display(df)
```

## Testing

The abstraction includes comprehensive tests:

```bash
python -m pytest tests/test_notebook_utils_abstraction.py -v
```

## Examples

See `examples/notebook_utils_example.py` for a complete demonstration of the abstraction in action.

## Benefits

1. **Environment Agnostic**: Same code works in both local and Fabric environments
2. **Testable**: Easy to test with local fallbacks
3. **Maintainable**: Single abstraction instead of environment-specific code
4. **Flexible**: Can force local mode for testing or development
5. **Backward Compatible**: Drop-in replacement for existing `notebookutils` usage