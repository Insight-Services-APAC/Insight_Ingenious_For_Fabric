# CLI Commands

## Overview

Authoritative CLI usage lives in the User Guide’s CLI Reference. This page provides quick entry points and defers detailed flags and examples to the single source of truth.

## Quick Links

- CLI Reference (full, maintained): user_guide/cli_reference.md
- Deploy Guide (common tasks): user_guide/deploy_guide.md

## Getting Help In-CLI

```bash
ingen_fab --help
ingen_fab deploy --help
ingen_fab deploy get-metadata --help
```

## Command Groups

- init: project creation and workspace config
- ddl: compile notebooks from DDL scripts
- deploy: environment deploy, uploads, metadata
- notebook: scanning and code replacement utilities
- test: local and platform testing
- package: ingestion, synapse sync, extract generation, synthetic data

For options and examples, see the CLI Reference.

#### `workspace list`
List available Fabric workspaces.

```bash
ingen_fab workspace list [OPTIONS]
```

**Options:**
- `--output-format TEXT`: Output format (table, json, yaml)
- `--filter TEXT`: Filter workspaces by name

**Examples:**
```bash
# List all workspaces
ingen_fab workspace list

# List workspaces in JSON format
ingen_fab workspace list --output-format json
```

#### `workspace info`
Get detailed information about a workspace.

```bash
ingen_fab workspace info [OPTIONS] WORKSPACE_ID
```

**Options:**
- `--output-format TEXT`: Output format
- `--show-items`: Include workspace items

**Examples:**
```bash
# Get workspace information
ingen_fab workspace info abc-123-def

# Include workspace items
ingen_fab workspace info abc-123-def --show-items
```

#### `workspace items`
List items in a workspace.

```bash
ingen_fab workspace items [OPTIONS] WORKSPACE_ID
```

**Options:**
- `--type TEXT`: Filter by item type
- `--output-format TEXT`: Output format

**Examples:**
```bash
# List all items
ingen_fab workspace items abc-123-def

# List only notebooks
ingen_fab workspace items abc-123-def --type notebook
```

### Testing

#### `test`
Run tests for the project.

```bash
ingen_fab test [OPTIONS]
```

**Options:**
- `--environment TEXT`: Test environment
- `--test-type TEXT`: Type of tests to run (unit, integration, all)
- `--coverage`: Generate coverage report
- `--parallel`: Run tests in parallel

**Examples:**
```bash
# Run all tests
ingen_fab test

# Run unit tests only
ingen_fab test --test-type unit

# Run with coverage
ingen_fab test --coverage
```

#### `test libraries`
Test Python libraries locally.

```bash
ingen_fab test libraries [OPTIONS]
```

**Options:**
- `--base-dir PATH`: Base directory for tests
- `--verbose`: Verbose output
- `--failfast`: Stop on first failure

**Examples:**
```bash
# Test libraries in current directory
ingen_fab test libraries --base-dir .

# Test with verbose output
ingen_fab test libraries --verbose
```

### Utilities

#### `clean`
Clean generated files and caches.

```bash
ingen_fab clean [OPTIONS]
```

**Options:**
- `--cache`: Clean cache files
- `--output`: Clean output files
- `--all`: Clean all generated files

**Examples:**
```bash
# Clean all generated files
ingen_fab clean --all

# Clean cache only
ingen_fab clean --cache
```

#### `version`
Show version information.

```bash
ingen_fab version [OPTIONS]
```

**Options:**
- `--verbose`: Show detailed version information
- `--check-updates`: Check for available updates

**Examples:**
```bash
# Show version
ingen_fab version

# Check for updates
ingen_fab version --check-updates
```

## Configuration

### Configuration File

The CLI looks for configuration in these locations:

1. `./ingen_fab.yaml` (project-specific)
2. `~/.ingen_fab/config.yaml` (user-specific)
3. `/etc/ingen_fab/config.yaml` (system-wide)

### Environment Variables

```bash
# Core settings
export INGEN_FAB_ENVIRONMENT=production
export FABRIC_WORKSPACE_ID=your-workspace-id

# Authentication
export AZURE_CLIENT_ID=your-client-id
export AZURE_CLIENT_SECRET=your-client-secret
export AZURE_TENANT_ID=your-tenant-id

# Logging
export INGEN_FAB_LOG_LEVEL=INFO
export INGEN_FAB_LOG_FILE=/path/to/log/file
```

### Configuration Schema

```yaml
# ingen_fab.yaml
environment: development
workspace_id: abc-123-def

logging:
  level: INFO
  file: logs/cli.log

templates:
  directory: templates/
  variables:
    database_name: my_database
    schema_name: dbo

deployment:
  parallel: true
  timeout: 300
  retry_count: 3
```

## Exit Codes

The CLI uses standard exit codes:

- `0`: Success
- `1`: General error
- `2`: Invalid arguments
- `3`: Configuration error
- `4`: Authentication error
- `5`: Network error
- `6`: Permission error

## Error Handling

### Common Errors

#### Authentication Errors
```bash
Error: Authentication failed. Please run 'az login' to authenticate.
```

**Solution**: Authenticate with Azure CLI

#### Workspace Not Found
```bash
Error: Workspace 'abc-123-def' not found or not accessible.
```

**Solution**: Verify workspace ID and permissions

#### Template Not Found
```bash
Error: Template 'custom-template' not found in template directory.
```

**Solution**: Check template name and directory

### Debugging

```bash
# Enable debug logging
ingen_fab --log-level DEBUG command

# Verbose output
ingen_fab --verbose command

# Check log file
tail -f ~/.ingen_fab/logs/cli.log
```

## Advanced Usage

### Scripting

```bash
#!/bin/bash
# deployment_script.sh

# Set environment
export INGEN_FAB_ENVIRONMENT=production

# Compile DDL scripts
ingen_fab ddl compile --environment production

# Deploy to Fabric
ingen_fab deploy --environment production --force

# Run tests
ingen_fab test --environment production
```

### CI/CD Integration

```yaml
# Azure DevOps Pipeline
steps:
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.12'

- script: |
    pip install insight-ingenious-for-fabric
    ingen_fab deploy --environment production
  displayName: 'Deploy to Fabric'
```

### Custom Commands

```python
# custom_commands.py
import typer
from ingen_fab.cli import app

@app.command()
def custom_command(name: str):
    """Custom command implementation"""
    typer.echo(f"Hello {name}!")

if __name__ == "__main__":
    app()
```

## Examples

### Complete Workflow

```bash
# Initialize new project
ingen_fab init analytics_project --template data-warehouse

# Navigate to project
cd analytics_project

# Configure workspace
ingen_fab config set workspace_id abc-123-def

# Compile DDL scripts
ingen_fab ddl compile --environment development

# Validate notebooks
ingen_fab notebook validate

# Deploy to development
ingen_fab deploy --environment development

# Run tests
ingen_fab test --environment development

# Deploy to production
ingen_fab deploy --environment production
```

### Automation Script

```bash
#!/bin/bash
# automated_deployment.sh

set -e  # Exit on error

echo "Starting deployment..."

# Validate configuration
ingen_fab config show

# Compile and validate
ingen_fab ddl compile --environment $ENVIRONMENT
ingen_fab ddl validate --strict

# Deploy
ingen_fab deploy --environment $ENVIRONMENT --force

# Test deployment
ingen_fab test --environment $ENVIRONMENT

echo "Deployment completed successfully!"
```

## Best Practices

### Command Organization

1. **Use consistent naming**: Follow verb-noun pattern
2. **Group related commands**: Use subcommands for related operations
3. **Provide help text**: Include descriptions for all commands and options
4. **Handle errors gracefully**: Provide meaningful error messages

### Configuration Management

1. **Use environment-specific configs**: Separate configs for dev/prod
2. **Validate configuration**: Check configuration before execution
3. **Use defaults**: Provide sensible defaults for common options
4. **Document configuration**: Include examples and explanations

### Error Handling

1. **Provide clear error messages**: Explain what went wrong and how to fix it
2. **Use appropriate exit codes**: Follow standard conventions
3. **Log errors**: Write detailed error information to log files
4. **Offer recovery options**: Suggest next steps when possible

## Support

For additional help:

- Use `--help` with any command for detailed information
- Check the log files for detailed error information
- Refer to the [User Guide](../user_guide/index.md) for comprehensive documentation
- Visit the [GitHub repository](https://github.com/your-org/ingen_fab) for issues and discussions
