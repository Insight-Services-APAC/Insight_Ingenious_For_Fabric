# Troubleshooting

[Home](../index.md) > [Guides](cli-reference.md) > Troubleshooting

Common issues and solutions when working with the Ingenious Fabric Accelerator.

## Environment Setup Issues

### Command not found: ingen_fab
**Problem**: After installation, the command `ingen_fab` is not recognized.

**Solutions**:
1. Activate your virtual environment:
   ```bash
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate     # Windows
   ```

2. Install in development mode:
   ```bash
   pip install -e .[dev]
   ```

3. Check PATH (for global installs):
   ```bash
   echo $PATH
   pip show insight-ingenious-for-fabric
   ```

### Environment variable errors
**Problem**: Commands fail with "FABRIC_WORKSPACE_REPO_DIR must be set" or similar.

**Solution**:
--8<-- "_includes/environment_setup.md"

## CLI Command Issues

### DDL compilation fails
**Problem**: `ingen_fab ddl compile` fails with template errors.

**Solutions**:
1. Check your DDL script syntax:
   ```bash
   python ddl_scripts/Lakehouses/Config/001_Initial_Creation/001_create_tables.py
   ```

2. Verify variable library exists:
   ```bash
   ls fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/
   ```

3. Check for missing Jinja template variables.

### Deployment failures
**Problem**: `ingen_fab deploy deploy` fails with authentication errors.

**Solutions**:
1. Use Azure CLI authentication:
   ```bash
   az login
   az account show  # Verify login
   ```

2. Or set service principal variables:
   ```bash
   export AZURE_TENANT_ID="your-tenant-id"
   export AZURE_CLIENT_ID="your-client-id" 
   export AZURE_CLIENT_SECRET="your-client-secret"
   ```

3. Verify workspace access in the Fabric portal.

## Testing Issues

### Local tests fail
**Problem**: `ingen_fab test local python` fails.

**Solutions**:
1. Set environment to local:
   ```bash
   export FABRIC_ENVIRONMENT=local
   ```

2. Install dev dependencies:
   ```bash
   uv sync  # or pip install -e .[dev]
   ```

3. Check for missing test dependencies like PostgreSQL/SQL Server for local testing.

### Import errors in notebooks
**Problem**: Generated notebooks can't import `python_libs`.

**Solutions**:
1. Upload Python libraries to Fabric:
   ```bash
   ingen_fab deploy upload-python-libs
   ```

2. Check library compilation:
   ```bash
   ingen_fab libs compile
   ```

## Configuration Issues

### Variable resolution fails
**Problem**: Variables like `{{fabric_environment}}` don't resolve in notebooks.

**Solutions**:
1. Verify your value set file exists:
   ```bash
   cat fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
   ```

2. Check variable names match exactly (case-sensitive).

3. Recompile notebooks after variable changes:
   ```bash
   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
   ```

### Workspace not found
**Problem**: Deployment fails with "workspace not found" errors.

**Solutions**:
1. Check workspace ID is correct (from Fabric portal URL).
2. Verify you have access to the workspace.
3. Try using workspace name instead:
   ```bash
   ingen_fab init workspace --workspace-name "My Workspace"
   ```

## Package Issues

### Package compilation fails
**Problem**: `ingen_fab package ingest compile` fails.

**Solutions**:
1. Check target datastore is valid:
   ```bash
   ingen_fab package ingest compile --target-datastore lakehouse  # or warehouse
   ```

2. Include samples for first-time setup:
   ```bash
   ingen_fab package ingest compile --include-samples
   ```

## Performance Issues

### Slow DDL compilation
**Problem**: DDL compilation takes very long.

**Solutions**:
1. Check for large Jinja templates.
2. Reduce number of DDL scripts being compiled.
3. Use specific generation mode rather than both:
   ```bash
   ingen_fab ddl compile --generation-mode Warehouse  # instead of both
   ```

## Getting Help

1. **Use built-in help**:
   ```bash
   ingen_fab --help
   ingen_fab COMMAND --help
   ```

2. **Check logs**: Look for detailed error messages in console output.

3. **Verify prerequisites**: Ensure Python 3.12+, required dependencies installed.

4. **Test step-by-step**: Break complex workflows into individual commands.

## Common Error Messages

| Error | Meaning | Solution |
|-------|---------|----------|
| `FABRIC_ENVIRONMENT must be set` | Missing environment variable | Set environment variables |
| `Workspace 'xyz' not found` | Invalid workspace ID/access | Check workspace ID and permissions |
| `Template 'abc.jinja' not found` | Missing template file | Verify template exists in correct location |
| `Authentication failed` | Azure auth issue | Use `az login` or set service principal env vars |
| `Variable 'name' not found` | Missing variable in value set | Add variable to your environment JSON file |

## Debug Mode

Enable verbose output for more detailed error information:

```bash
# Most commands support --verbose
ingen_fab ddl compile --verbose
```

For persistent issues, create a minimal reproducible example and check the project documentation for specific component troubleshooting.