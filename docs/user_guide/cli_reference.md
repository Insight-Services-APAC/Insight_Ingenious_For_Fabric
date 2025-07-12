# CLI Reference

Complete reference for all Ingenious Fabric Accelerator commands, options, and usage patterns.

## Global Options

These options are available for all commands:

```bash
ingen_fab [GLOBAL_OPTIONS] COMMAND [COMMAND_OPTIONS]
```

| Option | Description | Environment Variable |
|--------|-------------|---------------------|
| `--fabric-workspace-repo-dir` | Directory containing fabric workspace repository files | `FABRIC_WORKSPACE_REPO_DIR` |
| `--fabric-environment` | Target environment (development, test, production) | `FABRIC_ENVIRONMENT` |
| `--help` | Show help message | - |
| `--version` | Show version information | - |

## Command Groups

### init

Initialize solutions and projects.

#### `init init-solution`

Create a new Fabric workspace project with proper structure.

```bash
ingen_fab init init-solution --project-name "My Project"
```

**Options:**
- `--project-name` (required): Name of the project
- `--template-dir`: Custom template directory (default: built-in templates)
- `--force`: Overwrite existing files

**Examples:**
```bash
# Create a new project
ingen_fab init init-solution --project-name "Data Analytics Platform"

# Create project with custom template
ingen_fab init init-solution --project-name "ML Pipeline" --template-dir ./custom-templates

# Force overwrite existing project
ingen_fab init init-solution --project-name "Existing Project" --force
```

### ddl

Compile DDL notebooks from templates.

#### `ddl compile`

Generate DDL notebooks from SQL and Python scripts.

```bash
ingen_fab ddl compile --output-mode fabric --generation-mode warehouse
```

**Options:**
- `--output-mode`: Output destination (`fabric`, `local`)
- `--generation-mode`: Target platform (`warehouse`, `lakehouse`)
- `--force`: Overwrite existing notebooks

**Examples:**
```bash
# Generate warehouse notebooks for Fabric deployment
ingen_fab ddl compile --output-mode fabric --generation-mode warehouse

# Generate lakehouse notebooks locally
ingen_fab ddl compile --output-mode local --generation-mode lakehouse

# Force regeneration of all notebooks
ingen_fab ddl compile --output-mode fabric --generation-mode warehouse --force
```

### deploy

Deploy to environments and manage workspace items.

#### `deploy deploy`

Deploy all artifacts to a specific environment.

```bash
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development
```

**Options:**
- `--fabric-workspace-repo-dir`: Source directory for deployment
- `--fabric-environment`: Target environment
- `--dry-run`: Show what would be deployed without actually deploying
- `--force`: Force deployment even if validation fails

**Examples:**
```bash
# Deploy to development
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development

# Dry run deployment
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment production --dry-run

# Force deployment
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment test --force
```

### notebook

Manage and scan notebook content.

#### `notebook find-content-files`

Find all notebook-content.py files in a directory.

```bash
ingen_fab notebook find-content-files --base-dir ./fabric_workspace_items
```

#### `notebook scan-blocks`

Scan and analyze notebook code blocks.

```bash
ingen_fab notebook scan-blocks --base-dir ./fabric_workspace_items
```

**Options:**
- `--base-dir`: Directory to scan
- `--output-format`: Output format (`json`, `table`, `summary`)
- `--include-patterns`: File patterns to include
- `--exclude-patterns`: File patterns to exclude

**Examples:**
```bash
# Find all notebook files
ingen_fab notebook find-content-files --base-dir ./fabric_workspace_items

# Scan blocks with JSON output
ingen_fab notebook scan-blocks --base-dir ./fabric_workspace_items --output-format json

# Scan specific patterns
ingen_fab notebook scan-blocks --base-dir ./fabric_workspace_items --include-patterns "*.py"
```

### test

Test notebooks and Python blocks.

#### `test local`

Test components locally.

```bash
# Test libraries
ingen_fab test local libraries --base-dir .

# Test notebooks
ingen_fab test local notebooks --base-dir ./fabric_workspace_items
```

#### `test platform`

Test components on the Fabric platform.

```bash
# Test notebooks on platform
ingen_fab test platform notebooks --base-dir ./fabric_workspace_items
```

**Options:**
- `--base-dir`: Directory containing tests
- `--test-pattern`: Pattern to match test files
- `--verbose`: Show detailed output
- `--failfast`: Stop on first failure

**Examples:**
```bash
# Test all libraries locally
ingen_fab test local libraries --base-dir . --verbose

# Test specific pattern
ingen_fab test local notebooks --base-dir ./fabric_workspace_items --test-pattern "*test*"

# Test on platform with fail-fast
ingen_fab test platform notebooks --base-dir ./fabric_workspace_items --failfast
```

## Configuration

### Environment Variables

Set these to avoid specifying options repeatedly:

```bash
# Core configuration
export FABRIC_WORKSPACE_REPO_DIR="./sample_project"
export FABRIC_ENVIRONMENT="development"

# Authentication
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"

# Optional: SQL Server for local testing
export SQL_SERVER_SA_PASSWORD="YourStrong!Passw0rd"
```

### Configuration Files

The tool looks for configuration in:
1. `platform_manifest_*.yml` files in your project root
2. Variable library files in `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/`
3. Environment variables

## Common Usage Patterns

### Development Workflow

```bash
# 1. Create project
ingen_fab init init-solution --project-name "My Project"

# 2. Edit DDL scripts and configuration
# ... make changes ...

# 3. Generate notebooks
ingen_fab ddl compile --output-mode fabric --generation-mode warehouse
ingen_fab ddl compile --output-mode fabric --generation-mode lakehouse

# 4. Test locally
ingen_fab test local libraries --base-dir .

# 5. Deploy to development
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development

# 6. Test on platform
ingen_fab test platform notebooks --base-dir ./fabric_workspace_items
```

### Multi-Environment Deployment

```bash
# Deploy to different environments
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment test
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment production
```

### Debugging and Troubleshooting

```bash
# Dry run to see what would be deployed
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development --dry-run

# Verbose testing
ingen_fab test local libraries --base-dir . --verbose

# Scan notebooks for issues
ingen_fab notebook scan-blocks --base-dir ./fabric_workspace_items --output-format json
```

## Error Handling

### Common Error Codes

| Exit Code | Description |
|-----------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Configuration error |
| 3 | Authentication error |
| 4 | Deployment error |
| 5 | Test failure |

### Troubleshooting Tips

1. **Use `--help`** with any command for detailed usage
2. **Check environment variables** if commands fail unexpectedly
3. **Use `--dry-run`** to preview changes before deployment
4. **Enable verbose output** with `--verbose` for debugging
5. **Check log files** in your workspace for detailed error messages

## Advanced Usage

### Custom Templates

```bash
# Use custom project templates
ingen_fab init init-solution --project-name "Custom Project" --template-dir ./my-templates
```

### Batch Operations

```bash
# Process multiple environments
for env in development test production; do
    ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment $env
done
```

### Integration with CI/CD

```bash
# CI/CD pipeline example
ingen_fab ddl compile --output-mode fabric --generation-mode warehouse
ingen_fab test local libraries --base-dir . --failfast
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment production
```

## Getting Help

- **Command help**: `ingen_fab COMMAND --help`
- **Global help**: `ingen_fab --help`
- **Version info**: `ingen_fab --version`
- **Documentation**: This documentation site
- **Issues**: [GitHub Issues](https://github.com/your-org/ingen_fab/issues)