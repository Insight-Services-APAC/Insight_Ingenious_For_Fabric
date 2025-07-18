# CLI Reference

Complete reference for all Ingenious Fabric Accelerator commands, options, and usage patterns.

## Global Options

These options are available for all commands:

```bash
ingen_fab [GLOBAL_OPTIONS] COMMAND [COMMAND_OPTIONS]
```

| Option | Short | Description | Environment Variable |
|--------|-------|-------------|---------------------|
| `--fabric-workspace-repo-dir` | `-fwd` | Directory containing fabric workspace repository files | `FABRIC_WORKSPACE_REPO_DIR` |
| `--fabric-environment` | `-fe` | Target environment (e.g., development, production) | `FABRIC_ENVIRONMENT` |
| `--help` | - | Show help message | - |

## Command Groups

## init {#init}

Initialize solutions and projects.

#### `init new`

Create a new Fabric workspace project with complete starter template including variable library, sample DDL scripts, and platform manifests.

```bash
ingen_fab init new --project-name "My Project"
```

**Options:**
- `--project-name` (required): Name of the project
- `--path`: Path where to create the project (default: current directory)

**What gets created:**
- Complete variable library with environment-specific configurations
- Sample DDL scripts for both Lakehouse (Python) and Warehouse (SQL)
- Pre-configured Lakehouse and Warehouse definitions
- Platform manifests for deployment tracking
- Comprehensive README with setup instructions

**Examples:**
```bash
# Create a new project in current directory
ingen_fab init new --project-name "Data Analytics Platform"

# Create project in specific path
ingen_fab init new --project-name "ML Pipeline" --path ./projects
```

**Next steps after creation:**
1. Update variable values in `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json`
2. Generate DDL notebooks: `ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse`
3. Deploy to Fabric: `ingen_fab deploy deploy --fabric-environment development`

#### `init workspace`

Initialize workspace configuration by looking up workspace ID from name.

```bash
ingen_fab init workspace --workspace-name "My Workspace"
```

**Options:**
- `--workspace-name` / `-w`: Name of the Fabric workspace to lookup and configure (required)
- `--create-if-not-exists` / `-c`: Create the workspace if it doesn't exist

**Examples:**
```bash
# Look up existing workspace
ingen_fab init workspace --workspace-name "Analytics Workspace"

# Create workspace if it doesn't exist
ingen_fab init workspace --workspace-name "New Workspace" --create-if-not-exists
```

## ddl {#ddl}

Compile DDL notebooks from templates.

#### `ddl compile`

Generate DDL notebooks from SQL and Python scripts.

```bash
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
```

**Options:**
- `--output-mode` / `-o`: Output destination (`fabric_workspace_repo`, `local`)
- `--generation-mode` / `-g`: Target platform (`Warehouse`, `Lakehouse`)
- `--verbose` / `-v`: Enable verbose output

**Examples:**
```bash
# Generate warehouse notebooks for Fabric deployment
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse

# Generate lakehouse notebooks
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# Generate with verbose output
ingen_fab ddl compile -o fabric_workspace_repo -g Warehouse -v
```

## deploy {#deploy}

Deploy to environments and manage workspace items.

#### `deploy deploy`

Deploy all artifacts to a specific environment.

```bash
ingen_fab deploy deploy
```

Uses the global options `--fabric-workspace-repo-dir` and `--fabric-environment`.

**Examples:**
```bash
# Deploy to development
ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment development

# Using environment variables
export FABRIC_WORKSPACE_REPO_DIR="./sample_project"
export FABRIC_ENVIRONMENT="development"
ingen_fab deploy deploy
```

#### `deploy delete-all`

Delete all workspace items in an environment.

```bash
ingen_fab deploy delete-all
```

**Options:**
- `--force` / `-f`: Force deletion without confirmation

Uses the global options `--fabric-workspace-repo-dir` and `--fabric-environment`.

**Examples:**
```bash
# Delete all items in development environment (will prompt for confirmation)
ingen_fab deploy delete-all --fabric-environment development

# Force delete without confirmation
ingen_fab deploy delete-all --fabric-environment test --force
```

#### `deploy upload-python-libs`

Upload python_libs directory to Fabric config lakehouse using OneLakeUtils.

```bash
ingen_fab deploy upload-python-libs
```

Uses the global options `--fabric-workspace-repo-dir` and `--fabric-environment`.

## notebook {#notebook}

Manage and scan notebook content.

#### `notebook find-notebook-content-files`

Find all notebook-content.py files in a directory.

```bash
ingen_fab notebook find-notebook-content-files --base-dir ./fabric_workspace_items
```

**Options:**
- `--base-dir` / `-b`: Directory to scan (default: fabric_workspace_items)

#### `notebook scan-notebook-blocks`

Scan and analyze notebook code blocks.

```bash
ingen_fab notebook scan-notebook-blocks --base-dir ./fabric_workspace_items
```

**Options:**
- `--base-dir` / `-b`: Directory to scan (default: fabric_workspace_items)
- `--apply-replacements` / `-a`: Apply code replacements

#### `notebook perform-code-replacements`

Perform code replacements in notebooks.

```bash
ingen_fab notebook perform-code-replacements
```

Uses the global context for fabric workspace directory and environment.

## test {#test}

Test notebooks and Python blocks.

#### `test test-python-block`

Test a Python code block.

```bash
ingen_fab test test-python-block
```

#### `test run-simple-notebook`

Run a simple test notebook.

```bash
ingen_fab test run-simple-notebook
```

#### `test run-livy-notebook`

Run a notebook using Fabric Livy API.

```bash
ingen_fab test run-livy-notebook --workspace-id WORKSPACE_ID --lakehouse-id LAKEHOUSE_ID
```

**Options:**
- `--workspace-id` / `-w`: Workspace ID (required)
- `--lakehouse-id` / `-l`: Lakehouse ID (required)
- `--code` / `-c`: Code to execute (default: "print('Hello from Fabric Livy API!')")
- `--timeout` / `-t`: Timeout in seconds (default: 600)

#### Test Local Subcommands

##### `test local python`

Run pytest on Python library tests.

```bash
ingen_fab test local python
```

**Arguments:**
- `lib` (optional): Specific test file to run (without _pytest.py suffix)

**Examples:**
```bash
# Test all Python libraries
export FABRIC_ENVIRONMENT=local
ingen_fab test local python

# Test specific library
ingen_fab test local python ddl_utils
```

##### `test local pyspark`

Run pytest on PySpark library tests.

```bash
ingen_fab test local pyspark
```

**Arguments:**
- `lib` (optional): Specific test file to run (without _pytest.py suffix)

**Note:** Requires `FABRIC_ENVIRONMENT=local` to be set.

##### `test local common`

Run pytest on common library tests.

```bash
ingen_fab test local common
```

**Arguments:**
- `lib` (optional): Specific test file to run (without _pytest.py suffix)

#### Test Platform Subcommands

##### `test platform generate`

Generate platform tests using the script in python_libs_tests.

```bash
ingen_fab test platform generate
```

Uses the global context for fabric workspace directory and environment.

## package {#package}

Compile and run extension packages.

#### Package Ingest Subcommands

##### `package ingest compile`

Compile flat file ingestion package templates and DDL scripts.

```bash
ingen_fab package ingest compile
```

**Options:**
- `--template-vars` / `-t`: JSON string of template variables
- `--include-samples` / `-s`: Include sample data DDL and files

**Examples:**
```bash
# Basic compilation
ingen_fab package ingest compile

# With custom template variables
ingen_fab package ingest compile --template-vars '{"schema": "custom_schema"}'

# Include sample data
ingen_fab package ingest compile --include-samples
```

##### `package ingest run`

Run flat file ingestion for specified configuration or execution group.

```bash
ingen_fab package ingest run --config-id CONFIG_ID --execution-group 1
```

**Options:**
- `--config-id` / `-c`: Specific configuration ID to process
- `--execution-group` / `-g`: Execution group number (default: 1)
- `--environment` / `-e`: Environment name (default: development)

#### Package Synapse Subcommands

##### `package synapse compile`

Compile synapse sync package templates and DDL scripts.

```bash
ingen_fab package synapse compile
```

**Options:**
- `--template-vars` / `-t`: JSON string of template variables
- `--include-samples` / `-s`: Include sample data DDL and files

**Examples:**
```bash
# Basic compilation
ingen_fab package synapse compile

# With custom template variables
ingen_fab package synapse compile --template-vars '{"schema": "custom_schema"}'

# Include sample data
ingen_fab package synapse compile --include-samples
```

##### `package synapse run`

Run synapse sync extraction for specified configuration.

```bash
ingen_fab package synapse run --master-execution-id MASTER_ID
```

**Options:**
- `--master-execution-id` / `-m`: Master execution ID
- `--work-items-json` / `-w`: JSON string of work items for historical mode
- `--max-concurrency` / `-c`: Maximum concurrency level (default: 10)
- `--include-snapshots` / `-s`: Include snapshot tables
- `--environment` / `-e`: Environment name (default: development)

**Note:** The run commands currently display what parameters would be used for execution in a production environment.

## libs {#libs}

Compile and manage Python libraries.

#### `libs compile`

Compile Python libraries by injecting variables from the variable library.

```bash
ingen_fab libs compile
```

**Options:**
- `--target-file` / `-f`: Specific python file to compile (relative to project root)

**Examples:**
```bash
# Compile default config_utils.py
ingen_fab libs compile

# Compile specific file
ingen_fab libs compile --target-file path/to/mylib.py
```

## Configuration

### Environment Variables

Set these to avoid specifying options repeatedly:

```bash
# Core configuration
export FABRIC_WORKSPACE_REPO_DIR="./sample_project"
export FABRIC_ENVIRONMENT="development"

# For local testing
export FABRIC_ENVIRONMENT="local"  # Required for test local commands

# Authentication (for deployment)
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
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
ingen_fab init new --project-name "My Project"
cd "My Project"

# 2. Configure environment variables
export FABRIC_WORKSPACE_REPO_DIR="./My Project"
export FABRIC_ENVIRONMENT="development"

# 3. Update configuration
# Edit fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
# Replace placeholder GUIDs with your actual workspace and lakehouse IDs

# 4. Generate notebooks
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# 5. Test locally
export FABRIC_ENVIRONMENT=local
ingen_fab test local python
ingen_fab test local pyspark

# 6. Deploy to development
export FABRIC_ENVIRONMENT=development
ingen_fab deploy deploy

# 7. Generate and run platform tests
ingen_fab test platform generate
```

### Multi-Environment Deployment

```bash
# Deploy to different environments
for env in development test production; do
    ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment $env
done
```

### Working with Packages

```bash
# Compile the flat file ingestion package
ingen_fab package ingest compile --include-samples

# Compile the synapse sync package
ingen_fab package synapse compile --include-samples

# Note: The run commands are not yet fully implemented and will show what parameters would be used
ingen_fab package ingest run --config-id "customers_import" --environment development
ingen_fab package synapse run --master-execution-id "12345"
```

## Error Handling

Common exit codes:
- 0: Success
- 1: General error or invalid parameters
- Non-zero: Command-specific failure

### Troubleshooting Tips

1. **Use `--help`** with any command for detailed usage
2. **Check environment variables** if commands fail unexpectedly
3. **Set `FABRIC_ENVIRONMENT=local`** for local testing
4. **Enable verbose output** with `--verbose` where available
5. **Check log files** in your workspace for detailed error messages

## Getting Help

- **Command help**: `ingen_fab COMMAND --help`
- **Subcommand help**: `ingen_fab COMMAND SUBCOMMAND --help`
- **Global help**: `ingen_fab --help`