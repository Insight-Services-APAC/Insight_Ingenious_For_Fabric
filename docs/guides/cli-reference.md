# CLI Reference

[Home](../index.md) > [Guides](cli-reference.md) > CLI Reference

Complete reference for all Ingenious Fabric Accelerator commands, options, and usage patterns.

!!! tip "User Journey"
    **Regular User Path**: [CLI Reference](cli-reference.md) (you are here) → [Deployment Guide](deployment.md) → [Packages](packages.md) → [Troubleshooting](troubleshooting.md)

!!! tip "Before you begin"
    Activate your environment and set defaults to simplify commands.

    ```bash
    source .venv/bin/activate
    ```

    --8<-- "_includes/environment_setup.md"

## Command Groups at a Glance

| Group | Purpose | Common example |
|-------|---------|----------------|
| `init` | Create projects / configure workspace | `ingen_fab init new --project-name MyProj` |
| `ddl` | Compile notebooks from DDL scripts | `ingen_fab ddl compile -o fabric_workspace_repo -g Warehouse` |
| `deploy` | Deploy, upload libs, extract metadata | `ingen_fab deploy get-metadata --target both -f csv -o artifacts/meta.csv` |
| `notebook` | Scan and transform notebooks | `ingen_fab notebook scan-notebook-blocks -a` |
| `test` | Local and platform tests | `FABRIC_ENVIRONMENT=local ingen_fab test local python` |
| `package` | Run solution packages | `ingen_fab package ingest compile -d warehouse` |
| `libs` | Compile Python libs with injection | `ingen_fab libs compile` |
| `dbt` | Generate notebooks from dbt outputs | `ingen_fab dbt create-notebooks -p my_dbt_proj` |

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

#### `deploy delete-all` {#deploy-delete-all}

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

#### `deploy upload-python-libs` {#deploy-upload-python-libs}

Upload `python_libs` to the Fabric config lakehouse with variable injection performed during upload (via OneLake/ADLS APIs).

```bash
ingen_fab deploy upload-python-libs
```

Uses the global options `--fabric-workspace-repo-dir` and `--fabric-environment`.

#### `deploy get-metadata` {#deploy-get-metadata}

Extract schema/table/column metadata for Lakehouse, Warehouse, or both.

```bash
ingen_fab deploy get-metadata [OPTIONS]
```

**Target selection:**
- `--target` / `-tgt`: `lakehouse` (default), `warehouse`, or `both`

**Workspace/asset filters:**
- `--workspace-id` | `--workspace-name`
- Lakehouse: `--lakehouse-id` | `--lakehouse-name`
- Warehouse: `--warehouse-id` | `--warehouse-name`

**Metadata filters and connection:**
- `--schema` / `-s`: Schema name filter
- `--table` / `-t`: Table name substring filter
- `--method` / `-m`: `sql-endpoint` (default) or `sql-endpoint-odbc`
- `--sql-endpoint-id`: Explicit SQL endpoint ID
- `--sql-endpoint-server`: Endpoint server prefix (e.g., `myws-abc123`)

**Output control:**
- `--format` / `-f`: `csv` (default), `json`, or `table`
- `--output` / `-o`: Write output to a file; omit to print to console
- Lakehouse only: `--all`: Extract all lakehouses in the workspace

**Examples:**
```bash
# Lakehouse metadata for one asset; write CSV to file
ingen_fab deploy get-metadata \
  --workspace-name "Analytics Workspace" \
  --lakehouse-name "Config Lakehouse" \
  --schema config --table meta \
  --format csv --output ./artifacts/lakehouse_metadata.csv

# Warehouse metadata printed as a table
ingen_fab deploy get-metadata \
  --workspace-name "Analytics Workspace" \
  --warehouse-name "EDW" \
  --target warehouse --format table

# Both lakehouse and warehouse (filter by schema) using default CSV outputs
ingen_fab deploy get-metadata \
  --workspace-name "Analytics Workspace" \
  --schema sales --target both
```

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

Compile flat file ingestion package templates and DDL scripts for lakehouse or warehouse targets.

```bash
ingen_fab package ingest compile
```

**Options:**
- `--template-vars` / `-t`: JSON string of template variables
- `--include-samples` / `-s`: Include sample data DDL and files
- `--target-datastore` / `-d`: Target datastore type: `lakehouse`, `warehouse`, or `both` (default: `lakehouse`)

**Examples:**
```bash
# Basic compilation for lakehouse (default)
ingen_fab package ingest compile

# Compile for warehouse using COPY INTO operations
ingen_fab package ingest compile --target-datastore warehouse

# Compile both lakehouse and warehouse versions
ingen_fab package ingest compile --target-datastore both

# With custom template variables and samples
ingen_fab package ingest compile --template-vars '{"schema": "custom_schema"}' --include-samples

# Warehouse version with sample data
ingen_fab package ingest compile --target-datastore warehouse --include-samples
```

**Target Datastore Types:**
- `lakehouse`: Generates PySpark notebook using lakehouse_utils for Delta table operations
- `warehouse`: Generates Python notebook using warehouse_utils with COPY INTO for efficient bulk loading
- `both`: Generates separate notebooks for both lakehouse and warehouse targets

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

#### Package Extract Subcommands

##### `package extract compile`

Compile extract generation package templates and DDL scripts.

```bash
ingen_fab package extract compile
```

**Options:**
- `--template-vars` / `-t`: JSON string of template variables
- `--include-samples` / `-s`: Include sample data DDL and files

**Examples:**
```bash
# Basic compilation
ingen_fab package extract compile

# With sample configurations
ingen_fab package extract compile --include-samples

# With custom template variables
ingen_fab package extract compile --template-vars '{"output_path": "exports"}'
```

##### `package extract run`

Run extract generation for specified configuration.

```bash
ingen_fab package extract run --extract-name EXTRACT_NAME
```

**Options:**
- `--extract-name` / `-n`: Name of the extract configuration to run
- `--execution-group` / `-g`: Execution group number
- `--environment` / `-e`: Environment name (default: development)
- `--run-type` / `-r`: Run type: FULL or INCREMENTAL

**Examples:**
```bash
# Run specific extract
ingen_fab package extract run --extract-name CUSTOMER_DAILY_EXPORT

# Run all extracts in execution group
ingen_fab package extract run --execution-group 1

# Run incremental extract
ingen_fab package extract run --extract-name ORDERS_INCREMENTAL --run-type INCREMENTAL
```

#### Package Synthetic Data Subcommands

##### `package synthetic-data list-datasets`

List all available predefined synthetic datasets.

```bash
ingen_fab package synthetic-data list-datasets
```

**Examples:**
```bash
# Show all available datasets
ingen_fab package synthetic-data list-datasets
```

##### `package synthetic-data compile`

Compile synthetic data generation notebook for a specific dataset.

```bash
ingen_fab package synthetic-data compile --dataset-id DATASET_ID
```

**Options:**
- `--dataset-id` / `-d`: Predefined dataset ID to compile
- `--size` / `-s`: Dataset size preset: small, medium, large
- `--target-rows` / `-r`: Target number of rows to generate
- `--generation-mode` / `-m`: Generation mode: python, pyspark, or auto (default: auto)
- `--include-ddl`: Include DDL scripts for configuration tables
- `--target-environment` / `-e`: Target environment: lakehouse or warehouse (default: lakehouse)

**Examples:**
```bash
# Compile small retail dataset
ingen_fab package synthetic-data compile --dataset-id retail_oltp_small --size small

# Compile with specific row count
ingen_fab package synthetic-data compile --dataset-id retail_star_large --target-rows 1000000

# Compile for warehouse with DDL
ingen_fab package synthetic-data compile --dataset-id finance_oltp_small --target-environment warehouse --include-ddl

# Force PySpark mode for large dataset
ingen_fab package synthetic-data compile --dataset-id retail_star_large --target-rows 1000000000 --generation-mode pyspark
```

##### `package synthetic-data generate`

Generate synthetic data for a specific dataset (compile and run).

```bash
ingen_fab package synthetic-data generate DATASET_ID --target-rows ROWS
```

**Options:**
- `dataset_id`: Predefined dataset ID to generate (positional argument)
- `--target-rows` / `-r`: Target number of rows to generate
- `--seed` / `-s`: Random seed for reproducible data
- `--generation-mode` / `-m`: Generation mode: python, pyspark, or auto
- `--chunk-size` / `-c`: Rows per chunk for large datasets
- `--partition-by` / `-p`: Columns to partition output by
- `--optimize-delta` / `-o`: Enable Delta Lake optimizations
- `--sample-percent` / `-sp`: Generate only a percentage of target rows

**Examples:**
```bash
# Generate 10K rows of retail data
ingen_fab package synthetic-data generate retail_oltp_small --target-rows 10000

# Generate with seed for reproducibility
ingen_fab package synthetic-data generate retail_oltp_small --target-rows 10000 --seed 42

# Generate 100M rows with optimizations
ingen_fab package synthetic-data generate retail_star_large --target-rows 100000000 --optimize-delta --partition-by year,month

# Generate 10% sample of billion-row dataset
ingen_fab package synthetic-data generate retail_star_large --target-rows 1000000000 --sample-percent 10
```

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

## dbt {#dbt}

Generate notebooks from dbt outputs and proxy commands to dbt_wrapper.

#### `dbt create-notebooks`

Generate Fabric notebooks from dbt models and tests.

```bash
ingen_fab dbt create-notebooks --dbt-project-name my_dbt_project
```

**Options:**
- `--dbt-project-name` / `-p`: Name of the dbt project
- `--target-dir` / `-t`: Target directory for notebooks (default: dbt output path)
- `--incremental-models-cadence` / `-i`: Cadence for incremental models (default: DAILY)
- `--seed-refresh-cadence` / `-s`: Cadence for seed refresh (default: DAILY)
- `--master-notebook-cadence` / `-m`: Cadence for master notebook (default: DAILY)

**Examples:**
```bash
# Create notebooks for a dbt project
ingen_fab dbt create-notebooks --dbt-project-name analytics_models

# With custom cadences
ingen_fab dbt create-notebooks -p data_mart -i HOURLY -s WEEKLY -m DAILY
```

#### `dbt convert-metadata`

Convert dbt metadata to Fabric format.

```bash
ingen_fab dbt convert-metadata --dbt-project-dir ./dbt_project
```

**Options:**
- `--dbt-project-dir` / `-p`: Directory containing the dbt project

**Examples:**
```bash
# Convert metadata for dbt project
ingen_fab dbt convert-metadata --dbt-project-dir ./analytics/dbt_project
```

#### `dbt` (proxy command)

Proxy any dbt command to dbt_wrapper inside the Fabric workspace repo.

```bash
ingen_fab dbt [DBT_COMMAND] [DBT_OPTIONS]
```

**Examples:**
```bash
# Run dbt through the proxy
ingen_fab dbt run --models my_model

# Test dbt models
ingen_fab dbt test

# Generate dbt docs
ingen_fab dbt docs generate
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
# Flat File Ingestion
ingen_fab package ingest compile --include-samples
ingen_fab package ingest compile --target-datastore warehouse --include-samples
ingen_fab package ingest run --config-id "customers_import" --environment development

# Extract Generation
ingen_fab package extract compile --include-samples
ingen_fab package extract run --extract-name CUSTOMER_DAILY_EXPORT

# Synapse Sync
ingen_fab package synapse compile --include-samples
ingen_fab package synapse run --master-execution-id "12345"

# Synthetic Data Generation
ingen_fab package synthetic-data list-datasets
ingen_fab package synthetic-data generate retail_oltp_small --target-rows 10000 --seed 42
ingen_fab package synthetic-data compile --dataset-id retail_star_large --target-rows 1000000

# Note: The run commands display what parameters would be used for execution in a production environment
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

## Built-in --help (Synced)

The following sections embed live `--help` output generated by a script to keep docs in sync.

For the most up-to-date help information, use the built-in help commands:

```bash
# Get main help
ingen_fab --help

# Get help for specific command groups
ingen_fab deploy --help
ingen_fab ddl --help  
ingen_fab package --help

# Get help for specific commands
ingen_fab deploy get-metadata --help
ingen_fab ddl compile --help
ingen_fab package ingest compile --help
```
