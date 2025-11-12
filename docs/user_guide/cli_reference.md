# CLI Reference

[Home](../index.md) > [User Guide](index.md) > CLI Reference

Complete reference for all Ingenious Fabric Accelerator commands, options, and usage patterns.

!!! tip "Before you begin"
    Activate your environment and set defaults to simplify commands.

    ```powershell
    .\.venv\Scripts\Activate.ps1
    ```
    ```powershell
    # Set environment (development, UAT, production)
    $env:FABRIC_ENVIRONMENT = "development"

    # Set workspace directory 
    $env:FABRIC_WORKSPACE_REPO_DIR = "dp"
    ```

## Command Groups at a Glance

| Group | Purpose | Common example |
|-------|---------|----------------|
| `init` | Create projects / configure workspace | `ingen_fab init new --project-name MyProj` |
| `ddl` | Compile notebooks from DDL scripts and assist in creating ddl scripts | `ingen_fab ddl compile -o fabric_workspace_repo -g Warehouse` |
| `deploy` | Deploy, upload libs, extract/compare metadata and dwonload artefacts | `ingen_fab deploy get-metadata --target both -f csv -o meta.csv` |
| `dbt` | Generate notebooks from dbt outputs and convert metadata to dbt-adapter format | `ingen_fab dbt create-notebooks -p my_dbt_proj` |

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
- `--with-samples`: Use sample_project as template instead of project_templates (includes additional sample data and configurations)

**What gets created:**
- Complete variable library with environment-specific configurations
- Sample DDL scripts for both Lakehouse (Python) and Warehouse (SQL)
- Pre-configured Lakehouse and Warehouse definitions
- Platform manifests for deployment tracking
- Comprehensive README with setup instructions

!!! note "Template Options"
    By default, `init new` creates a minimal project structure from `project_templates`. 
    Use `--with-samples` to include the full sample project with platform manifests and example configurations.

**Examples:**
```bash
# Create a new project with minimal template
ingen_fab init new --project-name "Data Analytics Platform"

# Create project with sample data and configurations
ingen_fab init new --project-name "My Project" --with-samples

# Create project in specific path
ingen_fab init new --project-name "ML Pipeline" --path ./projects

# Create project with samples in specific path
ingen_fab init new --project-name "ML Pipeline" --path ./projects --with-samples
```

**Next steps after creation:**

1. Update variable values in `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json`

2. Generate DDL notebooks: `ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse`

3. Deploy to Fabric: `ingen_fab deploy deploy --fabric-environment development`

#### `init workspace`

Initialize workspace configuration and automatically update artifact GUIDs (lakehouses, warehouses, notebooks).

```bash
ingen_fab init workspace
```

**Features:**
- Sets workspace ID in variable library for the specified environment
- Automatically discovers and updates lakehouse, warehouse, and notebook GUIDs
- Matches variables ending in `_lakehouse_id`, `_warehouse_id`, `_notebook_id` with their corresponding `_name` variables

**Variable Pattern Matching:**

The command automatically updates GUIDs for variables following these naming patterns:

- `*_lakehouse_id` ↔ `*_lakehouse_name`
- `*_warehouse_id` ↔ `*_warehouse_name`
- `*_notebook_id` ↔ `*_notebook_name`

For example, if your `development.json` contains:
```json
{
  "variableOverrides": [
    {"name": "config_lakehouse_id", "value": "old-guid"},
    {"name": "config_lakehouse_name", "value": "config"},
    {"name": "analysis_notebook_id", "value": "old-guid"},
    {"name": "analysis_notebook_name", "value": "Data Analysis"}
  ]
}
```

Running `ingen_fab init workspace` will automatically find the "config" lakehouse and "Data Analysis" notebook in your workspace and update their GUIDs.

**Options:**
- `--workspace-id`: Fabric workspace ID (auto-detected if not provided)
- `--workspace-name`: Fabric workspace name (auto-detected if not provided)
- `--environment`: Environment to configure (defaults to `$FABRIC_ENVIRONMENT`)

**Examples:**
```powershell
# Configure workspace for development environment
$env:FABRIC_ENVIRONMENT = "development"
ingen_fab init workspace

# Specify workspace explicitly
ingen_fab init workspace --workspace-name "Analytics Workspace"

# Configure for specific environment
ingen_fab init workspace --environment production
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
# Generate both lakehouse and warehouse notebooks (defaults to fabric_workspace_items folder)
ingen_fab ddl compile

# Generate lakehouse notebooks (defaults to fabric_workspace_items folder)
ingen_fab ddl compile -g Lakehouse

# Generate lakehouse notebooks
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# Generate warehouse notebooks for Fabric deployment
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse

# Generate with verbose output
ingen_fab ddl compile -o fabric_workspace_repo -g Warehouse -v
```

#### `ddl ddls-from-metadata`

Generate DDL Python scripts from a metadata CSV file (typically `metadata/lakehouse_metadata_all.csv`).

```bash
ingen_fab ddl ddls-from-metadata --lakehouse lh_bronze
```

**Options:**
- `--lakehouse` / `-l`: Lakehouse name to generate DDLs for (required)

- `--table` / `-t`: Specific table name to generate (optional, generates all tables if not specified)

- `--metadata-file` / `-m`: Path to CSV metadata file (default: `metadata/lakehouse_metadata_all.csv`)

- `--subdirectory` / `-d`: Subdirectory for output files (default: `generated`)

- `--sequence-numbers` / `--no-sequence-numbers` / `-s` / `-ns`: Include sequence number prefixes in filenames (default: True)

- `--verbose` / `-v`: Enable verbose output

**Output:**
- With sequence numbers: `001_lh_silver2_customer.py`, `002_lh_silver2_orders.py`

- Without sequence numbers: `lh_silver2_customer.py`, `lh_silver2_orders.py`

- Output path: `{workspace}/ddl_scripts/Lakehouses/{lakehouse}/{subdirectory}/`

**System Table Exclusion:**

Tables with schemas `sys` or `queryinsights` are excluded.

**Examples:**
```bash
# Generate DDL for all tables in lakehouse
ingen_fab ddl ddls-from-metadata --lakehouse lh_bronze

# Generate DDL for specific table only
ingen_fab ddl ddls-from-metadata --lakehouse lh_bronze --table customer_data

# Generate without sequence numbers in filenames
ingen_fab ddl ddls-from-metadata --lakehouse lh_bronze --no-sequence-numbers

# Custom output subdirectory
ingen_fab ddl ddls-from-metadata --lakehouse lh_bronze --subdirectory staging_ddls

# Use custom metadata CSV file
ingen_fab ddl ddls-from-metadata --lakehouse lh_bronze --metadata-file custom_metadata.csv

# Use metadata file from different directory
ingen_fab ddl ddls-from-metadata --lakehouse lh_bronze --metadata-file /path/to/external_metadata.csv

# Combine options with short flags
ingen_fab ddl ddls-from-metadata -l lh_bronze -t orders -d production -ns -m custom_meta.csv
```

**Typical Workflow:**
```powershell
# 1. Extract metadata from lakehouse (creates metadata/lakehouse_metadata_all.csv)
ingen_fab deploy get-metadata --lakehouse-name lh_bronze --format csv

# 2. Generate DDL scripts from the metadata (uses default file)
ingen_fab ddl ddls-from-metadata --lakehouse lh_bronze

# 3. OR use a custom metadata file
ingen_fab ddl ddls-from-metadata --lakehouse lh_bronze --metadata-file my_custom_metadata.csv

# 4. Review and customize the generated scripts in ddl_scripts/Lakehouses/lh_silver2/generated/
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
$env:FABRIC_WORKSPACE_REPO_DIR = "./sample_project"
$env:FABRIC_ENVIRONMENT = "development"
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

- Workspace: `--workspace-id` | `--workspace-name`

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

#### `deploy compare-metadata` {#deploy-compare-metadata}

Compare two metadata CSV files and report differences between them.

```bash
ingen_fab deploy compare-metadata [OPTIONS]
```

**Required options:**
- `--file1` / `-f1`: First CSV metadata file for comparison
- `--file2` / `-f2`: Second CSV metadata file for comparison

**Output control:**
- `--format` / `-fmt`: `table` (default), `json`, or `csv`
- `--output` / `-o`: Write comparison report to file (optional)

**Detection capabilities:**

The command identifies and reports:

- **Missing tables**: Tables present in one file but not the other
- **Missing columns**: Columns present in one file but not the other  
- **Data type differences**: Same column with different data types
- **Nullable differences**: Same column with different nullable settings

**Output ordering:**

Results are automatically sorted by:
1. Asset name (lakehouse/warehouse name)
2. Schema name  
3. Table name
4. Column name
5. Difference type

**Examples:**
```bash
# Compare two lakehouse metadata files with table output
ingen_fab deploy compare-metadata \
  --file1 metadata_before.csv \
  --file2 metadata_after.csv

# Save detailed JSON comparison report
ingen_fab deploy compare-metadata \
  -f1 prod_metadata.csv \
  -f2 dev_metadata.csv \
  -o schema_diff_report.json --format json

# Generate CSV output for further analysis
ingen_fab deploy compare-metadata \
  --file1 old_schema.csv \
  --file2 new_schema.csv \
  --format csv --output differences.csv
```

**Sample output:**
```
Found 4 differences between metadata_before.csv and metadata_after.csv:
  - Missing Table: 1
  - Missing Column: 1
  - Data Type Diff: 1
  - Nullable Diff: 1

┏━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
┃ Type           ┃ Identifier                                       ┃ metadata_before  ┃ metadata_after   ┃
┡━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
│ Missing Table  │ Analytics.dbo.new_products                       │ missing          │ present          │
│ Missing Column │ Analytics.dbo.customers.phone                    │ missing          │ present          │
│ Data Type Diff │ Analytics.dbo.orders.total_amount                │ decimal(10,2)    │ decimal(12,2)    │
│ Nullable Diff  │ Analytics.dbo.customers.email                    │ true             │ false            │
└────────────────┴──────────────────────────────────────────────────┴──────────────────┴──────────────────┘
```

#### `deploy download-artefact` {#deploy-download-artefact}

Download artifacts from the Fabric workspace to local directory.

```bash
ingen_fab deploy download-artefact [OPTIONS]
```

**Options:**

- `--artifact-type` / `-t`: Type of artifact to download (`notebook`, `lakehouse`, `warehouse`, `all`)

- `--output-dir` / `-o`: Local directory to save artifacts (default: `./downloaded_artifacts`)

- `--workspace-id`: Workspace ID to download from (uses environment config if not specified)

- `--include-content`: Include notebook content files (default: false)

- `--overwrite`: Overwrite existing files (default: false)

Uses the global options `--fabric-workspace-repo-dir` and `--fabric-environment`.

**Examples:**
```bash
# Download an artefact from the existing environment's workspace
ingen_fab deploy download-artefact --artefact-name "rp_test" --artefact-type Report
```

#### `deploy upload-dbt-project` {#deploy-upload-dbt-project}

Upload a dbt project to the Fabric workspace for execution.

```bash
ingen_fab deploy upload-dbt-project [OPTIONS]
```

**Options:**

- `--project-path` / `-p`: Path to dbt project directory (default: current directory)

- `--target-lakehouse` / `-l`: Target lakehouse name for dbt execution

- `--target-workspace` / `-w`: Target workspace name (uses environment config if not specified)


- `--models` / `-m`: Specific dbt models to include (comma-separated)

- `--exclude` / `-e`: Models to exclude from upload (comma-separated)

- `--include-profiles`: Include dbt profiles.yml file (default: false)

- `--overwrite`: Overwrite existing dbt project files (default: false)

Uses the global options `--fabric-workspace-repo-dir` and `--fabric-environment`.

**Examples:**
```bash
# Upload entire dbt project to default lakehouse
ingen_fab deploy upload-dbt-project --project-path ./my_dbt_project

# Upload specific models to named lakehouse
ingen_fab deploy upload-dbt-project \
  --project-path ./analytics_dbt \
  --target-lakehouse "Analytics Lakehouse" \
  --models "staging.customers,marts.customer_summary"

# Upload project excluding test models
ingen_fab deploy upload-dbt-project \
  --project-path ./dbt_project \
  --exclude "tests.*,staging.test_*" \
  --overwrite

# Upload with profiles and specific workspace
ingen_fab deploy upload-dbt-project \
  --project-path ./enterprise_dbt \
  --target-workspace "Production Analytics" \
  --target-lakehouse "Gold Layer" \
  --include-profiles
```

## dbt {#dbt}

Generate notebooks from dbt outputs and proxy commands to dbt_wrapper. The dbt integration includes automatic profile management that helps you select and configure the appropriate lakehouse for your dbt models.

### Automatic Profile Management

When running dbt commands through `ingen_fab`, the system automatically manages your dbt profile:

1. **Discovers Available Lakehouses**: Scans your environment configuration for all configured lakehouses
2. **Interactive Selection**: Prompts you to choose which lakehouse to use (if multiple are available)
3. **Saves Preferences**: Remembers your selection for future use in the same environment
4. **Environment-Specific**: Maintains different selections for different environments (development, test, production)

The dbt profile is automatically created or updated at `~/.dbt/profiles.yml` with the name `fabric-spark-testnb`.

#### `dbt create-notebooks`

Generate Fabric notebooks from dbt models and tests. This command will prompt you to select a lakehouse if multiple options are available.

```bash
ingen_fab dbt create-notebooks --dbt-project my_dbt_project
```

**Options:**

- `--dbt-project` / `-p`: Name of the dbt project directory under the workspace repo

- `--skip-profile-confirmation`: Skip confirmation prompt when updating dbt profile

**Examples:**
```bash
# Create notebooks for a dbt project
ingen_fab dbt create-notebooks --dbt-project analytics_models

# Skip profile confirmation prompt
ingen_fab dbt create-notebooks -p data_mart --skip-profile-confirmation
```

#### `dbt convert-metadata`

Convert cached lakehouse metadata to dbt metaextracts format.

```bash
ingen_fab dbt convert-metadata --dbt-project my_dbt_project
```

**Options:**

- `--dbt-project` / `-p`: Name of the dbt project directory under the workspace repo

- `--skip-profile-confirmation`: Skip confirmation prompt when updating dbt profile

**Prerequisites:**
The metadata must first be extracted using:
```bash
ingen_fab deploy get-metadata --target lakehouse
```

This creates the required `metadata/lakehouse_metadata_all.csv` file.

**What it does:**
- Reads from `{workspace}/metadata/lakehouse_metadata_all.csv`
- Creates JSON files in `{workspace}/{dbt_project}/metaextracts/` for dbt_wrapper to use

**Examples:**
```bash
# Convert metadata for dbt project
ingen_fab dbt convert-metadata --dbt-project analytics_models

# Skip profile confirmation
ingen_fab dbt convert-metadata -p my_dbt_project --skip-profile-confirmation
```

#### `dbt generate-schema-yml`

Convert cached lakehouse metadata to dbt schema.yml format for a specific lakehouse and layer.

```bash
ingen_fab dbt generate-schema-yml --dbt-project my_dbt_project --lakehouse lh_bronze --layer staging --dbt-type model
```

**Options:**

- `--dbt-project` / `-p`: Name of the dbt project directory under the workspace repo

- `--lakehouse`: Name of the lakehouse to use as a filter for the csv

- `--layer`: Name of the dbt layer

- `--dbt-type`: Is this for a model or a snapshot?

- `--skip-profile-confirmation`: Skip confirmation prompt when updating dbt profile

**Prerequisites:**
The metadata must first be extracted using:
```bash
ingen_fab deploy get-metadata --target lakehouse
```

This creates the required `metadata/lakehouse_metadata_all.csv` file.

**What it does:**
- Reads from `{workspace}/metadata/lakehouse_metadata_all.csv`
- Creates schema.yml file in the appropriate dbt project directory
- Filters metadata for the specified lakehouse and layer
- Generates dbt-compatible schema definitions

**Examples:**
```bash
# Generate schema.yml for staging models in bronze lakehouse
ingen_fab dbt generate-schema-yml --dbt-project analytics_models --lakehouse lh_bronze --layer staging --dbt-type model

# Generate schema.yml for snapshots with short flags
ingen_fab dbt generate-schema-yml -p data_mart --lakehouse lh_silver --layer marts --dbt-type snapshot

# Skip profile confirmation
ingen_fab dbt generate-schema-yml -p my_dbt_project --lakehouse lh_gold --layer reporting --dbt-type model --skip-profile-confirmation
```

#### `dbt exec` (proxy command)

Proxy any dbt command to dbt_wrapper inside the Fabric workspace repo. The `exec` command provides intelligent lakehouse selection:

- **With saved preference**: Notifies user of chosen lakehouse and continues
- **Without saved preference**: Always prompts for interactive selection
- **Never fails silently**: Ensures user knows which lakehouse is being used

```bash
ingen_fab dbt [DBT_COMMAND] [DBT_OPTIONS]
```

**Examples:**
```bash
# build dbt models and snapshots
Ingen_fab dbt exec -- stage run build --project-dir dbt_project

# build dbt master notebooks
Ingen_fab dbt exec -- stage run post-scripts --project-dir dbt_project

```

**Typical Output:**
```
# With saved preference:
Using saved lakehouse preference: Bronze Layer (Environment: development)

# Without saved preference:
No valid lakehouse preference found for environment 'development'. Please select a lakehouse:
[Interactive selection table appears]
```

## Configuration

### Environment Variables

Set these to avoid specifying options repeatedly:

```bash
# Core configuration
$env:FABRIC_WORKSPACE_REPO_DIR = "./sample_project"
$env:FABRIC_ENVIRONMENT = "development"

# For local testing
$env:FABRIC_ENVIRONMENT = "local"  # Required for test local commands

# Authentication (for deployment)
$env:AZURE_TENANT_ID = "your-tenant-id"
$env:AZURE_CLIENT_ID = "your-client-id"
$env:AZURE_CLIENT_SECRET = "your-client-secret"
```

### Configuration Files

The tool uses the following configurations:

1. `platform_manifest_*.yml` files in your project root to determine what has been deployed to the current environment and uses file hashing to determine if any artefacts have been updated

2. Variable library files in `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/`

3. Environment variables

## Common Usage Patterns

### Development Workflow

```bash
# 1. Create project
ingen_fab init new --project-name "My Project"

# 2. Configure environment variables
$env:FABRIC_WORKSPACE_REPO_DIR = "./My Project"
$env:FABRIC_ENVIRONMENT = "development"

# 3. Update configuration
# Edit fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
# Replace placeholder GUIDs with your actual workspace and lakehouse IDs

# 4. Develop and build dbt notebooks, copy them to fabric_workspace_items

ingen_fab dbt exec -- stage run build --project-dir dbt_project
ingen_fab dbt exec -- stage run post-scripts --project-dir dbt_project
ingen_fab dbt create-notebooks -p my_dbt_project

# 5. Generate ddl notebooks
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse

# 6. Test locally
$env:FABRIC_ENVIRONMENT = 'local'
ingen_fab test local python
ingen_fab test local pyspark

# 7. Deploy to development
$env:FABRIC_ENVIRONMENT = 'development'
ingen_fab deploy deploy

# 8S. Generate and run platform tests
ingen_fab test platform generate
```

### Multi-Environment Deployment

```powershell
# Deploy to different environments
@('development', 'test', 'production') | ForEach-Object {
    $env:FABRIC_ENVIRONMENT = $_
    ingen_fab deploy deploy --fabric-workspace-repo-dir . --fabric-environment $_
}
```

### Working with Packages

```powershell
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
