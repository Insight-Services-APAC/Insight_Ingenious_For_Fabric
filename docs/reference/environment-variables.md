# Environment Variables

[Home](../index.md) > [Reference](index.md) > Environment Variables

Reference for environment variables used by the CLI and deploy workflows.

| Variable | Required For | Description | Example |
|----------|--------------|-------------|---------|
| `FABRIC_WORKSPACE_REPO_DIR` | Most commands | Path to the Fabric workspace repository root | `./sample_project` |
| `FABRIC_ENVIRONMENT` | Most commands | Target environment name matching a value set (e.g., `local`, `development`, `test`, `production`) | `development` |
| `AZURE_TENANT_ID` | Deploy/metadata (as needed) | Azure Entra tenant ID used for service principal auth | `11111111-2222-3333-4444-555555555555` |
| `AZURE_CLIENT_ID` | Deploy/metadata (as needed) | Service principal (app) client ID | `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee` |
| `AZURE_CLIENT_SECRET` | Deploy/metadata (as needed) | Client secret for the service principal | `…` |
| `IGEN_FAB_CONFIG` | Optional | Path to custom configuration file | `./config/custom.yml` |
| `SQL_SERVER_PASSWORD` | Local testing | Password for local SQL Server instance | `YourStrong!Passw0rd` |
| `MYSQL_PASSWORD` | Local testing | Password for local MySQL instance | `password` |
| `POSTGRES_PASSWORD` | Local testing | Password for local PostgreSQL instance | `postgres` |
| `POSTGRES_HOST` | Local testing | PostgreSQL host (default: localhost) | `localhost` |
| `POSTGRES_PORT` | Local testing | PostgreSQL port (default: 5432) | `5432` |
| `POSTGRES_USER` | Local testing | PostgreSQL user (default: postgres) | `postgres` |
| `POSTGRES_DATABASE` | Local testing | PostgreSQL database name (default: local) | `local` |
| `WORKSPACE_MANIFEST_LOCATION` | Remote manifest file | Used to allow manifest file to be stored in config lakehouse | `config_lakehouse`, `local` |
| `ITEM_TYPES_TO_DEPLOY` | Deploy | Used to control which type of artefacts are deployed; empty means every type fabric-cicd accepts | `VariableLibrary,Lakehouse`, '' |
| `AUTO_UPDATE_ITEM_IDS` | Deploy (optional) | After a deploy, set `<name>_<type>_id` variables in the value set from the live item ids | `true` |
| `IS_SINGLE_WORKSPACE` | Deploy | Used to allow unattended execution of ingen_fab init workspace | `Y` |
| `FABRIC_CICD_FILE_LOGGING_ENABLED` | Deploy (optional) | Ask fabric-cicd to also write its log to `fabric_cicd.error.log` in the working directory (off by default since fabric-cicd 1.2) | `true` |
| `FABRIC_CICD_RETRY_API_MAX_DURATION_SECONDS` | Deploy (optional) | Upper bound fabric-cicd applies to one API call including long-running-operation polling and retries (default 300) | `600` |

Notes:
- The CLI falls back to `FABRIC_WORKSPACE_REPO_DIR` and `FABRIC_ENVIRONMENT` if not provided by flags.
- For local development and tests, set `FABRIC_ENVIRONMENT=local`.
- When deploying to Fabric or extracting metadata, authenticate with `az login` or provide the service principal credentials above; the three `AZURE_*` variables are used together and take precedence over the Azure CLI login (see [Installation: Set Up Azure Authentication](../user_guide/installation.md#set-up-azure-authentication)).
- `FABRIC_CICD_*` variables are read by the fabric-cicd library itself; the full list is in its [documentation](https://microsoft.github.io/fabric-cicd/latest/).

Quick setup:

=== "Unix/Linux/macOS"

    ```bash
    export FABRIC_WORKSPACE_REPO_DIR="./sample_project"
    export FABRIC_ENVIRONMENT="development"
    # For service principal auth (optional)
    export AZURE_TENANT_ID="<tenant-guid>"
    export AZURE_CLIENT_ID="<client-id>"
    export AZURE_CLIENT_SECRET="<secret>"
    ```

=== "Windows"

    ```powershell
    $env:FABRIC_WORKSPACE_REPO_DIR = "dp"
    $env:FABRIC_ENVIRONMENT = "development"
    # For service principal auth (optional)
    $env:AZURE_TENANT_ID = "<tenant-guid>"
    $env:AZURE_CLIENT_ID = "<client-id>"
    $env:AZURE_CLIENT_SECRET = "<secret>"
    ```
