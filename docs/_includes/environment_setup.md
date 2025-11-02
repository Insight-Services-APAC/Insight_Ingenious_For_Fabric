Set up environment variables to avoid specifying them on each command:

=== "macOS/Linux"

    ```bash
    # Project location
    export FABRIC_WORKSPACE_REPO_DIR="./sample_project"

    # Target environment
    export FABRIC_ENVIRONMENT="development"
    ```

    For deployment and metadata extraction, you may also need:

    ```bash
    # Authentication (for deployment)
    export AZURE_TENANT_ID="your-tenant-id"
    export AZURE_CLIENT_ID="your-client-id"
    export AZURE_CLIENT_SECRET="your-client-secret"
    ```

=== "Windows"

    ```powershell
    # Project location
    $env:FABRIC_WORKSPACE_REPO_DIR = "./sample_project"

    # Target environment
    $env:FABRIC_ENVIRONMENT = "development"
    ```

    For deployment and metadata extraction, you may also need:

    ```powershell
    # Authentication (for deployment)
    $env:AZURE_TENANT_ID = "your-tenant-id"
    $env:AZURE_CLIENT_ID = "your-client-id"
    $env:AZURE_CLIENT_SECRET = "your-client-secret"
    ```

See [Environment Variables](../reference/environment-variables.md) for a complete list.