Set up environment variables to avoid specifying them on each command:

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

**PowerShell users:**
```powershell
$env:FABRIC_WORKSPACE_REPO_DIR = "./sample_project"
$env:FABRIC_ENVIRONMENT = "development"
```

See [Environment Variables](../user_guide/environment_variables.md) for a complete list.