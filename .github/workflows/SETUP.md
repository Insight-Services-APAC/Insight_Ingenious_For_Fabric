# GitHub Actions Setup Instructions - Prerequisites

## Step 1: Azure App Registration Setup

### 1.1 Create App Registration
1. Go to Azure Portal → Azure Active Directory → App registrations
2. Click **New registration**
3. Name: `GitHub-DP-Deployment`
4. Click **Register**
5. Copy the **Application (client) ID** and **Directory (tenant) ID**

### 1.2 Grant Fabric API Permissions
1. In the App Registration, go to **API permissions**
2. Add required permissions for Microsoft Fabric API
3. Grant admin consent

### 1.3 Configure Federated Credentials (OIDC)
1. In the App Registration, go to **Certificates & secrets**
2. Click **Federated credentials** tab
3. Click **Add credential**
4. Select **GitHub Actions deploying Azure resources**
5. Fill in:
   - **Organization**: Your GitHub org/username
   - **Repository**: Your repo name
   - **Entity type**: `Environment`
   - **Environment name**: `DEV`
   - **Name**: `GitHub-DP-Dev`
6. Repeat for `TST`

## Step 2: GitHub Repository Setup

### 2.1 Create GitHub Environments
1. Go to your repository → **Settings** → **Environments**
2. Click **New environment**
3. Create: `DEV`
4. Create: `TST`
5. Optional: Add protection rules and required reviewers

### 2.2 Add GitHub Secrets
1. Go to **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret** and add:

**AZURE_CLIENT_ID**
- Value: Application (client) ID from Step 1.1

**AZURE_TENANT_ID**
- Value: Directory (tenant) ID from Step 1.1

**FABRIC_ENVIRONMENT**
- Value: `development` (or your environment name)

**IS_SINGLE_WORKSPACE**
- Value: `true` or `false`

## Step 3: Verify Prerequisites

Before running the workflow, ensure:
- [ ] App Registration created with Fabric API permissions
- [ ] Federated credentials configured for both environments (DEV, TST)
- [ ] GitHub environments created (DEV, TST)
- [ ] All 4 secrets added to GitHub repository
- [ ] Repository contains `dp/` directory with Fabric workspace items
- [ ] Service Principal has access to target Fabric workspaces

## Step 4: Run the Workflow

1. Go to **Actions** tab
2. Select **DP Deployment Pipeline**
3. Click **Run workflow**
4. Select:
   - Environment: DEV or TST
   - Initial deployment: false (for normal deployments)
   - Run DDL orchestration: true (to execute DDL notebook)
   - Use notebook cache: true (to speed up deployments)
5. Click **Run workflow**

## Workflow Features

### Notebook Caching
- Caches generated notebooks between runs
- Speeds up deployments when code hasn't changed
- Set "Use notebook cache" to false to force regeneration

### Workspace Names
- DEV environment uses workspace: `dp_dev`
- TST environment uses workspace: `dp_tst`
