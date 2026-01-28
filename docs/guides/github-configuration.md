# GitHub Actions Setup

[Home](../index.md) > [User Guide](../user_guide/index.md) > GitHub Actions Setup

## Overview

This guide provides step-by-step instructions to configure GitHub Actions for automated deployment of Ingenious projects to Microsoft Fabric workspaces using OIDC (OpenID Connect) authentication and federated credentials.

## Sample Pipeline Scripts

A sample YAML file is included under `ingen_fab/project_templates/deployment/github/`:

| Script | Purpose | Use Case |
|--------|---------|----------|
| `dp_pipeline.yml` | Automated deployment with manual controls | Standard deployment of Fabric artifacts with caching and DDL orchestration support |

## Pipeline Architecture

The `dp_pipeline.yml` workflow implements a multi-stage deployment pattern with manual controls for flexibility.

### Workflow Triggers

The pipeline is triggered manually via `workflow_dispatch` with user-selectable options:

```yaml
on:
  workflow_dispatch:
    inputs:
      environment:
        description: 'Environment to deploy'
        required: true
        type: choice
        options:
          - DEV
          - TST
      is_initial:
        description: 'Initial deployment'
        required: false
        type: boolean
        default: false
      run_ddl_scripts:
        description: 'Run DDL orchestration'
        required: false
        type: boolean
        default: true
      use_cache:
        description: 'Use notebook cache'
        required: false
        type: boolean
        default: true
```

### Workflow Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| **environment** | choice | (required) | Target environment: DEV or TST |
| **is_initial** | boolean | false | Enable initial deployment mode (limited artifact types) |
| **run_ddl_scripts** | boolean | true | Execute DDL orchestration notebook after deployment |
| **use_cache** | boolean | true | Cache generated notebooks to speed up subsequent runs |

### Environment Configuration

The workflow sets environment variables that control Ingenious behavior:

```yaml
env:
  FABRIC_ENVIRONMENT: ${{ github.event.inputs.environment }}
  FABRIC_WORKSPACE_REPO_DIR: dp
  IS_INITIAL: ${{ github.event.inputs.is_initial }}
  IS_SINGLE_WORKSPACE: ${{ secrets.IS_SINGLE_WORKSPACE }}
```

| Variable | Source | Purpose |
|----------|--------|---------|
| `FABRIC_ENVIRONMENT` | Workflow input | Target environment (DEV, TST) for deployment |
| `FABRIC_WORKSPACE_REPO_DIR` | Hardcoded | Directory containing Fabric workspace items (`dp`) |
| `IS_INITIAL` | Workflow input | Enables initial deployment mode (limited artifacts) |
| `IS_SINGLE_WORKSPACE` | GitHub secret | Enable unattended workspace initialization |

### Deployment Steps

The workflow executes the following steps in sequence:

#### 1. Checkout Repository
```yaml
- name: Checkout repository
  uses: actions/checkout@v4
```
Retrieves the repository code with persistent credentials for later git commits.

#### 2. Azure CLI Login (OIDC)
```yaml
- name: Az CLI login
  uses: azure/login@v2
  with:
    client-id: ${{ secrets.AZURE_CLIENT_ID }}
    tenant-id: ${{ secrets.AZURE_TENANT_ID }}
    allow-no-subscriptions: true
```
Authenticates to Azure using OIDC federated credentials (no secrets stored).

#### 3. Set up Python Environment
```yaml
- name: Set up Python
  uses: actions/setup-python@v5
  with:
    python-version: '3.12'
```
Configures Python 3.12 runtime for Ingenious CLI execution.

#### 4. Install Ingenious
```yaml
- name: Install dependencies
  run: |
    python -m pip install --upgrade pip
    pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
```
Installs the latest Ingenious CLI from GitHub.

#### 5. Restore Cached Notebooks (Optional)
```yaml
- name: Restore cached notebooks
  if: github.event.inputs.use_cache == 'true'
  uses: actions/cache/restore@v4
  with:
    path: dp/fabric_workspace_items
    key: dp-notebooks-${{ github.event.inputs.environment }}-${{ github.sha }}
```
Retrieves cached notebooks if available to speed up deployment.

#### 6. Deploy Fabric Artifacts
```yaml
- name: Deploy Fabric artefacts
  run: |
    if [ "$IS_INITIAL" = "true" ]; then
      export WORKSPACE_MANIFEST_LOCATION="local"
      export ITEM_TYPES_TO_DEPLOY="VariableLibrary,Lakehouse"
    else
      export WORKSPACE_MANIFEST_LOCATION="config_lakehouse"
      export ITEM_TYPES_TO_DEPLOY=""
    fi
    ingen_fab deploy deploy
```
Creates or updates Fabric items based on deployment mode:
- **Initial mode**: Deploys only `VariableLibrary` and `Lakehouse` items with local manifest
- **Full mode**: Deploys all artifact types with manifest stored in Fabric lakehouse

#### 7. Update Workspace Variables
```yaml
- name: Update Workspace variables
  run: |
    if [ "${{ github.event.inputs.environment }}" = "DEV" ]; then
      ingen_fab init workspace --workspace-name dp_dev
    else
      ingen_fab init workspace --workspace-name dp_tst
    fi
```
Synchronizes variable library values to the target workspace based on environment.

#### 8. Upload Python Libraries
```yaml
- name: Upload Python Libs
  run: ingen_fab deploy upload-python-libs
```
Uploads custom Python libraries from `python_libs/` to the Fabric lakehouse for use in notebooks.

#### 9. Generate DDL Notebooks
```yaml
- name: Generate DDL notebooks
  run: ingen_fab ddl compile
```
Compiles DDL scripts into Fabric-ready notebooks.

#### 10. Cache Notebooks
```yaml
- name: Save notebooks to cache
  if: github.event.inputs.use_cache == 'true'
  uses: actions/cache/save@v4
  with:
    path: dp/fabric_workspace_items
    key: dp-notebooks-${{ github.event.inputs.environment }}-${{ github.sha }}
```
Saves generated notebooks to cache for future runs.

#### 11. Commit and Push Changes
```yaml
- name: Commit and push changes
  run: |
    git config --global user.email "user@domain.com"
    git config --global user.name "user"
    git add .
    git commit -m "Automated file update from pipeline" || echo "No changes to commit"
    git push origin HEAD:${{ github.ref_name }}
```
Commits auto-generated files back to the repository.

#### 12. Run DDL Orchestration (Conditional)
```yaml
- name: Run DDL Orchestration
  if: github.event.inputs.run_ddl_scripts == 'true' && github.event.inputs.is_initial == 'false'
  run: |
    TOKEN=$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
    WORKSPACE_ID=$(python3 -c "import os, sys; sys.path.append('dp'); from ingen_fab.config_utils.variable_lib_factory import get_workspace_id_from_environment; print(get_workspace_id_from_environment('$FABRIC_ENVIRONMENT', 'dp'))")
    NOTEBOOK_ID=$(curl -s -H "Authorization: Bearer $TOKEN" "https://api.fabric.microsoft.com/v1/workspaces/$WORKSPACE_ID/items?type=Notebook" | python3 -c "import sys, json; items = json.load(sys.stdin).get('value', []); print(next((item['id'] for item in items if item['displayName'] == '00_all_lakehouses_orchestrator_ddl_scripts'), ''))")
    curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" "https://api.fabric.microsoft.com/v1/workspaces/$WORKSPACE_ID/items/$NOTEBOOK_ID/jobs/instances?jobType=RunNotebook"
```
Executes the DDL orchestration notebook via Fabric REST API (only for full deployments after initial setup).

## Pipeline Execution Flow

1. **Manual Trigger** → User selects environment and deployment options
2. **Checkout** → Repository code retrieved
3. **OIDC Authentication** → Azure login via federated credentials
4. **Python Setup** → Python 3.12 environment configured
5. **Install Ingenious** → CLI tools installed from GitHub
6. **Restore Cache** → Previous notebooks retrieved if available
7. **Deploy Artifacts** → Fabric items created/updated based on mode
8. **Initialize Workspace** → Variables synchronized to target workspace
9. **Upload Libraries** → Python libraries transferred to lakehouse
10. **Generate DDL** → DDL scripts compiled to notebooks
11. **Save Cache** → Generated notebooks cached
12. **Commit Results** → Auto-generated files pushed to repository
13. **Execute DDL** → Orchestration notebook runs (conditional)

## Deployment Modes

### Initial Deployment
- **Trigger**: Set `is_initial: true`
- **Artifacts**: VariableLibrary, Lakehouse only
- **Manifest**: Stored locally in repository
- **Use Case**: First-time setup of new environments

### Full Deployment
- **Trigger**: Set `is_initial: false` (default)
- **Artifacts**: All types (notebooks, warehouses, data pipelines, semantic models, etc.)
- **Manifest**: Stored in Fabric lakehouse (config_lakehouse)
- **Use Case**: Regular deployments and promoting changes through environments



## Pre-requisites

### Subscription Requirements

| **Requirement No** | **Requirements** | **Description** |
| --- | --- | --- |
| **REQ 1.1** | GitHub Repository | A repository with Ingenious project files and GitHub Actions enabled |
| **REQ 1.2** | MS Fabric workspaces | Create dedicated Fabric workspace for Dev, Test/UAT and Prod environments |
| **REQ 1.3** | Service Principal | Create a new Service Principal in Azure Entra and save the ClientID in GitHub secrets |
| **REQ 1.4** | GitHub Environments | Create GitHub environments (DEV, TST) for deployment targets |
| **REQ 1.5** | Azure Admin Access | Azure Admin contact for Service Principal permission configuration |
| **REQ 1.6** | Fabric Admin Access | Fabric Admin contact for workspace role assignment and tenant settings |
| **REQ 1.7** | GitHub Admin Access | GitHub Admin for repository secrets and environment configuration |

### License/Access Requirements

| **Requirement No** | **Requirements** | **Description** |
| --- | --- | --- |
| **L-REQ 1.1** | Azure Portal Access | For the user who will configure Service Principal and federated credentials in Azure |
| **L-REQ 1.2** | Fabric Admin Portal Access** | For the user who will update Fabric tenant settings and assign workspace roles |
| **L-REQ 1.3** | GitHub Repository Admin | For the user who will configure repository secrets and environments |

## Phase 1: Create and Configure the Service Principal (Azure Entra)

### 1.1 Create App Registration
1. Go to **Azure Portal** → **Azure Active Directory** → **App registrations**
2. Click **New registration**
3. Name: `GitHub-DP-Deployment`
4. Click **Register**
5. Copy the **Application (client) ID** and **Directory (tenant) ID**

### 1.2 Grant Fabric API Permissions
1. In the App Registration, go to **API permissions**
2. Add required permissions for Microsoft Fabric API
3. Grant admin consent

## Phase 2: Configure Federated Credentials (OIDC)

### 2.1 Configure Federated Credentials
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

## Phase 3: GitHub Repository Setup

### 3.1 Create GitHub Environments
1. Go to your repository → **Settings** → **Environments**
2. Click **New environment**
3. Create: `DEV`
4. Create: `TST`
5. Optional: Add protection rules and required reviewers

### 3.2 Add GitHub Secrets
1. Go to **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret** and add:

| Secret Name | Value |
| --- | --- |
| **AZURE_CLIENT_ID** | Application (client) ID from Phase 1.1 |
| **AZURE_TENANT_ID** | Directory (tenant) ID from Phase 1.1 |
| **FABRIC_ENVIRONMENT** | `development` (or your environment name) |
| **IS_SINGLE_WORKSPACE** | `true` or `false` |

## Phase 4: Verify Prerequisites

Before running the workflow, ensure:
- [ ] App Registration created with Fabric API permissions
- [ ] Federated credentials configured for both environments (DEV, TST)
- [ ] GitHub environments created (DEV, TST)
- [ ] All 4 secrets added to GitHub repository
- [ ] Repository contains `dp/` directory with Fabric workspace items
- [ ] Service Principal has access to target Fabric workspaces

## Phase 5: Run the Workflow

### 5.1 Manual Workflow Execution
1. Go to **Actions** tab
2. Select **DP Deployment Pipeline**
3. Click **Run workflow**
4. Select:
   - Environment: DEV or TST
   - Initial deployment: false (for normal deployments)
   - Run DDL orchestration: true (to execute DDL notebook)
   - Use notebook cache: true (to speed up deployments)
5. Click **Run workflow**

### 5.2 Workflow Parameters

| Parameter | Value | Description |
| --- | --- | --- |
| **Environment** | DEV or TST | Target deployment environment |
| **Initial deployment** | false | For normal deployments; set true for first-time setup |
| **Run DDL orchestration** | true | Execute DDL notebook during deployment |
| **Use notebook cache** | true | Speed up deployments by caching generated notebooks |

## Workflow Features

### Notebook Caching
- Caches generated notebooks between runs
- Speeds up deployments when code hasn't changed
- Set "Use notebook cache" to false to force regeneration

### Workspace Names
- DEV environment uses workspace: `dp_dev`
- TST environment uses workspace: `dp_tst`

## Troubleshooting

### Common Issues

| Issue | Solution |
| --- | --- |
| **OIDC Authentication Fails** | Verify federated credentials are configured correctly in Service Principal. Check that Organization, Repository, Entity type, and Environment name match your GitHub values |
| **Permission Denied on Workspace** | Ensure Service Principal has been granted Contributor or Admin role on target Fabric workspaces (dp_dev, dp_tst) |
| **Secrets Not Found in Workflow** | Verify all 4 secrets are added to GitHub repository (AZURE_CLIENT_ID, AZURE_TENANT_ID, FABRIC_ENVIRONMENT, IS_SINGLE_WORKSPACE) with exact naming |
| **Deployment Fails - Item Already Exists** | Ensure target Fabric workspace is empty or re-run with appropriate deployment settings |
| **Notebook Cache Issues** | Set "Use notebook cache" to false to force regeneration of notebooks |

## Best Practices

1. **Use Federated Credentials**: Leverage OIDC authentication without storing secrets
2. **Separate Service Principals**: Consider using different Service Principals per environment for security isolation
3. **Enable Approval Gates**: Configure required reviewers for production environments in GitHub
4. **Monitor Workflow Runs**: Review GitHub Actions logs regularly to catch issues early
5. **Version Control**: Tag releases in Git to track what's deployed to each environment
6. **Repository Structure**: Maintain `dp/` directory structure for Fabric workspace items in your repository
