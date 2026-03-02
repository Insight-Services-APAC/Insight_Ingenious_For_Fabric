# Deploy Guide (CI/CD)

[Home](../index.md) > [User Guide](../user_guide/index.md) > Deploy Guide (CI/CD)

Practical guide to deploy Fabric artifacts using Azure DevOps CI/CD pipelines.

## Overview

This guide explains how to use Azure DevOps pipelines to automate deployment of Ingenious projects to Microsoft Fabric workspaces. Three sample YAML pipeline scripts are provided to support different deployment scenarios.

## Sample Pipeline Scripts

Three sample YAML files are included under `ingen_fab/project_templates/deployment/devops/`:

| Script | Purpose | Use Case |
|--------|---------|----------|
| `dp_initial.yml` | Initial deployment | First-time setup of lakehouses and variable libraries with limited artifact types |
| `dp_full.yml` | Full deployment | Complete deployment of all artifact types including notebooks, data pipelines, and semantic models |
| `dp_combined.yml` | Combined initial and full deployment | Single pipeline that dynamically switches between initial and full deployment modes using the `IS_INITIAL` variable |

## Key Differences Between Scripts

### Initial Deployment (`dp_initial.yml`)

- **Purpose**: Bootstrap new environments with foundational infrastructure
- **Artifact Types**: Limited to `VariableLibrary` and `Lakehouse` (via `ITEM_TYPES_TO_DEPLOY`)
- **Manifest Location**: Uses `local` (manifest stored in repository)
- **When to Use**: 
  - First deployment to a new environment
  - Setting up core infrastructure before full deployment
  - Testing workspace connectivity and permissions

### Full Deployment (`dp_full.yml`)

- **Purpose**: Deploy all project artifacts to established environments
- **Artifact Types**: All types (notebooks, warehouses, data pipelines, semantic models, etc.)
- **Manifest Location**: Uses `config_lakehouse` (manifest stored in Fabric lakehouse)
- **When to Use**:
  - Regular deployments after initial setup
  - Promoting changes through environments (DEV → TEST → PROD)
  - Standard CI/CD workflow

### Combined Deployment (`dp_combined.yml`)

- **Purpose**: Single flexible pipeline that handles both initial setup AND ongoing deployments
- **Key Feature**: Uses `IS_INITIAL` variable to dynamically switch deployment modes
- **When to Use**:
  - You want one pipeline for all deployment scenarios
  - Initial setup of new environments
  - Regular deployments after initial setup
  - Promoting changes through environments (DEV → TEST → PROD)

#### Deployment Modes

**Mode 1: Initial Deployment (`IS_INITIAL: true`)**

  - **Artifact Types**: `VariableLibrary`, `Lakehouse` only
  - **Manifest Location**: `local` (from repository)
  - **DDL Scripts**: Skipped (not yet available)
  - **Use Case**: First-time environment setup

**Mode 2: Full Deployment (`IS_INITIAL: false`)**

  - **Artifact Types**: All types (notebooks, warehouses, data pipelines, semantic models, etc.)
  - **Manifest Location**: `config_lakehouse` (from Fabric lakehouse)
  - **DDL Scripts**: Runs (optional via `RUN_DDL_SCRIPTS` variable)
  - **Use Case**: Regular deployments and promotions

#### How It Works

The pipeline uses bash conditional logic to switch modes:

```bash
if [ "$IS_INITIAL" = "true" ]; then
  export WORKSPACE_MANIFEST_LOCATION="local"
  export ITEM_TYPES_TO_DEPLOY="VariableLibrary,Lakehouse"
else
  export WORKSPACE_MANIFEST_LOCATION="config_lakehouse"
  export ITEM_TYPES_TO_DEPLOY=""  # Empty = deploy ALL types
fi
```

## Pipeline Architecture

All three scripts follow a multi-stage deployment pattern:

### Stage Structure

Each pipeline contains multiple stages (DEV, TST) that can be extended for additional environments (UAT, PROD):

```yaml
stages:
- stage: 'DEV'
  displayName: 'DEV'
  variables:
  - group: DP-Development-Lib
  
- stage: 'TST'
  displayName: 'TST'
  variables:
  - group: DP-Test-Lib
```

### Deployment Steps

Each stage executes the following steps in sequence:

#### 1. Install Ingenious
```bash
pip install git+https://github.com/Insight-Services-APAC/Insight_Ingenious_For_Fabric.git
```
Installs the latest version of the Ingenious CLI from GitHub.

#### 2. Deploy Fabric Artifacts
```bash
ingen_fab deploy deploy
```
Creates or updates Fabric items (lakehouses, warehouses, notebooks, etc.) based on the project repository structure and environment configuration.

##### Optional: Deploy Workspace Security (Security as Code)

`ingen_fab deploy deploy` also reads an optional file at `fabric_config/security_config.yaml` and applies workspace access role assignments.

Supported roles:

- `admins`
- `members`
- `contributors`
- `viewers`

Each role supports both `users` and `groups` lists.

```yaml
workspace_access:
  admins:
    users: []
    groups: []
  members:
    users: []
    groups: []
  contributors:
    users: []
    groups: []
  viewers:
    users: []
    groups: []
```

Use Microsoft Entra object IDs in these lists. Existing role assignments are preserved; only missing assignments are created.

#### 3. Update Workspace Variables
```bash
ingen_fab init workspace --workspace-name <workspace_name>
```
Synchronizes variable library values to the target workspace, ensuring environment-specific configurations are applied.

#### 4. Upload Python Libraries
```bash
ingen_fab deploy upload-python-libs
```
Uploads custom Python libraries from `python_libs/` to the Fabric lakehouse for use in notebooks and Spark jobs.

#### 5. Commit Changes to Repository
```bash
git commit -m "Automated file update from pipeline"
git push origin HEAD:$(Build.SourceBranchName)
```
Commits any auto-generated files (e.g., updated variable library configurations) back to the repository.

#### 6. Run DDL Orchestration (dp_combined.yml only)
**Condition**: Only runs when **BOTH** conditions are met:
  - `IS_INITIAL: false` (not an initial deployment) **AND**
  - `RUN_DDL_SCRIPTS: true` (DDL orchestration is enabled)

Automatically executes the `00_all_lakehouses_orchestrator_ddl_scripts` notebook to run data definition language (DDL) scripts:
- Retrieves Fabric API access token using Azure CLI
- Fetches workspace ID from environment configuration
- Locates the orchestrator notebook in the target workspace
- Executes the notebook via Fabric API to apply DDL changes

This step is **skipped** for initial deployments (`IS_INITIAL: true`) and can be disabled by setting `RUN_DDL_SCRIPTS: false`.

## Environment Variables

Each deployment task uses environment variables to control behavior:

| Variable | Purpose | Example Values | Used By |
|----------|---------|----------------|----------|
| `FABRIC_ENVIRONMENT` | Target environment name | `development`, `test`, `production` | All pipelines |
| `FABRIC_WORKSPACE_REPO_DIR` | Project directory in repository | `dp`, `sample_project` | All pipelines |
| `WORKSPACE_MANIFEST_LOCATION` | Where manifest file is stored | `local`, `config_lakehouse` | All pipelines |
| `IS_SINGLE_WORKSPACE` | Enable unattended workspace init | `Y`, `N` | All pipelines |
| `ITEM_TYPES_TO_DEPLOY` | Filter artifact types for deployment | `VariableLibrary,Lakehouse` (initial) or empty (full) | `dp_initial.yml`, `dp_combined.yml` |
| `IS_INITIAL` | Switch between initial and full deployment modes | `'true'` (initial), `'false'` (full) | `dp_combined.yml` only |
| `RUN_DDL_SCRIPTS` | Control DDL orchestration execution | `'true'` (run), `'false'` (skip) | `dp_combined.yml` only |
| `AUTO_UPDATE_ITEM_IDS` | Auto-update Item ID variables after deploy | `true`, `false` (default) |

These are typically configured via Azure DevOps Variable Groups.

### AUTO_UPDATE_ITEM_IDS

When set to `true`, automatically updates Item ID variables in the variable library after successful deployment:

- Queries workspace for Item IDs of deployed artifacts
- Updates variables using convention: `{artifact_name}_{type}_id`
- Only updates existing variables (no new variables created)
- Useful for single workspace deployments and CI/CD automation
- Complements `ingen_fab init workspace` which discovers all artifacts

**Recommended usage:**
- Enable for full deployments (`dp_full.yml`)
- Disable or omit for initial deployments (`dp_initial.yml`) where artifacts don't exist yet

## Prerequisites

### Fabric Configuration

Enable these tenant settings for the service principal:

- **Developer Settings** → Service principals can create workspaces, connections, and deployment pipelines
- **Developer Settings** → Service principals can call Fabric public APIs

### Fabric Workspace

Create target workspaces before running pipelines (e.g., `dp_dev`, `dp_tst`).

### Azure DevOps Configuration

!!! info "DevOps Environment Setup"
    For detailed instructions on setting up the Azure DevOps environment, service principals, and required configurations, see the [DevOps Configuration Guide](devops_configuration.md).

Configure the following in your Azure DevOps project:

| Resource Type | Example Name | Description |
|--------------|--------------|-------------|
| **Variable Group** | `DP-Development-Lib`, `DP-Test-Lib` | Contains environment-specific variables (`FABRIC_ENVIRONMENT`, `IS_SINGLE_WORKSPACE`, etc.) |
| **Environment** | `DP-Development-Env`, `DP-Test-Env` | Deployment targets (can include approvals and gates) |
| **Service Connection** | `dp_fabric_sc` | Azure CLI service connection using service principal with Fabric permissions |

### Repository Permissions

Grant the **Project Build Service** account **Contribute** permissions to the repository to allow the pipeline to commit variable library updates back to the repo.

## Customization

### Adding Stages

Extend pipelines with additional environments:

```yaml
- stage: 'UAT'
  displayName: 'UAT'
  variables:
  - group: DP-UAT-Lib
  jobs:
  - deployment: UAT_Deployment
    displayName: UAT Deployment
    environment:
      name: 'DP-UAT-Env'
    # ... (copy steps from DEV/TST)
```

### Modifying Steps

- **Change repository location**: Update `FABRIC_WORKSPACE_REPO_DIR` in variable groups
- **Control artifact types**: Set `ITEM_TYPES_TO_DEPLOY` for selective deployment
- **Skip Python libs upload**: Remove or comment out the upload-python-libs step
- **Add testing**: Insert test steps between deployment and commit stages

### Trigger Configuration

By default, pipelines use `trigger: none` (manual execution). To enable automatic triggers:

```yaml
trigger:
  branches:
    include:
    - main
    - develop
  paths:
    include:
    - fabric_workspace_items/*
```

## Pipeline Execution Flow

1. **Manual/Scheduled Trigger** → Pipeline starts
2. **Checkout** → Repository code retrieved with persistent credentials
3. **Python Setup** → Python 3.12 environment configured
4. **Install Ingenious** → CLI tools installed
5. **Deploy Artifacts** → Fabric items created/updated
6. **Initialize Workspace** → Variables synchronized
7. **Upload Libraries** → Python libs transferred to lakehouse
8. **Commit Results** → Auto-generated files pushed back to repo
9. **Repeat for next stage** → Process continues for TEST, UAT, PROD stages

## Troubleshooting

### Common Issues

**Permission Errors**
- Verify service principal has Fabric Admin or Contributor role on target workspace
- Check tenant settings are enabled (see Fabric Configuration above)

**Variable Group Not Found**
- Ensure variable groups exist and are linked to the pipeline
- Check variable group names match those in YAML

**Git Push Fails**
- Verify Project Build Service has Contribute permissions
- Check `persistCredentials: true` is set in checkout step

**Deployment Fails with "Item Already Exists"**
- Use `ingen_fab deploy deploy` with `--force` flag if needed (modify script)
- Check for naming conflicts in target workspace

## Best Practices

1. **Use Initial Script First**: Run `dp_initial.yml` before `dp_full.yml` for new environments
2. **Test in DEV**: Always validate changes in development before promoting
3. **Enable Approvals**: Configure approval gates in Azure DevOps Environments for production stages
4. **Monitor Pipeline Runs**: Review logs regularly to catch issues early
5. **Version Control**: Tag releases in Git to track what's deployed to each environment
6. **Separate Service Principals**: Use different service principals per environment for security isolation