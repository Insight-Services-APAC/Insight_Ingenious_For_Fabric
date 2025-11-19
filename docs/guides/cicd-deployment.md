# Deploy Guide (CI/CD)

[Home](../index.md) > [User Guide](../user_guide/index.md) > Deploy Guide (CI/CD)

Practical guide to deploy Fabric artifacts using Azure DevOps CI/CD pipelines.

## Overview

This guide explains how to use Azure DevOps pipelines to automate deployment of Ingenious projects to Microsoft Fabric workspaces. Two sample YAML pipeline scripts are provided to support different deployment scenarios.

## Sample Pipeline Scripts

Two sample YAML files are included under `ingen_fab/project_templates/deployment/`:

| Script | Purpose | Use Case |
|--------|---------|----------|
| `dp_initial.yml` | Initial deployment | First-time setup of lakehouses and variable libraries with limited artifact types |
| `dp_full.yml` | Full deployment | Complete deployment of all artifact types including notebooks, data pipelines, and semantic models |

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

## Pipeline Architecture

Both scripts follow a multi-stage deployment pattern:

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

## Environment Variables

Each deployment task uses environment variables to control behavior:

| Variable | Purpose | Example Values |
|----------|---------|----------------|
| `FABRIC_ENVIRONMENT` | Target environment name | `development`, `test`, `production` |
| `FABRIC_WORKSPACE_REPO_DIR` | Project directory in repository | `dp`, `sample_project` |
| `WORKSPACE_MANIFEST_LOCATION` | Where manifest file is stored | `local`, `config_lakehouse` |
| `IS_SINGLE_WORKSPACE` | Enable unattended workspace init | `Y`, `N` |
| `ITEM_TYPES_TO_DEPLOY` | Filter artifact types (initial only) | `VariableLibrary,Lakehouse` |

These are typically configured via Azure DevOps Variable Groups.

## Prerequisites

### Fabric Configuration

Enable these tenant settings for the service principal:

- **Developer Settings** → Service principals can create workspaces, connections, and deployment pipelines
- **Developer Settings** → Service principals can call Fabric public APIs

### Fabric Workspace

Create target workspaces before running pipelines (e.g., `dp_dev`, `dp_tst`).

### Azure DevOps Configuration

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