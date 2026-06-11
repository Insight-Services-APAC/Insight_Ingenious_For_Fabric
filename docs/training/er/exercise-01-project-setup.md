# Exercise 1 — Project Setup

[Home](../../index.md) > [Training](../index.md) > [ER Project](index.md) > Exercise 1

## Learning Objectives

By the end of this exercise you will be able to:

- Scaffold a new Enterprise Reporting project with `ingen_fab init new --with-er-samples`
- Configure the variable library with your workspace and resource GUIDs
- Compile DDL scripts into Fabric notebooks
- Deploy all artefacts to your Fabric workspace

## Prerequisites

Complete [Exercise 0 — Prerequisites](exercise-00-prerequisites.md) first.

## Background

The `--with-er-samples` flag initialises your project from the **Enterprise Reporting project template** — a supply chain / logistics data model with Bronze, Silver, Gold lakehouses and a Reporting warehouse pre-configured. This is different from `--with-samples` (which uses the DP cities/countries template).

## Steps

### 1. Initialise the project

```bash
ingen_fab init new --project-name my-er-project --with-er-samples
cd my-er-project
```

This creates a project with the supply chain / logistics DDL scripts, `storage_config.yaml`, and a pre-configured variable library stub.

### 2. Set environment variables

=== "macOS / Linux"

    ```bash
    export FABRIC_WORKSPACE_REPO_DIR="$(pwd)"
    export FABRIC_ENVIRONMENT="development"
    ```

=== "Windows (PowerShell)"

    ```powershell
    $env:FABRIC_WORKSPACE_REPO_DIR = (Get-Location).Path
    $env:FABRIC_ENVIRONMENT = "development"
    ```

### 3. Configure the variable library

Open `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json` and fill in your workspace values:

```json
{
  "name": "fabric_deployment_workspace_id",
  "value": "<your-workspace-id>"
},
{
  "name": "config_workspace_name",
  "value": "<your-workspace-name>"
},
{
  "name": "config_workspace_id",
  "value": "<your-config-workspace-id>"
}
```

!!! tip
    Your Workspace ID is the GUID in the URL when you open your workspace in the Fabric UI.

Lakehouse and Warehouse IDs will be populated automatically after the deploy step — see Step 6.

### 4. Review and generate storage artefacts

Review `fabric_config/storage_config.yaml`. The ER template is pre-configured with:

```yaml
storage:
  - lakehouse: local
    lh_bronze: lh_bronze
    lh_silver: lh_silver
    lh_gold: lh_gold

  - warehouses: local
    wh_reporting: wh_reporting
```

Generate the Fabric artefact folders and variable definitions:

```bash
ingen_fab init storage-config
```

### 5. Compile DDL notebooks

```bash
ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Lakehouse

ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Warehouse
```

This generates Fabric notebooks under `fabric_workspace_items/`. Each script folder becomes a notebook bundle, plus orchestrator notebooks per lakehouse/warehouse.

### 6. Deploy to Fabric

```bash
ingen_fab deploy deploy
```

This uploads all artefacts and creates the lakehouses (`lh_bronze`, `lh_silver`, `lh_gold`) and warehouse (`wh_reporting`) in your Fabric workspace.

??? failure "Deploy failed with `InvalidContent` or `ValueMismatch`?"

    This error occurs on first-time deploy because `development.json` still has placeholder GUIDs for lakehouses and warehouses.

    **Fix: deploy storage items first, then redeploy with the Variable Library.**

    **Step 6a — Deploy storage items only**

    === "macOS / Linux"

        ```bash
        rm -f platform_manifest_development.yml
        export ITEM_TYPES_TO_DEPLOY="Lakehouse,Warehouse,SQLDatabase,Notebook"
        ingen_fab deploy deploy
        ```

    === "Windows (PowerShell)"

        ```powershell
        Remove-Item -ErrorAction Ignore platform_manifest_development.yml
        $env:ITEM_TYPES_TO_DEPLOY = "Lakehouse,Warehouse,SQLDatabase,Notebook"
        ingen_fab deploy deploy
        ```

    **Step 6b — Populate GUIDs (Step 7 below), then redeploy**

    === "macOS / Linux"

        ```bash
        rm -f platform_manifest_development.yml
        unset ITEM_TYPES_TO_DEPLOY
        ingen_fab deploy deploy
        ```

    === "Windows (PowerShell)"

        ```powershell
        Remove-Item -ErrorAction Ignore platform_manifest_development.yml
        Remove-Item Env:\ITEM_TYPES_TO_DEPLOY -ErrorAction Ignore
        ingen_fab deploy deploy
        ```

### 7. Populate Lakehouse and Warehouse IDs

```bash
ingen_fab init workspace --workspace-name "<your-workspace-name>"
```

This automatically writes the IDs for `lh_bronze`, `lh_silver`, `lh_gold`, and `wh_reporting` into your variable library. Redeploy to push the updated variable library:

=== "macOS / Linux"

    ```bash
    rm -f platform_manifest_development.yml
    ingen_fab deploy deploy
    ```

=== "Windows (PowerShell)"

    ```powershell
    Remove-Item -ErrorAction Ignore platform_manifest_development.yml
    ingen_fab deploy deploy
    ```

### 8. Deploy Python libraries

```bash
ingen_fab deploy upload-python-libs
```

### 9. Run the DDL orchestrators

In your Fabric workspace, run:

1. `ddl_scripts/Lakehouses/00_all_lakehouses_orchestrator_ddl_scripts` — creates empty tables in `lh_bronze`, `lh_silver`, `lh_gold`
2. `ddl_scripts/Warehouses/00_all_warehouses_orchestrator_ddl_scripts` — creates the `Reporting` schema and views in `wh_reporting`

## Verification

After running both orchestrators, open your Fabric workspace and verify:

- `lh_bronze` Tables: `shipments`, `orders`, `products`, `suppliers`, `regions` (all empty at this stage)
- `lh_silver` Tables: same 5 tables (empty)
- `lh_gold` Tables: `fact_shipments`, `dim_product`, `dim_supplier`, `dim_region`, `dim_date` (empty)
- `wh_reporting` Views: `Reporting.vFact_Shipments`, `Reporting.vDim_Product`, `Reporting.vDim_Supplier`, `Reporting.vDim_Region`, `Reporting.vDim_Date`

---

---

[← Exercise 0 — Prerequisites](exercise-00-prerequisites.md) | [Exercise 2 — Generate Data →](exercise-02-generate-data.md)
