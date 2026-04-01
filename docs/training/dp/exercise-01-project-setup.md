# Exercise 1 — Project Setup

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 1

## Learning Objectives

By the end of this exercise you will be able to:

- Initialise a new Fabric project using `ingen_fab init new --with-samples`
- Configure environment variables and the variable library
- Compile DDL scripts into Fabric notebooks
- Deploy all artefacts to a Fabric workspace
- Verify that bronze tables are populated with sample data

## Prerequisites

Complete [Exercise 0 — Prerequisites](exercise-00-prerequisites.md) before starting. You should have the tool installed, Azure authentication working, your Fabric workspace resources created, and all GUIDs noted.

## Background

The accelerator uses a two-step workflow: first you **compile** DDL Python/SQL scripts into Fabric notebooks, then you **deploy** those notebooks (and all other workspace artefacts) to your Fabric workspace. This keeps your source scripts human-readable and version-controlled while producing the correct Fabric artefact format on demand.

## Steps

### 1. Initialise the project

```bash
ingen_fab init new --project-name my-dp-project --with-samples
cd my-dp-project
```

The `--with-samples` flag copies the full DP project template, including bronze bootstrap scripts for cities, countries, and state_provinces.

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

Lakehouse and Warehouse IDs will be populated automatically **after** the deploy step — see Step 6 below.

### 4. Review and generate storage artefacts

Open `fabric_config/storage_config.yaml`. For the sample project, the defaults are already set correctly:

```yaml
storage:
  - lakehouse: local
    lh_bronze: lh_bronze
    lh_silver: lh_silver
    lh_gold: lh_gold

  - warehouses: local
    wh_gold: wh_gold
```

!!! tip "Customising resource names"
    If you want different names, change the values here (e.g. `lh_bronze: my_bronze`). The left side is the variable key used in the project; the right side is the actual name that will be created in Fabric.

Now generate the Fabric artefact folders and variable definitions:

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

This generates individual DDL notebooks and orchestrator notebooks under `fabric_workspace_items/`.

### 6. Deploy to Fabric

```bash
ingen_fab deploy deploy
```

This uploads the variable library, all generated notebooks, and **creates the Lakehouses and Warehouse** (`lh_bronze`, `lh_silver`, `lh_gold`, `wh_gold`) in your Fabric workspace.

??? failure "Deploy failed with `InvalidContent` or `ValueMismatch`?"

    This error occurs on a first-time deploy because `development.json` still contains
    `REPLACE_WITH_*` placeholder strings for lakehouse, warehouse, and SQL Database IDs.
    Fabric's Variable Library schema validation rejects non-GUID values and aborts the
    entire publish pipeline — so no other items are created either.

    **Fix: deploy storage items first, then redeploy with the Variable Library once GUIDs are populated.**

    **Step 6a — Deploy storage items only (skip the Variable Library)**

    Delete any cached manifest, then restrict the deploy to storage artefacts:

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

    All lakehouses, warehouses, SQL databases, and notebooks will deploy. Continue to
    **Step 7** to populate the GUIDs, then return here for Step 6b.

    **Step 6b — Verify `variables.json` and `development.json` are in sync**

    A mismatch between the variable *definitions* (`variables.json`) and the value overrides
    (`development.json`) will also cause a `ValueMismatch` failure. Run the following diff to
    check:

    ```bash
    diff \
      <(cat fabric_workspace_items/config/var_lib.VariableLibrary/variables.json \
        | python3 -c "import json,sys; d=json.load(sys.stdin); [print(v['name']) for v in d['variables']]") \
      <(cat fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json \
        | python3 -c "import json,sys; d=json.load(sys.stdin); [print(v['name']) for v in d['variableOverrides']]")
    ```

    Expected output: *(no output — an empty diff means the files are in sync)*

    If there are lines shown, add the missing entries to `variables.json`:

    ```json
    { "name": "<missing-variable-name>", "type": "String", "value": "" }
    ```

    **Step 6c — Full redeploy (includes Variable Library)**

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

    All items — including the Variable Library — should now deploy successfully.

### 7. Populate Lakehouse and Warehouse IDs

Now that the resources exist in Fabric, run the following to automatically write their IDs into your variable library:

```bash
ingen_fab init workspace --workspace-name "<your-workspace-name>"
```

!!! note "Assumes default resource names"
    This command matches resources by name. It expects Lakehouses and Warehouses named exactly `lh_bronze`, `lh_silver`, `lh_gold`, and `wh_gold`.

    **If your resources have different names**, update the name variables manually in `valueSets/development.json` after running the command:

    ```json
    { "name": "lh_bronze_lakehouse_name", "value": "<your-actual-lakehouse-name>" },
    { "name": "lh_silver_lakehouse_name", "value": "<your-actual-lakehouse-name>" },
    { "name": "lh_gold_lakehouse_name",   "value": "<your-actual-lakehouse-name>" },
    { "name": "wh_gold_warehouse_name",   "value": "<your-actual-warehouse-name>" }
    ```

    The corresponding `_id` values will also need to be updated to match.

Now redeploy to push the updated variable library (with the real GUIDs) to Fabric:

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

The DDL notebooks depend on Python libraries bundled with the accelerator. Upload them to your Fabric workspace so the notebooks can import them at runtime:

=== "macOS / Linux"

    ```bash
    ingen_fab deploy upload-python-libs
    ```

=== "Windows (PowerShell)"

    ```powershell
    ingen_fab deploy upload-python-libs
    ```

This uploads the packaged `.whl` files to the `config` lakehouse in your workspace. The DDL orchestrator notebooks reference these libraries; without this step the notebooks will fail at import time.

### 9. Review deployment and verify Variable Library

Open your Fabric workspace in the browser and verify all artefacts deployed correctly:

1. Navigate to the **`config`** folder
2. Confirm the **`var_lib`** VariableLibrary item exists
3. Verify your lakehouses (`lh_bronze`, `lh_silver`, `lh_gold`) and warehouse (`wh_gold`) are visible
4. Check that DDL notebooks appear in the `ddl_scripts` folder

**Expected workspace structure:**

```
<your-workspace-name>/
├── config/
│   ├── config (Lakehouse)
│   │   └── config (SQL analytics endpoint)
│   └── var_lib (VariableLibrary)
├── ddl_scripts/
│   ├── Lakehouses/
│   │   ├── 00_all_lakehouses_orchestrator_ddl_scripts (Notebook)
│   │   ├── 00_orchestrator_lh_silver_lakehouse_ddl_scripts (Notebook)
│   │   ├── 00_orchestrator_lh_gold_lakehouse_ddl_scripts (Notebook)
│   │   └── lh_bronze/
│   │   |   ├── 00_orchestrator_lh_bronze_lakehouse_ddl_scripts (Notebook)
│   │   |   ├── 001_application_cities_table_create (Notebook)
│   │   |   └── 002_application_countries_table_create (Notebook)
│   │   └── lh_silver/
│   │   |   ├── 00_orchestrator_lh_silver_lakehouse_ddl_scripts (Notebook)
│   │   |   └── 001_Initial_Creation_lh_silver_Lakehouses_ddl_scripts (Notebook)
│   │   └── lh_gold/
│   │       ├── 00_orchestrator_lh_gold_lakehouse_ddl_scripts (Notebook)
│   │       └── 001_Initial_Creation_lh_gold_Lakehouses_ddl_scripts (Notebook)
│   └── Warehouses/
│       ├── 00_all_warehouses_orchestrator_ddl_scripts (Notebook)
│       └── wh_gold/
│           ├── 00_orchestrator_wh_gold_warehouse_ddl_scripts (Notebook)
│           └── 001_DW_vDim_Cities_view_create (Notebook)
├── lakehouses/
│   ├── lh_bronze (Lakehouse)
│   │   └── lh_bronze (SQL analytics endpoint)
│   ├── lh_silver (Lakehouse)
│   │   └── lh_silver (SQL analytics endpoint)
│   └── lh_gold (Lakehouse)
│       └── lh_gold (SQL analytics endpoint)
└── warehouses/
    └── wh_gold (Warehouse)
```

??? failure "Variable Library (`var_lib`) missing or failed to deploy?"

    If the Variable Library is missing from the `config` folder, the deployment may have failed due to a GUID mismatch.

    **Fix: Update the logicalId in `.platform`**

    The `logicalId` in `.platform` must match the resource GUID in Fabric. If you're re-deploying to an existing workspace with a different Variable Library instance:

    1. Get the existing Variable Library GUID from the Fabric UI (in the URL when viewing the item, or create a new one if none exists)
    2. Open `fabric_workspace_items/config/var_lib.VariableLibrary/.platform`
    3. Update the `logicalId` value:

        ```json
        {
          "config": {
            "version": "2.0",
            "logicalId": "<your-variable-library-guid>"
          }
        }
        ```

    4. Redeploy:

        ```bash
        rm -f platform_manifest_development.yml
        ingen_fab deploy deploy
        ```

    5. Verify the `var_lib` item now appears in the `config` folder

### 10. Run the bronze orchestrator in Fabric

This notebook was generated by the compile step and deployed in Step 6. You do not edit it locally — just run it in Fabric.

**Where to find it:**

- **Locally** (read-only, auto-generated):  
  `fabric_workspace_items/ddl_scripts/Lakehouses/00_all_lakehouses_orchestrator.Notebook/notebook-content.py`

- **In the Fabric workspace** (after deploy):  
  Open your workspace → `ddl_scripts/Lakehouses` folder → **`00_all_lakehouses_orchestrator_ddl_scripts`**

**To run it:**

1. Open your Fabric workspace in the browser
2. Navigate to the **`ddl_scripts/Lakehouses`** folder
3. Open **`00_all_lakehouses_orchestrator_ddl_scripts`** and click **Run all**

This executes the bronze DDL scripts in order, creating and seeding:

| Table | Rows |
|-------|------|
| `lh_bronze.cities` | 10 |
| `lh_bronze.countries` | 10 |

### 11. Run the warehouse orchestrator

Similarly, find **`00_all_warehouses_orchestrator`** in the `ddl_scripts` folder of your Fabric workspace and run it. This creates the `DW` schema and the `vDim_Cities` view in `wh_gold`.

## Verification

After running both orchestrators, confirm the tables exist:

1. In the Fabric UI, open `lh_bronze` → **Tables** — you should see `cities` and `countries`.
2. Open a new notebook **from within `lh_bronze`** so it is automatically attached as the default lakehouse:
    1. With `lh_bronze` open in the Fabric UI, click **Open notebook** in the top ribbon → **New notebook**
    2. The notebook opens with `lh_bronze` already listed as the default lakehouse in the Explorer — no manual attachment needed.

3. Run the following in a cell:

    ```python
    display(spark.read.table("cities"))
    display(spark.read.table("countries"))
    ```

4. Each table should return 10 rows.

---

**Next:** [Exercise 2 — New Bronze Table →](exercise-02-bronze-table.md)
