# Exercise 5 — Semantic Model

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 5

## Learning Objectives

By the end of this exercise you will be able to:

- Create a Fabric semantic model over Warehouse views using Direct Lake on SQL mode
- Define table relationships in the semantic model
- Add basic measures using DAX
- Publish the semantic model for use by Power BI reports

## Prerequisites

- [Exercise 4](exercise-04-gold-layer.md) completed — `DW.vDim_Cities` and `DW.vDim_CityGeography` views exist in `wh_gold`

## Background

A **semantic model** (formerly "dataset" in Power BI) is a reusable, governed layer that sits between your data and your reports. In Fabric, the **Direct Lake on SQL** storage mode lets the semantic model read data directly via the Warehouse's SQL analytics endpoint — no import step required, and SQL views are fully supported.

The DP project's `wh_gold` Warehouse exposes the gold layer through views in the `DW` schema, making it an ideal source for a semantic model.

## Steps

### 0. Confirm Gold LH and WH have tables and views

Before creating the semantic model, verify that the gold layer infrastructure is in place:

1. In the Fabric UI, navigate to your workspace
2. Open `lh_gold` Lakehouse:
   - Confirm the **Tables** section shows `Gold.Dim_Cities` and `Gold.Dim_CityGeography`
3. Open `wh_gold` Warehouse:
   - Confirm the **DW** schema contains views `vDim_Cities` and `vDim_CityGeography`
   - If views are missing, rerun the deployment: `ingen_fab deploy deploy` from your project root

### 1. Open the Warehouse and create a semantic model

1. In the Fabric UI, navigate to your workspace
2. Open the `wh_gold` **Warehouse**
3. From the warehouse ribbon, select **New semantic model**
4. In the **New semantic model** dialog:
   - In the **Direct Lake semantic model name** box, enter `sm_dp_geography`
   - For **Storage mode**, select **Direct Lake on SQL**
   - Expand the **DW** schema and check:
     - `vDim_Cities`
     - `vDim_CityGeography`
5. Select **Confirm**

!!! warning
    Adding views to your semantic model may slow down report performance, as querying views is typically slower than querying tables. For this project the gold views are lightweight and performance is acceptable.

### 2. Define relationships

After clicking **Confirm**, you'll be taken to the `sm_dp_geography` semantic model overview page showing `vDim_Cities` and `vDim_CityGeography` listed as tables.

1. Click **Open** in the top-left to launch the model designer
2. You'll see both views laid out as table cards on the canvas with their columns listed
3. In the top-right corner, click the **Viewing** dropdown and switch to **Editing** mode — ribbon controls like **Manage relationships** are greyed out until you do this
4. In the ribbon, select **Manage relationships** → **New relationship**
4. Configure the relationship:

| From table | From column | To table | To column | Cardinality | Cross filter direction |
|-----------|-------------|----------|-----------|-------------|----------------------|
| `vDim_CityGeography` | `CityID` | `vDim_Cities` | `CityID` | Many-to-one | Single |

5. Select **Save**, then **Close**

### 3. Add measures

Measures are calculations defined in DAX that appear in reports. You'll add them to the `vDim_Cities` table.

1. In the **Data** panel on the right, click on `vDim_Cities` to select it
2. In the ribbon, under **Calculations**, click **New measure**
3. A formula bar will appear at the top of the canvas — replace the default text with:

```dax
Total Population = SUM(vDim_Cities[LatestRecordedPopulation])
```

4. Press **Enter** or click the checkmark ✓ to confirm
5. Repeat steps 2–4 to add a second measure:

```dax
City Count = COUNTROWS(vDim_Cities)
```

Both measures will appear under `vDim_Cities` in the **Data** panel, marked with a calculator icon.

### 4. Save

In the model designer, go to **File** (top-left menu) → **Save**, or press `Ctrl+S` / `Cmd+S`. The semantic model is now available for Power BI reports in the same workspace.

## Verification — Part A (Fabric UI)

1. The `sm_dp_geography` semantic model overview page shows both `vDim_Cities` and `vDim_CityGeography` listed with type **Table**
2. Click **Open** → in the model designer, the relationship between the two views is visible
3. Click **Explore** in the ribbon → a card visual using `Total Population` aggregates the summed population correctly

---

## Part B — Version-Control & Deploy with IngenFab *(Optional)*

Now that the semantic model exists in Fabric, bring it under source control so it can be promoted across environments.

### 5. Download the semantic model definition

From your project root (where `ingen_fab.yml` lives):

```bash
ingen_fab deploy download-artefact \
  -n "sm_dp_geography" \
  -t SemanticModel
```

This downloads the definition into `fabric_workspace_items/downloaded/SemanticModel/sm_dp_geography/`.

!!! note
    Semantic models are downloaded as **TMDL** (Tabular Model Definition Language) — a human-readable format for editing model metadata, relationships, and measures.

### 6. Inspect the downloaded files

```bash
find fabric_workspace_items/downloaded/SemanticModel/sm_dp_geography -type f
```

You should see:

```
fabric_workspace_items/downloaded/SemanticModel/sm_dp_geography/
├── definition/
│   ├── model.tmdl               ← model-level settings
│   ├── relationships.tmdl       ← relationship definitions
│   └── tables/
│       ├── vDim_Cities.tmdl     ← table schema, measures
│       └── vDim_CityGeography.tmdl
└── .platform                    ← Fabric item metadata
```

Open `.platform` — it contains the item type, display name, and a unique `logicalId`:

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
  "metadata": {
    "type": "SemanticModel",
    "displayName": "sm_dp_geography"
  },
  "config": {
    "version": "2.0",
    "logicalId": "a3f7c1d2-..."  
  }
}
```

!!! warning "Check your logicalId"
    The `logicalId` must be a **unique UUID**. If it shows `00000000-0000-0000-0000-000000000000`, generate a real one before deploying:
    ```bash
    python3 -c "import uuid; print(uuid.uuid4())"
    ```
    Then replace the zero-GUID in `.platform`.

### 7. Move into the deployable location

The `downloaded/` folder is a **staging area** — items left there will cause duplicate-item errors during deployment. Move the item to its final location and remove the staging copy:

```bash
mkdir -p fabric_workspace_items/SemanticModel
cp -r fabric_workspace_items/downloaded/SemanticModel/sm_dp_geography \
      fabric_workspace_items/SemanticModel/

# Remove the staging copy to prevent duplicates during deploy
rm -rf fabric_workspace_items/downloaded/SemanticModel/sm_dp_geography
```

!!! warning "Always remove the staging copy"
    `ingen_fab deploy deploy` scans the **entire** `fabric_workspace_items/` directory — including `downloaded/`. If the same item exists in both locations, deployment will fail with `Duplicate logicalId` errors.

Your project structure should now look like:

```
your-project/
├── fabric_workspace_items/
│   ├── SemanticModel/
│   │   └── sm_dp_geography/
│   │       ├── definition/
│   │       └── .platform
│   ├── downloaded/                  ← should be empty for this type
│   └── ...
└── ingen_fab.yml
```

### 8. Deploy to verify the round-trip

Run a deployment to confirm the semantic model is published correctly:

```bash
ingen_fab deploy deploy
```

After deployment, verify `sm_dp_geography` still works in the Fabric UI by opening the model designer and checking that relationships and measures are intact.

## Verification — Part B (IngenFab) *(Optional)*

- `fabric_workspace_items/SemanticModel/sm_dp_geography/definition/` exists locally with `.tmdl` files
- The `.platform` file contains a valid non-zero `logicalId`
- `git log --oneline -1` shows your commit with the semantic model files
- `ingen_fab deploy deploy` reports the semantic model was published successfully
- The model still shows relationships and measures when opened in the Fabric UI after redeployment

## Notes

- **Direct Lake on SQL** reads data via the Warehouse SQL analytics endpoint, supporting both tables and views
- **Direct Lake on OneLake** reads Parquet files directly but only supports tables — use this when views are not required
- **TMDL format** is human-readable and git-friendly — you can edit relationships and measures directly in the `.tmdl` files
- Semantic models can reference multiple warehouses or lakehouses — edit `model.tmdl` to add additional data sources
- Part B is optional but **strongly recommended** for production workflows where models need to be promoted across environments

---

← [Exercise 4 — Gold Layer](exercise-04-gold-layer.md) | **Next:** [Exercise 6 — Power BI Report →](exercise-06-report.md)
