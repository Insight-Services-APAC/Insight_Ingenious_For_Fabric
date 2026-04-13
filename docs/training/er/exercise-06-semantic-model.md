# Exercise 6 — Semantic Model

[Home](../../index.md) > [Training](../index.md) > [ER Project](index.md) > Exercise 6

## Learning Objectives

By the end of this exercise you will be able to:

- Create a Fabric semantic model connected to `wh_reporting`
- Define relationships between the fact and dimension tables
- Add calculated measures for supply chain KPIs
- Use IngenFab to download the semantic model artefact for version control

## Background

The semantic model is the **business layer** that sits between the warehouse views (created by IngenFab in Exercise 1, verified in Exercise 5) and the Power BI reports you will build in Exercise 7. It defines relationships, measures, and friendly names so that every report built against it uses the same logic.

Semantic models are created in the Fabric UI — IngenFab's role is to **version-control** them after creation using `deploy download-artefact`, and to **redeploy** them to other environments using `deploy deploy`.

## Steps

### 1. Create a new semantic model

!!! warning "Create from the warehouse, not from + New item"
    Creating a semantic model via **+ New item → Semantic model** launches the **Direct Lake** flow, which connects to OneLake parquet files and **cannot include SQL views**. You must create the semantic model from inside `wh_reporting` to get a DirectQuery model that supports views.

1. In your Fabric workspace, open **`wh_reporting`**
2. In the warehouse toolbar, click **New semantic model**
3. Name it `sem_supply_chain`
4. You will now see a table/view selection dialog scoped to `wh_reporting`

### 2. Add tables

Select all 5 views from the `Reporting` schema:
- `vFact_Shipments`
- `vDim_Product`
- `vDim_Supplier`
- `vDim_Region`
- `vDim_Date`

Click **Confirm**.

### 3. Define relationships

In the model diagram, create the following relationships (all are Many-to-One from fact to dimension):

| From (Fact) | To (Dimension) | Join Columns |
|---|---|---|
| `vFact_Shipments.product_key` | `vDim_Product.product_key` | Many → One |
| `vFact_Shipments.supplier_key` | `vDim_Supplier.supplier_key` | Many → One |
| `vFact_Shipments.destination_region_key` | `vDim_Region.region_key` | Many → One |
| `vFact_Shipments.date_key` | `vDim_Date.date_key` | Many → One |

!!! note "Origin region"
    The `origin_region_key` column can be related to a second copy of `vDim_Region` (role-playing dimension) — this is optional for the tutorial.

### 4. Add DAX measures

In the semantic model editor, add the following measures to `vFact_Shipments`:

```dax
On-Time Delivery Rate % = 
DIVIDE(
    COUNTROWS(FILTER(vFact_Shipments, vFact_Shipments[is_on_time] = TRUE())),
    COUNTROWS(vFact_Shipments),
    0
) * 100
```
```dax
Total Shipment Value = SUM(vFact_Shipments[total_cost])
```
```dax
Avg Actual Days = AVERAGE(vFact_Shipments[actual_days])
```
```dax
Avg Scheduled Days = AVERAGE(vFact_Shipments[scheduled_days])
```
```dax
Late Shipments = 
COUNTROWS(FILTER(vFact_Shipments, vFact_Shipments[is_on_time] = FALSE()))
```
```dax
Avg Delay Days = 
AVERAGEX(
    FILTER(vFact_Shipments, vFact_Shipments[is_on_time] = FALSE()),
    vFact_Shipments[actual_days] - vFact_Shipments[scheduled_days]
)
```

### 5. Save the semantic model

Click **Save** and then **Publish**.

### 6. Version-control with IngenFab

Now that the semantic model is created in Fabric, use IngenFab to download it into your project for version control:

=== "macOS / Linux"

    ```bash
    ingen_fab deploy download-artefact \
        --artefact-name sem_supply_chain \
        --artefact-type SemanticModel
    ```

=== "Windows (PowerShell)"

    ```powershell
    ingen_fab deploy download-artefact `
        --artefact-name sem_supply_chain `
        --artefact-type SemanticModel
    ```

This downloads the semantic model definition into your `fabric_workspace_items/downloaded/SemanticModel/sem_supply_chain.SemanticModel` directory. From here it can be committed to source control and redeployed to other environments (test, production) using `ingen_fab deploy deploy`.

!!! tip "Redeployment across environments"
    Once the semantic model artefact is in your project, IngenFab treats it like any other workspace item. When you run `ingen_fab deploy deploy` against a different environment, the semantic model is created there automatically — no manual Fabric UI steps needed.

## Verification

- The semantic model should show 5 tables with relationships drawn between them
- All 6 measures should appear under `vFact_Shipments`
- Try a quick test: create a simple table visual with `vDim_Region[zone]` and `[Total Shipment Value]`

---

[← Exercise 5 — Warehouse Views](exercise-05-warehouse-views.md) | [Exercise 7 — Report →](exercise-07-report.md)
