# Exercise 7 — Supply Chain Report

[Home](../../index.md) > [Training](../index.md) > [ER Project](index.md) > Exercise 7

## Learning Objectives

By the end of this exercise you will be able to:

- Create a Power BI report connected to the `sem_supply_chain` semantic model
- Build visuals for key supply chain KPIs
- Use slicers to filter by region, product category, supplier, and date
- Use IngenFab to download the report artefact for version control

## Steps

### 1. Create a new report

1. Open `sem_supply_chain` in your Fabric workspace
2. Click **Create report** → **Auto-create report** for a quick start, or **Start from scratch** for manual control

### 2. Build the report layout

Create a 3-page report:

---

#### Page 1 — Executive Summary

| Visual | Fields |
|---|---|
| **KPI card** | `[On-Time Delivery Rate %]` |
| **KPI card** | `[Total Shipment Value]` |
| **KPI card** | `[Late Shipments]` |
| **Bar chart** | X: `[Total Shipment Value]`, Y: `vDim_Region[zone]` |
| **Line chart** | X: `vDim_Date[month_name]`, Y: `[On-Time Delivery Rate %]` |

---

#### Page 2 — Delivery Performance

| Visual | Fields |
|---|---|
| **Scatter chart** | X: `[Avg Scheduled Days]`, Y: `[Avg Actual Days]`, Legend: `vDim_Supplier[region]` |
| **Table** | `vDim_Supplier[supplier_name]`, `[On-Time Delivery Rate %]`, `[Total Shipment Value]`, `[Avg Delay Days]` sorted by on-time rate |
| **Button Slicer** | `vDim_Date[year]` |
| **Button Slicer** | `vDim_Region[zone]` (destination) |

---

#### Page 3 — Product & Supplier Analysis

| Visual | Fields |
|---|---|
| **Treemap** | Group: `vDim_Product[category]`, Size: `[Total Shipment Value]` |
| **Bar chart** | X: `[Total Shipment Value]`, Y: `vDim_Product[sub_category]` |
| **Matrix** | Rows: `vDim_Supplier[supplier_name]`, Columns: `vDim_Product[category]`, Values: `[Total Shipment Value]` |
| **Slicer** | `vDim_Date[quarter]` |

### 3. Add cross-filter interactions

Cross-filtering lets users click a value in one visual to filter all related visuals on the same page. Configure this for each page:

1. Select a visual (e.g. the **Bar chart** on Page 1)
2. Go to **Format** tab → **Edit interactions**
3. For each other visual on the page, choose the interaction type:
    - **Filter** (funnel icon) — the selected visual filters this visual's data
    - **Highlight** (bar icon) — the selected visual highlights matching data
    - **None** (circle-slash icon) — no interaction
4. Repeat for each visual on the page. A good starting configuration:
    - **Page 1**: Bar chart and line chart cross-filter each other; KPI cards receive filters from both
    - **Page 2**: Slicers filter all visuals; scatter chart and table cross-filter each other
    - **Page 3**: Treemap and bar chart cross-filter each other; slicer filters all visuals

!!! tip
    Cross-filter is configured per page. Switch to each page and repeat the process.

### 4. Save and version-control

Click **Save**, name your report `rpt_supply_chain_overview`, and click **Share** to distribute to stakeholders.

Then use IngenFab to download the report artefact for version control:

=== "macOS / Linux"

    ```bash
    ingen_fab deploy download-artefact \
        --artefact-name rpt_supply_chain_overview \
        --artefact-type Report
    ```

=== "Windows (PowerShell)"

    ```powershell
    ingen_fab deploy download-artefact `
        --artefact-name rpt_supply_chain_overview `
        --artefact-type Report
    ```

This downloads the report definition into your `fabric_workspace_items/downloaded/Report/rpt_supply_chain_overview` directory alongside the semantic model from Exercise 6. Both can be committed to source control and redeployed to other environments.

## What IngenFab Automated

Looking back across Exercises 1–7, here is what IngenFab handled vs what you did manually:

| Layer | IngenFab Automated | You Did Manually |
|---|---|---|
| **Bronze** (Ex 1–2) | DDL scripts, table creation, synthetic data notebook | Configured variables, ran notebooks in Fabric |
| **Silver** (Ex 3) | DDL scripts, table schemas, deployment | Wrote transformation logic (PySpark notebook) |
| **Gold** (Ex 4) | Star schema DDL scripts (surrogate keys, FKs, types), deployment | Wrote transformation logic (PySpark notebook) |
| **Warehouse** (Ex 5) | T-SQL view scripts, schema creation, deployment | Verified and queried views |
| **Semantic Model** (Ex 6) | Version control via `download-artefact`, cross-env deployment | Created model, relationships, and DAX measures |
| **Report** (Ex 7) | Version control via `download-artefact`, cross-env deployment | Designed report pages and visuals |

In a production CI/CD pipeline, `ingen_fab deploy deploy` pushes **all** of these artefacts — DDL notebooks, warehouse scripts, semantic models, and reports — to any target environment in a single command.

## What You Have Built

Congratulations! You have completed the Enterprise Reporting tutorial. You now have:

✅ A fully deployed supply chain data platform on Microsoft Fabric  
✅ Realistic synthetic supply chain data in Bronze, Silver, and Gold lakehouses  
✅ A star schema optimised for analytics  
✅ T-SQL warehouse views ready for semantic model connectivity  
✅ A Fabric semantic model with 6 KPI measures  
✅ A 3-page Power BI supply chain report  

## Next Steps

- **Scale up data**: Re-run synthetic data with `supply_chain_star_large` for 1 million rows
- **Add incremental loads**: Use the IngenFab incremental data generation feature to simulate daily data arrivals
- **CI/CD**: Automate deployments using the IngenFab [deployment guide](../../guides/cicd-deployment.md)
- **Custom DDL**: Add new tables or change the schema using the [DDL scripts guide](../../developer_guide/ddl_scripts.md)

---

[← Exercise 6 — Semantic Model](exercise-06-semantic-model.md) | [← Back to Training Index](../index.md) | [← Back to ER Tutorial Index](index.md)
