# Exercise 5 — Warehouse Views

[Home](../../index.md) > [Training](../index.md) > [ER Project](index.md) > Exercise 5

## Learning Objectives

By the end of this exercise you will be able to:

- Review the T-SQL DDL scripts that IngenFab generated for `wh_reporting`
- Explain why warehouse views form the reporting contract between data engineers and analysts
- Use `ingen_fab deploy get-metadata` to inspect the warehouse schema
- Verify data flows correctly from `lh_gold` through the views
- (Optional) Extend the reporting layer by adding a new DDL script and recompiling

## Background

### Why Warehouse Views?

Microsoft Fabric semantic models work best against a **Warehouse** with T-SQL views rather than directly against Lakehouse Delta tables. This pattern provides three key benefits:

1. **Stable reporting contract** — Analysts build semantic models and reports against named views. Data engineers can refactor the underlying gold tables without breaking downstream consumers, as long as the view signatures remain the same.
2. **Type safety** — Views cast columns to precise SQL types (e.g. `DECIMAL(14,2)` for currency, `BIT` for booleans), ensuring consistent behaviour in DAX measures and Power BI visuals.
3. **Security boundary** — Views can expose only the columns needed for reporting, and row-level security can be layered on top.

### What IngenFab Generated

When you ran `ingen_fab ddl compile --generation-mode Warehouse` in Exercise 1, IngenFab compiled 6 SQL scripts into Fabric DDL notebooks. The DDL orchestrator then created the `Reporting` schema and 5 views in `wh_reporting`:

| DDL Script | View Created | Source Table | Purpose |
|---|---|---|---|
| `001_create_reporting_schema.sql` | — | — | Creates the `Reporting` schema |
| `002_vFact_Shipments.sql` | `Reporting.vFact_Shipments` | `lh_gold.dbo.fact_shipments` | Shipment metrics with `DECIMAL` casts for cost columns and `BIT` cast for `is_on_time` |
| `003_vDim_Product.sql` | `Reporting.vDim_Product` | `lh_gold.dbo.dim_product` | Product hierarchy (category → sub-category) with `DECIMAL` weight |
| `004_vDim_Supplier.sql` | `Reporting.vDim_Supplier` | `lh_gold.dbo.dim_supplier` | Supplier details with `DECIMAL` performance rating |
| `005_vDim_Region.sql` | `Reporting.vDim_Region` | `lh_gold.dbo.dim_region` | Region and zone geography |
| `006_vDim_Date.sql` | `Reporting.vDim_Date` | `lh_gold.dbo.dim_date` | Calendar dimension (year, quarter, month, week) |

## Steps

### 1. Review the IngenFab-generated warehouse DDL scripts

The warehouse SQL scripts live under:

```
ddl_scripts/Warehouses/wh_reporting/001_Initial_Creation/
├── 001_create_reporting_schema.sql
├── 002_vFact_Shipments.sql
├── 003_vDim_Product.sql
├── 004_vDim_Supplier.sql
├── 005_vDim_Region.sql
└── 006_vDim_Date.sql
```

Open `001_create_reporting_schema.sql` to see how IngenFab handles schema creation:

```sql
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Reporting')
BEGIN
    EXEC('CREATE SCHEMA [Reporting]')
END
```

Now open `002_vFact_Shipments.sql` to see the main fact view:

```sql
CREATE OR ALTER VIEW Reporting.vFact_Shipments
AS
SELECT
    shipment_key,
    shipment_id,
    order_id,
    product_key,
    supplier_key,
    origin_region_key,
    destination_region_key,
    date_key,
    quantity,
    CAST(unit_cost AS DECIMAL(12, 2))   AS unit_cost,
    CAST(freight_cost AS DECIMAL(12, 2)) AS freight_cost,
    CAST(total_cost AS DECIMAL(14, 2))  AS total_cost,
    scheduled_days,
    actual_days,
    CAST(is_on_time AS BIT)             AS is_on_time,
    status,
    priority
FROM [lh_gold].[dbo].[fact_shipments]
```

!!! info "What IngenFab handles in these scripts"
    Notice the patterns:

    - **Idempotent schema creation** — the `IF NOT EXISTS` guard means the script can run repeatedly without error
    - **`CREATE OR ALTER VIEW`** — views are safely re-creatable, enabling iterative development
    - **Explicit type casts** — `DECIMAL` for monetary values and `BIT` for booleans ensure Power BI interprets columns correctly
    - **Cross-lakehouse references** — views select from `[lh_gold].[dbo].[...]`, connecting the warehouse to the lakehouse gold layer

    IngenFab compiled each of these `.sql` files into a Fabric notebook using `ingen_fab ddl compile --generation-mode Warehouse`. The DDL orchestrator then ran them in sequence.

### 2. Inspect the warehouse schema with IngenFab

Use `deploy get-metadata` to inspect the deployed warehouse schema:

=== "macOS / Linux"

    ```bash
    ingen_fab deploy get-metadata \
        --target warehouse \
        --warehouse-name wh_reporting \
        --format table
    ```

=== "Windows (PowerShell)"

    ```powershell
    ingen_fab deploy get-metadata `
        --target warehouse `
        --warehouse-name wh_reporting `
        --format table
    ```

!!! info "Auto-ODBC fallback"
    Unlike Lakehouses, Fabric Warehouses do not appear in the standard SQL endpoint list returned by the Fabric REST API — they expose their connection string through a separate warehouse-specific endpoint. IngenFab automatically detects this and falls back to an ODBC connection using the warehouse's connection string, so no extra flags are required.

    If the fallback is triggered you will see a yellow warning line such as:

    ```
    REST SQL endpoint lookup failed (...); falling back to ODBC using warehouse connection string.
    ```

    This is expected and the metadata query will still succeed.

You should see the `Reporting` schema with all 5 views and their column definitions.

### 3. Verify the views exist

In your Fabric workspace, open `wh_reporting` and run the following T-SQL query in the SQL editor:

```sql
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'Reporting'
ORDER BY TABLE_NAME;
```

Expected output:

| TABLE_SCHEMA | TABLE_NAME |
|---|---|
| Reporting | vDim_Date |
| Reporting | vDim_Product |
| Reporting | vDim_Region |
| Reporting | vDim_Supplier |
| Reporting | vFact_Shipments |

### 4. Inspect a view definition

Pick any view and inspect its definition to see the type casts and column mappings:

```sql
SELECT OBJECT_DEFINITION(OBJECT_ID('Reporting.vFact_Shipments'));
```

Compare the output against `002_vFact_Shipments.sql` in your project — they should match. This is the benefit of IngenFab's DDL workflow: the SQL in your repo is the single source of truth for what runs in Fabric.

### 5. Query the views

!!! warning "Run these queries in `wh_reporting`, not `lh_gold`"

Run these queries to confirm data flows correctly from `lh_gold` through the views:

```sql
-- Row counts per view
SELECT 'vFact_Shipments' AS view_name, COUNT(*) AS row_count FROM Reporting.vFact_Shipments
UNION ALL
SELECT 'vDim_Product',   COUNT(*) FROM Reporting.vDim_Product
UNION ALL
SELECT 'vDim_Supplier',  COUNT(*) FROM Reporting.vDim_Supplier
UNION ALL
SELECT 'vDim_Region',    COUNT(*) FROM Reporting.vDim_Region
UNION ALL
SELECT 'vDim_Date',      COUNT(*) FROM Reporting.vDim_Date;
```

Expected row counts (with `supply_chain_star_small` dataset):

| view_name | row_count |
|---|---|
| vFact_Shipments | ~5,000 |
| vDim_Product | 200 |
| vDim_Supplier | 50 |
| vDim_Region | 20 |
| vDim_Date | 365 |

```sql
-- Spot-check: on-time delivery rate
SELECT
    CAST(SUM(CASE WHEN is_on_time = 1 THEN 1 ELSE 0 END) AS FLOAT)
    / COUNT(*) * 100 AS on_time_pct,
    COUNT(*) AS total_shipments
FROM Reporting.vFact_Shipments;
```

```sql
-- Shipment value by region
SELECT
    r.region_name,
    r.zone,
    SUM(f.total_cost) AS total_shipment_value,
    COUNT(*) AS shipment_count
FROM Reporting.vFact_Shipments f
JOIN Reporting.vDim_Region r ON f.destination_region_key = r.region_key
GROUP BY r.region_name, r.zone
ORDER BY total_shipment_value DESC;
```

### 6. (Optional) Extend the reporting layer with a new DDL script

To see how IngenFab makes schema changes repeatable, add a `product_cost` calculated column to the fact view. Instead of editing the view directly in Fabric, modify the DDL script in your project:

**Step 1 — Edit the DDL script locally:**

Open `ddl_scripts/Warehouses/wh_reporting/001_Initial_Creation/002_vFact_Shipments.sql` and add the calculated column:

```sql
CREATE OR ALTER VIEW Reporting.vFact_Shipments
AS
SELECT
    shipment_key, shipment_id, order_id,
    product_key, supplier_key,
    origin_region_key, destination_region_key, date_key,
    quantity,
    CAST(unit_cost AS DECIMAL(12, 2))       AS unit_cost,
    CAST(freight_cost AS DECIMAL(12, 2))    AS freight_cost,
    CAST(total_cost AS DECIMAL(14, 2))      AS total_cost,
    CAST(total_cost - freight_cost AS DECIMAL(14, 2)) AS product_cost,
    scheduled_days, actual_days,
    CAST(is_on_time AS BIT)                 AS is_on_time,
    status, priority
FROM [lh_gold].[dbo].[fact_shipments];
```

**Step 2 — Recompile and redeploy:**

=== "macOS / Linux"

    ```bash
    ingen_fab ddl compile \
        --output-mode fabric_workspace_repo \
        --generation-mode Warehouse

    ingen_fab deploy deploy
    ```

=== "Windows (PowerShell)"

    ```powershell
    ingen_fab ddl compile `
        --output-mode fabric_workspace_repo `
        --generation-mode Warehouse

    ingen_fab deploy deploy
    ```

**Step 3 — Run the warehouse DDL orchestrator** in Fabric to apply the change, then re-run the row-count query from Step 5 to confirm the view still returns the same number of rows.

!!! note "The IngenFab workflow advantage"
    This demonstrates the version-controlled DDL pattern: the change was made in your local SQL file, compiled into a notebook, deployed to Fabric, and executed — all tracked in source control. In a CI/CD pipeline, this entire flow is automated.

---

[← Exercise 4 — Gold Layer](exercise-04-gold-layer.md) | [Exercise 6 — Semantic Model →](exercise-06-semantic-model.md)
