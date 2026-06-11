# Enterprise Reporting on Microsoft Fabric

[Home](../../index.md) > [Training](../index.md) > ER Project

## What is Enterprise Reporting?

Enterprise reporting means giving business stakeholders **consistent, trustworthy answers to operational questions** — delivered through dashboards and reports that automatically stay current as data changes.

For a supply chain team, those questions look like:

> *"Which suppliers are causing delivery delays this quarter?"*  
> *"What is our on-time delivery rate by region — and is it getting better or worse?"*  
> *"Which product categories have the highest freight cost relative to unit value?"*

Without a structured reporting layer, analysts answer these ad hoc — writing different queries each time, producing inconsistent numbers, and spending hours on data prep instead of insight. Enterprise reporting solves this by defining the answers **once**, in a semantic model, so every report and stakeholder gets the same number.

---

## The Enterprise Reporting Stack on Fabric

Microsoft Fabric's reporting stack has three layers:

### 1. Warehouse + Views (the data contract)

A **Warehouse** exposes your gold layer data via T-SQL views. The views form a **stable contract** between your data engineers and your report builders — if the underlying tables change shape, only the view needs updating, not every report.

In this tutorial the warehouse is `wh_reporting`, and it exposes views like `Reporting.vFact_Shipments` and `Reporting.vDim_Supplier`.

### 2. Semantic Model (the business layer)

A **semantic model** sits on top of the warehouse and adds:

- **Relationships** — joining fact and dimension tables so analysts don't have to
- **DAX measures** — pre-defined, reusable calculations like *On-Time Delivery Rate %* or *Avg Delay Days*
- **Hierarchies** — e.g. Year → Quarter → Month for drill-down
- **Friendly names** — renaming `destination_region_key` to something a business user understands

Once published, the semantic model appears in Power BI as `sem_supply_chain`. Every report built on it automatically gets the latest data and uses the same measure definitions.

### 3. Power BI Report (the output)

Reports connect to the semantic model — not to raw tables. Analysts drag measures and dimensions onto canvases. Because the business logic lives in the semantic model, the reports stay light and consistent.

The report you will build — `rpt_supply_chain_overview` — answers the three questions above across three pages:

| Page | Audience | Key Visual |
|---|---|---|
| Executive Summary | Management | On-time rate trend + total value by region |
| Delivery Performance | Operations | Supplier scatter plot + delay league table |
| Product & Supplier Analysis | Procurement | Treemap by category + supplier × product matrix |

---

## How IngenFab Accelerates This

Building the data platform beneath a semantic model is the slow part. Without tooling, you manually:

- Write PySpark DDL notebooks for each Bronze / Silver / Gold table
- Create the warehouse and write T-SQL views by hand
- Deploy each item individually through the Fabric UI
- Repeat for every environment (dev, test, prod)

IngenFab automates all of it with a single scaffold command:

```bash
ingen_fab init new --project-name my-er-project --with-er-samples
```

This creates a complete supply chain project with:

- **15 DDL scripts** across Bronze, Silver, and Gold layers — compiled into Fabric notebooks by `ingen_fab ddl compile`
- **6 warehouse SQL scripts** — the `Reporting` schema and all 5 views, compiled and deployed automatically
- **Pre-wired variable library** for environment-specific GUIDs
- **Synthetic data generation** for `lh_bronze` — no source system needed to start building reports

Then two commands build and push everything to Fabric:

```bash
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
ingen_fab deploy deploy
```

After that, you run the DDL orchestrators in Fabric, generate data, and your reporting layer is ready for a semantic model.

---

## End-to-End Data Flow

```
ingen_fab init new --with-er-samples
        │
        ▼
┌─────────────────────────────────────────────────────┐
│  lh_bronze  — raw shipments, orders, products,      │
│              suppliers, regions (empty tables)      │
└───────────────────────┬─────────────────────────────┘
                        │
    ingen_fab package synthetic-data generate
    supply_chain_star_small  →  5,000 shipment rows
                        │
                        ▼
┌─────────────────────────────────────────────────────┐
│  lh_silver  — deduplicated, null-safe tables        │
└───────────────────────┬─────────────────────────────┘
                        │  silver transformation notebook
                        ▼
┌─────────────────────────────────────────────────────┐
│  lh_gold   — star schema                            │
│  fact_shipments (5 000 rows)                        │
│  dim_product (200) · dim_supplier (50)              │
│  dim_region (20)  · dim_date (3 653)                │
└───────────────────────┬─────────────────────────────┘
                        │  ingen_fab DDL (warehouse)
                        ▼
┌─────────────────────────────────────────────────────┐
│  wh_reporting  — Reporting schema                   │
│  vFact_Shipments · vDim_Product                     │
│  vDim_Supplier · vDim_Region · vDim_Date            │
└───────────────────────┬─────────────────────────────┘
                        │  Fabric UI
                        ▼
┌─────────────────────────────────────────────────────┐
│  sem_supply_chain  — semantic model                 │
│  4 relationships + 6 DAX measures                  │
└───────────────────────┬─────────────────────────────┘
                        │  Power BI
                        ▼
┌─────────────────────────────────────────────────────┐
│  rpt_supply_chain_overview                          │
│  Page 1: Executive Summary                         │
│  Page 2: Delivery Performance                      │
│  Page 3: Product & Supplier Analysis               │
└─────────────────────────────────────────────────────┘
```

---

## DAX Measures You Will Build

These six measures cover the core supply chain KPIs. You will add them to the semantic model in Exercise 6:

| Measure | What It Answers |
|---|---|
| `On-Time Delivery Rate %` | What % of shipments arrived within the scheduled window? |
| `Total Shipment Value` | What is the total cost of goods + freight? |
| `Late Shipments` | How many shipments missed their delivery date? |
| `Avg Actual Days` | How long does delivery actually take on average? |
| `Avg Scheduled Days` | How long was it supposed to take? |
| `Avg Delay Days` | For late shipments, how many days late on average? |

---

## Prerequisites

- Python 3.12+
- Microsoft Fabric workspace with capacity (F2 or higher)
- Azure CLI installed and authenticated

No prior IngenFab or Fabric experience required — the exercises are fully step-by-step.

---

## Exercises

| # | Exercise | What You Will Do |
|---|---|---|
| 0 | [Environment Setup](exercise-00-prerequisites.md) | Install tools, authenticate, verify workspace |
| 1 | [Project Setup](exercise-01-project-setup.md) | Scaffold with `--with-er-samples`, configure, compile DDL, deploy to Fabric |
| 2 | [Generate Data](exercise-02-generate-data.md) | Populate `lh_bronze` with 5,000 synthetic shipment rows |
| 3 | [Silver Layer](exercise-03-silver-layer.md) | Review IngenFab silver DDL scripts, inspect schemas with `get-metadata`, populate silver tables |
| 4 | [Gold Layer](exercise-04-gold-layer.md) | Review IngenFab star schema DDL scripts, populate `lh_gold` dimensions and fact table |
| 5 | [Warehouse Views](exercise-05-warehouse-views.md) | Review IngenFab warehouse SQL scripts, inspect with `get-metadata`, verify and extend views |
| 6 | [Semantic Model](exercise-06-semantic-model.md) | Create relationships, add 6 DAX measures, version-control with `download-artefact` |
| 7 | [Report](exercise-07-report.md) | Build the 3-page `rpt_supply_chain_overview` report, version-control with `download-artefact` |

---

**Start here:** [Exercise 0 — Environment Setup →](exercise-00-prerequisites.md)
