# Exercise 4 — Gold Layer

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 4

## Learning Objectives

By the end of this exercise you will be able to:

- Create a gold DDL script to define an enriched dimension table
- Write a dbt gold model that joins multiple silver tables
- Understand the purpose of the gold layer in a medallion architecture
- Deploy and verify the enriched dimension, and the warehouse views the next exercises use

## Prerequisites

- [Exercise 3](exercise-03-silver-transformation.md) completed — `suppliers` exists in `lh_silver`, and the `dbtload_silver` run built the sample's silver tables (`cities`, `state_provinces`, `countries` among them)

## Background

The **gold layer** holds analytics-ready data. Where silver tables mirror the source entities, gold tables are **shaped for consumption** — joins are pre-computed, surrogate keys may be added, and column names match the semantic model's expectations.

The sample project ships six gold models under `dbt_project/models/gold/`, among them `dim_cities` (the city dimension) and `dim_geography` (a city flattened with its state/province and country). In this exercise you add `dim_suppliers` — each supplier with the geography of the city it delivers to — and the warehouse view over `dim_geography` that Exercise 5 needs.

## Steps

### 0. Confirm the silver tables have data

The gold model joins four silver tables: `suppliers` (yours), `cities`, `state_provinces` and `countries` (the sample's). Exercise 3's `dbtload_silver` run built all of them. If you skipped 5c and 5d of Exercise 3, build the silver folder now:

```bash
ingen_fab dbt build -- --select path:models/silver
```

And you can review the data in a notebook attached to `lh_silver` with the below code:

```python
# In a Fabric notebook attached to lh_silver
display(spark.read.table("suppliers"))
display(spark.read.table("cities"))
```

### 1. Create the gold DDL script

Create a new file:

```
ddl_scripts/Lakehouses/lh_gold/001_Initial_Creation/002_lh_gold_dim_suppliers.py
```

Define the schema for the enriched dimension:

```python
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# DDL script for creating table 'dim_suppliers' in lakehouse 'lh_gold'

schema = StructType(
    [
        StructField("SupplierID", IntegerType(), True),
        StructField("SupplierName", StringType(), True),
        StructField("SupplierCategory", StringType(), True),
        StructField("CityID", IntegerType(), True),
        StructField("CityName", StringType(), True),
        StructField("StateProvinceCode", StringType(), True),
        StructField("StateProvinceName", StringType(), True),
        StructField("SalesTerritory", StringType(), True),
        StructField("CountryName", StringType(), True),
        StructField("Continent", StringType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="dim_suppliers", schema_name=""
)
```

### 2. Create the dbt gold model

Create a new file:

```
dbt_project/models/gold/dim_suppliers.sql
```

Join the four silver tables:

```sql
{{ config(materialized='incremental', unique_key='SupplierID') }}

-- One row per supplier with the geography of the city it delivers to.
SELECT
    s.SupplierID,
    s.SupplierName,
    s.SupplierCategory,
    c.CityID,
    c.CityName,
    sp.StateProvinceCode,
    sp.StateProvinceName,
    sp.SalesTerritory,
    co.CountryName,
    co.Continent
FROM {{ ref('suppliers') }} s
LEFT JOIN {{ ref('cities') }} c ON s.DeliveryCityID = c.CityID
LEFT JOIN {{ ref('state_provinces') }} sp ON c.StateProvinceID = sp.StateProvinceID
LEFT JOIN {{ ref('countries') }} co ON sp.CountryID = co.CountryID
```

`ref()` resolves the sample's silver models and yours alike: all of them live in `lh_silver`, and the `gold` folder's `+schema` rule in `dbt_project.yml` (a dbt var with the default `lh_gold`) sends this model to `lh_gold`.

### 3. Add a warehouse view

Exercise 5 builds a semantic model over two views in `wh_gold`: `DW.vDim_Cities`, which the sample ships, and `DW.vDim_CityGeography`, which you add now over the sample's `dim_geography`. Create a new SQL file in `ddl_scripts/Warehouses/wh_gold/001_Initial_Creation/`:

```
003_wh_gold_create_city_geography_view.sql
```

```sql
CREATE OR ALTER VIEW DW.vDim_CityGeography
AS
SELECT * FROM [lh_gold].[dbo].[dim_geography]
```

The warehouse reads the gold lakehouse through its SQL analytics endpoint, as a three-part name, within the same workspace.

### 4. Compile and deploy

```bash
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
ingen_fab deploy deploy
```

### 5. Create the gold table, build it with dbt, and schedule it

**5a. Create the empty gold table in Fabric**

Step 4 deployed your DDL notebooks to Fabric. Now run them. In your Fabric workspace, run the `lh_gold` orchestrator notebook:

```
00_orchestrator_lh_gold_lakehouse_ddl_scripts
```

The sample's `001_lh_gold_dim_cities` ran in Exercise 1 and is skipped; your `002_lh_gold_dim_suppliers` cell creates the empty `dim_suppliers` table in `lh_gold`.

**5b. Build the model with dbt**

```bash
ingen_fab dbt build -- --select dim_suppliers
```

dbt reads the four silver tables and writes `lh_gold.dim_suppliers`. You should see `PASS=1`.

!!! warning "All four silver tables must have data"
    If `cities`, `state_provinces` or `countries` in `lh_silver` are empty (Step 0 not
    completed), the run succeeds but the geography columns are `NULL`.

**5c. Make it schedulable**

```bash
ingen_fab dbt orchestrator --name dbtload_gold --select "path:models/gold"
ingen_fab deploy upload-dbt-project --dbt-project dbt_project
ingen_fab deploy deploy
```

`path:models/gold` selects the sample's six gold models and your `dim_suppliers`; every silver table they read exists after Exercise 3.

**5d. Run the orchestrator notebook in Fabric**

In your Fabric workspace, run `dbtload_gold`.

**5e. Create the warehouse views**

The views in `wh_gold` read `lh_gold` through its SQL analytics endpoint, so they come after the
gold build. Run the `wh_gold` warehouse orchestrator notebook:

```
00_orchestrator_wh_gold_warehouse_ddl_scripts
```

DDL scripts run once: the ones Exercise 1 already ran (the `DW` schema, `DW.vDim_Cities`) are
skipped unless they failed then, and the `DW.vDim_CityGeography` view you wrote in Step 3 is
created. Exercise 5 needs both views.

!!! tip "If the view fails with `Invalid object name 'lh_gold.dbo.dim_geography'`"
    The SQL analytics endpoint of `lh_gold` has not yet picked up the tables the gold build
    wrote. Open the endpoint in the portal and choose **Refresh**, then run the orchestrator
    again.

## Verification

In a Fabric notebook attached to `lh_gold`:

```python
display(spark.read.table("dim_suppliers"))
```

You should see **5 rows** with every column populated: each supplier carries its delivery city's state/province and country:

| SupplierID | SupplierName | SupplierCategory | CityID | CityName | StateProvinceCode | SalesTerritory | CountryName |
|---|---|---|---|---|---|---|---|
| 1 | A Datum Corporation | Novelty Goods Supplier | 1 | Aaronsburg | AZ | Southwest | United States |
| 2 | Contoso, Ltd. | Toy Supplier | 3 | Abanda | AL | Southeast | United States |
| 3 | Fabrikam, Inc. | Packaging Supplier | 4 | Abbeville | CA | Far West | United States |
| 4 | Litware, Inc. | Clothing Supplier | 5 | Abbeville | AK | Far West | United States |
| 5 | Northwind Electric Cars | Courier Services Supplier | 6 | Abbeville | AL | Southeast | United States |

If the geography columns show `NULL`, check that the silver tables `cities`, `state_provinces` and `countries` have data (Step 0).

In `wh_gold`, open the **DW** schema: `vDim_Cities` and `vDim_CityGeography` are there, which is what Exercise 5 checks first.

---

← [Exercise 3 — Silver Transformation](exercise-03-silver-transformation.md) | **Next:** [Exercise 5 — Semantic Model →](exercise-05-semantic-model.md)
