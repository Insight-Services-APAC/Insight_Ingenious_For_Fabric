# Exercise 4 — Gold Layer

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 4

## Learning Objectives

By the end of this exercise you will be able to:

- Create a gold DDL script to define an enriched dimension table
- Write a dbt gold model that joins multiple silver tables
- Understand the purpose of the gold layer in a medallion architecture
- Deploy and verify the enriched dimension

## Prerequisites

- [Exercise 3](exercise-03-silver-transformation.md) completed — `state_provinces` exists in `lh_silver`
- The sample-project silver models for `cities` and `countries` are present in `dbt_project/models/silver/` (they ship with the project)

## Background

The **gold layer** holds analytics-ready data. Where silver tables mirror the source entities, gold tables are **shaped for consumption** — joins are pre-computed, surrogate keys may be added, and column names match the semantic model's expectations.

The DP project currently has `dim_cities` in `lh_gold`, built from silver `cities` only. In this exercise you will create an enriched version — `dim_city_geography` — that joins cities with state/provinces and countries to produce a single, flat dimension.

## Steps

### 0. Populate cities and countries in silver

The gold model joins three silver tables: `cities`, `state_provinces`, and `countries`. Exercise 3 populated `state_provinces`, but `cities` and `countries` still only exist in bronze. The silver dbt models for these two tables ship with the sample project — you just need to run both notebooks in Fabric and to confirm data lands.

These notebooks are:
- model.dbt_project.cities
- model.dbt_project.countries

And you can review the data in a notebook attached to `lh_silver` with the below code: 

```python
# In a Fabric notebook attached to lh_silver
display(spark.read.table("cities"))
display(spark.read.table("countries"))
```

### 1. Create the gold DDL script

Create a new file:

```
ddl_scripts/Lakehouses/lh_gold/001_Initial_Creation/002_lh_gold_dim_city_geography.py
```

Define the schema for the enriched dimension:

```python
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# DDL script for creating table 'dim_city_geography' in lakehouse 'lh_gold'

schema = StructType(
    [
        StructField("CityID", IntegerType(), True),
        StructField("CityName", StringType(), True),
        StructField("StateProvinceID", IntegerType(), True),
        StructField("StateProvinceCode", StringType(), True),
        StructField("StateProvinceName", StringType(), True),
        StructField("SalesTerritory", StringType(), True),
        StructField("CountryID", IntegerType(), True),
        StructField("CountryName", StringType(), True),
        StructField("Continent", StringType(), True),
        StructField("Region", StringType(), True),
        StructField("LatestRecordedPopulation", LongType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="dim_city_geography", schema_name=""
)
```

### 2. Create the dbt gold model

Create a new file:

```
dbt_project/models/gold/dim_city_geography.sql
```

Join the three silver tables:

```sql
{{ config(
        materialized='incremental',
        unique_key = 'CityID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

WITH cities AS (
    SELECT * FROM {{ ref('cities') }}        -- silver cities
),
state_provinces AS (
    SELECT * FROM {{ ref('state_provinces') }}  -- silver state_provinces
),
countries AS (
    SELECT * FROM {{ ref('countries') }}     -- silver countries
)

SELECT
    c.CityID,
    c.CityName,
    sp.StateProvinceID,
    sp.StateProvinceCode,
    sp.StateProvinceName,
    sp.SalesTerritory,
    co.CountryID,
    co.CountryName,
    co.Continent,
    co.Region,
    c.LatestRecordedPopulation
FROM cities c
LEFT JOIN state_provinces sp ON c.StateProvinceID = sp.StateProvinceID
LEFT JOIN countries co ON sp.CountryID = co.CountryID
```

### 3. Add a warehouse view

Create a new SQL file in `ddl_scripts/Warehouses/wh_gold/001_Initial_Creation/`:

```
003_wh_gold_create_city_geography_view.sql
```

```sql
CREATE OR ALTER VIEW DW.vDim_CityGeography
AS
SELECT * FROM [lh_gold].[dbo].[dim_city_geography]
```

### 4. Compile and deploy

```bash
ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Lakehouse

ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Warehouse

ingen_fab deploy deploy 
```

### 5. Create the gold table, build the dbt notebook, and run

**5a. Create the empty gold table in Fabric**

Step 4 deployed your DDL notebooks to Fabric. Now run them. In your Fabric workspace, run the `lh_gold` orchestrator notebook:

```
001_Initial_Creation_lh_gold_Lakehouses_ddl_scripts
```

This creates the empty `dim_city_geography` table in `lh_gold`. 

**5b. Build the dbt notebook locally**

```bash
ingen_fab dbt exec -- build-local dbt_project --select dim_city_geography
```

When prompted, select your silver lakehouse (i.e `lh_silver`).

This compiles the gold model against your live Fabric lakehouses and writes a Fabric-native Python notebook to `dbt_project/target/notebooks_fabric_py/`. You should see `PASS=1` in the output.

!!! warning "All three silver tables must have data"
    `build-local` validates the model against live Fabric. If `cities` or `countries` in `lh_silver` are empty (Step 0 not completed), the compilation will still succeed but the dbt run in Step 5d will produce no rows.

**5c. Stage and deploy the generated notebook to Fabric**

```bash
ingen_fab dbt create-notebooks --dbt-project dbt_project
ingen_fab deploy deploy
```

**5d. Run the dbt notebook in Fabric**

In your Fabric workspace, run the generated notebook:

```
model.dbt_project.dim_city_geography
```

This executes the incremental merge joining all three silver tables into `lh_gold.dim_city_geography`. Alternatively, run the master orchestrator notebook `master_dbt_project_notebook` to execute all dbt models.

**5e. Create the warehouse view**

Run the `wh_gold` warehouse orchestrator notebook:

```
001_Initial_Creation_wh_gold_Warehouses_ddl_scripts
```

This creates the `DW.vDim_CityGeography` view in `wh_gold`.

## Verification

In a Fabric notebook attached to `lh_gold`:

```python
display(spark.read.table("dim_city_geography"))
```

You should see **10 rows** with all columns fully populated. Every city is enriched with state/province and country information:

| CityID | CityName | StateProvinceCode | StateProvinceName | SalesTerritory | CountryName | Continent | Region |
|--------|------------|:-:|-----------|:-----------:|-------------|-----------|--------|
| 1 | Aaronsburg | AZ | Arizona | Southwest | United States | North America | Americas |
| 3 | Abanda | AL | Alabama | Southeast | United States | North America | Americas |
| 4 | Abbeville | CA | California | Far West | United States | North America | Americas |
| 5 | Abbeville | AK | Alaska | Far West | United States | North America | Americas |
| 6 | Abbeville | AL | Alabama | Southeast | United States | North America | Americas |
| 7 | Abbeville | AZ | Arizona | Southwest | United States | North America | Americas |
| 8 | Abbeville | AR | Arkansas | Southeast | United States | North America | Americas |
| 9 | Abbotsford | CA | California | Far West | United States | North America | Americas |
| 10 | Abbott | AK | Alaska | Far West | United States | North America | Americas |
| 11 | Abbott | AR | Arkansas | Southeast | United States | North America | Americas |

If any columns show `NULL`, check that:

1. All three silver tables (`cities`, `state_provinces`, `countries`) have data (Step 0)
2. The `state_provinces` seed includes StateProvinceIDs matching the cities (1–5)
3. The `countries` seed includes CountryID 230 (United States)

In `wh_gold`, run:

```sql
SELECT TOP 10 * FROM DW.vDim_CityGeography
```

The same 10 rows should appear.

---

← [Exercise 3 — Silver Transformation](exercise-03-silver-transformation.md) | **Next:** [Exercise 5 — Semantic Model →](exercise-05-semantic-model.md)
