# Exercise 2 — New Bronze Table

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 2

## Learning Objectives

By the end of this exercise you will be able to:

- Write a bronze DDL script following the framework's conventions
- Use the `target_lakehouse` abstraction to write a Delta table
- Compile and deploy the new script
- Verify the table appears in `lh_bronze`

## Prerequisites

- [Exercise 1](exercise-01-project-setup.md) completed — your project is initialised and deployed

## Background

In the DP project, **bronze** is the raw ingestion layer. Each table corresponds to a source entity and holds exactly what arrived — no transformation, no business logic. Bootstrap scripts use the PySpark `Row` / `StructType` pattern to seed the table with a small representative dataset.

All bronze DDL scripts use the `target_lakehouse` variable which is injected by the framework when the notebook runs in Fabric. You never instantiate it directly in your DDL script.

When you compile, the framework bundles **all scripts in the same folder** into a single Fabric notebook. Your `003_application_state_provinces_table_create.py` will become a cell inside `001_Initial_Creation_lh_bronze_Lakehouses_ddl_scripts` — it does not appear as its own standalone notebook in the workspace.

## Your Task

Add a `state_provinces` bronze table to the sample project. This table links cities (already in `lh_bronze`) to countries and is the next logical addition to the dataset.

The schema is:

| Column | Type | Description |
|--------|------|-------------|
| `StateProvinceID` | Integer | Unique identifier |
| `StateProvinceCode` | String | e.g. "CA", "TX" |
| `StateProvinceName` | String | e.g. "California" |
| `CountryID` | Integer | FK to countries table |
| `SalesTerritory` | String | e.g. "Southwest" |
| `LatestRecordedPopulation` | Integer | Population figure |
| `LastEditedBy` | String | User reference |
| `ValidFrom` | Timestamp | SCD start date |
| `ValidTo` | Timestamp | SCD end date |

## Steps

### 1. Create the script file

Create a new file:

```
ddl_scripts/Lakehouses/lh_bronze/001_Initial_Creation/003_application_state_provinces_table_create.py
```

### 2. Write the script

Copy and paste the following into your new file:

```python
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql import Row
from datetime import datetime

schema = StructType(
    [
        StructField("StateProvinceID", IntegerType(), nullable=True),
        StructField("StateProvinceCode", StringType(), nullable=True),
        StructField("StateProvinceName", StringType(), nullable=True),
        StructField("CountryID", IntegerType(), nullable=True),
        StructField("SalesTerritory", StringType(), nullable=True),
        StructField("LatestRecordedPopulation", IntegerType(), nullable=True),
        StructField("LastEditedBy", StringType(), nullable=True),
        StructField("ValidFrom", TimestampType(), nullable=True),
        StructField("ValidTo", TimestampType(), nullable=True),
    ]
)

sample_data = [
    Row(StateProvinceID=1, StateProvinceCode="AL", StateProvinceName="Alabama", CountryID=230, SalesTerritory="Southeast", LatestRecordedPopulation=4833722, LastEditedBy="1", ValidFrom=datetime(2013, 1, 1, 0, 0, 0), ValidTo=datetime(9999, 12, 31, 23, 59, 59)),
    Row(StateProvinceID=2, StateProvinceCode="AK", StateProvinceName="Alaska", CountryID=230, SalesTerritory="Far West", LatestRecordedPopulation=735132, LastEditedBy="1", ValidFrom=datetime(2013, 1, 1, 0, 0, 0), ValidTo=datetime(9999, 12, 31, 23, 59, 59)),
    Row(StateProvinceID=3, StateProvinceCode="AZ", StateProvinceName="Arizona", CountryID=230, SalesTerritory="Southwest", LatestRecordedPopulation=6626624, LastEditedBy="1", ValidFrom=datetime(2013, 1, 1, 0, 0, 0), ValidTo=datetime(9999, 12, 31, 23, 59, 59)),
    Row(StateProvinceID=4, StateProvinceCode="AR", StateProvinceName="Arkansas", CountryID=230, SalesTerritory="Southeast", LatestRecordedPopulation=2959373, LastEditedBy="1", ValidFrom=datetime(2013, 1, 1, 0, 0, 0), ValidTo=datetime(9999, 12, 31, 23, 59, 59)),
    Row(StateProvinceID=5, StateProvinceCode="CA", StateProvinceName="California", CountryID=230, SalesTerritory="Far West", LatestRecordedPopulation=38332521, LastEditedBy="1", ValidFrom=datetime(2013, 1, 1, 0, 0, 0), ValidTo=datetime(9999, 12, 31, 23, 59, 59)),
]

df_with_data = target_lakehouse.spark.createDataFrame(sample_data, schema)
target_lakehouse.write_to_table(
    df=df_with_data, table_name="state_provinces", schema_name=""
)
```

### 3. Compile DDL notebooks

```bash
ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Lakehouse
```

### 4. Deploy

```bash
ingen_fab deploy deploy
```

### 5. Run in Fabric

In your Fabric workspace, run the `lh_bronze` orchestrator notebook:

```
00_orchestrator_lh_bronze_lakehouse_ddl_scripts
```

This calls `001_Initial_Creation_lh_bronze_Lakehouses_ddl_scripts` (and `002_Change_lh_bronze_Lakehouses_ddl_scripts`) in sequence. Because every script is wrapped in a `run_once` guard, previously executed scripts (`001_application_cities_table_create`, `002_application_countries_table_create`) are **skipped** — only your new `003_application_state_provinces_table_create` cell runs, writing the `state_provinces` table to `lh_bronze`.

!!! tip "Running `001_Initial_Creation` directly also works"
    If you only want to run the initial creation phase, you can open and run `001_Initial_Creation_lh_bronze_Lakehouses_ddl_scripts` directly. The same `run_once` guard applies — existing scripts are skipped and only the new `003` cell executes. The orchestrator is the recommended default as it mirrors how the full pipeline operates.

## Verification

In a Fabric notebook attached to `lh_bronze`:

```python
display(spark.read.table("state_provinces"))
```

You should see 5 rows with all columns populated.

---

← [Exercise 1 — Project Setup](exercise-01-project-setup.md) | **Next:** [Exercise 3 — Silver Transformation →](exercise-03-silver-transformation.md)
