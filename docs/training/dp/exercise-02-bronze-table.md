# Exercise 2 — New Bronze Table

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 2

## Learning Objectives

By the end of this exercise you will be able to:

- Write a bronze DDL script following the framework's conventions
- Use the `target_lakehouse` abstraction to write a Delta table
- Compile and deploy the new script
- Verify the table appears in `lh_bronze`

## Prerequisites

- [Exercise 1](exercise-01-project-setup.md) completed — your project is initialised and deployed, and the bronze orchestrator has run

## Background

In the DP project, **bronze** is the raw ingestion layer. Each table corresponds to a source entity and holds exactly what arrived — no transformation, no business logic. Bootstrap scripts use the PySpark `Row` / `StructType` pattern to seed the table with a small representative dataset.

All bronze DDL scripts use the `target_lakehouse` variable which is injected by the framework when the notebook runs in Fabric. You never instantiate it directly in your DDL script.

When you compile, the framework bundles **all scripts in the same folder** into a single Fabric notebook. Your `003_application_suppliers_table_create.py` will become a cell inside `001_Initial_Creation_lh_bronze_Lakehouses_ddl_scripts` — it does not appear as its own standalone notebook in the workspace.

The sample project already ships seven bronze tables (`cities`, `countries`, `state_provinces`, `customers`, `stock_items`, `orders`, `order_lines`), loaded by the scripts under `001_Initial_Creation` and `003_Sales_Demo_Data`. In this exercise you add an entity the sample does not have, so your work stays separate from the sample's and nothing overwrites it.

## Your Task

Add a `suppliers` bronze table to the project. A supplier delivers to a city, which links it to the geography the sample already holds (cities, state/provinces, countries).

The schema is:

| Column | Type | Description |
|--------|------|-------------|
| `SupplierID` | Integer | Unique identifier |
| `SupplierName` | String | e.g. "Fabrikam, Inc." |
| `SupplierCategory` | String | e.g. "Packaging" |
| `DeliveryCityID` | Integer | FK to `cities` (`CityID`) |
| `PhoneNumber` | String | Contact number |
| `LastEditedBy` | String | User reference |
| `ValidFrom` | Timestamp | SCD start date |
| `ValidTo` | Timestamp | SCD end date |

## Steps

### 1. Create the script file

Create a new file:

```
ddl_scripts/Lakehouses/lh_bronze/001_Initial_Creation/003_application_suppliers_table_create.py
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
        StructField("SupplierID", IntegerType(), nullable=True),
        StructField("SupplierName", StringType(), nullable=True),
        StructField("SupplierCategory", StringType(), nullable=True),
        StructField("DeliveryCityID", IntegerType(), nullable=True),
        StructField("PhoneNumber", StringType(), nullable=True),
        StructField("LastEditedBy", StringType(), nullable=True),
        StructField("ValidFrom", TimestampType(), nullable=True),
        StructField("ValidTo", TimestampType(), nullable=True),
    ]
)

valid_from = datetime(2013, 1, 1, 0, 0, 0)
valid_to = datetime(9999, 12, 31, 23, 59, 59)

sample_data = [
    Row(SupplierID=1, SupplierName="A Datum Corporation", SupplierCategory="Novelty Goods Supplier", DeliveryCityID=1, PhoneNumber="(847) 555-0100", LastEditedBy="1", ValidFrom=valid_from, ValidTo=valid_to),
    Row(SupplierID=2, SupplierName="Contoso, Ltd.", SupplierCategory="Toy Supplier", DeliveryCityID=3, PhoneNumber="(425) 555-0101", LastEditedBy="1", ValidFrom=valid_from, ValidTo=valid_to),
    Row(SupplierID=3, SupplierName="Fabrikam, Inc.", SupplierCategory="Packaging Supplier", DeliveryCityID=4, PhoneNumber="(323) 555-0102", LastEditedBy="1", ValidFrom=valid_from, ValidTo=valid_to),
    Row(SupplierID=4, SupplierName="Litware, Inc.", SupplierCategory="Clothing Supplier", DeliveryCityID=5, PhoneNumber="(907) 555-0103", LastEditedBy="1", ValidFrom=valid_from, ValidTo=valid_to),
    Row(SupplierID=5, SupplierName="Northwind Electric Cars", SupplierCategory="Courier Services Supplier", DeliveryCityID=6, PhoneNumber="(334) 555-0104", LastEditedBy="1", ValidFrom=valid_from, ValidTo=valid_to),
]

df_with_data = target_lakehouse.spark.createDataFrame(sample_data, schema)
target_lakehouse.write_to_table(
    df=df_with_data, table_name="suppliers", schema_name=""
)
```

The `DeliveryCityID` values are `CityID`s that exist in the sample's `cities` table, so Exercise 4 can join suppliers to their geography.

### 3. Compile DDL notebooks

```bash
ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse
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

This calls the bundle notebooks of `001_Initial_Creation`, `002_Change` and `003_Sales_Demo_Data` in sequence. Because every script is wrapped in a `run_once` guard, the scripts that ran in Exercise 1 are **skipped** — only your new `003_application_suppliers_table_create` cell runs, writing the `suppliers` table to `lh_bronze`.

!!! tip "Running `001_Initial_Creation` directly also works"
    If you only want to run the initial creation phase, you can open and run `001_Initial_Creation_lh_bronze_Lakehouses_ddl_scripts` directly. The same `run_once` guard applies — existing scripts are skipped and only the new `003` cell executes. The orchestrator is the recommended default as it mirrors how the full pipeline operates.

## Verification

In a Fabric notebook attached to `lh_bronze`:

```python
display(spark.read.table("suppliers"))
```

You should see 5 rows with all columns populated.

---

← [Exercise 1 — Project Setup](exercise-01-project-setup.md) | **Next:** [Exercise 3 — Silver Transformation →](exercise-03-silver-transformation.md)
