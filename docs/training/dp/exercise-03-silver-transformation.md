# Exercise 3 — Silver Transformation

[Home](../../index.md) > [Training](../index.md) > [DP Project](index.md) > Exercise 3

## Learning Objectives

By the end of this exercise you will be able to:

- Create an `lh_silver` DDL script that defines a clean table schema
- Write a dbt model that reads from bronze and merges into silver
- Understand the CDC (change data capture) merge pattern used in this project
- Register a new bronze source in the dbt schema file using `ingen_fab dbt generate-schema-yml`
- Compile, deploy and run the silver transformation

## Prerequisites

- [Exercise 2](exercise-02-bronze-table.md) completed — `state_provinces` exists in `lh_bronze`

## Background

The **silver layer** holds clean, current records. The DP project uses **dbt** with an incremental merge strategy to move records from bronze into silver:

- Bronze holds all ingested records (including historic rows where `ValidTo != 9999-12-31`)
- The silver dbt model filters to the latest valid record per key using a merge on the primary key

The DDL script in `lh_silver` creates the **empty target table** (schema only). The dbt model then populates it.

## Steps

### 0. Refresh bronze metadata

Exercise 2 added `state_provinces` to `lh_bronze`. Before doing anything else, pull a fresh metadata snapshot across all lakehouses:

```bash
ingen_fab deploy get-metadata --target lakehouse --all
```

This writes `metadata/lakehouse_metadata_all.csv` with all lakehouses in the workspace. Steps 1–3 below depend on it.

!!! note "Why `--all` and not `--lakehouse-name`?"
    `--lakehouse-name` writes a per-lakehouse file (e.g. `lakehouse_<bronze_lakehouse_name>_columns.csv`) which `generate-schema-yml` does not read. The `--all` flag writes `lakehouse_metadata_all.csv`, which is the file that all `dbt` and `ddl` subcommands use by default.

### 1. Create the silver DDL script

Create the DDL script:

```
ddl_scripts/Lakehouses/lh_silver/001_Initial_Creation/003_lh_silver_state_provinces.py
```

Define the same schema as the bronze table (silver keeps the same columns at this stage):

```python
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# DDL script for creating table 'state_provinces' in lakehouse 'lh_silver'

schema = StructType(
    [
        StructField("StateProvinceID", IntegerType(), True),
        StructField("StateProvinceCode", StringType(), True),
        StructField("StateProvinceName", StringType(), True),
        StructField("CountryID", IntegerType(), True),
        StructField("SalesTerritory", StringType(), True),
        StructField("LatestRecordedPopulation", IntegerType(), True),
        StructField("LastEditedBy", StringType(), True),
        StructField("ValidFrom", TimestampType(), True),
        StructField("ValidTo", TimestampType(), True),
    ]
)

empty_df = target_lakehouse.spark.createDataFrame([], schema)

target_lakehouse.write_to_table(
    df=empty_df, table_name="state_provinces", schema_name=""
)
```

!!! tip "Reference"
    Compare with `ddl_scripts/Lakehouses/lh_silver/001_Initial_Creation/001_lh_silver_cities.py` — the pattern is identical.

### 2. Create the dbt silver model

Create a new file:

```
dbt_project/models/silver/state_provinces.sql
```

Use the incremental merge pattern with a CDC filter to keep only current records:

```sql
{{ config(
        materialized='incremental',
        unique_key = 'StateProvinceID',
        incremental_strategy='merge',
        file_format='delta'
        ) 
}}

-- Filter to current (non-expired) records only — CDC pattern
WITH source_data AS (
    SELECT * FROM {{ source('bronze', 'state_provinces') }}
    WHERE ValidTo >= '9999-12-31'
    {% if is_incremental() %}
    AND ValidFrom > (SELECT MAX(ValidFrom) FROM {{ this }})
    {% endif %}
)

SELECT * FROM source_data
```

!!! note "How the CDC filter works"
    `WHERE ValidTo >= '9999-12-31'` retains only active/current rows — bronze stores historical rows too, with expired records having a past `ValidTo` date. The `{% if is_incremental() %}` block is inactive on the first run (full load), then on subsequent runs it restricts processing to records newer than the highest `ValidFrom` already in silver — avoiding a full bronze scan on every run.

### 3. Register the source in dbt

Generate the bronze source schema file from the refreshed metadata:

```bash
ingen_fab dbt generate-schema-yml --dbt-project dbt_project --lakehouse <your_bronze_lakehouse_name> --layer bronze --dbt-type source
```

This writes `dbt_project/schema_yml/bronze/schema.yml` with all bronze tables and their column definitions auto-populated from metadata.

Copy the generated file over the existing source schema — always replace the **entire** file, never manually merge individual table entries:

```bash
cp dbt_project/schema_yml/bronze/schema.yml dbt_project/models/bronze/schema.yml
```

Verify the copy worked — `state_provinces` must appear in the target file:

```bash
grep 'state_provinces' dbt_project/models/bronze/schema.yml
```

You should see `- name: state_provinces`. If not, the copy didn't work — repeat the `cp` command.

Also verify the `schema:` value matches the bronze lakehouse name:

```yaml
# This value must match dbt_project.yml → models → dbt_project → bronze → schema:
  schema: <bronze_lakehouse_name>
```

!!! danger "Always full-file replace, never manual merge"
    Do not hand-edit `models/bronze/schema.yml` to add individual tables. This risks duplicate entries (dbt will error with "found two sources with the same name") or missing columns. The `cp` command replaces the entire file, which is the intended workflow.

!!! note "Why these must match"
    `generate-schema-yml` writes the actual Fabric lakehouse name into `schema:`. The `schema:` field in `models/bronze/schema.yml` is what dbt uses to resolve `{{ source('bronze', ...) }}` references — and it must equal the value in `dbt_project.yml` under `bronze: schema:`. In the compiled SQL, dbt uses this name as the Spark schema (e.g. `lh_1_bronze.state_provinces`), which Fabric resolves to the attached lakehouse of the same name.

### 4. Compile and deploy

```bash
ingen_fab ddl compile \
    --output-mode fabric_workspace_repo \
    --generation-mode Lakehouse

ingen_fab deploy deploy
```

### 5. Create the silver table, build it with dbt, and schedule it

**5a. Create the empty silver table in Fabric**

Step 4 deployed your DDL notebooks to Fabric. Now run them. In your Fabric workspace, run the `lh_silver` orchestrator notebook:

```
001_Initial_Creation_lh_silver_Lakehouses_ddl_scripts
```

This creates the empty `state_provinces` table in `lh_silver`.

**5b. Build the model with dbt**

The profile is generated from your value set (`dbt_default_lakehouse` names the lakehouse the
Livy session starts in; the sample sets it to `lh_silver`):

```bash
ingen_fab dbt build -- --select state_provinces
```

dbt compiles against the live catalog, starts a Livy session (the first run takes a minute),
reads `lh_bronze.state_provinces` and merges into `lh_silver.state_provinces`. You should see
`PASS=1`.

!!! note "Where models land"
    The `schema:` set per folder in `dbt_project.yml` is the lakehouse a folder's models write
    to (on plain lakehouses the lakehouse is the schema of a two-part name). To check without a
    connection: `ingen_fab dbt ls -- --output json --output-keys name relation_name`. See the
    [dbt integration guide](../../user_guide/dbt_integration.md#lakehouses-with-and-without-schemas).

**5c. Make it schedulable**

Upload the project to the config lakehouse and create the orchestrator notebook that runs the
same command inside Fabric, then deploy it:

```bash
ingen_fab dbt profile
ingen_fab deploy upload-dbt-project --dbt-project dbt_project
ingen_fab dbt orchestrator --name dbtload_silver --select "path:models/silver"
ingen_fab deploy deploy
```

(`path:models/silver` selects every model in that folder; a bare `silver` would select a
model named silver.)

**5d. Run the orchestrator notebook in Fabric**

In your Fabric workspace, run `dbtload_silver`. It installs the adapter from the uploaded
`requirements.txt`, selects the `<environment>-notebook` profile target from the Variable
Library, and runs `dbt build --select path:models/silver` with the notebook's own identity.

## Verification

In a Fabric notebook attached to `lh_silver`:

```python
display(spark.read.table("state_provinces"))
```

You should see 5 rows — the current (non-expired) records from bronze.

---

← [Exercise 2 — New Bronze Table](exercise-02-bronze-table.md) | **Next:** [Exercise 4 — Gold Layer →](exercise-04-gold-layer.md)
