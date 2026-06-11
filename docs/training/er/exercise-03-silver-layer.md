# Exercise 3 — Silver Layer Transformation

[Home](../../index.md) > [Training](../index.md) > [ER Project](index.md) > Exercise 3

## Learning Objectives

By the end of this exercise you will be able to:

- Review the silver layer DDL scripts that IngenFab generated and deployed in Exercise 1
- Understand the role of the silver layer in a medallion architecture
- Use `ingen_fab deploy get-metadata` to inspect deployed table schemas
- Create a transformation notebook to populate the IngenFab-defined silver tables
- Verify clean data in `lh_silver`

## Background

The **silver layer** (`lh_silver`) holds cleaned, deduplicated, and type-safe data. In a production pipeline, silver notebooks apply business rules, handle nulls, and enforce referential integrity. For this tutorial, we perform a straightforward pass-through transformation to illustrate the pattern.

When you ran `ingen_fab ddl compile --generation-mode Lakehouse` and the DDL orchestrator in Exercise 1, IngenFab **already created the empty silver tables** with defined schemas. In this exercise you will first inspect what IngenFab built, then write a transformation notebook to populate those tables from bronze.

!!! info "IngenFab's role: schema definition vs data transformation"
    IngenFab handles the **structural** side — defining table schemas, compiling DDL notebooks, deploying artefacts. The **transformation logic** (cleaning, deduplication, business rules) is written separately, either as manual notebooks (this exercise), dbt models, or custom PySpark pipelines. This separation keeps schema definitions version-controlled and repeatable while leaving transformation logic flexible.

## Steps

### 1. Review the IngenFab-generated silver DDL scripts

IngenFab's `--with-er-samples` scaffold created 5 DDL source scripts for the silver layer:

```
ddl_scripts/Lakehouses/lh_silver/001_Initial_Creation/
├── 001_silver_shipments.py
├── 002_silver_orders.py
├── 003_silver_products.py
├── 004_silver_suppliers.py
└── 005_silver_regions.py
```

When you ran `ingen_fab ddl compile --generation-mode Lakehouse` in Exercise 1, these 5 scripts were packaged into a **single compiled notebook** (`001_Initial_Creation_lh_silver_Lakehouses.Notebook`) with one cell per script. Each cell is wrapped in `du.run_once()` for idempotency — so re-running the notebook never recreates a table that already exists.

The DDL orchestrator in Exercise 1 ran that notebook, creating the empty silver tables.

Open `ddl_scripts/Lakehouses/lh_silver/001_Initial_Creation/001_silver_shipments.py` in your project to see the schema IngenFab defined:

```python
schema = StructType([
    StructField("shipment_id", StringType(), False),      # PK — not nullable
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("supplier_id", StringType(), True),
    StructField("origin_region_id", StringType(), True),
    StructField("destination_region_id", StringType(), True),
    StructField("shipment_date", DateType(), True),
    StructField("estimated_delivery_date", DateType(), True),
    StructField("actual_delivery_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_cost", DecimalType(12, 2), True),
    StructField("freight_cost", DecimalType(12, 2), True),
    StructField("total_cost", DecimalType(14, 2), True),
    StructField("scheduled_days", IntegerType(), True),
    StructField("actual_days", IntegerType(), True),
    StructField("is_on_time", BooleanType(), True),
    StructField("status", StringType(), True),
    StructField("priority", StringType(), True),
])

empty_df = target_lakehouse.spark.createDataFrame([], schema)
target_lakehouse.write_to_table(df=empty_df, table_name="shipments", schema_name="")
```

!!! note "How IngenFab DDL scripts work"
    Each source script uses the `target_lakehouse` helper (from IngenFab's `python_libs`) to create an empty Delta table with a strict schema. `ingen_fab ddl compile` packages all scripts from a folder into one notebook — each script becomes a cell wrapped in `du.run_once()`, so executing the notebook is fully idempotent. The `write_to_table` abstraction handles lakehouse path resolution and Delta format details.

### 2. Inspect deployed schemas with IngenFab

Use `deploy get-metadata` to inspect the silver tables that are now live in your Fabric workspace:

=== "macOS / Linux"

    ```bash
    ingen_fab deploy get-metadata \
        --target lakehouse \
        --lakehouse-name lh_silver \
        --format table
    ```

=== "Windows (PowerShell)"

    ```powershell
    ingen_fab deploy get-metadata `
        --target lakehouse `
        --lakehouse-name lh_silver `
        --format table
    ```

You should see all 5 tables (`shipments`, `orders`, `products`, `suppliers`, `regions`) with their column definitions — matching the schemas defined in the DDL scripts.

!!! tip "Export metadata for comparison"
    Save the metadata to a CSV file for later comparison:

    === "macOS / Linux"

        ```bash
        ingen_fab deploy get-metadata \
            --target lakehouse \
            --lakehouse-name lh_silver \
            --format csv \
            --output metadata/lh_silver_metadata.csv
        ```

    === "Windows (PowerShell)"

        ```powershell
        ingen_fab deploy get-metadata `
            --target lakehouse `
            --lakehouse-name lh_silver `
            --format csv `
            --output metadata/lh_silver_metadata.csv
        ```

### 3. Create a silver transformation notebook in Fabric

The tables exist but are empty — now you need transformation logic to populate them. In your Fabric workspace, open a **new notebook** and attach `lh_silver` as the **default lakehouse**.

!!! warning "Set `lh_silver` as the default lakehouse before running"
    The transformation cells use `saveAsTable("shipments")` with no lakehouse qualifier — Spark resolves unqualified table names to the notebook's **default lakehouse**. If you forget to set the default, or it is set to `lh_bronze`, the data will appear to write successfully but will land in the wrong lakehouse and `lh_silver` tables will remain empty.

    To set the default: in the Fabric notebook UI, click **Add lakehouse** in the left panel → select `lh_silver` → check **Set as default**.

Add the following cells:

**Cell 1 — Load bronze data:**

!!! note "Reading from synthetic data tables"
    The synthetic data generator (Exercise 2) writes tables using star schema names (`dim_product`, `fact_shipments`, etc.) rather than the OLTP names created by the DDL scripts (`products`, `shipments`, etc.). The bronze fact table uses **surrogate keys** (`product_key`, `supplier_key`, etc.) to reference dimensions, but the silver layer's DDL schema expects **natural keys** (`product_id`, `supplier_id`, etc.). Cells 2–3 below join the fact table back to the dimensions to resolve these natural keys — this is the core transformation that the silver layer performs.

```python
from pyspark.sql import functions as F

# Load bronze tables (synthetic data uses star schema naming).
# fact_shipments contains surrogate keys (product_key, supplier_key, etc.)
# that reference the dimension tables. The silver layer must resolve these
# back to natural keys (product_id, supplier_id, etc.).
df_shipments  = spark.read.table("lh_bronze.fact_shipments")
df_products   = spark.read.table("lh_bronze.dim_product")
df_suppliers  = spark.read.table("lh_bronze.dim_supplier")
df_regions    = spark.read.table("lh_bronze.dim_region")
df_dates      = spark.read.table("lh_bronze.dim_date")
```

**Cell 2 — Resolve surrogate keys to natural keys and clean:**

!!! info "Why the silver layer resolves surrogate keys"
    The bronze `fact_shipments` stores integer surrogate keys (`product_key`, `supplier_key`, `origin_region_key`, `destination_region_key`, `date_key`) because the synthetic data generator produced a star schema. The silver layer's role is to normalise this into OLTP-style records with natural business keys (`product_id`, `supplier_id`, `origin_region_id`, `destination_region_id`, `shipment_date`). The gold layer (Exercise 4) will then re-assign surrogate keys when building the analytics star schema.

```python
# Join fact_shipments to dimension tables to resolve surrogate keys → natural keys.
# Each LEFT JOIN brings in the natural key from the corresponding dimension.
# The dim_region table is joined twice (role-playing dimension) for origin and destination.
df_shipments_resolved = (
    df_shipments
    .join(df_products.select("product_key", "product_id"), on="product_key", how="left")
    .join(df_suppliers.select("supplier_key", "supplier_id"), on="supplier_key", how="left")
    .join(
        df_regions.select(F.col("region_key").alias("origin_region_key"),
                          F.col("region_id").alias("origin_region_id")),
        on="origin_region_key", how="left"
    )
    .join(
        df_regions.select(F.col("region_key").alias("destination_region_key"),
                          F.col("region_id").alias("destination_region_id")),
        on="destination_region_key", how="left"
    )
    .join(
        df_dates.select(F.col("date_key"),
                        F.col("full_date").alias("shipment_date")),
        on="date_key", how="left"
    )
    # Select only the columns that match the silver DDL schema
    .select(
        "shipment_id", "order_id", "product_id", "supplier_id",
        "origin_region_id", "destination_region_id", "shipment_date",
        "quantity", "unit_cost", "freight_cost", "total_cost",
        "scheduled_days", "actual_days", "is_on_time", "status", "priority"
    )
)

# Apply quality filters
df_shipments_clean = (
    df_shipments_resolved
    .dropDuplicates(["shipment_id"])
    .filter(F.col("shipment_id").isNotNull())
    .filter(F.col("quantity") > 0)
    .filter(F.col("total_cost") > 0)
)
print(f"Shipments: {df_shipments.count()} raw → {df_shipments_clean.count()} clean")
```

**Cell 3 — Enable Delta features:**

!!! info "Why you need to enable Delta features before writing"
    The DDL orchestrator (Exercise 1) created the silver tables with a minimal Delta protocol (supports: `appendOnly`, `invariants`). Fabric's Spark 3.4+ runtime requires the `timestampNtz` feature to be explicitly enabled on existing tables before any write that uses `overwriteSchema`. The `ALTER TABLE` command is idempotent — safe to re-run.

```python
# Enable timestampNtz on all silver tables so overwriteSchema writes succeed.
silver_tables = ["shipments", "orders", "products", "suppliers", "regions"]
for table in silver_tables:
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')")
print("✓ Delta features enabled on silver tables")
```

**Cell 4 — Write silver tables:**
```python
def write_silver(df, table_name):
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table_name))
    print(f"✓ Written {table_name}")

write_silver(df_shipments_clean, "shipments")
write_silver(df_products.dropDuplicates(["product_id"]), "products")
write_silver(df_suppliers.dropDuplicates(["supplier_id"]), "suppliers")
write_silver(df_regions.dropDuplicates(["region_id"]), "regions")
```

### 4. Run the notebook

Click **Run all** and verify all four cells complete without errors.

!!! tip "Production approach: dbt integration"
    In a production project, you would write these transformations as **dbt models** rather than manual notebooks. IngenFab supports this workflow with `ingen_fab dbt create-notebooks` — which converts dbt output into deployable Fabric notebooks. See the [dbt integration guide](../../user_guide/dbt_integration.md) for details.

## Verification

```python
# Use fully qualified names to verify data landed in the correct lakehouse,
# regardless of which lakehouse is currently set as the notebook default.
for table in ["shipments", "products", "suppliers", "regions"]:
    df = spark.read.table(f"lh_silver.{table}")
    print(f"lh_silver.{table}: {df.count()} rows")

# Verify the shipments schema has natural keys (not surrogate keys)
_cols = {f.name for f in spark.table("lh_silver.shipments").schema.fields}
assert "product_id" in _cols, f"Expected 'product_id' in silver shipments, got: {_cols}"
print("✓ Silver schema verified — natural keys present")
```

---

[← Exercise 2 — Generate Data](exercise-02-generate-data.md) | [Exercise 4 — Gold Layer →](exercise-04-gold-layer.md)
