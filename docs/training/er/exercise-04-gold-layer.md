# Exercise 4 — Gold Layer (Star Schema)

[Home](../../index.md) > [Training](../index.md) > [ER Project](index.md) > Exercise 4

## Learning Objectives

By the end of this exercise you will be able to:

- Review the star schema DDL scripts that IngenFab generated for `lh_gold`
- Understand surrogate key assignment and conformed dimension design
- Use `ingen_fab deploy get-metadata` to inspect the gold layer schema
- Build a transformation notebook to populate IngenFab's star schema tables
- Verify the gold star schema in `lh_gold`

## Background

The **gold layer** (`lh_gold`) holds the analytics-ready star schema. When you scaffolded the project with `--with-er-samples`, IngenFab defined this star schema in DDL scripts — including surrogate keys, foreign key relationships, and typed measure columns. The DDL orchestrator in Exercise 1 created these empty tables. In this exercise you will review IngenFab's design, then write a notebook to populate it.

```
                    ┌─────────────────┐
                    │  dim_date       │
                    └────────┬────────┘
                             │ date_key
┌──────────────┐    ┌────────┴─────────────┐    ┌──────────────────┐
│  dim_product │────┤   fact_shipments     ├────│   dim_supplier   │
└──────────────┘    │                      │    └──────────────────┘
                    │  - total_cost        │
                    │  - scheduled_days    │
┌──────────────┐    │  - actual_days       │    ┌──────────────────┐
│  dim_region  │────┤  - is_on_time        ├────│   dim_region     │
│  (origin)    │    └──────────────────────┘    │  (destination)   │
└──────────────┘                                └──────────────────┘
```

## Steps

### 1. Review the IngenFab-generated gold DDL scripts

IngenFab created 5 DDL scripts for the gold layer star schema:

```
ddl_scripts/Lakehouses/lh_gold/001_Initial_Creation/
├── 001_dim_date.py
├── 002_dim_product.py
├── 003_dim_supplier.py
├── 004_dim_region.py
└── 005_fact_shipments.py
```

Open `002_dim_product.py` to see how IngenFab defines a dimension table:

```python
schema = StructType([
    StructField("product_key", IntegerType(), False),      # Surrogate PK
    StructField("product_id", StringType(), True),          # Natural key
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("unit_weight_kg", DecimalType(8, 2), True),
])

empty_df = target_lakehouse.spark.createDataFrame([], schema)
target_lakehouse.write_to_table(df=empty_df, table_name="dim_product", schema_name="")
```

Now open `005_fact_shipments.py` to see the central fact table:

```python
schema = StructType([
    StructField("shipment_key", IntegerType(), False),            # Surrogate PK
    StructField("shipment_id", StringType(), True),               # Natural key (degenerate)
    StructField("order_id", StringType(), True),                  # Degenerate dimension
    StructField("product_key", IntegerType(), True),              # FK → dim_product
    StructField("supplier_key", IntegerType(), True),             # FK → dim_supplier
    StructField("origin_region_key", IntegerType(), True),        # FK → dim_region
    StructField("destination_region_key", IntegerType(), True),   # FK → dim_region
    StructField("date_key", IntegerType(), True),                 # FK → dim_date
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
target_lakehouse.write_to_table(df=empty_df, table_name="fact_shipments", schema_name="")
```

!!! info "IngenFab's star schema design decisions"
    Notice the patterns IngenFab applied:

    - **Surrogate keys** (`product_key`, `supplier_key`, etc.) — integer keys for efficient joins and indexing, separate from the natural business keys
    - **Conformed dimensions** — `dim_region` is referenced twice by the fact table (`origin_region_key` and `destination_region_key`), a role-playing dimension pattern
    - **Typed measures** — cost columns use `DecimalType(12, 2)` / `DecimalType(14, 2)` for precision; `is_on_time` is `BooleanType` for clean filtering
    - **Degenerate dimensions** — `shipment_id` and `order_id` are kept on the fact table directly, avoiding unnecessary dimension lookups

    Your transformation notebook below will implement this exact design.

### 2. Inspect deployed schemas with IngenFab

Use `deploy get-metadata` to inspect the gold tables live in your Fabric workspace:

=== "macOS / Linux"

    ```bash
    ingen_fab deploy get-metadata \
        --target lakehouse \
        --lakehouse-name lh_gold \
        --format table
    ```

=== "Windows (PowerShell)"

    ```powershell
    ingen_fab deploy get-metadata `
        --target lakehouse `
        --lakehouse-name lh_gold `
        --format table
    ```

You should see all 5 tables (`dim_date`, `dim_product`, `dim_supplier`, `dim_region`, `fact_shipments`) with their column definitions — including the surrogate keys and foreign key columns.

### 3. Create a gold transformation notebook in Fabric

The star schema tables exist but are empty — now you populate them. Open a **new notebook** in your Fabric workspace and attach `lh_gold` as the **default lakehouse**.

!!! warning "Set `lh_gold` as the default lakehouse before running"
    The transformation cells use unqualified `saveAsTable("dim_product")` etc. — Spark resolves these to the notebook's **default lakehouse**. If the default is not set to `lh_gold`, writes succeed silently but data lands in the wrong lakehouse.

    To set the default: click **Add lakehouse** in the left panel → select `lh_gold` → check **Set as default**.

**Cell 1 — Load silver:**

!!! note "Why `dim_date` comes from bronze"
    The synthetic data package you ran in Exercise 2 generated `dim_date` directly into `lh_bronze` as a pre-built calendar dimension. Because it is already a clean, complete dimension table, it does not require silver-layer cleansing and is loaded directly from bronze.

```python
# Load source tables from silver (cleansed) and bronze (pre-built calendar dimension).
# Using fully qualified names (lakehouse.table) ensures Spark resolves each table
# against the correct lakehouse regardless of which is set as the notebook default.

# Pre-flight: verify lh_silver.shipments has the expected silver schema.
# If it shows product_key/shipment_key instead of product_id/shipment_id, the table
# has been physically overwritten — see the warning below Cell 4 for recovery steps.
_silver_cols = {f.name for f in spark.table("lh_silver.shipments").schema.fields}
assert "product_id" in _silver_cols, (
    "lh_silver.shipments has wrong schema — missing 'product_id'. "
    "The table was overwritten with gold fact data. "
    "Run: spark.sql('DESCRIBE HISTORY lh_silver.shipments').show(10, False) "
    "then restore: spark.sql('RESTORE TABLE lh_silver.shipments TO VERSION AS OF <N>')"
)
print("✓ lh_silver.shipments schema is correct")

df_shipments = spark.read.table("lh_silver.shipments")
df_products  = spark.read.table("lh_silver.products")
df_suppliers = spark.read.table("lh_silver.suppliers")
df_regions   = spark.read.table("lh_silver.regions")
df_date      = spark.read.table("lh_bronze.dim_date")  # pre-built calendar dimension from Exercise 2
```

!!! info "Why you need to enable Delta features before writing"
    The gold tables were created in Exercise 1 by the DDL orchestrator, which initialised them with a minimal Delta protocol (supports: `appendOnly`, `invariants`). Fabric's Spark 3.4+ runtime requires the `timestampNtz` feature to be explicitly enabled on existing tables before any write that uses `overwriteSchema`. Cell 2 below enables it on all five gold tables in a single step. The `ALTER TABLE` command is idempotent — safe to re-run.

**Cell 2 — Enable Delta features:**
```python
# The DDL orchestrator (Exercise 1) created the gold tables with a minimal Delta
# protocol. Fabric's Spark 3.4+ requires the timestampNtz feature to be explicitly
# enabled on existing tables before any overwriteSchema write. This loop enables it
# on all five gold tables at once. The command is idempotent — safe to re-run.
gold_tables = ["dim_product", "dim_supplier", "dim_region", "dim_date", "fact_shipments"]
for table in gold_tables:
    spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported')")
print("✓ Delta features enabled on gold tables")
```

**Cell 3 — Write dimension tables:**
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assign integer surrogate keys by ordering on the natural business key.
# row_number() produces a stable 1-based sequence; the Window order ensures
# the same key is assigned to the same row on each full overwrite.
def add_surrogate_key(df, key_col, id_col):
    w = Window.orderBy(id_col)
    return df.withColumn(key_col, F.row_number().over(w))

df_dim_product  = add_surrogate_key(df_products, "product_key",  "product_id")
df_dim_supplier = add_surrogate_key(df_suppliers, "supplier_key", "supplier_id")
df_dim_region   = add_surrogate_key(df_regions,   "region_key",   "region_id")

# Write each dimension as a full overwrite. overwriteSchema replaces the empty
# DDL-created schema with the enriched schema including the new surrogate key column.
# dim_date is written as-is — it already has date_key from the synthetic data package.
for df, name in [
    (df_dim_product,  "dim_product"),
    (df_dim_supplier, "dim_supplier"),
    (df_dim_region,   "dim_region"),
    (df_date,         "dim_date"),
]:
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(name))
    print(f"✓ {name}: {df.count()} rows")
```

**Cell 4 — Build and write fact table:**

!!! warning "If Cell 1 raises an `AssertionError` about `lh_silver.shipments` wrong schema"
    **Root cause**: `lh_silver.shipments` was physically overwritten with gold fact data during a failed run. Re-running Exercise 3 alone will not fix this — Exercise 3 uses `saveAsTable("shipments")` (unqualified), so if `lh_gold` is the active default lakehouse it writes to `lh_gold.shipments`, leaving `lh_silver.shipments` unchanged.

    **To recover using Delta time travel:**

    1. In this notebook, run:
       ```python
       spark.sql("DESCRIBE HISTORY lh_silver.shipments").show(10, False)
       ```
    2. Find the version written by Exercise 3 — it will be the last write before the fact schema appeared. Note that version number (e.g. `1`).
    3. Restore:
       ```python
       spark.sql("RESTORE TABLE lh_silver.shipments TO VERSION AS OF 1")  # adjust version
       ```
    4. Verify: `spark.sql("DESCRIBE lh_silver.shipments").show()` should show `product_id`, `supplier_id`, `origin_region_id`, `shipment_date`.
    5. Re-run Cells 1–4 in order.

```python
# Join silver shipments to the gold dimension tables to produce the star schema fact table.
# Spark SQL is used here because it resolves all table names at parse/analysis time,
# which is more predictable than the Python DataFrame API after Delta writes have updated
# the metastore in earlier cells.
# lh_silver.shipments is fully qualified; unqualified dimension names (dim_product etc.)
# resolve to lh_gold because that is the notebook's default lakehouse.
spark.sql("""
    CREATE OR REPLACE TABLE fact_shipments AS
    SELECT
        CAST(ROW_NUMBER() OVER (ORDER BY s.shipment_id) AS INT) AS shipment_key,
        s.shipment_id,
        s.order_id,
        p.product_key,
        sup.supplier_key,
        ro.region_key  AS origin_region_key,
        rd.region_key  AS destination_region_key,
        d.date_key,
        s.quantity,
        s.unit_cost,
        s.freight_cost,
        s.total_cost,
        s.scheduled_days,
        s.actual_days,
        s.is_on_time,
        s.status,
        s.priority
    FROM  lh_silver.shipments   s
    LEFT JOIN dim_product  p   ON s.product_id            = p.product_id
    LEFT JOIN dim_supplier sup ON s.supplier_id           = sup.supplier_id
    LEFT JOIN dim_region   ro  ON s.origin_region_id      = ro.region_id
    LEFT JOIN dim_region   rd  ON s.destination_region_id = rd.region_id
    LEFT JOIN dim_date     d   ON s.shipment_date         = d.full_date
""")
print(f"✓ fact_shipments: {spark.table('fact_shipments').count()} rows")
```

### 4. Run the notebook

Click **Run all** and verify all cells complete.

!!! tip "Production approach: dbt integration"
    In a production project, you would write these transformations as **dbt models** and use `ingen_fab dbt create-notebooks` to convert them into deployable Fabric notebooks. This keeps transformation logic version-controlled alongside IngenFab's schema definitions.

## Verification

```python
tables = ["dim_date", "dim_product", "dim_supplier", "dim_region", "fact_shipments"]
for t in tables:
    print(f"lh_gold.{t}: {spark.read.table(t).count()} rows")
```

---

[← Exercise 3 — Silver Layer](exercise-03-silver-layer.md) | [Exercise 5 — Warehouse Views →](exercise-05-warehouse-views.md)
