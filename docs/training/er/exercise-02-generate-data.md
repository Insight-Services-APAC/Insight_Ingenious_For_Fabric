# Exercise 2 — Generate Synthetic Supply Chain Data

[Home](../../index.md) > [Training](../index.md) > [ER Project](index.md) > Exercise 2

## Learning Objectives

By the end of this exercise you will be able to:

- Explore IngenFab's synthetic data generation capabilities
- Browse the catalogue of predefined datasets and templates
- Compile a synthetic data notebook for the supply chain star schema
- Inspect the generated notebook and DDL artefacts
- Understand how these artefacts would be deployed to Fabric

## Background

IngenFab includes a **synthetic data generation** package that can create realistic datasets for testing and demonstration purposes. The package ships with multiple predefined datasets across different domains (retail, finance, healthcare, e-commerce, supply chain) in both small and large variants.

For the Enterprise Reporting tutorial we use the `supply_chain_star_small` predefined dataset which generates:

| Table | Rows | Description |
|---|---|---|
| `dim_product` | 200 | Product catalogue with categories and sub-categories |
| `dim_supplier` | 50 | Supplier master data with performance ratings |
| `dim_region` | 20 | Global shipping regions |
| `dim_date` | 3,653 | 10-year date dimension (2015–2024) |
| `fact_shipments` | 5,000 | Shipment transactions with costs and delivery performance |

!!! note "Target: lh_bronze"
    When deployed and run in Fabric, synthetic data writes directly to **lh_bronze** Delta tables. The silver and gold layers will be populated in subsequent exercises via transformation notebooks.

## Steps

### 1. List available datasets

Browse the full catalogue of predefined datasets, incremental datasets, and templates:

```bash
ingen_fab package synthetic-data list
```

You should see output including:

- **Datasets** — predefined configurations like `supply_chain_star_small`, `retail_oltp_small`, `finance_oltp_small`, and more
- **Incremental Datasets** — date-based incremental variants of each dataset (e.g. `supply_chain_star_small_incremental`)
- **Templates** — enhanced configurable templates (e.g. `retail_oltp_enhanced`)
- **Generic Templates** — runtime-parameterised templates for lakehouse and warehouse environments

!!! tip "Filter the list"
    Use `--type` to filter what you see:
    ```bash
    ingen_fab package synthetic-data list --type datasets
    ingen_fab package synthetic-data list --type templates
    ```
    Or use `--format json` for machine-readable output.

### 2. Compile the synthetic data notebook

Compile generates a deployable Fabric notebook **and** supporting DDL configuration scripts:

```bash
ingen_fab package synthetic-data compile supply_chain_star_small
```

You should see output confirming:

- A **notebook** was created under `fabric_workspace_items/synthetic_data_generation/`
- **DDL scripts** were copied to `ddl_scripts/Lakehouses/Config/001_Initial_Creation_SyntheticData/`

### 3. Inspect the generated artefacts

Take a look at what was generated:

```bash
# The Fabric notebook that will generate data when run in Fabric
ls fabric_workspace_items/synthetic_data_generation/

# Supporting DDL scripts for config lakehouse tables
ls ddl_scripts/Lakehouses/Config/001_Initial_Creation_SyntheticData/
```

Open the generated notebook file to examine its structure.

!!! info "What the notebook contains"
    The generated notebook:

    - Detects whether it is running in Fabric or locally
    - Loads shared Python libraries from the `config` lakehouse
    - Generates synthetic data using `faker` and seed-based random generation for reproducibility
    - Writes the resulting DataFrames to Delta tables in `lh_bronze`

### 4. Try a dry-run of the generate command

The `generate` command creates a ready-to-run notebook (unlike `compile`, which creates a deployable template). Use `--dry-run` to validate without creating files:

```bash
ingen_fab package synthetic-data generate supply_chain_star_small --dry-run
```

Or generate the notebook without executing it:

```bash
ingen_fab package synthetic-data generate supply_chain_star_small --no-execute
```

!!! tip "Explore other datasets"
    Try compiling a different dataset to see how the generated notebooks differ:
    ```bash
    ingen_fab package synthetic-data compile retail_star_small
    ingen_fab package synthetic-data compile finance_oltp_small
    ```

### 5. What happens next (in Fabric)

When you are ready to deploy to a real Fabric workspace, the workflow is:

1. **Deploy** the compiled artefacts: `ingen_fab deploy deploy`
2. **Upload** library files: `ingen_fab deploy upload-python-libs`
3. **Run** the synthetic data notebook from within your Fabric workspace. The notebook name is `Synthetic Data Generation - Supply Chain Star Schema - Small`.
4. **Verify** the data in `lh_bronze` using a Spark notebook:

```python
for table in ["dim_product", "dim_region", "dim_supplier", "fact_shipments", "dim_date"]:
    df = spark.read.table(table)
    print(f"{table}: {df.count()} rows")
```

Expected output:
```
dim_product: 200 rows
dim_region: 20 rows
dim_supplier: 50 rows
fact_shipments: 5000 rows
dim_date: 2192 rows
```

!!! note "Synthetic data table names differ from DDL tables"
    The synthetic data generator writes tables using **star schema naming** (`dim_product`, `fact_shipments`, etc.), while the DDL scripts in Exercise 1 created tables with **OLTP naming** (`products`, `shipments`, etc.). This is expected — the bronze layer is a raw landing zone, and the silver layer transformation in Exercise 3 will read from the synthetic data tables and write to the conformed OLTP names.

!!! tip "Scale up for more data"
    For more data volume, use `supply_chain_star_large` instead:
    ```bash
    ingen_fab package synthetic-data compile supply_chain_star_large
    ```
    This generates 1 million shipment rows — useful for testing report performance.

---

[← Exercise 1 — Project Setup](exercise-01-project-setup.md) | [Exercise 3 — Silver Layer →](exercise-03-silver-layer.md)
