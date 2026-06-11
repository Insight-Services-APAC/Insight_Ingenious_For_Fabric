# Exercise 2 — TMDL Anatomy

[Home](../../index.md) > [Training](../index.md) > [SM Project](index.md) > Exercise 2

## Learning Objectives

By the end of this exercise you will be able to:

- Read and interpret the key TMDL files that define a semantic model
- Identify data source expressions, column definitions, measures, and relationships
- Understand which parts of the TMDL change between environments

## Prerequisites

- [Exercise 1](exercise-01-download-and-version-control.md) completed — TMDL files are in `fabric_workspace_items/SemanticModel/`

## Background

**TMDL** (Tabular Model Definition Language) is a human-readable, text-based format for describing Analysis Services / semantic models. It replaced the monolithic JSON-based `model.bim` format, making semantic models diffable and version-control friendly.

Each `.tmdl` file uses an indentation-based syntax (similar to YAML) with keywords like `table`, `column`, `measure`, `relationship`, and `expression`.

## Steps

### 1. Examine `model.tmdl`

Open the top-level model file:

```bash
cat fabric_workspace_items/SemanticModel/<semantic_model_name>.SemanticModel/definition/model.tmdl
```

You will see something like:

```tmdl
model Model
    culture: en-AU
    defaultPowerBIDataSourceVersion: powerBI_V3
    sourceQueryCulture: en-AU
```

| Property | Meaning |
|----------|---------|
| `culture` | Locale for formatting (dates, numbers) |
| `defaultPowerBIDataSourceVersion` | Protocol version for Power BI connectivity |
| `sourceQueryCulture` | Locale used when generating source queries |

This file rarely needs editing — it is the same across environments.

### 2. Examine a table file

Open one of the table definitions:

```bash
cat fabric_workspace_items/SemanticModel/<semantic_model_name>.SemanticModel/definition/tables/vDim_Cities.tmdl
```

A typical table TMDL looks like:

```tmdl
table vDim_Cities
    lineageTag: 7a3b1c2d-...

    column CityID
        dataType: int64
        formatString: 0
        lineageTag: ...
        summarizeBy: none
        sourceColumn: CityID

    column CityName
        dataType: string
        lineageTag: ...
        summarizeBy: none
        sourceColumn: CityName

    column LatestRecordedPopulation
        dataType: int64
        formatString: #,0
        lineageTag: ...
        summarizeBy: sum
        sourceColumn: LatestRecordedPopulation

    measure 'Total Population' =
        SUM(vDim_Cities[LatestRecordedPopulation])
        lineageTag: ...
        formatString: #,0

    measure 'City Count' =
        COUNTROWS(vDim_Cities)
        lineageTag: ...
        formatString: #,0

    partition vDim_Cities = entity
        mode: directLake
        source
            entityName: vDim_Cities
            schemaName: DW
            expressionSource: DatabaseQuery
```

Key sections to note:

| Section | Purpose |
|---------|---------|
| `column` blocks | Define each column's data type, format, and summarization |
| `measure` blocks | DAX expressions — the business logic of your model |
| `partition` block | Tells the model **where** data comes from — this is the part that changes per environment |
| `lineageTag` | Unique GUID for internal tracking — do not edit these |

### 3. Examine the source expression

The `partition` block references `expressionSource: DatabaseQuery`. This points to a shared expression defined in `expressions.tmdl` (or within the table file itself, depending on model structure).

```bash
cat fabric_workspace_items/SemanticModel/<semantic_model_name>.SemanticModel/definition/expressions.tmdl
```

For a **Direct Lake on SQL** model connecting to a Warehouse, you may see:

```tmdl
expression DatabaseQuery =
    let
        database = Sql.Database("your-server.datawarehouse.fabric.microsoft.com", "wh_gold")
    in
        database
    lineageTag: ...
```

For a **Direct Lake on OneLake** model connecting to a Lakehouse:

```tmdl
expression DatabaseQuery =
    let
        Source = Lakehouse.Contents("514ebe8f-2bf9-4a31-88f7-13d84706431c"),
        Navigation = Source{[workspaceId="544530ea-a8c9-4464-8878-f666d2a8f418"]}[Data]
    in
        Navigation
    lineageTag: ...
```

!!! warning "Environment-specific values"
    Notice the hard-coded GUIDs and server names in the expression. These **will differ** between your development, test, and production workspaces. Exercise 3 shows you how to parameterize them.

### 4. Examine relationships

```bash
cat fabric_workspace_items/SemanticModel/<semantic_model_name>.SemanticModel/definition/relationships.tmdl
```

```tmdl
relationship 7f2a...
    fromColumn: vDim_CityGeography.CityID
    toColumn: vDim_Cities.CityID
```

| Property | Meaning |
|----------|---------|
| `fromColumn` | The "many" side of the relationship |
| `toColumn` | The "one" side (the lookup table) |

Relationships are environment-agnostic — they reference table and column names, not GUIDs.

### 5. Identify what changes between environments

Review each file and classify it:

| File | Environment-specific? | Why |
|------|----------------------|-----|
| `model.tmdl` | No | Culture and protocol settings are global |
| `tables/*.tmdl` | No | Column definitions and DAX measures are the same everywhere |
| `relationships.tmdl` | No | References table/column names, not IDs |
| `expressions.tmdl` | **Yes** | Contains workspace IDs, lakehouse IDs, or server names |
| Table `partition` blocks | **Possibly** | If they inline source expressions with IDs |

The key takeaway: **data source expressions are the only part that must be parameterized** for multi-environment deployment. Everything else — columns, measures, relationships — stays identical.

## Verification

- [ ] You can identify the DAX measures in your table TMDL files
- [ ] You can locate the data source expression (either in `expressions.tmdl` or inlined in a table's `partition` block)
- [ ] You can identify which GUIDs or server names are environment-specific

## Notes

- TMDL is documented in the [Microsoft TMDL overview](https://learn.microsoft.com/en-us/analysis-services/tmdl/tmdl-overview)
- `lineageTag` values are stable GUIDs assigned when the object is created — changing them can break report bindings
- If you add a new measure or column, you can edit the TMDL directly in your editor and deploy — there is no need to round-trip through the Fabric UI

---

[← Exercise 1 — Download & Version Control](exercise-01-download-and-version-control.md) | **Next:** [Exercise 3 — Variable Substitution →](exercise-03-variable-substitution.md)
