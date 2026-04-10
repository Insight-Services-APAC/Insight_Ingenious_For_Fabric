# Exercise 1 — Download & Version Control

[Home](../../index.md) > [Training](../index.md) > [SM Project](index.md) > Exercise 1

## Learning Objectives

By the end of this exercise you will be able to:

- Download a semantic model definition from Fabric using the IngenFab CLI
- Understand where the downloaded files land in the project structure
- Commit the TMDL files to Git for version control

## Prerequisites

- A semantic model exists in your Fabric workspace (from DP Exercise 5, ER Exercise 6, or any model you created)
- You are in an IngenFab project directory with environment variables set:

```bash
export FABRIC_WORKSPACE_REPO_DIR="$(pwd)"
export FABRIC_ENVIRONMENT="development"
```

## Background

Semantic models created in the Fabric UI are stored in the service — they are not automatically part of your source control. To bring them under version control, IngenFab provides the `download-artefact` command which calls the Fabric REST API, retrieves the full model definition (as **TMDL** — Tabular Model Definition Language files), and writes them to your local repository.

Once downloaded, the TMDL files can be:

- Committed to Git alongside your DDL scripts and notebooks
- Parameterized with `{{varlib:...}}` placeholders for multi-environment deployment
- Deployed back to Fabric via `ingen_fab deploy deploy`

## Steps

### 1. List your workspace to confirm the semantic model

Before downloading, verify the model exists. You can do this in the Fabric UI or by checking the workspace items list.

!!! note "Model name"
    The examples below use the DP project's `sm_dp_geography`. Replace with your own model name if you are using a different semantic model.

### 2. Download the semantic model

Run the download command from your project root:

```bash
ingen_fab deploy download-artefact \
  --artefact-name "sm_dp_geography" \
  --artefact-type SemanticModel
```

Expected output:

```
✅ Downloaded artefact 'sm_dp_geography' (SemanticModel)
  Saved to: fabric_workspace_items/downloaded/SemanticModel/sm_dp_geography.SemanticModel/
```

!!! tip "Overwriting existing files"
    If you have already downloaded this model before, add `--force` to overwrite:
    ```bash
    ingen_fab deploy download-artefact \
      --artefact-name "sm_dp_geography" \
      --artefact-type SemanticModel \
      --force
    ```

### 3. Inspect the downloaded files

List the downloaded directory:

```bash
find fabric_workspace_items/downloaded/SemanticModel/<semantic_model_name>.SemanticModel -type f
```

You should see a structure like:

```
fabric_workspace_items/downloaded/SemanticModel/sm_dp_geography.SemanticModel/
├── definition.pbism
├── definition/
│   ├── database.tmdl
│   ├── model.tmdl
│   ├── tables/
│   │   ├── vDim_Cities.tmdl
│   │   └── vDim_CityGeography.tmdl
│   ├── relationships.tmdl
│   └── expressions.tmdl
└── .platform
```

The key files are:

| File | Purpose |
|------|---------|
| `model.tmdl` | Top-level model settings (compatibility level, culture, default mode) |
| `database.tmdl` | Database-level metadata for the semantic model definition |
| `tables/*.tmdl` | One file per table — columns, measures, source expressions |
| `relationships.tmdl` | Table relationships (cardinality, cross-filter direction) |
| `expressions.tmdl` | Shared M (Power Query) expressions used by tables |
| `definition.pbism` | Power BI semantic model metadata file stored alongside the definition folder |
| `.platform` | Fabric platform metadata (item ID, type) |

### 4. Move the files into the deployable location

The `downloaded/` folder is a **staging area**. To include the semantic model in future deployments, copy it into the main `fabric_workspace_items/` directory and remove the staging copy:

```bash
mkdir -p fabric_workspace_items/SemanticModel
cp -r fabric_workspace_items/downloaded/SemanticModel/sm_dp_geography.SemanticModel \
  fabric_workspace_items/SemanticModel/sm_dp_geography.SemanticModel

# Remove the staging copy to prevent duplicate-item errors during deploy
rm -rf fabric_workspace_items/downloaded/SemanticModel/sm_dp_geography.SemanticModel
```

!!! warning "Always remove the staging copy"
    `ingen_fab deploy deploy` scans the **entire** `fabric_workspace_items/` directory — including `downloaded/`. If the same item exists in both locations, deployment will fail with `Duplicate logicalId` errors.

Your project structure should now look like:

```
your-project/
├── ddl_scripts/
│   └── ...
├── fabric_workspace_items/
│   ├── SemanticModel/
│   │   └── sm_dp_geography.SemanticModel/  ← ready for deployment
│   │       ├── definition.pbism
│   │       ├── definition/
│   │       │   ├── database.tmdl
│   │       │   ├── model.tmdl
│   │       │   ├── tables/
│   │       │   ├── relationships.tmdl
│   │       │   └── expressions.tmdl
│   │       └── .platform
│   └── downloaded/                  ← should be empty for this type
└── ingen_fab.yml
```

### 5. Commit to Git

```bash
git add fabric_workspace_items/SemanticModel/sm_dp_geography.SemanticModel/
git commit -m "feat: add sm_dp_geography semantic model TMDL definition"
```

## Verification

- [ ] The `fabric_workspace_items/SemanticModel/sm_dp_geography.SemanticModel/` directory exists and contains `.tmdl` files
- [ ] `git log --oneline -1` shows your commit with the semantic model files
- [ ] Running `find fabric_workspace_items/SemanticModel -name "*.tmdl"` returns at least `model.tmdl` and one table file

## Notes

- The `download-artefact` command works for many artefact types — Notebooks, Reports, DataPipelines, GraphQLApis, and more. Run `ingen_fab deploy download-artefact --help` for the full list.
- The `.platform` file contains the Fabric item ID. During deployment, IngenFab uses the artefact **name** to match items, so the ID is informational.
- If your model uses **Direct Lake on SQL** mode, the TMDL will contain M expressions referencing the Warehouse SQL endpoint. If it uses **Direct Lake on OneLake**, references will point to lakehouse Parquet paths.

---

**Next:** [Exercise 2 — TMDL Anatomy →](exercise-02-tmdl-anatomy.md)
