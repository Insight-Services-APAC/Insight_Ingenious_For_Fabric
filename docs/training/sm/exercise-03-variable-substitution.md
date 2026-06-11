# Exercise 3 — Variable Substitution

[Home](../../index.md) > [Training](../index.md) > [SM Project](index.md) > Exercise 3

## Learning Objectives

By the end of this exercise you will be able to:

- Replace hard-coded workspace and lakehouse IDs in TMDL files with `{{varlib:...}}` placeholders
- Understand how IngenFab resolves these placeholders at deploy time
- Configure the Variable Library with environment-specific values

## Prerequisites

- [Exercise 2](exercise-02-tmdl-anatomy.md) completed — you can identify the environment-specific values in your TMDL files
- Your project has a Variable Library configuration under `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/`

## Background

When you downloaded your semantic model in Exercise 1, the TMDL files contained **hard-coded GUIDs** — the workspace ID, lakehouse ID, or warehouse server name from the workspace where the model was created. If you deploy these files to a different workspace (e.g. test or production), the model will still point to the original development data sources.

IngenFab's **variable replacement system** solves this. You replace the hard-coded values with `{{varlib:variable_name}}` placeholders, and during `ingen_fab deploy deploy`, the system substitutes them with the correct values for the target environment.

## Steps

### 1. Identify the hard-coded values

Open your data source expression file. Depending on your model type, this will be either:

- `fabric_workspace_items/SemanticModel/<semantic_model_name>.SemanticModel/definition/expressions.tmdl`, or
- Inlined in a table file under `fabric_workspace_items/SemanticModel/<semantic_model_name>.SemanticModel/definition/tables/`

**For a Direct Lake on OneLake (Lakehouse) model**, you will see something like:

```tmdl
expression DatabaseQuery =
    let
        Source = Lakehouse.Contents("<guid>"),
        Navigation = Source{[workspaceId="<guid>"]}[Data]
    in
        Navigation
    lineageTag: ...
```

The two GUIDs are:

| Value | Variable |
|-------|----------|
| `514ebe8f-...` (in `Lakehouse.Contents`) | The lakehouse ID |
| `544530ea-...` (in `workspaceId`) | The workspace ID |

**For a Direct Lake on SQL (Warehouse) model**, you will see:

```tmdl
expression DatabaseQuery =
    let
        database = Sql.Database("your-server.datawarehouse.fabric.microsoft.com", "wh_gold")
    in
        database
    lineageTag: ...
```

The environment-specific values here are the server name and warehouse name.

### 2. Check your Variable Library

List the available variables for your current environment:

```bash
cat fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json
```

You should see entries like:

```json
{
  "variableOverrides": [
    {
      "name": "fabric_deployment_workspace_id",
      "value": "<guid>"
    },
    {
      "name": "config_lakehouse_id",
      "value": "<guid>"
    }
  ]
}
```

!!! tip "Adding new variables"
    If your model references a value that does not yet have a variable (for example, a warehouse server name), add a new entry to the Variable Library JSON. Use a descriptive name like `gold_warehouse_server` or `gold_warehouse_name`.

### 3. Replace hard-coded values with placeholders

Edit the expression file to swap GUIDs for `{{varlib:...}}` placeholders.

**Before (Lakehouse model):**

```tmdl
expression DatabaseQuery =
    let
        Source = Lakehouse.Contents("<guid>"),
        Navigation = Source{[workspaceId="<guid>"]}[Data]
    in
        Navigation
    lineageTag: abc123-def456
```

**After:**

```tmdl
expression DatabaseQuery =
    let
        Source = Lakehouse.Contents("{{varlib:config_lakehouse_id}}"),
        Navigation = Source{[workspaceId="{{varlib:fabric_deployment_workspace_id}}"]}[Data]
    in
        Navigation
    lineageTag: abc123-def456
```

**Before (Warehouse model):**

```tmdl
expression DatabaseQuery =
    let
        database = Sql.Database("myserver.datawarehouse.fabric.microsoft.com", "wh_gold")
    in
        database
    lineageTag: abc123-def456
```

**After:**

```tmdl
expression DatabaseQuery =
    let
        database = Sql.Database("{{varlib:gold_warehouse_server}}.datawarehouse.fabric.microsoft.com", "{{varlib:gold_warehouse_name}}")
    in
        database
    lineageTag: abc123-def456
```

!!! warning "Do not modify `lineageTag` values"
    Leave all `lineageTag` GUIDs untouched. These are internal identifiers that Power BI uses to track objects — replacing them with placeholders will break report bindings.

### 4. Add variables for other environments

If you have test or production environments, create or update their Variable Library files:

```bash
ls fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/
```

For example, create `test.json`:

```json
{
  "variableOverrides": [
    {
      "name": "fabric_deployment_workspace_id",
      "value": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    },
    {
      "name": "config_lakehouse_id",
      "value": "11111111-2222-3333-4444-555555555555"
    }
  ]
}
```

Replace the placeholder GUIDs above with the actual IDs from your test workspace. You can find them in the Fabric UI under the lakehouse or warehouse **Settings** page.

### 5. Verify the substitution will work

You can do a dry-run check by searching for unresolved placeholders. The full substitution only happens during `ingen_fab deploy deploy`, but you can verify your placeholders are well-formed:

```bash
grep -rn '{{varlib:' fabric_workspace_items/SemanticModel/
```

Each match should correspond to a variable name that exists in your Variable Library JSON. If you see a placeholder whose name does not match any variable, deployment will leave it unresolved.

### 6. Commit the parameterized TMDL

```bash
git add fabric_workspace_items/SemanticModel/<semantic_model_name>.SemanticModel/
git commit -m "feat: parameterize semantic model TMDL with varlib placeholders"
```

## Verification

- [ ] All hard-coded workspace/lakehouse/warehouse IDs in TMDL files have been replaced with `{{varlib:...}}` placeholders
- [ ] Each placeholder name matches a variable defined in `development.json`
- [ ] `lineageTag` values remain unchanged (still contain raw GUIDs)
- [ ] `grep -rn '{{varlib:' fabric_workspace_items/SemanticModel/` shows only your intentional placeholders

## Notes

- The full variable replacement reference is in the [Developer Guide — Variable Replacement](../../developer_guide/variable_replacement.md)
- Variable substitution applies to all `.tmdl` files recursively during deployment — table files, expression files, and any other TMDL content
- The same `{{varlib:...}}` syntax is used across all artefact types (notebooks, pipelines, GraphQL APIs, reports) for a consistent experience

---

[← Exercise 2 — TMDL Anatomy](exercise-02-tmdl-anatomy.md) | **Next:** [Exercise 4 — Deploy & Promote →](exercise-04-deploy-and-promote.md)
