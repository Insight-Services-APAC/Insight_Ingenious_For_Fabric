# Semantic Models and Reports

[Home](../index.md) > [User Guide](index.md) > Semantic Models and Reports

Deploy Power BI semantic models and reports with `ingen_fab deploy deploy`, with the report
bound to its model in every environment and no extra configuration file.

## Overview

A semantic model and a report are two Fabric items. The model points at data (here, a
warehouse in the target environment); the report points at the model. The two links are
resolved differently:

| Link | How it is resolved | Who does it |
| --- | --- | --- |
| Model to warehouse | `{{varlib:...}}` placeholders in the model's `expressions.tmdl`, replaced from the value set of the target environment | ingen_fab, at deploy time (same substitution as notebooks and pipelines) |
| Report to model | a relative path (`byPath`) in the report's `definition.pbir`, resolved to the item id of that model in the target workspace, whether published in the same run or already there | fabric-cicd, at publish time, automatically |

The sample project ships one of each: `sm_gold_cities.SemanticModel` and
`rpt_gold_cities.Report`, used as the worked example below.

## Conditions

The report-to-model binding works when all three hold:

1. **Same repository.** The report and its model are both under `fabric_workspace_items/`, so
   the relative path in the report resolves to the model's folder.
2. **Same workspace, model deployed.** Both items belong to the project's workspace. fabric-cicd
   resolves the path through its index of the repository and takes the model's id from the
   workspace, so the model must either exist there already or be published in the same run,
   which means `SemanticModel` has to be in the deploy scope whenever the model is new or
   changed (`ITEM_TYPES_TO_DEPLOY` unset, or listing both types). A report-only deploy against
   a model that is already in the workspace works with `Report` alone in scope.
3. **The report keeps the `byPath` form.** This is what Power BI Desktop writes when a report is
   saved as a PBIP project next to its model, and what the sample report carries.

A project deploys to one workspace. A model in one workspace and its report in another is not
covered by this guide (see [Limitations](#limitations)).

## Repository layout

```
fabric_workspace_items/
├── semantic_models/
│   └── sm_gold_cities.SemanticModel/
│       ├── .platform
│       ├── definition.pbism
│       └── definition/
│           ├── database.tmdl
│           ├── model.tmdl
│           ├── expressions.tmdl        ← data source, with placeholders
│           └── tables/
│               └── vDim_Cities.tmdl
└── reports/
    └── rpt_gold_cities.Report/
        ├── .platform
        ├── definition.pbir             ← byPath reference to the model
        ├── definition/
        │   ├── version.json
        │   ├── report.json
        │   └── pages/...
        └── StaticResources/SharedResources/BaseThemes/CY21SU02.json
```

Folder names follow the `<name>.<Type>` convention used by every other item, and each folder
has a `.platform` file with a unique `logicalId`. Power BI Desktop's local cache folder
(`.pbi/`) may exist in either item; it is ignored when the item's hash is computed and is not
published.

## The model: data source from the value set

The sample model is Direct Lake on SQL over the `wh_gold` warehouse. Its data source is one
expression whose server and database come from the value set:

```text
// semantic_models/sm_gold_cities.SemanticModel/definition/expressions.tmdl
expression DatabaseQuery =
		let
			database = Sql.Database("{{varlib:wh_gold_warehouse_endpoint}}", "{{varlib:wh_gold_warehouse_name}}")
		in
			database
```

Tables use Direct Lake partitions that name the entity and schema and point at that expression:

```text
	partition vDim_Cities = entity
		mode: directLake
		source
			entityName: vDim_Cities
			schemaName: DW
			expressionSource: DatabaseQuery
```

Two variables carry the environment-specific values, per value set:

| Variable | Value |
| --- | --- |
| `wh_gold_warehouse_endpoint` | SQL analytics endpoint host name of the warehouse in that environment (`<...>.datawarehouse.fabric.microsoft.com`) |
| `wh_gold_warehouse_name` | database name, usually the warehouse name |

`ingen_fab init workspace` and `AUTO_UPDATE_ITEM_IDS` maintain the `_id` variables; the
endpoint is set once per environment (it is shown on the warehouse's settings page).
Every `*.tmdl` file under `fabric_workspace_items/` is substituted, so placeholders can be
used anywhere in a model, not only in `expressions.tmdl`.

## The report: bound by path

The report references its model by relative path, and nothing else in the report is
environment-specific:

```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
  "version": "4.0",
  "datasetReference": {
    "byPath": {
      "path": "../../semantic_models/sm_gold_cities.SemanticModel"
    }
  }
}
```

At publish time fabric-cicd looks the path up in its index of the repository, takes that
model's item id in the target workspace (the id it was just given if published in this run,
its existing id otherwise), and rewrites the reference to it before sending the definition. The workspace stores the result as a connection string naming the
model, and the report's dataset is the deployed model. This is what makes the same source bind
correctly in development, test and production.

`definition.pbir` is still substituted like other files, so a report that already carries a
`byConnection` reference (for example one exported from a workspace) can use a
`{{varlib:...}}` placeholder in its connection string instead; see
[Variable Replacement](variable-replacement.md#power-bi-reports-report).

## Deploying

Both item types are deployed by the normal command. With `ITEM_TYPES_TO_DEPLOY` unset every
type is in scope; to deploy only these two:

```bash
export ITEM_TYPES_TO_DEPLOY="SemanticModel,Report"
ingen_fab deploy deploy
```

The first deploy publishes the model, then the report:

```text
Updated 1 semantic model files with variable substitution
No Power BI report files needed variable substitution
Items to publish: ['sm_gold_cities.SemanticModel', 'rpt_gold_cities.Report']
Publishing SemanticModel 'sm_gold_cities'
Published SemanticModel 'sm_gold_cities'
Publishing Report 'rpt_gold_cities'
Published Report 'rpt_gold_cities'
sm_gold_cities.SemanticModel: Deployed
rpt_gold_cities.Report: Deployed
Deploy complete! Items: 2 deployed, 0 failed, ...
```

fabric-cicd publishes semantic models before reports in the same run, so a new model and its
report can go out together. A second deploy with no changes publishes nothing; both items show
as unchanged in the manifest.

### Rebinding a report to another model

Change the path in `definition.pbir` to the other model's folder and deploy. Only the report is
published:

```text
Items to publish: ['rpt_gold_cities.Report']
Publishing Report 'rpt_gold_cities'
Published Report 'rpt_gold_cities'
rpt_gold_cities.Report: Deployed
Deploy complete! Items: 1 deployed, 0 failed, ...
```

The other model must be in the repository and already deployed, or published in the same run with `SemanticModel` in scope, like the first.

### Item ids in the value set

With `AUTO_UPDATE_ITEM_IDS=true`, variables named `<model>_semanticmodel_id` and
`<report>_report_id`, when they exist in the value set, are filled with the workspace item ids
after a successful deploy, the same convention as lakehouses, warehouses and notebooks.

## Authoring

- **Power BI Desktop**: save the report as a PBIP project next to its model. Desktop writes
  the `byPath` reference and the folder layout above. Replace the literal server and database
  in `expressions.tmdl` with the two placeholders before committing.
- **Fabric portal**: create the model from the warehouse (Direct Lake on SQL) and the report
  from the model, then `ingen_fab deploy download-artefact -n <name> -t SemanticModel` and
  `-t Report`. Move the downloaded folders out of `downloaded/` into `semantic_models/` and
  `reports/`, add the placeholders to `expressions.tmdl`, and change the report's
  `datasetReference` to a `byPath` reference to the model folder.

## Limitations

- **One workspace per project.** A report in one workspace bound to a model in another is
  outside the `byPath` mechanism; today that layout means separate ingen_fab projects and a
  model id maintained in the report project's value set.
- **Reports exported with a literal model id** (`byConnection`) deploy as they are, still
  pointing at the original model, unless the id is replaced by a placeholder.
- **Refresh and connection binding** of the deployed model (cloud connection, credentials for
  scheduled refresh) are not managed by `deploy deploy`.

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `Semantic model not found in the repository. Cannot deploy a report with a relative path without deploying the model.` | the `byPath` value does not resolve to a model folder in the repository (wrong relative path, model folder missing or renamed) | fix the path in `definition.pbir` so it points at the `<name>.SemanticModel` folder |
| `Cannot replace logical ID '...' as referenced item is not yet deployed.` | the model is in the repository but not in the workspace, and it was not published in this run (for example `SemanticModel` left out of the scope on a first deploy) | include `SemanticModel` in `ITEM_TYPES_TO_DEPLOY`, or deploy the model first |
| `Report_Import_FailedToImportReport ... Required properties are missing from object: reportVersionAtImport` | `report.json` base theme lacks `reportVersionAtImport` (reports written by hand or by older tools) | add `"reportVersionAtImport": {"visual": "...", "report": "...", "page": "..."}` under `themeCollection.baseTheme`, as in the sample report |
| Model or report shows as `updated` on every deploy without changes | Desktop's `.pbi/` cache is not the cause (it is ignored); check line endings or a generated file inside the item | commit the item as Fabric or Desktop wrote it |
| Report opens but visuals show a data error | the model's warehouse table or view is missing or empty in that environment | the binding is fine; run the warehouse DDL / dbt build first |

## Related Topics

- [Variable Replacement](variable-replacement.md)
- [Deploy Guide](deploy_guide.md)
- [Environment Variables](../reference/environment-variables.md)
