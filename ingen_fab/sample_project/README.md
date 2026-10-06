# {project_name}

A Microsoft Fabric workspace project created with Ingenious for Fabric (`ingen_fab init new
--with-samples`). It is a complete, runnable example: generated demo data in a bronze
lakehouse, a dbt transformation to silver and gold, and the same transformation run four ways
with Microsoft's dbt adapters, so you can compare them in your own workspace.

## What is in it

| Folder | Holds |
| --- | --- |
| `ddl_scripts/Lakehouses/lh_bronze/` | DDL scripts, including `003_Sales_Demo_Data`: seven bronze tables (`countries`, `state_provinces`, `cities`, `customers`, `stock_items`, `orders`, `order_lines`, about 150,000 order lines) generated deterministically, so every environment gets identical data |
| `dbt_project/` | Spark SQL models for the lakehouse path (`dbt-fabricspark`): seven silver models, six gold models (`dim_cities`, `dim_geography`, `dim_customers`, `dim_stock_items`, `fct_sales`, `agg_sales_country_month`), 27 tests |
| `dbt_warehouse/` | the same models in T-SQL for the warehouse path (`dbt-fabric`); reads bronze through the lakehouse SQL endpoint, routes gold to `wh_gold` with `+database` |
| `fabric_config/storage_config.yaml` | the lakehouses and warehouses the project creates: `lh_bronze`, `lh_silver`, `lh_gold`, `lh_silver_nb`, `lh_gold_nb` (targets of the notebook example), `lh_log` (every run log), `wh_silver`, `wh_gold`, `config` |
| `fabric_workspace_items/` | the Variable Library, the lakehouse and warehouse items, the two orchestrator notebooks (`dbtload_lakehouse`, `dbtload_warehouse`) and the Fabric dbt job item (`dbtjob_warehouse`) |

The model SQL exists once, under `dbt_project/models`. `dbt_warehouse/models` and the copy
inside the dbt job item (`Code/dbt`) are kept identical to it; edit the models under
`dbt_project` and copy them on.

## Set it up

1. Fill the value set for your environment,
   `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json`: the
   workspace id and the config lakehouse id are the values you must type first. With
   `AUTO_UPDATE_ITEM_IDS=true` the deploy writes every item id (`REPLACE_WITH_..._ID`) and
   each item's workspace id back into the value set once the item exists. The two warehouse SQL endpoints
   (`wh_silver_warehouse_endpoint`, `wh_gold_warehouse_endpoint`) are not written back: copy
   each from the warehouse's settings (SQL connection string, the host name without a port)
   after the first deploy. Ways 3 and 4 below connect through them.
2. Compile the DDL notebooks, deploy, write the profile, upload:

   ```
   export FABRIC_WORKSPACE_REPO_DIR="./{project_name}"
   export FABRIC_ENVIRONMENT="development"
   export AUTO_UPDATE_ITEM_IDS=true
   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse
   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
   ingen_fab deploy deploy --environment development       # creates the items, writes their ids back
   # fill the two warehouse endpoints in the value set, then
   ingen_fab deploy deploy --environment development       # items now carry the ids and endpoints
   ingen_fab dbt profile --dbt-project dbt_project         # before the upload: the notebook needs the profile
   ingen_fab deploy upload-python-libs                     # the runtime libraries, to the config lakehouse
   ingen_fab deploy upload-dbt-project --dbt-project dbt_project
   ingen_fab deploy upload-dbt-project --dbt-project dbt_warehouse
   ```

   `upload-dbt-project` copies the files as they are at that moment: after changing models or
   regenerating the profile, upload again.

3. Run the lakehouse DDL orchestrator notebook once (it creates the bronze tables and the demo
   data), then run the transformation any of the four ways below.

## The same transformation, four ways

| # | Way | What runs where | Writes to |
| --- | --- | --- | --- |
| 1 | dbt over Livy, from your machine or a CI runner | `ingen_fab dbt profile` writes `dbt_project/profiles/profiles.yml` for your environments; `ingen_fab dbt build` runs dbt with Microsoft's `dbt-fabricspark` adapter; the models execute in a Spark (Livy) session named `dbt-{project_name}-development` | `lh_silver`, `lh_gold` |
| 2 | Lakehouse orchestrator notebook | the Python notebook `dbtload_lakehouse` installs the adapter, downloads the project from the config lakehouse and runs the same `dbt build`; the models execute in a Livy session | `lh_silver_nb`, `lh_gold_nb` |
| 3 | Warehouse orchestrator notebook | the Python notebook `dbtload_warehouse` runs `dbt_warehouse` with the `dbt-fabric` adapter against the warehouses, no Spark | `wh_silver.dbt_notebook`, `wh_gold.dbt_notebook` |
| 4 | Fabric dbt job item | the item `dbtjob_warehouse` holds a copy of `dbt_warehouse`; Fabric runs dbt itself with the connection declared in `dbt-content.json` | `wh_silver.dbt_job`, `wh_gold.dbt_job` |

Each way writes to its own targets, so the four outputs can be compared: the row counts are
identical.

The two notebooks are generated, never edited by hand:

```
ingen_fab dbt orchestrator --name dbtload_lakehouse --select path:models --dbt-project dbt_project --vars "{silver_lakehouse: lh_silver_nb, gold_lakehouse: lh_gold_nb}"
ingen_fab dbt orchestrator --name dbtload_warehouse --select path:models --dbt-project dbt_warehouse --target notebook --env DBT_WAREHOUSE_ENDPOINT=wh_silver_warehouse_endpoint
```

`--vars` points the lakehouse project at other lakehouses without a second copy of the models
(the targets are dbt vars with defaults in `dbt_project.yml`); `--env` exports a Variable
Library value to the dbt process, which is how the warehouse profile gets the SQL endpoint of
the right environment without storing it.

Logs: every notebook run publishes its run folder (dbt logs, `run_results.json`, a
`run_summary.json`) to `lh_log/Files/dbt_runs/<dbt project>/<notebook>/<UTC stamp>/`. The dbt
job item keeps its own output inside the item, as Fabric's item works. Way 1 logs on the
machine that runs it.

## Things to know

- The Fabric dbt job item supports the warehouse adapter only and needs a connection at
  creation; the `{{varlib:...}}` tokens in `dbt-content.json` give it the right warehouse per
  environment at deploy time. The Spark path is ways 1 and 2.
- All four ways work inside one workspace: a dbt project has one connection, and three-part
  names resolve across the warehouses and SQL endpoints of that workspace only.
- Spark work wants a capacity of F8 or more: an F2 refuses back-to-back Livy sessions.
- `ingen_fab deploy cleanup` and `--sync` treat every item that is not in the repo as an
  orphan; keep them away from a workspace that holds items created by hand or by the service.

## Environments

`local` is for running the code without a workspace; `development` and `test` are Fabric
workspaces. Switch with `FABRIC_ENVIRONMENT` and the matching `valueSets/<environment>.json`.
