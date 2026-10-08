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
| `fabric_config/storage_config.yaml` | the lakehouses, warehouses and SQL database the project creates: `lh_bronze`, `lh_silver`, `lh_gold`, `lh_silver_nb`, `lh_gold_nb` (targets of the notebook example), `lh_log` (the lakehouse run log), `wh_silver`, `wh_gold`, `wh_log` (the warehouse run log), `db_config`; the `config` lakehouse comes with the template |
| `fabric_workspace_items/` | the Variable Library, the lakehouse and warehouse items, the two orchestrator notebooks (`dbtload_lakehouse`, `dbtload_warehouse`) and the Fabric dbt job item (`dbtjob_warehouse`) |

The model SQL exists once, under `dbt_project/models`. `dbt_warehouse/models` and the copy
inside the dbt job item (`Code/dbt`) are kept identical to it; edit the models under
`dbt_project` and copy them on.

## Set it up

Before you start: the workspace's capacity must be Active (a paused capacity rejects every
deployment with `CapacityNotActive`), and the terminal must have the virtual environment
that holds `ingen_fab` active. Every command below runs from the parent folder of
`{project_name}`, the folder you ran `ingen_fab init new` in.

1. Fill the value set for your environment,
   `fabric_workspace_items/config/var_lib.VariableLibrary/valueSets/development.json`. Only
   the workspace values can be typed now: `fabric_deployment_workspace_id`,
   `config_workspace_id`, every `*_workspace_id`, and `config_workspace_name`. The item ids
   (`REPLACE_WITH_..._ID`, the config lakehouse's included) do not exist until the deploy
   creates the items: with `AUTO_UPDATE_ITEM_IDS=true` the deploy writes each of them back
   into the value set. The SQL endpoints are not written back. One is required:
   `wh_silver_warehouse_endpoint`, which ways 3 and 4 below connect through. After the first
   deploy, open `wh_silver` in the workspace, choose **Settings** > **SQL endpoint** and copy
   the SQL connection string (a host name like `<id>.datawarehouse.fabric.microsoft.com`, no
   port) into the value set. The other endpoint variables (`wh_gold_...`, `wh_log_...`, the
   two `lh_..._sql_endpoint`) are read by nothing in the sample; every warehouse of a
   workspace shares the same host, so the same value can be pasted there.
2. Set the environment for the shell (bash, then the PowerShell equivalent):

   ```bash
   export FABRIC_WORKSPACE_REPO_DIR="{project_name}"
   export FABRIC_ENVIRONMENT="development"
   export AUTO_UPDATE_ITEM_IDS=true
   ```

   ```powershell
   $env:FABRIC_WORKSPACE_REPO_DIR = "{project_name}"
   $env:FABRIC_ENVIRONMENT = "development"
   $env:AUTO_UPDATE_ITEM_IDS = "true"
   ```

   Then compile the DDL notebooks, deploy, write the profile, upload (the same in both shells):

   ```
   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Lakehouse
   ingen_fab ddl compile --output-mode fabric_workspace_repo --generation-mode Warehouse
   ingen_fab deploy deploy       # creates the items, writes their ids back
   # fill the SQL endpoints in the value set, then
   ingen_fab deploy deploy       # items now carry the ids and endpoints
   pip install "dbt-fabricspark>=1.13,<2"              # the adapter: dbt profile validates the profile with it
   ingen_fab dbt profile --dbt-project dbt_project     # before the upload: the notebook needs the profile
   ingen_fab deploy upload-python-libs                 # the runtime libraries, to the config lakehouse
   ingen_fab deploy upload-dbt-project --dbt-project dbt_project
   ingen_fab deploy upload-dbt-project --dbt-project dbt_warehouse
   ```

   The first deploy writes `{project_name}/platform_manifest_development.yml`; the message
   `Manifest file not found` on that run is expected. The dbt job item is created with its
   connection still pointing at the placeholder endpoint; the second deploy fixes it. `upload-dbt-project` copies the files as
   they are at that moment: after changing models or regenerating the profile, upload again.

3. Run the lakehouse DDL orchestrator notebook `00_all_lakehouses_orchestrator` once: it
   creates the bronze tables and the demo data (seven tables in `lh_bronze`).

## dbt in Fabric

dbt builds silver and gold from bronze: 13 models and 27 tests, the model SQL written once
under `dbt_project/models`. The sample runs that one transformation four ways, each into its
own targets, so you can run them side by side and compare (the outputs hold identical rows):

| # | Way | What runs where | Writes to | Logs to |
| --- | --- | --- | --- | --- |
| 1 | dbt from your machine | `ingen_fab dbt build`; the models execute in a Spark (Livy) session on the workspace | `lh_silver`, `lh_gold` | `lh_log` |
| 2 | Lakehouse notebook | the notebook `dbtload_lakehouse` runs the same project inside Fabric | `lh_silver_nb`, `lh_gold_nb` | `lh_log` |
| 3 | Warehouse notebook | the notebook `dbtload_warehouse` runs `dbt_warehouse` (the same models in T-SQL) with `dbt-fabric` | `wh_silver.dbt_notebook`, `wh_gold.dbt_notebook` | `wh_log` |
| 4 | Fabric dbt job item | the item `dbtjob_warehouse`; Fabric runs dbt on the copy of `dbt_warehouse` inside the item | `wh_silver.dbt_job`, `wh_gold.dbt_job` | `wh_log` |

Each run is logged by dbt itself, through the `on-run-end` hook in the project
(`macros/ingen_fab_logging.sql`, maintained by ingen_fab): one row in `dbt_batch`, one row
per model and test in `dbt_execution_log`, in the log store of the engine. If a store of that
name does not exist in the workspace, the run is logged into the project's own lakehouse or
warehouse instead.

### Before the first run

1. Bronze is loaded (step 3 above).
2. The item ids are in the value set: open `valueSets/development.json` and look for
   `REPLACE_WITH_..._ID`. The deploy wrote the ids back if `AUTO_UPDATE_ITEM_IDS=true` was
   set; if any remain, run `ingen_fab init workspace --workspace-name <name>` (it asks
   whether this is a single workspace; answer yes). `ingen_fab dbt profile` needs the
   lakehouse ids and the upload needs the config lakehouse id.
3. `wh_silver_warehouse_endpoint` is in the value set (step 1 above) and
   `ingen_fab deploy deploy` has run again since: that deploy puts it into the Variable
   Library item (the warehouse notebook reads it at run time) and into the dbt job item's
   connection. Ways 3 and 4 cannot run before it.
4. The adapter is installed on your machine, in the same environment as `ingen_fab`:
   `pip install "dbt-fabricspark>=1.13,<2"`. `dbt profile` validates the profile with it, so
   ways 1 and 2 both need it. Ways 3 and 4 install what they need inside Fabric.
5. The profile is written and both projects are uploaded, in this order (step 2 above):
   `ingen_fab dbt profile --dbt-project dbt_project`, then `upload-dbt-project` for
   `dbt_project` and for `dbt_warehouse`. The upload copies each project to the config
   lakehouse under `Files/<project>`, profile included.
6. Ways 1 and 2 each open a Spark session. On a small capacity run one at a time, leave a
   buffer between them, and wait a few minutes after stopping a run before starting the next.

### Way 1: dbt from your machine

```
ingen_fab dbt build --dbt-project dbt_project
```

The profile is regenerated, `dbt build` runs with the `development` target, the first
statement opens a Livy session on the workspace (`dbt-{project_name}-development`, visible in
the Monitor hub), the models are built and the tests run, the session closes when dbt exits.
dbt ends with `Done. PASS=40 WARN=0 ERROR=0 SKIP=0 TOTAL=40`.

Result: seven tables in `lh_silver`, six in `lh_gold`; one row in `lh_log.dbt_batch` with
`runner = cli`, and 40 rows for that batch in `lh_log.dbt_execution_log`.

### Way 2: the lakehouse notebook

In the workspace folder `notebooks`, open `dbtload_lakehouse` and run all cells. Its
parameters are set: `build`, `path:models`, and
`dbt_vars = "{silver_lakehouse: lh_silver_nb, gold_lakehouse: lh_gold_nb}"`, which points the
same models at the `_nb` lakehouses so this run does not overwrite way 1.

The notebook attaches the config lakehouse, installs the adapter from the uploaded
`requirements.txt`, fetches a Fabric token in the kernel, and runs `dbt build` against
`Files/dbt_project` with the `development-notebook` target (Livy session
`dbt-{project_name}-development-notebook`). The last cell prints a summary with
`"status": "succeeded"`.

Result: the same tables in `lh_silver_nb` and `lh_gold_nb`; one row in `lh_log.dbt_batch`
with `runner = notebook/dbtload_lakehouse`, and 40 rows for that batch in
`lh_log.dbt_execution_log`.

### Way 3: the warehouse notebook

In the folder `notebooks`, open `dbtload_warehouse` and run all cells (parameters: `build`,
`path:models`, target `notebook`).

The notebook installs `dbt-fabric`, reads `wh_silver_warehouse_endpoint` from the Variable
Library and exports it to dbt as `DBT_WAREHOUSE_ENDPOINT` (the profile's `server`), and runs
`dbt build` against `Files/dbt_warehouse`, connected to `wh_silver` with the notebook's
identity. Bronze is read through the lakehouse SQL endpoint (`lh_bronze.dbo.<table>`); the
gold folder is routed to `wh_gold` with `+database`. No Spark. The summary prints
`"status": "succeeded"`.

Result: seven tables in `wh_silver` under schema `dbt_notebook`, six in `wh_gold` under
`dbt_notebook`; one row in `wh_log.dbo.dbt_batch` with `runner = notebook/dbtload_warehouse`,
and 40 rows for that batch in `wh_log.dbo.dbt_execution_log`.

### Way 4: the Fabric dbt job item

In the folder `dbt_jobs`, open `dbtjob_warehouse` and select **Run**. The item holds its own
copy of `dbt_warehouse` under `Code/dbt` and a connection to `wh_silver` that the deploy
filled from the value set. Fabric runs `dbt build` itself with schema `dbt_job`; the output
lands under `Output/<run id>/` in the item's OneLake folder. The item reports **Completed**
when dbt finishes, including when the logging hook failed: read the log row, not only the
status.

Result: seven tables in `wh_silver` under schema `dbt_job`, six in `wh_gold` under `dbt_job`;
one row in `wh_log.dbo.dbt_batch` with `runner = cli` (the item passes no runner var), and
40 rows for that batch in `wh_log.dbo.dbt_execution_log`.

### Confirming the runs

Lakehouse log, through the SQL analytics endpoint of `lh_log`, or from your machine:

```
ingen_fab dbt show --dbt-project dbt_project -- --inline "select batch_id, runner, command, status, total_nodes, success_count, error_count, started_at from lh_log.dbt_batch order by started_at desc" --limit 20
```

Warehouse log, in the query editor of `wh_log`:

```sql
SELECT batch_id, runner, command, status, total_nodes, success_count, error_count, started_at
FROM wh_log.dbo.dbt_batch ORDER BY started_at DESC;
```

Expect one row per run with `status = success`, `total_nodes = 40`, `success_count = 40`,
`error_count = 0`, and 40 rows per `batch_id` in `dbt_execution_log`. The four outputs hold
the same rows: compare `fct_sales` in `lh_gold`, `lh_gold_nb`, `wh_gold.dbt_notebook` and
`wh_gold.dbt_job`.

### What a first run hits

| Symptom | Cause | Fix |
| --- | --- | --- |
| way 3 fails at the connection (login error, or a server named `REPLACE_WITH_...`) | `wh_silver_warehouse_endpoint` is not in the Variable Library item | fill it, deploy again |
| way 3 or 4: `Invalid object name 'lh_bronze.dbo.<table>'` right after the DDL run | the SQL analytics endpoint of `lh_bronze` has not yet picked up the new tables | wait a few minutes, or open the endpoint once in the portal, and run again |
| way 4: the run fails at the connection | the item was deployed while the endpoint was a placeholder and not deployed again | same |
| the notebook cannot find the project, or `pip_install` finds no `requirements.txt` | `upload-dbt-project` not run for that project | upload |
| way 2: target `development-notebook` does not exist | the project was uploaded before `dbt profile` ran | `dbt profile`, upload again |
| way 1: `no lakehouse with a real id in the value set`, or `lakehouse 'lh_silver' is declared but has no id yet` | ids still `REPLACE_WITH_...` | before-the-first-run step 2 |
| `dbt profile`: `dbt-fabricspark is not installed in this environment` | the adapter is missing on your machine | before-the-first-run step 4 |
| the Livy session is refused | another Spark session is still open, or the previous one just stopped | one at a time, with a buffer |
| the Monitor hub shows a **Failed** `LivySession` on `lh_silver` right after a successful way 1 or 2 run, or the next Spark run fails with error 430 `TooManyRequestsForCapacity` | the run's Livy session stays alive for about a minute after dbt exits, then Livy ends it with `LIVY_JOB_TIMED_OUT`; a session started inside that minute is refused on a small capacity | the run succeeded, check its log row; wait a minute before the next Spark run |
| a run is missing from `dbt_batch` | the hook runs after `build`, `run`, `test`, `seed`, `snapshot`; a selection that matches nothing leaves no row | check the selector |
| a failed notebook run | its step logs, dbt's log files and `notebook_error.log` are under `Files/dbt_failures/<project>/<notebook>/<UTC timestamp>_<run id>/` of `lh_log` | open `lh_log` in the workspace, then **Files** |

### How the notebooks are made

The two notebooks are generated, never edited by hand:

```
ingen_fab dbt orchestrator --name dbtload_lakehouse --select path:models --dbt-project dbt_project --vars "{silver_lakehouse: lh_silver_nb, gold_lakehouse: lh_gold_nb}"
ingen_fab dbt orchestrator --name dbtload_warehouse --select path:models --dbt-project dbt_warehouse --target notebook --env DBT_WAREHOUSE_ENDPOINT=wh_silver_warehouse_endpoint
```

`--vars` points the lakehouse project at other lakehouses without a second copy of the models
(the targets are dbt vars with defaults in `dbt_project.yml`); `--env` exports a Variable
Library value to the dbt process, which is how the warehouse profile gets the SQL endpoint of
the right environment without storing it.

## Things to know

- The Fabric dbt job item supports the warehouse adapter only and needs a connection at
  creation; the `{{varlib:...}}` tokens in `dbt-content.json` give it the right warehouse per
  environment at deploy time. The Spark path is ways 1 and 2.
- All four ways work inside one workspace: a dbt project has one connection, and three-part
  names resolve across the warehouses and SQL endpoints of that workspace only.
- Ways 1 and 2 run Spark sessions over Livy. On a small capacity, run one notebook at a
  time, leave a buffer between Spark runs, and wait a few minutes after stopping a run
  before starting the next; back-to-back sessions are refused.
- `ingen_fab deploy cleanup` and `--sync` treat every item that is not in the repo as an
  orphan; keep them away from a workspace that holds items created by hand or by the service.

## Environments

`local` is for running the code without a workspace; `development` and `test` are Fabric
workspaces. Switch with `FABRIC_ENVIRONMENT` and the matching `valueSets/<environment>.json`.
