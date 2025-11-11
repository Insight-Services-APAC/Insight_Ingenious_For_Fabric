# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# [comment]: # (Attach Default Lakehouse Markdown Cell)
# # 📌 Attach Default Lakehouse
# ❗**Note the code in the cell that follows is required to programatically attach the lakehouse and enable the running of spark.sql(). If this cell fails simply restart your session as this cell MUST be the first command executed on session start.**

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {  
# MAGIC         "name": "lh_bronze",
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 📦 Pip
# Pip installs reqired specifically for this template should occur here

# CELL ********************

# No pip installs needed for this notebook

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🔗 Imports

# CELL ********************

from notebookutils import mssparkutils # type: ignore

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # #️⃣ Functions

# CELL ********************

def pre_execute_notebook(notebook_file):

    try:
        mssparkutils.notebook.run(notebook_file, 1800)
        status = 'PreExecute Notebook Executed'
        error = None
    except Exception as e:
        status = 'No PreExecute Notebook Found'
        error = str(e)

    return status

def post_execute_notebook(notebook_file):

    try:
        mssparkutils.notebook.run(notebook_file, 1800)
        status = 'PostExecute Notebook Executed'
        error = None
    except Exception as e:
        status = 'No PostExecute Notebook Found'
        error = str(e)

    return status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Pre-Execution Python Script

# MARKDOWN ********************

# # Declare and Execute Pre-Execution Notebook

# CELL ********************

#Read the context of the notebook
notebook_info = mssparkutils.runtime.context

# Extract the currentNotebookName from the dictionary
current_notebook_name = notebook_info.get("currentNotebookName")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Execute Pre-Execute Notebook
preexecute_notebook_name  = current_notebook_name+ ".preexecute"
pre_execute_notebook(preexecute_notebook_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Declare and Execute SQL Statements

# CELL ********************


sql = '''
    create or replace temporary view dim_cities__dbt_tmp as
-- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
WITH source_data AS (
    select * FROM lh_silver.cities t1
)
select * from source_data'''

for s in sql.split(';\n'):
    spark.sql(s)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


sql = '''
    -- back compat for old kwarg name
  merge into lh_gold.dim_cities as DBT_INTERNAL_DEST
      using dim_cities__dbt_tmp as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.CityID = DBT_INTERNAL_DEST.CityID
      when matched then update set
         * 
      when not matched then insert *'''

for s in sql.split(';\n'):
    spark.sql(s)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🛑 Execution Stop

# CELL ********************

# Execute Post-Execute Notebook
postexecute_notebook_name  = current_notebook_name + ".postexecute"
post_execute_notebook(postexecute_notebook_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Exit to prevent spark sql debug cell running 
mssparkutils.notebook.exit("value string")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # SPARK SQL Cells for Debugging

# CELL ********************

# MAGIC %%sql
# MAGIC     create or replace temporary view dim_cities__dbt_tmp as
# MAGIC -- CTE to rank CDC records by Id, meta_ExtractedDate, and SYS_CHANGE_VERSION
# MAGIC WITH source_data AS (
# MAGIC     select * FROM lh_silver.cities t1
# MAGIC )
# MAGIC select * from source_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC     -- back compat for old kwarg name
# MAGIC   merge into lh_gold.dim_cities as DBT_INTERNAL_DEST
# MAGIC       using dim_cities__dbt_tmp as DBT_INTERNAL_SOURCE
# MAGIC       on 
# MAGIC               DBT_INTERNAL_SOURCE.CityID = DBT_INTERNAL_DEST.CityID
# MAGIC       when matched then update set
# MAGIC          * 
# MAGIC       when not matched then insert *

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
